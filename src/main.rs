//! StellarGate binary entry point: boots configuration, storage and HTTP
//! clients, spawns the background listeners, serves the API, and drains
//! everything on shutdown.

use anyhow::Result;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use stellargate::{
    api,
    config::{Config, ListenerMode},
    db, expiry, horizon,
    metrics::{AuthMetrics, HorizonMetrics, WebhookMetrics},
    retention, supervise, webhook, AppState, TaskHealth,
};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

const USER_AGENT: &str = concat!("StellarGate/", env!("CARGO_PKG_VERSION"));

/// Timeout for general outbound HTTP (Horizon). Webhook delivery uses its own
/// configurable per-attempt timeout instead.
const HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// How long shutdown waits for background tasks to drain before forcing exit.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    dotenvy::dotenv().ok();

    let cfg = Config::from_env()?;

    /* Client-IP trust boundary (issue #330): make the effective strategy
    visible at boot so an operator can confirm forwarding headers are honored
    exactly where they intend — only from configured trusted proxies, never
    from arbitrary callers. */
    if cfg.trusted_proxy_cidrs.is_empty() {
        info!(
            "client IP strategy: no trusted proxies configured — \
             X-Forwarded-For/X-Real-IP are ignored; the socket peer address is \
             used for rate limiting and auth attribution"
        );
    } else {
        info!(
            trusted_proxies = ?cfg.trusted_proxy_cidrs,
            "client IP strategy: forwarding headers are honored only from \
             trusted proxies; all other peers are attributed by socket address"
        );
    }

    let pool = open_pool(&cfg).await?;
    db::migrate(&pool).await?;
    db::backfill_asset_issuers(&pool, &cfg.accepted_assets).await?;

    let state = Arc::new(AppState {
        pool,
        http: http_client(HTTP_TIMEOUT)?,
        webhook_http: http_client(Duration::from_secs(cfg.webhook_timeout_secs))?,
        webhook_metrics: WebhookMetrics::new(),
        auth_metrics: AuthMetrics::new(),
        horizon_metrics: HorizonMetrics::new(),
        task_health: TaskHealth::new(),
        config: cfg,
    });

    if state.config.gateway_configured() {
        report_trustlines(&state).await;
    }

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let health = state.task_health.clone();

    /* Declare which background tasks are expected to keep running: `/health`
    fails while any required task is not running, so a poller or listener that
    died at startup stops being invisible (issue #315). The poller and stream
    are only expected once a gateway wallet is configured — without one they
    idle by design ("the listener stays idle until this is set"). */
    if state.config.gateway_configured() {
        health.require("poller");
        if state.config.listener_mode == ListenerMode::Stream {
            health.require("stream");
        }
    }
    health.require("sweeper");
    health.require("retention");
    health.require("redrive");

    let stream = (state.config.listener_mode == ListenerMode::Stream).then(|| {
        let state = state.clone();
        let rx = shutdown_rx.clone();
        supervise::supervise(health.clone(), "stream", shutdown_rx.clone(), move || {
            horizon::run_stream_listener(state.clone(), rx.clone())
        })
    });
    let poller = {
        let state = state.clone();
        let rx = shutdown_rx.clone();
        supervise::supervise(health.clone(), "poller", shutdown_rx.clone(), move || {
            horizon::run_poller(state.clone(), rx.clone())
        })
    };
    let sweeper = {
        let state = state.clone();
        let rx = shutdown_rx.clone();
        supervise::supervise(health.clone(), "sweeper", shutdown_rx.clone(), move || {
            expiry::run_sweeper(state.clone(), rx.clone())
        })
    };
    let retention = {
        let state = state.clone();
        let rx = shutdown_rx.clone();
        supervise::supervise(
            health.clone(),
            "retention",
            shutdown_rx.clone(),
            move || retention::run_retention_worker(state.clone(), rx.clone()),
        )
    };
    let redrive = {
        let state = state.clone();
        let rx = shutdown_rx.clone();
        supervise::supervise(health.clone(), "redrive", shutdown_rx.clone(), move || {
            webhook::run_redrive_worker(state.clone(), rx.clone())
        })
    };

    let addr = format!("0.0.0.0:{}", state.config.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("StellarGate API listening on {addr}");

    axum::serve(
        listener,
        api::router(state).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;

    let _ = shutdown_tx.send(true);
    let drain = async {
        join_task(poller, &health, "poller").await;
        join_task(sweeper, &health, "sweeper").await;
        join_task(redrive, &health, "redrive").await;
        join_task(retention, &health, "retention").await;
        if let Some(handle) = stream {
            join_task(handle, &health, "stream").await;
        }
    };
    if tokio::time::timeout(SHUTDOWN_GRACE, drain).await.is_err() {
        info!(
            timeout_secs = SHUTDOWN_GRACE.as_secs(),
            "background tasks did not drain in time; forcing exit"
        );
    }

    info!("shutdown complete");
    Ok(())
}

/// Open the SQLite pool in WAL mode so a single writer and many readers can
/// proceed concurrently.
///
/// `wal_autocheckpoint` and `journal_size_limit` are set explicitly rather
/// than left at SQLite's compiled-in defaults (issue #274):
///
/// - `wal_autocheckpoint = 1000` (SQLite's own default, made explicit here
///   as a documented choice rather than an inherited one) triggers a
///   `PASSIVE` checkpoint after roughly 1000 pages (~4 MB at the default
///   4 KiB page size) accumulate in the WAL following a commit.
/// - `journal_size_limit = 67108864` (64 MiB) is the backstop `PASSIVE`
///   checkpointing alone does not provide: a `PASSIVE` checkpoint skips
///   rather than blocks when it cannot get the read lock it needs, so a
///   long-lived reader can defer it indefinitely and let the WAL grow
///   without bound under sustained write load. `journal_size_limit` caps
///   how large the `-wal` file is allowed to grow before SQLite truncates
///   it back down on the next checkpoint that *does* run, regardless of how
///   much of the WAL that checkpoint actually flushed — so the on-disk
///   footprint has a hard ceiling even when checkpoints are being starved.
async fn open_pool(cfg: &Config) -> Result<db::Db> {
    let opts = SqliteConnectOptions::from_str(&cfg.database_url)?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .busy_timeout(Duration::from_millis(cfg.db_busy_timeout_ms))
        .pragma("wal_autocheckpoint", "1000")
        .pragma("journal_size_limit", "67108864");

    Ok(SqlitePoolOptions::new()
        .max_connections(cfg.db_pool_max_connections)
        .connect_with(opts)
        .await?)
}

fn http_client(timeout: Duration) -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .timeout(timeout)
        .user_agent(USER_AGENT)
        .build()?)
}

/// Report whether every accepted asset has a trustline on the gateway account.
/// Advisory only: a missing trustline doesn't block boot, it just means
/// payments in that asset will bounce until the trustline is added.
async fn report_trustlines(state: &Arc<AppState>) {
    match horizon::check_trustlines(state).await {
        Ok(missing) if missing.is_empty() => {
            info!("gateway trustlines verified for all accepted assets")
        }
        Ok(missing) => info!(
            ?missing,
            "accepted assets with no trustline on the gateway account"
        ),
        Err(e) => warn!(error = %e, "could not verify gateway trustlines at startup"),
    }
}

/// Await a supervisor during shutdown. Panics are caught inside the
/// supervisor's child spawn, so a `JoinError` here means the supervisor
/// itself failed — record it so the failure counter still fires.
async fn join_task(handle: JoinHandle<()>, health: &TaskHealth, name: &'static str) {
    if let Err(e) = handle.await {
        if e.is_panic() {
            warn!(task = name, "supervisor panicked");
            health.task_failed(name);
        }
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl-C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    info!("shutdown signal received");
}
