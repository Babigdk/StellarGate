//! Trustline checking, at boot and on a recurring interval (issue #116 and
//! its follow-up).
//!
//! `horizon::check_trustlines` queries Horizon for the gateway account's
//! balances and surfaces any accepted asset the account has no trustline for —
//! such assets would otherwise mint unpayable intents. These tests drive it
//! against a mock Horizon endpoint, and also cover `horizon::run_trustline_checker`
//! (the background task that re-runs the check after boot) and the
//! `TrustlineMetrics` state both of them feed.

use std::str::FromStr;
use std::sync::Arc;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use stellargate::{
    config::{AcceptedAsset, Config, ListenerMode},
    db, horizon, AppState,
};
use uuid::Uuid;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const GATEWAY: &str = "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5";
const USDC_ISSUER: &str = "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN";

/// A fresh, uniquely-named in-memory SQLite database with `cache=shared`, so
/// every connection the pool opens talks to the SAME database rather than
/// each getting its own private one, which a bare `sqlite::memory:` DSN
/// would do with this pool's default multi-connection size (issue #309).
fn shared_memory_dsn() -> String {
    format!("sqlite:file:{}?mode=memory&cache=shared", Uuid::new_v4())
}

/// Build an `AppState` whose Horizon client points at `horizon_url` and which
/// accepts XLM plus USDC issued by `USDC_ISSUER`.
async fn make_state(horizon_url: String) -> Arc<AppState> {
    let dsn = shared_memory_dsn();
    let pool = SqlitePoolOptions::new()
        // A shared-cache in-memory database is dropped once its last
        // connection closes — keep exactly one open for the pool's lifetime.
        .min_connections(1)
        .connect_with(SqliteConnectOptions::from_str(&dsn).unwrap())
        .await
        .unwrap();
    db::migrate(&pool).await.unwrap();

    Arc::new(AppState {
        pool,
        config: Config {
            port: 0,
            database_url: dsn,
            network: "testnet".into(),
            horizon_url,
            gateway_public: GATEWAY.into(),
            accepted_assets: vec![
                AcceptedAsset {
                    code: "XLM".into(),
                    issuer: None,
                },
                AcceptedAsset {
                    code: "USDC".into(),
                    issuer: Some(USDC_ISSUER.into()),
                },
            ],
            webhook_secret: "a-very-long-and-secure-webhook-signing-secret-32-chars".into(),
            webhook_retry_attempts: 1,
            webhook_retry_delay_ms: 0,
            webhook_retry_max_delay_ms: 60_000,
            allowed_webhook_schemes: vec!["https".into()],
            webhook_payload_detail: stellargate::config::WebhookPayloadDetail::Minimal,
            webhook_timeout_secs: 10,
            webhook_redrive_interval_secs: 30,
            webhook_redrive_concurrency: 4,
            webhook_redrive_max_attempts: 8,
            webhook_redrive_grace_secs: 60,
            webhook_redrive_backoff_initial_secs: 0,
            webhook_redrive_backoff_max_secs: 0,
            webhook_redrive_jitter_secs: 0,
            retention_interval_secs: 3600,
            webhook_delivery_retention_days: 30,
            idempotency_retention_days: 7,
            poll_interval_secs: 10,
            cursor_staleness_multiple: 3,
            payment_ttl_secs: 3600,
            expiry_batch_size: 500,
            rate_limit_requests_per_sec: 10000,
            db_pool_max_connections: 5,
            db_busy_timeout_ms: 5000,
            cors_allowed_origins: vec![],
            listener_mode: ListenerMode::Poll,
            webhook_allow_private_targets: true,
            admin_provisioning_secret: String::new(),
            request_timeout_secs: 30,
            stream_idle_timeout_secs: 30,
            trusted_proxy_cidrs: vec![],
            max_payment_amount: Default::default(),
            min_payment_amount: Default::default(),
            max_body_bytes: 256 * 1024,
            rate_limiter_max_keys: 10_000,
            rate_limiter_idle_ttl_secs: 60,
            pagination_default_limit: 20,
            pagination_max_limit: 100,
            shutdown_grace_secs: 30,
            horizon_page_limit: 200,
            db_prune_batch_size: 500,
            retention_max_rows_per_cycle: 50_000,
        },
        http: reqwest::Client::new(),
        webhook_http: reqwest::Client::new(),
        webhook_metrics: stellargate::metrics::WebhookMetrics::new(),
        auth_metrics: stellargate::metrics::AuthMetrics::new(),
        horizon_metrics: stellargate::metrics::HorizonMetrics::new(),
        trustline_metrics: stellargate::metrics::TrustlineMetrics::new(),
        http_metrics: stellargate::metrics::HttpMetrics::new(),
        payment_metrics: stellargate::metrics::PaymentMetrics::new(),
        task_health: stellargate::TaskHealth::new(),
    })
}

/// An accepted asset with no trustline on the gateway account is surfaced.
#[tokio::test]
async fn check_trustlines_surfaces_a_missing_trustline() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/accounts/{GATEWAY}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            // Account holds only native XLM — no USDC trustline.
            "balances": [ { "balance": "100.0", "asset_type": "native" } ]
        })))
        .mount(&server)
        .await;

    let state = make_state(server.uri()).await;
    let missing = horizon::check_trustlines(&state).await.unwrap();
    assert_eq!(missing, vec!["USDC".to_string()]);
}

/// When every accepted asset has a trustline, nothing is surfaced.
#[tokio::test]
async fn check_trustlines_passes_when_all_trustlines_present() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/accounts/{GATEWAY}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "balances": [
                { "balance": "100.0", "asset_type": "native" },
                {
                    "balance": "0.0",
                    "asset_type": "credit_alphanum4",
                    "asset_code": "USDC",
                    "asset_issuer": USDC_ISSUER
                }
            ]
        })))
        .mount(&server)
        .await;

    let state = make_state(server.uri()).await;
    assert!(horizon::check_trustlines(&state).await.unwrap().is_empty());
}

/// A Horizon error (e.g. the account does not exist yet) is returned to the
/// caller rather than panicking, so startup can log it and carry on.
#[tokio::test]
async fn check_trustlines_errors_are_recoverable() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/accounts/{GATEWAY}")))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let state = make_state(server.uri()).await;
    assert!(horizon::check_trustlines(&state).await.is_err());
}

/// A successful check feeds `TrustlineMetrics`, which is what `GET /metrics`
/// and `POST /payments` actually read (this issue's fix).
#[tokio::test]
async fn check_trustlines_updates_trustline_metrics_on_success() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/accounts/{GATEWAY}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "balances": [ { "balance": "100.0", "asset_type": "native" } ]
        })))
        .mount(&server)
        .await;

    let state = make_state(server.uri()).await;
    assert_eq!(state.trustline_metrics.is_missing("USDC"), None);

    horizon::check_trustlines(&state).await.unwrap();

    assert_eq!(state.trustline_metrics.is_missing("USDC"), Some(true));
    assert!(state.trustline_metrics.last_success_unix() > 0);
    assert_eq!(state.trustline_metrics.check_failures(), 0);
}

/// A Horizon failure must not be reported as a confirmed-absent trustline: it
/// bumps the failure counter and leaves the per-asset gauge untouched, so a
/// scrape can tell "Horizon is unreachable" apart from "trustline confirmed
/// missing" (acceptance criterion of this issue).
#[tokio::test]
async fn check_trustlines_failure_does_not_report_a_confirmed_absence() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/accounts/{GATEWAY}")))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;

    let state = make_state(server.uri()).await;
    assert!(horizon::check_trustlines(&state).await.is_err());

    assert_eq!(
        state.trustline_metrics.is_missing("USDC"),
        None,
        "a Horizon failure must not read as a confirmed-absent trustline"
    );
    assert_eq!(state.trustline_metrics.last_success_unix(), 0);
    assert_eq!(state.trustline_metrics.check_failures(), 1);
}

/// A trustline confirmed present earlier is not silently downgraded to
/// "unknown" or "missing" by a later Horizon outage — the last confirmed
/// answer survives until the next successful check overwrites it.
#[tokio::test]
async fn check_trustlines_failure_preserves_the_prior_confirmed_state() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/accounts/{GATEWAY}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "balances": [
                { "balance": "100.0", "asset_type": "native" },
                {
                    "balance": "0.0",
                    "asset_type": "credit_alphanum4",
                    "asset_code": "USDC",
                    "asset_issuer": USDC_ISSUER
                }
            ]
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/accounts/{GATEWAY}")))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;

    let state = make_state(server.uri()).await;
    horizon::check_trustlines(&state).await.unwrap();
    assert_eq!(state.trustline_metrics.is_missing("USDC"), Some(false));
    let ts = state.trustline_metrics.last_success_unix();

    assert!(horizon::check_trustlines(&state).await.is_err());
    assert_eq!(
        state.trustline_metrics.is_missing("USDC"),
        Some(false),
        "the prior confirmed-present answer must survive a later Horizon outage"
    );
    assert_eq!(state.trustline_metrics.last_success_unix(), ts);
    assert_eq!(state.trustline_metrics.check_failures(), 1);
}

/// The background checker idles rather than polling when no gateway wallet is
/// configured — mirroring `run_poller`, since without one there is no
/// account to hold trustlines on.
#[tokio::test]
async fn run_trustline_checker_disabled_without_a_gateway() {
    let mut state_arc = make_state("http://127.0.0.1:1".to_string()).await;
    Arc::get_mut(&mut state_arc).unwrap().config.gateway_public = "UNCONFIGURED".into();

    let (_tx, rx) = tokio::sync::watch::channel(false);
    let exit = horizon::run_trustline_checker(state_arc, rx).await;
    assert!(matches!(
        exit,
        stellargate::supervise::TaskExit::DisabledByConfig(_)
    ));
}

/// The checker re-evaluates trustlines on its own cadence, not only once at
/// boot: with a short interval it must have refreshed `TrustlineMetrics` on
/// its own before shutdown fires, without anything driving it explicitly
/// (this issue's core acceptance criterion — "not only at boot").
#[tokio::test]
async fn run_trustline_checker_refreshes_state_on_its_interval() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/accounts/{GATEWAY}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "balances": [ { "balance": "100.0", "asset_type": "native" } ]
        })))
        .mount(&server)
        .await;

    let mut state_arc = make_state(server.uri()).await;
    Arc::get_mut(&mut state_arc)
        .unwrap()
        .config
        .retention_interval_secs = 1;

    let (tx, rx) = tokio::sync::watch::channel(false);
    let handle = tokio::spawn(horizon::run_trustline_checker(state_arc.clone(), rx));

    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    tx.send(true).unwrap();
    let exit = tokio::time::timeout(std::time::Duration::from_secs(2), handle)
        .await
        .expect("checker did not shut down promptly")
        .unwrap();
    assert!(matches!(
        exit,
        stellargate::supervise::TaskExit::ShutdownRequested
    ));

    assert_eq!(
        state_arc.trustline_metrics.is_missing("USDC"),
        Some(true),
        "the periodic checker must have run at least once on its own"
    );
}
