//! Horizon SSE stream idle-timeout behavior (issue #312).
//!
//! `stream_once` used to await `stream.next()` with nothing bounding it, so a
//! half-open connection — one that stops delivering bytes without closing,
//! which is exactly what a NAT or load balancer reaping idle state, or a
//! silently stalled upstream, produces — left the listener parked forever.
//! The reconnect-with-backoff loop in `run_stream_listener` is only reached
//! when `stream_once` returns, so it never ran, and detection silently
//! degraded to the interval poller's cadence with no log line and no metric.
//!
//! This test drives a real TCP connection that sends valid SSE response
//! headers and a greeting event, then goes completely silent without closing
//! the socket — a genuinely half-open connection, not a mock library's
//! idea of one. It asserts the listener notices within `STREAM_IDLE_TIMEOUT_SECS`
//! and reconnects (a second TCP accept), and that the reconnect is counted on
//! `HorizonMetrics` and exported by `metrics::render`.

use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use stellargate::{
    config::{AcceptedAsset, Config, ListenerMode},
    db, horizon, AppState,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;

const GATEWAY: &str = "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5";

async fn make_state(horizon_url: String, stream_idle_timeout_secs: u64) -> Arc<AppState> {
    let pool = SqlitePoolOptions::new()
        .connect_with(
            SqliteConnectOptions::from_str("sqlite::memory:")
                .unwrap()
                .create_if_missing(true),
        )
        .await
        .unwrap();
    db::migrate(&pool).await.unwrap();

    Arc::new(AppState {
        pool,
        config: Config {
            port: 0,
            database_url: "sqlite::memory:".into(),
            network: "testnet".into(),
            horizon_url,
            gateway_public: GATEWAY.into(),
            accepted_assets: vec![AcceptedAsset {
                code: "XLM".into(),
                issuer: None,
            }],
            webhook_secret: "a-very-long-and-secure-webhook-signing-secret-32-chars".into(),
            webhook_retry_attempts: 1,
            webhook_retry_delay_ms: 0,
            webhook_retry_max_delay_ms: 60_000,
            allowed_webhook_schemes: vec!["https".into()],
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
            listener_mode: ListenerMode::Stream,
            webhook_allow_private_targets: true,
            admin_provisioning_secret: String::new(),
            request_timeout_secs: 30,
            stream_idle_timeout_secs,
            trusted_proxy_cidrs: vec![],
        },
        http: reqwest::Client::new(),
        webhook_http: reqwest::Client::new(),
        webhook_metrics: stellargate::metrics::WebhookMetrics::new(),
        auth_metrics: stellargate::metrics::AuthMetrics::new(),
        horizon_metrics: stellargate::metrics::HorizonMetrics::new(),
        task_health: stellargate::TaskHealth::new(),
    })
}

/// One complete SSE "open" greeting event, chunk-encoded, that Horizon sends
/// as the first frame on every stream connection.
const GREETING_CHUNK: &[u8] = b"1e\r\nevent: open\r\ndata: \"hello\"\r\n\r\n\r\n";

/// Accept connections forever, and for each one: read (and discard) the
/// request, write valid SSE response headers plus one greeting event, then go
/// completely silent — no more bytes, and the socket is never closed. This is
/// a genuinely half-open connection, the exact case an idle timeout exists to
/// detect: the client's `stream.next()` would otherwise wait forever.
async fn run_stalling_sse_server(listener: TcpListener, accepts: Arc<AtomicUsize>) {
    loop {
        let Ok((socket, _)) = listener.accept().await else {
            return;
        };
        accepts.fetch_add(1, Ordering::SeqCst);

        // Each connection is handled on its own task so a stalled (never
        // closed) connection doesn't block the listener from accepting the
        // next one — exactly the reconnect this test is waiting to observe.
        tokio::spawn(async move {
            let mut socket = socket;

            // Drain the request line/headers so the client isn't waiting on
            // us to read before we respond.
            let mut buf = [0u8; 4096];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if buf[..n].windows(4).any(|w| w == b"\r\n\r\n") {
                            break;
                        }
                    }
                }
            }

            let headers = b"HTTP/1.1 200 OK\r\n\
Content-Type: text/event-stream\r\n\
Transfer-Encoding: chunked\r\n\
\r\n";
            if socket.write_all(headers).await.is_err() {
                return;
            }
            if socket.write_all(GREETING_CHUNK).await.is_err() {
                return;
            }

            // Hold the connection open, sending nothing further, until the
            // test's runtime tears it down. This is the half-open connection.
            tokio::time::sleep(Duration::from_secs(3600)).await;
        });
    }
}

/// A stalled stream — headers and a greeting delivered, then total silence
/// with the socket left open — must be detected within `STREAM_IDLE_TIMEOUT_SECS`
/// and trigger a reconnect: a second TCP accept, a `HorizonMetrics` counter
/// bump, and a corresponding line in the `/metrics` exposition.
#[tokio::test]
async fn stalled_stream_is_detected_and_reconnected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepts = Arc::new(AtomicUsize::new(0));

    tokio::spawn(run_stalling_sse_server(listener, accepts.clone()));

    // A short idle timeout keeps the test fast without being flaky under CI
    // scheduling jitter.
    let state = make_state(format!("http://{addr}"), 1).await;

    let (_tx, rx) = watch::channel(false);
    let task = tokio::spawn(horizon::run_stream_listener(state.clone(), rx));

    // Wait for a second accept — proof the listener detected the stall and
    // reconnected — bounded well above the 1s idle timeout plus the 1s base
    // reconnect backoff.
    let reconnected = tokio::time::timeout(Duration::from_secs(15), async {
        while accepts.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    assert!(
        reconnected.is_ok(),
        "listener never reconnected to the stalled stream within 15s; \
         accepts so far: {}",
        accepts.load(Ordering::SeqCst)
    );

    assert!(
        state.horizon_metrics.stream_reconnects() >= 1,
        "a detected stall must be counted as a stream reconnect"
    );

    let rendered = stellargate::metrics::render(
        &state.webhook_metrics,
        &state.auth_metrics,
        &state.task_health,
        &state.horizon_metrics,
    );
    assert!(
        rendered.contains("stellargate_horizon_stream_reconnects_total"),
        "the reconnect counter must be exported on /metrics:\n{rendered}"
    );

    // The background task is only reachable via shutdown or abort — dropping
    // `_tx` would also do it, but aborting keeps the test's intent explicit.
    task.abort();
}
