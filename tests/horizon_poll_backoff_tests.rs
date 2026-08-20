//! Horizon poller throttling behavior (issue #313).
//!
//! `horizon::fetch_recent_payments` used to turn every non-2xx Horizon
//! response into an opaque `reqwest::Error` via `error_for_status()`,
//! discarding the status code and any `Retry-After` header. These tests drive
//! it against a mock Horizon and assert the error it returns actually carries
//! that information, and that `poll_once`'s catch-up loop is bounded so a
//! post-throttling backlog cannot immediately re-trip the same limit.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use stellargate::{
    config::{AcceptedAsset, Config, ListenerMode},
    db, horizon, AppState,
};
use uuid::Uuid;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const GATEWAY: &str = "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5";

/// A fresh, uniquely-named in-memory SQLite database with `cache=shared`, so
/// every connection the pool opens talks to the SAME database rather than
/// each getting its own private one, which a bare `sqlite::memory:` DSN
/// would do with this pool's default multi-connection size (issue #309).
fn shared_memory_dsn() -> String {
    format!("sqlite:file:{}?mode=memory&cache=shared", Uuid::new_v4())
}

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
            accepted_assets: vec![AcceptedAsset {
                code: "XLM".into(),
                issuer: None,
            }],
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
        },
        http: reqwest::Client::new(),
        webhook_http: reqwest::Client::new(),
        webhook_metrics: stellargate::metrics::WebhookMetrics::new(),
        auth_metrics: stellargate::metrics::AuthMetrics::new(),
        horizon_metrics: stellargate::metrics::HorizonMetrics::new(),
        trustline_metrics: stellargate::metrics::TrustlineMetrics::new(),
        task_health: stellargate::TaskHealth::new(),
    })
}

/// A `429` with `Retry-After` must surface that duration on the returned
/// error, not just an opaque "request failed".
#[tokio::test]
async fn fetch_recent_payments_reports_retry_after_on_429() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/accounts/{GATEWAY}/payments")))
        .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "30"))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let err = horizon::fetch_recent_payments(&client, &server.uri(), GATEWAY, "0", 200)
        .await
        .expect_err("a 429 must be reported as an error");

    let horizon_err = err
        .downcast_ref::<horizon::HorizonHttpError>()
        .expect("error must downcast to HorizonHttpError, not an opaque reqwest error");
    assert!(horizon_err.is_rate_limited());
    assert_eq!(horizon_err.retry_after, Some(Duration::from_secs(30)));
}

/// A `429` with no `Retry-After` is still flagged as rate-limited, just with
/// no explicit wait to honor — the caller falls back to its own backoff.
#[tokio::test]
async fn fetch_recent_payments_429_without_retry_after_has_none() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/accounts/{GATEWAY}/payments")))
        .respond_with(ResponseTemplate::new(429))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let err = horizon::fetch_recent_payments(&client, &server.uri(), GATEWAY, "0", 200)
        .await
        .unwrap_err();

    let horizon_err = err.downcast_ref::<horizon::HorizonHttpError>().unwrap();
    assert!(horizon_err.is_rate_limited());
    assert_eq!(horizon_err.retry_after, None);
}

/// An ordinary server error is not mistaken for throttling — the poller must
/// not honor a nonexistent `Retry-After` or otherwise treat it as a rate
/// limit.
#[tokio::test]
async fn fetch_recent_payments_500_is_not_rate_limited() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/accounts/{GATEWAY}/payments")))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let err = horizon::fetch_recent_payments(&client, &server.uri(), GATEWAY, "0", 200)
        .await
        .unwrap_err();

    let horizon_err = err.downcast_ref::<horizon::HorizonHttpError>().unwrap();
    assert!(!horizon_err.is_rate_limited());
    assert_eq!(horizon_err.retry_after, None);
}

/// `poll_once`'s catch-up loop stops after a bounded number of pages even
/// when Horizon still has more — a backlog built up while throttled must not
/// be drained in one cycle, reissuing the same request volume that tripped
/// the limit the moment it lifts (issue #313). The next cycle resumes from
/// the persisted cursor instead.
#[tokio::test]
async fn poll_once_stops_at_the_per_cycle_page_cap() {
    let server = MockServer::start().await;

    // Every page is exactly PAGE_LIMIT (200) full-size, never-ending records,
    // so the catch-up loop would otherwise never stop on its own. None carry
    // a text memo, so nothing matches a pending intent and reconciliation is
    // a cheap no-op — this test is purely about how many requests get made.
    // A shared counter (rather than parsing the requested cursor back out)
    // keeps every page's paging_token strictly increasing across calls.
    let next_token = Arc::new(std::sync::atomic::AtomicU64::new(0));
    Mock::given(method("GET"))
        .and(path(format!("/accounts/{GATEWAY}/payments")))
        .respond_with(move |_req: &wiremock::Request| {
            let base = next_token.fetch_add(200, std::sync::atomic::Ordering::SeqCst);
            let records: Vec<_> = (0..200)
                .map(|i| {
                    serde_json::json!({
                        "type": "payment",
                        "paging_token": format!("{}", base + i + 1),
                    })
                })
                .collect();
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "_embedded": { "records": records }
            }))
        })
        .mount(&server)
        .await;

    let state = make_state(server.uri()).await;
    let settled = horizon::poll_once(&state).await.unwrap();
    assert_eq!(settled, 0);

    let requests = server.received_requests().await.unwrap();
    // 1 baselining request (starting_cursor, no persisted cursor yet) + 25
    // (MAX_PAGES_PER_CYCLE) pages of catch-up, capped even though Horizon
    // still had more to give.
    assert_eq!(
        requests.len(),
        26,
        "poll_once must stop at its per-cycle page cap instead of looping forever"
    );
}
