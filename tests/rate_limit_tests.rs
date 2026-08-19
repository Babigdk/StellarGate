//! Rate-limit behaviour lives in its own integration binary on purpose.
//!
//! The broader API tests run at a high limit and exercise merchant auth heavily.
//! Keeping the low-quota assertion here makes the expected 429 path explicit.

use axum::http::StatusCode;
use axum_test::TestServer;
use serde_json::{json, Value};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::str::FromStr;
use std::sync::Arc;
use stellargate::{
    api,
    config::{Config, ListenerMode},
    db, AppState,
};

fn make_config(rate_limit_requests_per_sec: u32) -> Config {
    Config {
        port: 0,
        database_url: "sqlite::memory:".into(),
        network: "testnet".into(),
        horizon_url: String::new(),
        gateway_public: "UNCONFIGURED".into(),
        accepted_assets: stellargate::config::AcceptedAsset::default_list(),
        webhook_secret: String::new(),
        webhook_retry_attempts: 1,
        webhook_retry_delay_ms: 0,
        webhook_retry_max_delay_ms: 60_000,
        allowed_webhook_schemes: vec!["https".into(), "http".into()],
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
        rate_limit_requests_per_sec,
        db_pool_max_connections: 10,
        db_busy_timeout_ms: 5000,
        cors_allowed_origins: vec![],
        listener_mode: ListenerMode::Poll,
        webhook_allow_private_targets: false,
        admin_provisioning_secret: TEST_ADMIN_SECRET.into(),
        request_timeout_secs: 30,
        stream_idle_timeout_secs: 30,
        trusted_proxy_cidrs: vec![],
    }
}

const TEST_ADMIN_SECRET: &str = "test-admin-secret";

async fn server_with_config(cfg: Config) -> (TestServer, db::Db) {
    let pool = SqlitePoolOptions::new()
        .connect_with(
            SqliteConnectOptions::from_str(&cfg.database_url)
                .unwrap()
                .create_if_missing(true),
        )
        .await
        .unwrap();
    db::migrate(&pool).await.unwrap();
    let http = reqwest::Client::new();
    let router = api::router(Arc::new(AppState {
        pool: pool.clone(),
        config: cfg,
        http,
        webhook_http: reqwest::Client::new(),
        webhook_metrics: stellargate::metrics::WebhookMetrics::new(),
        auth_metrics: stellargate::metrics::AuthMetrics::new(),
        horizon_metrics: stellargate::metrics::HorizonMetrics::new(),
        task_health: stellargate::TaskHealth::new(),
    }))
    .into_make_service_with_connect_info::<std::net::SocketAddr>();
    (TestServer::new(router).unwrap(), pool)
}

async fn provision_merchant(server: &TestServer) -> String {
    let res = server
        .post("/merchants")
        .add_header("X-Admin-Secret", TEST_ADMIN_SECRET)
        .await;
    res.assert_status(StatusCode::CREATED);
    res.json::<Value>()["api_key"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn test_rate_limit_exceeded_returns_429() {
    let (server, _pool) = server_with_config(make_config(1)).await;
    let key = provision_merchant(&server).await;
    let auth = format!("Bearer {key}");

    // The first request consumes the single per-second token.
    let first = server
        .post("/payments")
        .add_header("Authorization", auth.clone())
        .json(&json!({ "amount": "1", "asset": "XLM" }))
        .await;
    first.assert_status(StatusCode::CREATED);

    // A second immediate request exceeds the quota and is rejected.
    let second = server
        .post("/payments")
        .add_header("Authorization", auth)
        .json(&json!({ "amount": "1", "asset": "XLM" }))
        .await;
    second.assert_status(StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(second.json::<Value>()["code"], "rate_limit_exceeded");
}

// ── Retry-After and the X-RateLimit-* family (issue #327) ────────────────────

fn header(res: &axum_test::TestResponse, name: &str) -> String {
    res.headers()
        .get(name)
        .unwrap_or_else(|| panic!("response is missing the {name} header"))
        .to_str()
        .unwrap()
        .to_string()
}

/// End-to-end shape of a throttled response.
///
/// This deliberately does **not** try to prove the value is derived rather than
/// fabricated. Under `Quota::per_second(n)` a cell replenishes every `1/n`
/// seconds, so the wait is always under a second and `Retry-After` — an integer
/// per RFC 9110 — rounds to `1` at *every* configured rate. The constant was
/// numerically indistinguishable from the truth here, which is why it survived.
/// The derivation is pinned where it can actually differ, in the
/// `reset_and_retry_after` unit tests in `src/api/mod.rs`, using a quota slow
/// enough that a hard-coded `1` fails.
#[tokio::test]
async fn throttled_response_carries_a_coherent_retry_hint() {
    let (server, _pool) = server_with_config(make_config(1)).await;
    let key = provision_merchant(&server).await;
    let auth = format!("Bearer {key}");

    // Drain the single-token "payments" bucket.
    server
        .post("/payments")
        .add_header("Authorization", auth.clone())
        .json(&json!({ "amount": "1", "asset": "XLM" }))
        .await
        .assert_status(StatusCode::CREATED);

    let throttled = server
        .post("/payments")
        .add_header("Authorization", auth)
        .json(&json!({ "amount": "1", "asset": "XLM" }))
        .await;
    throttled.assert_status(StatusCode::TOO_MANY_REQUESTS);

    let retry_after: u64 = header(&throttled, "retry-after").parse().unwrap();
    assert!(
        retry_after >= 1,
        "Retry-After must never be 0 — a client honouring it would hot-loop"
    );

    // `X-RateLimit-Reset` covers refilling the *whole* bucket, so at a quota of
    // 1/sec with the bucket empty it is at least the single-cell wait.
    let reset: u64 = header(&throttled, "x-ratelimit-reset").parse().unwrap();
    assert!(
        reset >= retry_after,
        "reset ({reset}) is time to a full bucket and cannot be less than \
         Retry-After ({retry_after}), the time to a single cell"
    );
}

/// The bucket multiplier is part of the advertised contract: a read-only route
/// gets `requests_per_sec × 5`, and `X-RateLimit-Limit` must say so rather than
/// reporting the base rate the operator configured.
#[tokio::test]
async fn rate_limit_headers_report_the_effective_bucket_quota() {
    let (server, _pool) = server_with_config(make_config(4)).await;

    // /health is a read-only route → the "default" bucket, ×5 = 20.
    let res = server.get("/health").await;
    res.assert_status_ok();
    assert_eq!(header(&res, "x-ratelimit-limit"), "20");

    let key = provision_merchant(&server).await;
    // POST /payments is the "payments" bucket → ×1 = 4.
    let res = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .json(&json!({ "amount": "1", "asset": "XLM" }))
        .await;
    res.assert_status(StatusCode::CREATED);
    assert_eq!(header(&res, "x-ratelimit-limit"), "4");
}

/// A client must be able to pace itself *before* being throttled, which means
/// the headers have to be on successful responses too and `remaining` has to
/// actually decrease.
#[tokio::test]
async fn remaining_decreases_on_successful_requests() {
    let (server, _pool) = server_with_config(make_config(10)).await;
    let key = provision_merchant(&server).await;
    let auth = format!("Bearer {key}");

    let mut seen = Vec::new();
    for _ in 0..3 {
        let res = server
            .post("/payments")
            .add_header("Authorization", auth.clone())
            .json(&json!({ "amount": "1", "asset": "XLM" }))
            .await;
        res.assert_status(StatusCode::CREATED);
        seen.push(
            header(&res, "x-ratelimit-remaining")
                .parse::<u32>()
                .unwrap(),
        );
    }

    assert!(
        seen.windows(2).all(|w| w[1] < w[0]),
        "remaining must fall with each consumed request, got {seen:?}"
    );
}

/// A throttled response reports nothing left.
#[tokio::test]
async fn throttled_response_reports_zero_remaining() {
    let (server, _pool) = server_with_config(make_config(1)).await;
    let key = provision_merchant(&server).await;
    let auth = format!("Bearer {key}");

    server
        .post("/payments")
        .add_header("Authorization", auth.clone())
        .json(&json!({ "amount": "1", "asset": "XLM" }))
        .await
        .assert_status(StatusCode::CREATED);

    let res = server
        .post("/payments")
        .add_header("Authorization", auth)
        .json(&json!({ "amount": "1", "asset": "XLM" }))
        .await;
    res.assert_status(StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(header(&res, "x-ratelimit-remaining"), "0");
    assert_eq!(header(&res, "x-ratelimit-limit"), "1");
}

/// Headers that a browser client cannot read are the same as headers that were
/// never sent, so the CORS `expose_headers` list has to name every one of them.
#[tokio::test]
async fn rate_limit_headers_are_exposed_to_browser_clients() {
    let mut cfg = make_config(10);
    cfg.cors_allowed_origins = vec!["https://shop.example".into()];
    let (server, _pool) = server_with_config(cfg).await;

    let res = server
        .get("/health")
        .add_header("Origin", "https://shop.example")
        .await;
    res.assert_status_ok();

    let exposed = header(&res, "access-control-expose-headers").to_lowercase();
    for name in [
        "retry-after",
        "x-ratelimit-limit",
        "x-ratelimit-remaining",
        "x-ratelimit-reset",
    ] {
        assert!(
            exposed.contains(name),
            "{name} must be in Access-Control-Expose-Headers, got: {exposed}"
        );
    }
}

/// A client cannot evade the per-IP limiter by rotating `X-Forwarded-For` on
/// each request: forwarding headers are client-supplied and are honored only
/// when the socket peer is a configured trusted proxy (issue #330). With no
/// trusted proxies configured, every request keys on the peer address — or on
/// the single fail-closed key when no peer is available — so the second
/// request below exceeds the quota no matter which header value it sends.
#[tokio::test]
async fn spoofed_forwarded_for_cannot_evade_rate_limit() {
    let (server, _pool) = server_with_config(make_config(1)).await;
    let key = provision_merchant(&server).await;
    let auth = format!("Bearer {key}");

    // The first request consumes the single per-second token.
    let first = server
        .post("/payments")
        .add_header("Authorization", auth.clone())
        .add_header("X-Forwarded-For", "198.51.100.1")
        .json(&json!({ "amount": "1", "asset": "XLM" }))
        .await;
    first.assert_status(StatusCode::CREATED);

    // A second immediate request with a *different* spoofed header still hits
    // the same rate-limit key and is rejected.
    let second = server
        .post("/payments")
        .add_header("Authorization", auth)
        .add_header("X-Forwarded-For", "198.51.100.2")
        .json(&json!({ "amount": "1", "asset": "XLM" }))
        .await;
    second.assert_status(StatusCode::TOO_MANY_REQUESTS);
}

/// Redelivery is rate-limited independently of `POST /payments` — a merchant
/// (or anyone who knows a payment/delivery id) can't use it to trigger
/// unbounded outbound requests to the stored webhook_url.
#[tokio::test]
async fn test_redeliver_rate_limit_exceeded_returns_429() {
    let (server, pool) = server_with_config(make_config(1)).await;
    let key = provision_merchant(&server).await;
    let auth = format!("Bearer {key}");

    let id = server
        .post("/payments")
        .add_header("Authorization", auth.clone())
        .json(&json!({ "amount": "1", "asset": "XLM" }))
        .await
        .json::<Value>()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // A port nothing is listening on: the redelivery attempt fails fast
    // (connection refused) without depending on real network access.
    stellargate::db::save_webhook_delivery(
        &pool,
        "delivery-1",
        &id,
        "http://127.0.0.1:1/hook",
        r#"{"event":"payment.completed"}"#,
        "payment.completed",
    )
    .await
    .unwrap();

    // The first redelivery consumes the single per-second token (its outcome
    // doesn't matter — the rate limiter runs before the handler).
    let first = server
        .post(&format!("/payments/{id}/webhooks/delivery-1/redeliver"))
        .add_header("Authorization", auth.clone())
        .await;
    assert_ne!(first.status_code(), StatusCode::TOO_MANY_REQUESTS);

    // A second immediate redelivery exceeds the quota and is rejected.
    let second = server
        .post(&format!("/payments/{id}/webhooks/delivery-1/redeliver"))
        .add_header("Authorization", auth)
        .await;
    second.assert_status(StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(second.json::<Value>()["code"], "rate_limit_exceeded");
}
