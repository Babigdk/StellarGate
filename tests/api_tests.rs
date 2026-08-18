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
use time::format_description::well_known::Rfc3339;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn make_config() -> Config {
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
        /* Both schemes are allowed here so the scheme allow-list isn't what
        rejects http:// — these tests cover the network-based rule (http is fine
        on testnet, HTTPS-only on public), which runs after this gate. */
        allowed_webhook_schemes: vec!["https".into(), "http".into()],
        webhook_timeout_secs: 10,
        webhook_redrive_interval_secs: 30,
        webhook_redrive_concurrency: 4,
        webhook_redrive_max_attempts: 8,
        webhook_redrive_grace_secs: 60,
        webhook_redrive_backoff_initial_secs: 0,
        webhook_redrive_backoff_max_secs: 0,
        retention_interval_secs: 3600,
        webhook_delivery_retention_days: 30,
        idempotency_retention_days: 7,
        poll_interval_secs: 10,
        cursor_staleness_multiple: 3,
        payment_ttl_secs: 3600,
        expiry_batch_size: 500,
        /* High enough that these tests never trip the limiter; dedicated
        rate-limit coverage lives in tests/rate_limit_tests.rs. */
        rate_limit_requests_per_sec: 1000,
        db_pool_max_connections: 10,
        db_busy_timeout_ms: 5000,
        cors_allowed_origins: vec![],
        listener_mode: ListenerMode::Poll,
        webhook_allow_private_targets: false,
        admin_provisioning_secret: TEST_ADMIN_SECRET.into(),
        request_timeout_secs: 30,
        trusted_proxy_cidrs: vec![],
    }
}

/// Shared admin secret used by tests to provision merchants.
const TEST_ADMIN_SECRET: &str = "test-admin-secret";

async fn test_server_with_pool() -> (TestServer, db::Db) {
    server_with_config(make_config()).await
}

async fn server_with_config(cfg: Config) -> (TestServer, db::Db) {
    server_with_config_and_health(cfg, stellargate::TaskHealth::new()).await
}

/// Like [`server_with_config`], but with an explicitly-provided [`TaskHealth`]
/// so tests can simulate a dead background task or a stale detection cursor
/// (issue #315).
async fn server_with_config_and_health(
    cfg: Config,
    task_health: stellargate::TaskHealth,
) -> (TestServer, db::Db) {
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
        task_health,
    }))
    .into_make_service_with_connect_info::<std::net::SocketAddr>();
    let server = TestServer::new(router).unwrap();
    (server, pool)
}

async fn test_server() -> TestServer {
    test_server_with_pool().await.0
}

/// Provision a merchant via POST /merchants and return the API key.
async fn provision_merchant(server: &TestServer) -> String {
    let res = server
        .post("/merchants")
        .add_header("X-Admin-Secret", TEST_ADMIN_SECRET)
        .await;
    res.assert_status(StatusCode::CREATED);
    res.json::<Value>()["api_key"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn test_provision_merchant_without_admin_secret_is_rejected() {
    let server = test_server().await;
    let res = server.post("/merchants").await;
    res.assert_status(StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_provision_merchant_with_wrong_admin_secret_is_rejected() {
    let server = test_server().await;
    let res = server
        .post("/merchants")
        .add_header("X-Admin-Secret", "not-the-right-secret")
        .await;
    res.assert_status(StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_provision_merchant_disabled_when_secret_unconfigured() {
    let mut cfg = make_config();
    cfg.admin_provisioning_secret = String::new();
    let (server, _pool) = server_with_config(cfg).await;
    let res = server
        .post("/merchants")
        .add_header("X-Admin-Secret", "")
        .await;
    res.assert_status(StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_health() {
    let res = test_server().await.get("/health").await;
    res.assert_status_ok();
    assert_eq!(res.json::<serde_json::Value>()["status"], "ok");
}

#[tokio::test]
async fn test_ready_ok_with_live_db() {
    let res = test_server().await.get("/ready").await;
    res.assert_status_ok();
    assert_eq!(res.json::<serde_json::Value>()["status"], "ok");
}

/// A gateway that is configured enough for the readiness probe to run its
/// on-chain checks (a valid strkey; validation happens only in `from_env`).
const CONFIGURED_GATEWAY: &str = "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5";

#[tokio::test]
async fn test_health_ok_when_required_task_running() {
    let health = stellargate::TaskHealth::new();
    health.require("poller");
    health.task_started("poller");
    let (server, _pool) = server_with_config_and_health(make_config(), health).await;

    let res = server.get("/health").await;
    res.assert_status_ok();
    assert_eq!(res.json::<Value>()["status"], "ok");
}

/// A required background task that stopped (a poller that died at startup)
/// must make /health fail — a process whose payment detection is dead must
/// not look healthy forever (issue #315).
#[tokio::test]
async fn test_health_fails_when_required_task_stopped() {
    let health = stellargate::TaskHealth::new();
    health.require("poller");
    health.task_started("poller");
    health.task_stopped("poller");
    let (server, _pool) = server_with_config_and_health(make_config(), health).await;

    let res = server.get("/health").await;
    res.assert_status(StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(res.json::<Value>()["status"], "unavailable");
    assert!(res.json::<Value>()["reason"]
        .as_str()
        .unwrap()
        .contains("poller"));
}

/// A required task that keeps panicking must fail /health even if the
/// supervisor has already spawned a replacement (issue #316).
#[tokio::test]
async fn test_health_fails_when_required_task_crash_looping() {
    let health = stellargate::TaskHealth::new();
    health.require("poller");
    health.task_started("poller");
    for _ in 0..stellargate::CRASH_LOOP_THRESHOLD {
        health.task_failed("poller");
        health.task_restarted("poller");
        health.task_started("poller");
    }
    let (server, _pool) = server_with_config_and_health(make_config(), health).await;

    let res = server.get("/health").await;
    res.assert_status(StatusCode::SERVICE_UNAVAILABLE);
    let body = res.json::<Value>();
    assert_eq!(body["status"], "unavailable");
    assert!(
        body["reason"].as_str().unwrap().contains("crash-looping"),
        "got: {body}"
    );
}

/// Task panics and restarts must show up on /metrics so a crash-loop is
/// scrapeable (issue #316).
#[tokio::test]
async fn test_task_health_is_exported_on_metrics() {
    let health = stellargate::TaskHealth::new();
    health.require("poller");
    health.task_started("poller");
    health.task_failed("poller");
    health.task_restarted("poller");
    health.task_started("poller");
    let (server, _pool) = server_with_config_and_health(make_config(), health).await;

    let res = server.get("/metrics").await;
    res.assert_status_ok();
    let body = res.text();
    assert!(
        body.contains("stellargate_tasks_failed_total 1"),
        "got: {body}"
    );
    assert!(
        body.contains("stellargate_task_restarts_total{task=\"poller\"} 1"),
        "got: {body}"
    );
    assert!(
        body.contains("stellargate_task_running{task=\"poller\"} 1"),
        "got: {body}"
    );
}

/// A stale detection cursor must make /ready fail even though Horizon itself
/// is reachable — reachable dependencies plus a dead poller is not readiness
/// (issue #315).
#[tokio::test]
async fn test_ready_fails_when_cursor_stale() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock)
        .await;

    let mut cfg = make_config();
    cfg.gateway_public = CONFIGURED_GATEWAY.into();
    cfg.horizon_url = mock.uri();

    let health = stellargate::TaskHealth::new();
    health.set_last_success_unix(0); // never succeeded → maximally stale
    let (server, _pool) = server_with_config_and_health(cfg, health).await;

    let res = server.get("/ready").await;
    res.assert_status(StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(res.json::<Value>()["status"], "unavailable");
    assert!(res.json::<Value>()["reason"]
        .as_str()
        .unwrap()
        .contains("stalled"));
}

/// A fresh cursor (the poller recently completed a cycle) must keep /ready
/// green when Horizon is reachable.
#[tokio::test]
async fn test_ready_ok_when_cursor_fresh() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock)
        .await;

    let mut cfg = make_config();
    cfg.gateway_public = CONFIGURED_GATEWAY.into();
    cfg.horizon_url = mock.uri();

    let health = stellargate::TaskHealth::new();
    health.note_success();
    let (server, _pool) = server_with_config_and_health(cfg, health).await;

    let res = server.get("/ready").await;
    res.assert_status_ok();
    assert_eq!(res.json::<Value>()["status"], "ok");
}

#[tokio::test]
async fn test_unauthenticated_create_returns_401() {
    let res = test_server()
        .await
        .post("/payments")
        .json(&json!({ "amount": "10", "asset": "XLM" }))
        .await;
    res.assert_status(StatusCode::UNAUTHORIZED);
    assert_eq!(res.json::<Value>()["code"], "unauthorized");
}

#[tokio::test]
async fn test_unauthenticated_list_returns_401() {
    let res = test_server().await.get("/payments").await;
    res.assert_status(StatusCode::UNAUTHORIZED);
    assert_eq!(res.json::<Value>()["code"], "unauthorized");
}

#[tokio::test]
async fn test_invalid_api_key_returns_401() {
    let res = test_server()
        .await
        .post("/payments")
        .add_header("Authorization", "Bearer not-a-real-key")
        .json(&json!({ "amount": "10", "asset": "XLM" }))
        .await;
    res.assert_status(StatusCode::UNAUTHORIZED);
}

/// Auth outcomes must be observable via `/metrics` (issue #139), not just as
/// a bare 401 with nothing left behind for an operator to alert on.
#[tokio::test]
async fn test_auth_outcomes_are_counted_in_metrics() {
    let server = test_server().await;

    server.get("/payments").await; // missing key
    server
        .post("/payments")
        .add_header("Authorization", "Bearer not-a-real-key")
        .json(&json!({ "amount": "10", "asset": "XLM" }))
        .await; // invalid key
    let key = provision_merchant(&server).await;
    server
        .get("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .await; // valid key

    let res = server.get("/metrics").await;
    res.assert_status_ok();
    let body = res.text();
    assert!(
        body.contains("stellargate_auth_attempts_total{outcome=\"success\"} 1"),
        "got: {body}"
    );
    assert!(
        body.contains(
            "stellargate_auth_attempts_total{outcome=\"failure\",reason=\"missing_key\"} 1"
        ),
        "got: {body}"
    );
    assert!(
        body.contains(
            "stellargate_auth_attempts_total{outcome=\"failure\",reason=\"invalid_key\"} 1"
        ),
        "got: {body}"
    );
}

#[tokio::test]
async fn test_create_payment() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;
    let res = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .json(&json!({ "amount": "10", "asset": "XLM" }))
        .await;
    res.assert_status(StatusCode::CREATED);
    let body: Value = res.json();
    assert_eq!(body["status"], "pending");
    assert_eq!(body["asset"], "XLM");
    assert_eq!(body["memo"].as_str().unwrap().len(), 8);
}

/// Timestamps must be strict RFC 3339 UTC with an explicit Z suffix.
#[tokio::test]
async fn test_timestamps_are_rfc3339_utc() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;
    let res = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .json(&json!({ "amount": "1", "asset": "XLM" }))
        .await;
    res.assert_status(StatusCode::CREATED);
    let body: Value = res.json();

    for field in ["created_at", "updated_at"] {
        let ts = body[field]
            .as_str()
            .unwrap_or_else(|| panic!("{field} missing"));
        time::OffsetDateTime::parse(ts, &Rfc3339)
            .unwrap_or_else(|e| panic!("{field} = {ts:?} is not valid RFC 3339: {e}"));
        assert!(
            ts.ends_with('Z'),
            "{field} = {ts:?} must have explicit Z suffix"
        );
    }
}

#[tokio::test]
async fn test_idempotency_key_returns_same_payment() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;
    let auth = format!("Bearer {key}");

    // First request mints a new payment (201 Created).
    let res1 = server
        .post("/payments")
        .add_header("Authorization", auth.clone())
        .add_header("Idempotency-Key", "retry-abc-123")
        .json(&json!({ "amount": "10", "asset": "XLM" }))
        .await;
    res1.assert_status(StatusCode::CREATED);
    let id1 = res1.json::<Value>()["id"].as_str().unwrap().to_string();

    // Identical retry with the same key returns the original payment (200 OK).
    let res2 = server
        .post("/payments")
        .add_header("Authorization", auth.clone())
        .add_header("Idempotency-Key", "retry-abc-123")
        .json(&json!({ "amount": "10", "asset": "XLM" }))
        .await;
    res2.assert_status_ok();
    let id2 = res2.json::<Value>()["id"].as_str().unwrap().to_string();

    assert_eq!(id1, id2, "same idempotency key must yield the same payment");

    // Exactly one payment visible to this merchant.
    let list: Value = server
        .get("/payments")
        .add_header("Authorization", auth)
        .await
        .json();
    assert_eq!(list["total"], 1);
}

#[tokio::test]
async fn test_different_or_missing_idempotency_key_creates_new_payment() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;
    let auth = format!("Bearer {key}");

    let id_a = server
        .post("/payments")
        .add_header("Authorization", auth.clone())
        .add_header("Idempotency-Key", "key-a")
        .json(&json!({ "amount": "1", "asset": "XLM" }))
        .await
        .json::<Value>()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let res_b = server
        .post("/payments")
        .add_header("Authorization", auth.clone())
        .add_header("Idempotency-Key", "key-b")
        .json(&json!({ "amount": "1", "asset": "XLM" }))
        .await;
    res_b.assert_status(StatusCode::CREATED);
    let id_b = res_b.json::<Value>()["id"].as_str().unwrap().to_string();

    let res_c = server
        .post("/payments")
        .add_header("Authorization", auth.clone())
        .json(&json!({ "amount": "1", "asset": "XLM" }))
        .await;
    res_c.assert_status(StatusCode::CREATED);
    let id_c = res_c.json::<Value>()["id"].as_str().unwrap().to_string();

    assert_ne!(id_a, id_b);
    assert_ne!(id_a, id_c);
    assert_ne!(id_b, id_c);

    let list: Value = server
        .get("/payments")
        .add_header("Authorization", auth)
        .await
        .json();
    assert_eq!(list["total"], 3);
}

#[tokio::test]
async fn test_idempotency_key_scoped_per_merchant() {
    let server = test_server().await;
    let key1 = provision_merchant(&server).await;
    let key2 = provision_merchant(&server).await;

    // Same idempotency key, different merchants → two distinct payments.
    let id_m1 = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key1}"))
        .add_header("Idempotency-Key", "shared-key")
        .json(&json!({ "amount": "1", "asset": "XLM" }))
        .await
        .json::<Value>()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let id_m2 = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key2}"))
        .add_header("Idempotency-Key", "shared-key")
        .json(&json!({ "amount": "1", "asset": "XLM" }))
        .await
        .json::<Value>()["id"]
        .as_str()
        .unwrap()
        .to_string();

    assert_ne!(
        id_m1, id_m2,
        "same key under different merchants must not collide"
    );

    // Re-using key1's idempotency key under merchant1 returns merchant1's original payment.
    let res_retry = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key1}"))
        .add_header("Idempotency-Key", "shared-key")
        .json(&json!({ "amount": "1", "asset": "XLM" }))
        .await;
    res_retry.assert_status_ok();
    assert_eq!(res_retry.json::<Value>()["id"].as_str().unwrap(), id_m1);
}

#[tokio::test]
async fn test_merchant_list_scoped_to_own_payments() {
    let server = test_server().await;
    let key1 = provision_merchant(&server).await;
    let key2 = provision_merchant(&server).await;

    // Merchant 1 creates 2 payments.
    for _ in 0..2 {
        server
            .post("/payments")
            .add_header("Authorization", format!("Bearer {key1}"))
            .json(&json!({ "amount": "1", "asset": "XLM" }))
            .await
            .assert_status(StatusCode::CREATED);
    }
    // Merchant 2 creates 1 payment.
    server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key2}"))
        .json(&json!({ "amount": "2", "asset": "XLM" }))
        .await
        .assert_status(StatusCode::CREATED);

    // Each merchant only sees their own payments.
    let list1: Value = server
        .get("/payments")
        .add_header("Authorization", format!("Bearer {key1}"))
        .await
        .json();
    assert_eq!(list1["total"], 2, "merchant1 should see 2 payments");

    let list2: Value = server
        .get("/payments")
        .add_header("Authorization", format!("Bearer {key2}"))
        .await
        .json();
    assert_eq!(list2["total"], 1, "merchant2 should see 1 payment");
}

#[tokio::test]
async fn test_create_invalid_asset() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;
    let res = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .json(&json!({ "amount": "10", "asset": "BTC" }))
        .await;
    res.assert_status(StatusCode::BAD_REQUEST);
    assert_eq!(res.json::<Value>()["code"], "unsupported_asset");
    res.assert_contains_header("x-request-id");
}

// ── Unknown request-body fields (issue #329) ─────────────────────────────────

/// `merchant_id` is the sharpest case: `openapi.yaml` advertised it, so an
/// integrator following the spec sent it believing they were choosing the
/// tenant. It must be rejected, not silently dropped in favour of whichever
/// merchant owns the key.
#[tokio::test]
async fn test_create_payment_rejects_unknown_field() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;
    let res = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .json(&json!({ "amount": "10", "merchant_id": "someone-elses-shop" }))
        .await;
    res.assert_status(StatusCode::BAD_REQUEST);
    let body = res.json::<Value>();
    assert_eq!(body["code"], "unknown_field");
    assert!(
        body["error"].as_str().unwrap().contains("merchant_id"),
        "the error must name the offending field, got: {}",
        body["error"]
    );
}

/// The interaction that made silent discarding expensive rather than untidy:
/// `asset` defaults to `XLM`, so one transposed character used to mint a
/// 100 XLM intent and return `201` describing it.
#[tokio::test]
async fn test_create_payment_rejects_misspelled_asset() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;
    let res = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .json(&json!({ "amount": "100", "assset": "USDC" }))
        .await;
    res.assert_status(StatusCode::BAD_REQUEST);
    let body = res.json::<Value>();
    assert_eq!(
        body["code"], "unknown_field",
        "a misspelled `asset` must not silently fall back to the XLM default"
    );
    assert!(body["error"].as_str().unwrap().contains("assset"));
}

/// The correctly-spelled fields still work — `deny_unknown_fields` must not
/// have narrowed the accepted body.
#[tokio::test]
async fn test_create_payment_accepts_every_documented_field() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;
    let res = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .json(&json!({
            "amount": "100",
            "asset": "USDC",
            "webhook_url": "https://example.com/hook",
        }))
        .await;
    res.assert_status(StatusCode::CREATED);
    assert_eq!(res.json::<Value>()["asset"], "USDC");
}

#[tokio::test]
async fn test_issue_key_rejects_unknown_field() {
    let server = test_server().await;
    let res = server
        .post("/merchants")
        .add_header("X-Admin-Secret", TEST_ADMIN_SECRET)
        .await;
    let merchant_id = res.json::<Value>()["merchant_id"]
        .as_str()
        .unwrap()
        .to_string();

    let res = server
        .post(&format!("/merchants/{merchant_id}/keys"))
        .add_header("X-Admin-Secret", TEST_ADMIN_SECRET)
        .json(&json!({ "lable": "typo" }))
        .await;
    res.assert_status(StatusCode::BAD_REQUEST);
    let body = res.json::<Value>();
    assert_eq!(body["code"], "unknown_field");
    assert!(body["error"].as_str().unwrap().contains("lable"));
}

/// The body on this endpoint is genuinely optional, so omitting it must still
/// issue a key. Rejecting unknown fields must not turn "no body" into an error.
#[tokio::test]
async fn test_issue_key_without_a_body_still_succeeds() {
    let server = test_server().await;
    let res = server
        .post("/merchants")
        .add_header("X-Admin-Secret", TEST_ADMIN_SECRET)
        .await;
    let merchant_id = res.json::<Value>()["merchant_id"]
        .as_str()
        .unwrap()
        .to_string();

    let res = server
        .post(&format!("/merchants/{merchant_id}/keys"))
        .add_header("X-Admin-Secret", TEST_ADMIN_SECRET)
        .await;
    res.assert_status(StatusCode::CREATED);
    assert!(res.json::<Value>()["label"].is_null());
}

/// A wrong *type* on a known field is still `invalid_request` — the new code is
/// specific to unrecognised field names, not a rename of the generic one.
#[tokio::test]
async fn test_wrong_type_on_known_field_is_still_invalid_request() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;
    let res = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .json(&json!({ "amount": 10 }))
        .await;
    res.assert_status(StatusCode::BAD_REQUEST);
    assert_eq!(res.json::<Value>()["code"], "invalid_request");
}

#[tokio::test]
async fn test_create_invalid_amount() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;
    let res = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .json(&json!({ "amount": "-1", "asset": "XLM" }))
        .await;
    res.assert_status(StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_get_by_id() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;
    let id = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .json(&json!({ "amount": "5", "asset": "USDC" }))
        .await
        .json::<Value>()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Full detail now requires the owning merchant's key (issues #67, #85).
    let res = server
        .get(&format!("/payments/{id}"))
        .add_header("Authorization", format!("Bearer {key}"))
        .await;
    res.assert_status_ok();
    let body = res.json::<Value>();
    assert_eq!(body["id"], id);

    // Timestamps on the GET response must also be strict RFC 3339.
    for field in ["created_at", "updated_at"] {
        let ts = body[field]
            .as_str()
            .unwrap_or_else(|| panic!("{field} missing"));
        time::OffsetDateTime::parse(ts, &Rfc3339)
            .unwrap_or_else(|e| panic!("{field} = {ts:?} is not valid RFC 3339: {e}"));
    }
}

#[tokio::test]
async fn test_get_not_found() {
    let res = test_server().await.get("/payments/does-not-exist").await;
    res.assert_status(StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_reject_too_many_decimals() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;
    let res = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .json(&json!({ "amount": "1.00000001", "asset": "XLM" }))
        .await;
    res.assert_status(StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_asset_is_case_insensitive() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;
    let res = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .json(&json!({ "amount": "1", "asset": "usdc" }))
        .await;
    res.assert_status(StatusCode::CREATED);
    assert_eq!(res.json::<Value>()["asset"], "USDC");
}

#[tokio::test]
async fn test_create_persists_asset_issuer() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;
    let body = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .json(&json!({ "amount": "5", "asset": "USDC" }))
        .await
        .json::<Value>();
    assert_eq!(body["asset"], "USDC");
    assert_eq!(
        body["asset_issuer"],
        "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"
    );

    let xlm = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .json(&json!({ "amount": "1", "asset": "XLM" }))
        .await
        .json::<Value>();
    assert_eq!(xlm["asset"], "XLM");
    assert!(xlm["asset_issuer"].is_null());
}

#[tokio::test]
async fn test_webhook_url_https_accepted_on_testnet() {
    let mut cfg = make_config();
    cfg.webhook_allow_private_targets = true;
    let (server, _db) = server_with_config(cfg).await;
    let key = provision_merchant(&server).await;
    let res = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .json(
            &json!({ "amount": "1", "asset": "XLM", "webhook_url": "https://127.0.0.1:9/webhook" }),
        )
        .await;
    res.assert_status(StatusCode::CREATED);
}

#[tokio::test]
async fn test_webhook_url_http_accepted_on_testnet() {
    let mut cfg = make_config();
    cfg.webhook_allow_private_targets = true;
    let (server, _db) = server_with_config(cfg).await;
    let key = provision_merchant(&server).await;
    let res = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .json(
            &json!({ "amount": "1", "asset": "XLM", "webhook_url": "http://127.0.0.1:9/webhook" }),
        )
        .await;
    res.assert_status(StatusCode::CREATED);
}

#[tokio::test]
async fn test_webhook_url_http_rejected_on_public_network() {
    let mut cfg = make_config();
    cfg.network = "public".into();
    cfg.webhook_allow_private_targets = true;
    let (server, _db) = server_with_config(cfg).await;
    let key = provision_merchant(&server).await;
    let res = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .json(
            &json!({ "amount": "1", "asset": "XLM", "webhook_url": "http://127.0.0.1:9/webhook" }),
        )
        .await;
    res.assert_status(StatusCode::BAD_REQUEST);
    let body: Value = res.json();
    assert_eq!(body["code"], "invalid_webhook_url");
    assert!(body["error"]
        .as_str()
        .unwrap()
        .contains("must be an HTTPS URL on public network"));
}

#[tokio::test]
async fn test_webhook_url_https_accepted_on_public_network() {
    let mut cfg = make_config();
    cfg.network = "public".into();
    cfg.webhook_allow_private_targets = true;
    let (server, _db) = server_with_config(cfg).await;
    let key = provision_merchant(&server).await;
    let res = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .json(
            &json!({ "amount": "1", "asset": "XLM", "webhook_url": "https://127.0.0.1:9/webhook" }),
        )
        .await;
    res.assert_status(StatusCode::CREATED);
}

#[tokio::test]
async fn test_webhook_url_invalid_rejected() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;

    // ftp scheme
    let res = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .json(&json!({ "amount": "1", "asset": "XLM", "webhook_url": "ftp://example.com" }))
        .await;
    res.assert_status(StatusCode::BAD_REQUEST);

    // malformed string
    let res = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .json(&json!({ "amount": "1", "asset": "XLM", "webhook_url": "not-a-url" }))
        .await;
    res.assert_status(StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_reject_webhook_url_targeting_loopback() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;
    let res = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .json(&json!({ "amount": "1", "asset": "XLM", "webhook_url": "http://127.0.0.1:9/hook" }))
        .await;
    res.assert_status(StatusCode::BAD_REQUEST);
    assert_eq!(
        res.json::<Value>()["code"],
        "invalid_webhook_url",
        "loopback webhook targets must be rejected at creation"
    );
}

#[tokio::test]
async fn test_reject_webhook_url_targeting_link_local_metadata_address() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;
    let res = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .json(&json!({
            "amount": "1",
            "asset": "XLM",
            "webhook_url": "http://169.254.169.254/latest/meta-data/"
        }))
        .await;
    res.assert_status(StatusCode::BAD_REQUEST);
    assert_eq!(res.json::<Value>()["code"], "invalid_webhook_url");
}

/// A delivery row can predate this guard (or be forged some other way), so the
/// redeliver endpoint must re-validate the target on every call rather than
/// trusting whatever URL was stored — merchant auth alone is not enough.
#[tokio::test]
async fn test_redeliver_rejects_ssrf_target_even_for_a_stored_delivery() {
    let (server, pool) = test_server_with_pool().await;
    let key = provision_merchant(&server).await;
    let id = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .json(&json!({ "amount": "5", "asset": "XLM" }))
        .await
        .json::<Value>()["id"]
        .as_str()
        .unwrap()
        .to_string();

    stellargate::db::save_webhook_delivery(
        &pool,
        "delivery-ssrf",
        &id,
        "http://127.0.0.1:9/hook",
        r#"{"event":"payment.completed"}"#,
        "payment.completed",
    )
    .await
    .unwrap();

    let res = server
        .post(&format!("/payments/{id}/webhooks/delivery-ssrf/redeliver"))
        .add_header("Authorization", format!("Bearer {key}"))
        .await;
    res.assert_status(StatusCode::BAD_REQUEST);
    assert_eq!(res.json::<Value>()["code"], "webhook_target_blocked");
}

#[tokio::test]
async fn test_list_payments() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;
    let auth = format!("Bearer {key}");
    for amt in ["1", "2", "3"] {
        server
            .post("/payments")
            .add_header("Authorization", auth.clone())
            .json(&json!({ "amount": amt, "asset": "XLM" }))
            .await;
    }

    let res = server
        .get("/payments")
        .add_header("Authorization", auth)
        .await;
    res.assert_status_ok();
    let body: Value = res.json();
    assert_eq!(body["total"], 3);
    assert_eq!(body["payments"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn test_list_filter_by_status() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;
    let auth = format!("Bearer {key}");
    server
        .post("/payments")
        .add_header("Authorization", auth.clone())
        .json(&json!({ "amount": "1", "asset": "XLM" }))
        .await;

    let res = server
        .get("/payments?status=completed")
        .add_header("Authorization", auth.clone())
        .await;
    res.assert_status_ok();
    assert_eq!(res.json::<Value>()["total"], 0);

    let res = server
        .get("/payments?status=pending")
        .add_header("Authorization", auth)
        .await;
    assert_eq!(res.json::<Value>()["total"], 1);
}

/// Settlement puts a partially-paid intent in `underpaid`, so merchants must
/// be able to list them — it's how you find payments still owed money.
#[tokio::test]
async fn test_list_filters_by_underpaid_status() {
    let (server, pool) = test_server_with_pool().await;
    let key = provision_merchant(&server).await;
    let auth = format!("Bearer {key}");

    let mut ids = vec![];
    for amt in ["5", "6"] {
        ids.push(
            server
                .post("/payments")
                .add_header("Authorization", auth.clone())
                .json(&json!({ "amount": amt, "asset": "XLM" }))
                .await
                .json::<Value>()["id"]
                .as_str()
                .unwrap()
                .to_string(),
        );
    }

    // Mirror what horizon::settle does for a short payment.
    stellargate::db::update_payment_status(&pool, &ids[0], "underpaid", "TX_PARTIAL", "3")
        .await
        .unwrap();

    let res = server
        .get("/payments?status=underpaid")
        .add_header("Authorization", auth)
        .await;
    res.assert_status_ok();
    let body: Value = res.json();
    assert_eq!(body["total"], 1);
    assert_eq!(body["payments"][0]["id"], ids[0]);
    assert_eq!(body["payments"][0]["status"], "underpaid");
}

#[tokio::test]
async fn test_list_invalid_status() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;
    let res = server
        .get("/payments?status=bogus")
        .add_header("Authorization", format!("Bearer {key}"))
        .await;
    res.assert_status(StatusCode::BAD_REQUEST);
}

/// No code path ever writes `failed` to a payment — underpayment settles as
/// `underpaid` — so accepting it as a filter would only ever return an empty
/// page while implying the gateway has a lifecycle state it doesn't.
#[tokio::test]
async fn test_list_rejects_failed_status() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;
    let res = server
        .get("/payments?status=failed")
        .add_header("Authorization", format!("Bearer {key}"))
        .await;
    res.assert_status(StatusCode::BAD_REQUEST);
    assert_eq!(res.json::<Value>()["code"], "invalid_status");
}

/// Every status the filter accepts must be one the code can actually produce,
/// and every status the code produces must be filterable.
#[tokio::test]
async fn test_filterable_statuses_match_producible_statuses() {
    let (server, pool) = test_server_with_pool().await;
    let key = provision_merchant(&server).await;
    let auth = format!("Bearer {key}");

    // `pending` on create, `completed`/`underpaid` from horizon::settle,
    // `expired` from the TTL sweeper.
    for (i, status) in ["pending", "completed", "underpaid", "expired"]
        .into_iter()
        .enumerate()
    {
        let id = server
            .post("/payments")
            .add_header("Authorization", auth.clone())
            .json(&json!({ "amount": format!("{}", i + 1), "asset": "XLM" }))
            .await
            .json::<Value>()["id"]
            .as_str()
            .unwrap()
            .to_string();
        if status != "pending" {
            stellargate::db::update_payment_status(&pool, &id, status, "TX", "1")
                .await
                .unwrap();
        }

        let res = server
            .get(&format!("/payments?status={status}"))
            .add_header("Authorization", auth.clone())
            .await;
        res.assert_status_ok();
        assert_eq!(
            res.json::<Value>()["total"],
            1,
            "status {status} must be filterable"
        );
    }
}

#[tokio::test]
async fn test_list_cursor_pagination() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;
    let auth = format!("Bearer {key}");
    for amt in ["1", "2", "3", "4", "5"] {
        server
            .post("/payments")
            .add_header("Authorization", auth.clone())
            .json(&json!({ "amount": amt, "asset": "XLM" }))
            .await;
    }

    // Page 1 via offset path — also returns next_cursor for migration.
    let res = server
        .get("/payments?limit=2")
        .add_header("Authorization", auth.clone())
        .await;
    res.assert_status_ok();
    let body: Value = res.json();
    assert_eq!(body["payments"].as_array().unwrap().len(), 2);
    let cursor = body["next_cursor"]
        .as_str()
        .expect("next_cursor must be present on a full page");

    // Page 2 via keyset cursor.
    let res2 = server
        .get(&format!("/payments?cursor={cursor}&limit=2"))
        .add_header("Authorization", auth.clone())
        .await;
    res2.assert_status_ok();
    let body2: Value = res2.json();
    assert_eq!(body2["payments"].as_array().unwrap().len(), 2);
    let cursor2 = body2["next_cursor"]
        .as_str()
        .expect("next_cursor must be present on a full page");

    // Page 3 — last page, fewer items than limit.
    let res3 = server
        .get(&format!("/payments?cursor={cursor2}&limit=2"))
        .add_header("Authorization", auth.clone())
        .await;
    res3.assert_status_ok();
    let body3: Value = res3.json();
    assert_eq!(body3["payments"].as_array().unwrap().len(), 1);
    assert!(
        body3["next_cursor"].is_null(),
        "last page must have null next_cursor"
    );

    // All 5 IDs are unique across all pages.
    let ids: Vec<String> = [&body, &body2, &body3]
        .iter()
        .flat_map(|b| b["payments"].as_array().unwrap().iter())
        .map(|p| p["id"].as_str().unwrap().to_string())
        .collect();
    let unique: std::collections::HashSet<_> = ids.iter().collect();
    assert_eq!(unique.len(), 5);
}

/// Regression for #328: the offset branch mints a `next_cursor` from its last
/// row, but its query ordered ties on `created_at` alone while the keyset
/// query broke them on `id DESC`. `created_at` is whole-second, so a page
/// whose last row sits inside a tie group handed that cursor to the keyset
/// query, which resumed *after* the whole group — skipping the members that
/// sorted above the boundary. Both branches must share one ordering, and a
/// cursor taken from an offset page must walk the tie group without skipping
/// or repeating rows.
#[tokio::test]
async fn test_offset_cursor_tie_group_agreement() {
    let (server, pool) = test_server_with_pool().await;
    let key = provision_merchant(&server).await;
    let auth = format!("Bearer {key}");

    let merchant_id = stellargate::db::find_merchant_by_key(&pool, &key)
        .await
        .unwrap()
        .expect("merchant must exist");

    // Six payments stamped in the *same* second. The ids sort inversely to
    // insertion order, so id DESC (the keyset ordering) and rowid order (what
    // a bare `ORDER BY created_at DESC` may fall back to) disagree at every
    // position — page 1 of the offset path is exactly the tie group.
    let ts = "2026-08-17T12:00:00Z";
    for id in ["a", "b", "c", "d", "e", "f"] {
        sqlx::query(
            "INSERT INTO payments
                (id, merchant_id, destination_address, memo, amount, asset, status,
                 created_at, updated_at, expires_at)
             VALUES (?, ?, 'GDEST', ?, '1', 'XLM', 'pending', ?, ?, ?)",
        )
        .bind(id)
        .bind(&merchant_id)
        .bind(format!("MEMO-{id}"))
        .bind(ts)
        .bind(ts)
        .bind("2026-08-17T13:00:00Z")
        .execute(&pool)
        .await
        .unwrap();
    }

    // Page 1 via the offset path. Its next_cursor must be a valid entry point
    // into cursor mode.
    let res = server
        .get("/payments?limit=3")
        .add_header("Authorization", auth.clone())
        .await;
    res.assert_status_ok();
    let body: Value = res.json();
    let page1: Vec<String> = body["payments"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        page1,
        ["f", "e", "d"],
        "offset page 1 must order whole-second ties by id DESC"
    );
    let cursor = body["next_cursor"]
        .as_str()
        .expect("full offset page must carry a migration cursor");

    // Page 2 via the cursor. The tie group must continue, not resume after it.
    let res2 = server
        .get(&format!("/payments?cursor={cursor}&limit=3"))
        .add_header("Authorization", auth.clone())
        .await;
    res2.assert_status_ok();
    let body2: Value = res2.json();
    let page2: Vec<String> = body2["payments"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        page2,
        ["c", "b", "a"],
        "cursor minted on the offset branch must resume inside the tie group"
    );

    // No rows skipped or repeated across the two pages.
    let all: std::collections::HashSet<_> = page1.iter().chain(&page2).collect();
    assert_eq!(all.len(), 6);

    // The offset path walks the same sequence the cursor path did.
    let res3 = server
        .get("/payments?limit=3&offset=3")
        .add_header("Authorization", auth)
        .await;
    res3.assert_status_ok();
    let body3: Value = res3.json();
    let page2_offset: Vec<String> = body3["payments"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(page2_offset, page2, "offset and cursor pages must agree");
}

#[tokio::test]
async fn test_list_cursor_invalid() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;
    let res = server
        .get("/payments?cursor=notvalidhex!!")
        .add_header("Authorization", format!("Bearer {key}"))
        .await;
    res.assert_status(StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_unknown_route_returns_json_404() {
    let res = test_server().await.get("/nope").await;
    res.assert_status(StatusCode::NOT_FOUND);
    let body: Value = res.json();
    assert_eq!(body["error"], "not found");
    assert_eq!(body["code"], "not_found");
    res.assert_contains_header("x-request-id");
}

#[tokio::test]
async fn test_list_webhooks_unauthenticated_returns_401() {
    let res = test_server()
        .await
        .get("/payments/nonexistent/webhooks")
        .await;
    res.assert_status(StatusCode::UNAUTHORIZED);
    assert_eq!(res.json::<Value>()["code"], "unauthorized");
}

#[tokio::test]
async fn test_list_webhooks_not_found() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;
    let res = server
        .get("/payments/nonexistent/webhooks")
        .add_header("Authorization", format!("Bearer {key}"))
        .await;
    res.assert_status(StatusCode::NOT_FOUND);
    let body: Value = res.json();
    assert_eq!(body["error"], "payment not found");
    assert_eq!(body["code"], "payment_not_found");
}

#[tokio::test]
async fn test_list_webhooks_empty() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;
    let auth = format!("Bearer {key}");
    let id = server
        .post("/payments")
        .add_header("Authorization", auth.clone())
        .json(&json!({ "amount": "5", "asset": "XLM" }))
        .await
        .json::<Value>()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let res = server
        .get(&format!("/payments/{id}/webhooks"))
        .add_header("Authorization", auth)
        .await;
    res.assert_status_ok();
    let body: Value = res.json();
    assert_eq!(body["payment_id"], id);
    assert_eq!(body["deliveries"].as_array().unwrap().len(), 0);
}

/// A merchant cannot read another merchant's webhook deliveries — the payment
/// id alone must not be enough, and the response must not distinguish "not
/// yours" from "doesn't exist".
#[tokio::test]
async fn test_list_webhooks_rejects_other_merchants_payment() {
    let (server, pool) = test_server_with_pool().await;

    let owner_key = provision_merchant(&server).await;
    let id = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {owner_key}"))
        .json(&json!({ "amount": "5", "asset": "XLM" }))
        .await
        .json::<Value>()["id"]
        .as_str()
        .unwrap()
        .to_string();

    stellargate::db::save_webhook_delivery(
        &pool,
        "delivery-owned",
        &id,
        "https://example.com/webhook",
        r#"{"event":"payment.completed"}"#,
        "payment.completed",
    )
    .await
    .unwrap();

    let other_key = provision_merchant(&server).await;
    let res = server
        .get(&format!("/payments/{id}/webhooks"))
        .add_header("Authorization", format!("Bearer {other_key}"))
        .await;
    res.assert_status(StatusCode::NOT_FOUND);
    assert_eq!(res.json::<Value>()["code"], "payment_not_found");
}

#[tokio::test]
async fn test_redeliver_unauthenticated_returns_401() {
    let res = test_server()
        .await
        .post("/payments/nonexistent/webhooks/xyz/redeliver")
        .await;
    res.assert_status(StatusCode::UNAUTHORIZED);
    assert_eq!(res.json::<Value>()["code"], "unauthorized");
}

#[tokio::test]
async fn test_redeliver_webhook_not_found() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;
    let res = server
        .post("/payments/nonexistent/webhooks/xyz/redeliver")
        .add_header("Authorization", format!("Bearer {key}"))
        .await;
    res.assert_status(StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_redeliver_delivery_not_found() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;
    let auth = format!("Bearer {key}");
    let id = server
        .post("/payments")
        .add_header("Authorization", auth.clone())
        .json(&json!({ "amount": "5", "asset": "XLM" }))
        .await
        .json::<Value>()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let res = server
        .post(&format!("/payments/{id}/webhooks/nonexistent/redeliver"))
        .add_header("Authorization", auth)
        .await;
    res.assert_status(StatusCode::NOT_FOUND);
}

/// A merchant cannot trigger redelivery of another merchant's webhook — the
/// payment id alone must not be enough, and the response must not
/// distinguish "not yours" from "doesn't exist".
#[tokio::test]
async fn test_redeliver_rejects_other_merchants_payment() {
    let (server, pool) = test_server_with_pool().await;

    let owner_key = provision_merchant(&server).await;
    let id = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {owner_key}"))
        .json(&json!({ "amount": "5", "asset": "XLM" }))
        .await
        .json::<Value>()["id"]
        .as_str()
        .unwrap()
        .to_string();

    stellargate::db::save_webhook_delivery(
        &pool,
        "delivery-owned",
        &id,
        "https://example.com/webhook",
        r#"{"event":"payment.completed"}"#,
        "payment.completed",
    )
    .await
    .unwrap();

    let other_key = provision_merchant(&server).await;
    let res = server
        .post(&format!("/payments/{id}/webhooks/delivery-owned/redeliver"))
        .add_header("Authorization", format!("Bearer {other_key}"))
        .await;
    res.assert_status(StatusCode::NOT_FOUND);
    assert_eq!(res.json::<Value>()["code"], "payment_not_found");
}

/// A redelivered webhook must carry the event the payload actually describes.
/// Hard-coding `payment.completed` here would tell a receiver that routes on
/// `X-StellarGate-Event` the opposite of what the body says.
#[tokio::test]
async fn test_redeliver_echoes_the_original_event_type() {
    let mock = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path("/hook"))
        .respond_with(wiremock::ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock)
        .await;

    // The mock listens on loopback, which the SSRF guard blocks by default.
    let mut cfg = make_config();
    cfg.webhook_allow_private_targets = true;
    let (server, pool) = server_with_config(cfg).await;
    let key = provision_merchant(&server).await;
    let id = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .json(&json!({ "amount": "5", "asset": "XLM" }))
        .await
        .json::<Value>()["id"]
        .as_str()
        .unwrap()
        .to_string();

    stellargate::db::save_webhook_delivery(
        &pool,
        "delivery-underpaid",
        &id,
        &format!("{}/hook", mock.uri()),
        r#"{"event":"payment.underpaid","status":"underpaid"}"#,
        "payment.underpaid",
    )
    .await
    .unwrap();

    let res = server
        .post(&format!(
            "/payments/{id}/webhooks/delivery-underpaid/redeliver"
        ))
        .add_header("Authorization", format!("Bearer {key}"))
        .await;
    res.assert_status_ok();

    let received = mock.received_requests().await.unwrap();
    assert_eq!(received.len(), 1);
    assert_eq!(
        received[0].headers.get("X-StellarGate-Event").unwrap(),
        "payment.underpaid",
        "redelivered header must match the event the payload carries"
    );
}

/// Deliveries written before `event_type` existed have a NULL column, so the
/// event has to come from the stored payload rather than a hard-coded default.
#[tokio::test]
async fn test_redeliver_falls_back_to_payload_event_for_legacy_rows() {
    let mock = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path("/hook"))
        .respond_with(wiremock::ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock)
        .await;

    let mut cfg = make_config();
    cfg.webhook_allow_private_targets = true;
    let (server, pool) = server_with_config(cfg).await;
    let key = provision_merchant(&server).await;
    let id = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .json(&json!({ "amount": "5", "asset": "XLM" }))
        .await
        .json::<Value>()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Insert directly, leaving event_type NULL the way a pre-migration row is.
    sqlx::query(
        "INSERT INTO webhook_deliveries (id, payment_id, url, payload) VALUES (?, ?, ?, ?)",
    )
    .bind("delivery-legacy")
    .bind(&id)
    .bind(format!("{}/hook", mock.uri()))
    .bind(r#"{"event":"payment.overpaid","status":"completed"}"#)
    .execute(&pool)
    .await
    .unwrap();

    let res = server
        .post(&format!(
            "/payments/{id}/webhooks/delivery-legacy/redeliver"
        ))
        .add_header("Authorization", format!("Bearer {key}"))
        .await;
    res.assert_status_ok();

    let received = mock.received_requests().await.unwrap();
    assert_eq!(
        received[0].headers.get("X-StellarGate-Event").unwrap(),
        "payment.overpaid"
    );
}

#[tokio::test]
async fn test_webhook_delivery_isolation() {
    let (server, pool) = test_server_with_pool().await;
    let key = provision_merchant(&server).await;
    let auth = format!("Bearer {key}");

    // Create two payments
    let id1 = server
        .post("/payments")
        .add_header("Authorization", auth.clone())
        .json(&json!({ "amount": "5", "asset": "XLM" }))
        .await
        .json::<Value>()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let id2 = server
        .post("/payments")
        .add_header("Authorization", auth.clone())
        .json(&json!({ "amount": "10", "asset": "USDC" }))
        .await
        .json::<Value>()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Manually insert a delivery for payment 1
    stellargate::db::save_webhook_delivery(
        &pool,
        "delivery-1",
        &id1,
        "https://example.com/webhook",
        r#"{"event":"payment.completed"}"#,
        "payment.completed",
    )
    .await
    .unwrap();

    // List webhooks for payment 1 should find it
    let res1 = server
        .get(&format!("/payments/{id1}/webhooks"))
        .add_header("Authorization", auth.clone())
        .await;
    res1.assert_status_ok();
    assert_eq!(
        res1.json::<Value>()["deliveries"].as_array().unwrap().len(),
        1
    );

    // List webhooks for payment 2 should be empty
    let res2 = server
        .get(&format!("/payments/{id2}/webhooks"))
        .add_header("Authorization", auth.clone())
        .await;
    res2.assert_status_ok();
    assert_eq!(
        res2.json::<Value>()["deliveries"].as_array().unwrap().len(),
        0
    );

    // Try to redeliver delivery from payment 1 on payment 2 (should fail)
    let res_cross = server
        .post(&format!("/payments/{id2}/webhooks/delivery-1/redeliver"))
        .add_header("Authorization", auth)
        .await;
    res_cross.assert_status(StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_amount_canonicalization_on_create_get_list() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;
    let auth = format!("Bearer {key}");

    // Test various representations of the same value.
    // All should serialize to "10.5" regardless of input format.
    let test_cases = vec![
        ("10.5", "10.5"),
        ("10.50", "10.5"),
        ("10.500", "10.5"),
        ("10.5000", "10.5"),
        ("10.50000", "10.5"),
        ("10.500000", "10.5"),
        ("10.5000000", "10.5"),
    ];

    let mut payment_ids = Vec::new();

    for (input, expected_canonical) in test_cases {
        let res = server
            .post("/payments")
            .add_header("Authorization", auth.clone())
            .json(&json!({ "amount": input, "asset": "XLM" }))
            .await;
        res.assert_status(StatusCode::CREATED);
        let body: Value = res.json();

        // Verify that the created payment has the canonical form
        assert_eq!(
            body["amount"].as_str().unwrap(),
            expected_canonical,
            "create response should canonicalize amount: {input} -> {expected_canonical}"
        );

        let payment_id = body["id"].as_str().unwrap().to_string();
        payment_ids.push((input, expected_canonical, payment_id));
    }

    // Verify canonicalization persists across GET requests
    for (input, expected_canonical, payment_id) in &payment_ids {
        let res = server
            .get(&format!("/payments/{payment_id}"))
            .add_header("Authorization", auth.clone())
            .await;
        res.assert_status_ok();
        let body: Value = res.json();

        assert_eq!(
            body["amount"].as_str().unwrap(),
            *expected_canonical,
            "get response should return canonical form for input: {input}"
        );
    }

    // Verify canonicalization in list endpoint
    let res = server
        .get("/payments?limit=100")
        .add_header("Authorization", auth.clone())
        .await;
    res.assert_status_ok();
    let list: Value = res.json();

    for payment in list["payments"].as_array().unwrap() {
        let amount_str = payment["amount"].as_str().unwrap();
        // All amounts should be in canonical form (no trailing zeros)
        for (_, expected_canonical, _) in &payment_ids {
            if amount_str == *expected_canonical {
                // Found one of our test payments, good
                break;
            }
        }
    }
}

#[tokio::test]
async fn test_whole_amount_canonicalization() {
    // Test that whole amounts are serialized without decimal point
    let server = test_server().await;
    let key = provision_merchant(&server).await;

    let test_cases = vec![
        ("1", "1"),
        ("1.0", "1"),
        ("1.00", "1"),
        ("100", "100"),
        ("100.0000000", "100"),
    ];

    for (input, expected) in test_cases {
        let res = server
            .post("/payments")
            .add_header("Authorization", format!("Bearer {key}"))
            .json(&json!({ "amount": input, "asset": "XLM" }))
            .await;
        res.assert_status(StatusCode::CREATED);
        let body: Value = res.json();

        assert_eq!(
            body["amount"].as_str().unwrap(),
            expected,
            "whole amount {input} should canonicalize to {expected}"
        );
    }
}

/// The dashboard shell is static and carries no merchant data, so it is served
/// without authentication — the browser then authenticates every data call
/// with the API key the operator types in.
#[tokio::test]
async fn test_dashboard_assets_served_unauthenticated() {
    let server = test_server().await;

    for (path, content_type) in [
        ("/dashboard", "text/html; charset=utf-8"),
        ("/dashboard/app.css", "text/css; charset=utf-8"),
        ("/dashboard/app.js", "text/javascript; charset=utf-8"),
    ] {
        let res = server.get(path).await;
        res.assert_status_ok();
        assert_eq!(
            res.header("content-type"),
            content_type,
            "{path} served with the wrong content type"
        );
        assert!(
            !res.text().is_empty(),
            "{path} served an empty body — asset missing from the binary?"
        );
    }
}

/// The dashboard must stay locked to its own origin. Without a CSP a single
/// injected third-party script would sit on the page an operator pastes an
/// API key into.
#[tokio::test]
async fn test_dashboard_sets_security_headers() {
    let server = test_server().await;
    let res = server.get("/dashboard").await;
    res.assert_status_ok();

    let csp = res.header("content-security-policy");
    let csp = csp.to_str().unwrap();
    assert!(csp.contains("default-src 'none'"), "got: {csp}");
    assert!(csp.contains("script-src 'self'"), "got: {csp}");
    assert!(csp.contains("frame-ancestors 'none'"), "got: {csp}");
    // An inline-script allowance would defeat the point of the policy.
    assert!(!csp.contains("unsafe-inline"), "got: {csp}");

    assert_eq!(res.header("x-content-type-options"), "nosniff");
}

/// The dashboard is a client of the documented API, so the endpoints it
/// depends on must keep working unauthenticated/authenticated as it expects.
#[tokio::test]
async fn test_dashboard_data_endpoints_reject_missing_key() {
    let server = test_server().await;
    // The list the dashboard loads on sign-in is the key-validation call.
    let res = server.get("/payments?limit=1").await;
    res.assert_status(StatusCode::UNAUTHORIZED);
    assert_eq!(res.json::<Value>()["code"], "unauthorized");
}

/// Regression guard for a bug that shipped: the sign-in panel stayed on screen
/// after signing in, with the payments list pushed below it.
///
/// The script hides elements by setting the `hidden` attribute, but the browser
/// applies `[hidden] { display: none }` from its own stylesheet, which any
/// author `display` rule outranks — and `.gate` sets `display: grid`. The
/// stylesheet must therefore assert the rule itself.
///
/// There is no browser in CI to catch this visually, so pin it here: every
/// element the dashboard toggles depends on it.
#[tokio::test]
async fn test_dashboard_css_forces_hidden_to_win() {
    let server = test_server().await;
    let raw = server.get("/dashboard/app.css").await.text();

    // Strip comments first — the rule is *explained* in a comment that also
    // contains the text `[hidden] { display: none }`, and matching that instead
    // of the real declaration would make this test pass on a broken stylesheet.
    let mut css = String::with_capacity(raw.len());
    let mut rest = raw.as_str();
    while let Some(open) = rest.find("/*") {
        css.push_str(&rest[..open]);
        rest = match rest[open..].find("*/") {
            Some(close) => &rest[open + close + 2..],
            None => "",
        };
    }
    css.push_str(rest);

    let rule_start = css.find("[hidden]").expect(
        "stylesheet must define a [hidden] rule; without it any author \
         `display` declaration keeps `hidden` elements visible",
    );
    let rule = &css[rule_start
        ..css[rule_start..]
            .find('}')
            .map(|i| rule_start + i)
            .unwrap_or(css.len() - rule_start)];

    assert!(
        rule.contains("display") && rule.contains("none"),
        "[hidden] must set display:none, got: {rule}"
    );
    assert!(
        rule.contains("!important"),
        "[hidden] must be !important to outrank author rules like `.gate \
         {{ display: grid }}`, got: {rule}"
    );
}

/// A browser opening the service root should land on the dashboard. Hosting
/// platforms and port-forwarding UIs hand you the root URL, so a bare version
/// string there reads as "nothing is running" and hides the UI entirely.
#[tokio::test]
async fn test_root_redirects_browsers_to_the_dashboard() {
    let server = test_server().await;
    let res = server
        .get("/")
        .add_header("Accept", "text/html,application/xhtml+xml,*/*;q=0.8")
        .await;
    res.assert_status(StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(res.header("location"), "/dashboard");
}

/// ...but API clients must keep the plaintext version string. `fetch()` and
/// `curl` send `*/*`, and the dashboard's own version lookup depends on this.
#[tokio::test]
async fn test_root_serves_version_to_api_clients() {
    let server = test_server().await;

    for accept in ["*/*", "application/json"] {
        let res = server.get("/").add_header("Accept", accept).await;
        res.assert_status_ok();
        assert!(
            res.text().starts_with("StellarGate API v"),
            "Accept: {accept} should get the version string, got: {}",
            res.text()
        );
    }

    // No Accept header at all (plain curl) must not redirect either.
    let res = server.get("/").await;
    res.assert_status_ok();
    assert!(res.text().starts_with("StellarGate API v"));
}

/// Regression guard: the sign-in form must be visible in the markup as shipped.
///
/// Both panels used to start hidden, with the script revealing one once it had
/// decided which to show. Any failure before that decision — script blocked,
/// an exception, a session resume failing for a reason other than 401 — left
/// both hidden and rendered a blank page with no way forward.
///
/// Defaulting to the gate means the worst case degrades to "sign in" instead.
#[tokio::test]
async fn test_dashboard_gate_is_visible_without_javascript() {
    let server = test_server().await;
    let html = server.get("/dashboard").await.text();

    let gate = html
        .find(r#"id="gate""#)
        .map(|i| &html[i..html[i..].find('>').map(|j| i + j).unwrap_or(html.len())])
        .expect("dashboard must contain the sign-in gate");

    assert!(
        !gate.contains("hidden"),
        "the sign-in gate must not be hidden in the markup, or a script failure \
         leaves a blank page; got: <section {gate}>"
    );

    // The app panel is the one that starts hidden.
    let app = html
        .find(r#"id="app""#)
        .map(|i| &html[i..html[i..].find('>').map(|j| i + j).unwrap_or(html.len())])
        .expect("dashboard must contain the app panel");
    assert!(app.contains("hidden"), "the app panel should start hidden");
}

/// The sign-in form must not submit via GET. The script calls preventDefault,
/// but if it never ran, a default GET submit would put the merchant's API key
/// in the URL and the browser history. POST keeps it in a request body that
/// this route answers with 405.
#[tokio::test]
async fn test_dashboard_form_does_not_leak_key_via_get() {
    let server = test_server().await;
    let html = server.get("/dashboard").await.text();

    let form = html
        .find(r#"id="gate-form""#)
        .map(|i| &html[i..html[i..].find('>').map(|j| i + j).unwrap_or(html.len())])
        .expect("dashboard must contain the sign-in form");

    assert!(
        form.contains(r#"method="post""#),
        "sign-in form must be method=post so a no-script submit cannot place \
         the API key in the URL; got: <form {form}>"
    );
}

// ── API key lifecycle (issues #74, #81) ──────────────────────────────────

/// Keys must be CSPRNG bearer tokens, not UUIDs. A v4 UUID carries 122 random
/// bits and spends 6 encoding version/variant — fine as an identifier, wrong
/// as a credential.
#[tokio::test]
async fn test_api_keys_are_high_entropy_prefixed_tokens() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;

    assert!(key.starts_with("sg_"), "key should be recognisable: {key}");
    // sg_ + 32 bytes hex = 67 chars, i.e. 256 bits of entropy.
    assert_eq!(key.len(), 67, "expected 256 bits of entropy, got: {key}");
    assert!(
        key[3..].chars().all(|c| c.is_ascii_hexdigit()),
        "body should be hex: {key}"
    );
    assert!(
        !key.contains('-'),
        "should not be a UUID (issue #81): {key}"
    );

    // Two keys must never collide.
    let second = provision_merchant(&server).await;
    assert_ne!(key, second);
}

/// Rotation is issue-then-revoke: both keys work in the overlap window, so a
/// merchant can deploy the new credential before retiring the old one.
#[tokio::test]
async fn test_key_rotation_keeps_both_keys_live_during_handover() {
    let server = test_server().await;
    let res = server
        .post("/merchants")
        .add_header("X-Admin-Secret", TEST_ADMIN_SECRET)
        .await;
    res.assert_status(StatusCode::CREATED);
    let body: Value = res.json();
    let merchant_id = body["merchant_id"].as_str().unwrap().to_string();
    let old_key = body["api_key"].as_str().unwrap().to_string();

    // Issue a replacement.
    let res = server
        .post(&format!("/merchants/{merchant_id}/keys"))
        .add_header("X-Admin-Secret", TEST_ADMIN_SECRET)
        .json(&json!({ "label": "rotation-2026-08" }))
        .await;
    res.assert_status(StatusCode::CREATED);
    let issued: Value = res.json();
    let new_key = issued["api_key"].as_str().unwrap().to_string();
    let new_key_id = issued["key_id"].as_str().unwrap().to_string();
    assert_ne!(old_key, new_key);

    // Both authenticate during the handover.
    for k in [&old_key, &new_key] {
        server
            .get("/payments")
            .add_header("Authorization", format!("Bearer {k}"))
            .await
            .assert_status_ok();
    }

    // Retire the old one.
    let old_id = server
        .get(&format!("/merchants/{merchant_id}/keys"))
        .add_header("X-Admin-Secret", TEST_ADMIN_SECRET)
        .await
        .json::<Value>()["keys"]
        .as_array()
        .unwrap()
        .iter()
        .find(|k| k["key_id"] != json!(new_key_id))
        .unwrap()["key_id"]
        .as_str()
        .unwrap()
        .to_string();

    server
        .delete(&format!("/merchants/{merchant_id}/keys/{old_id}"))
        .add_header("X-Admin-Secret", TEST_ADMIN_SECRET)
        .await
        .assert_status_ok();

    // The revoked key stops working immediately; the new one keeps working.
    server
        .get("/payments")
        .add_header("Authorization", format!("Bearer {old_key}"))
        .await
        .assert_status(StatusCode::UNAUTHORIZED);
    server
        .get("/payments")
        .add_header("Authorization", format!("Bearer {new_key}"))
        .await
        .assert_status_ok();
}

/// Revoking a merchant's only key would lock them out of an API with no
/// self-service recovery, turning a routine revocation into an incident.
#[tokio::test]
async fn test_cannot_revoke_the_last_active_key() {
    let server = test_server().await;
    let res = server
        .post("/merchants")
        .add_header("X-Admin-Secret", TEST_ADMIN_SECRET)
        .await;
    let body: Value = res.json();
    let merchant_id = body["merchant_id"].as_str().unwrap();
    let key = body["api_key"].as_str().unwrap().to_string();
    let key_id = body["key_id"].as_str().unwrap();

    let res = server
        .delete(&format!("/merchants/{merchant_id}/keys/{key_id}"))
        .add_header("X-Admin-Secret", TEST_ADMIN_SECRET)
        .await;
    res.assert_status(StatusCode::BAD_REQUEST);
    assert_eq!(res.json::<Value>()["code"], "last_active_key");

    // And it really is still usable.
    server
        .get("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .await
        .assert_status_ok();
}

/// Listing keys must never expose a usable credential.
#[tokio::test]
async fn test_listing_keys_never_returns_the_secret() {
    let server = test_server().await;
    let res = server
        .post("/merchants")
        .add_header("X-Admin-Secret", TEST_ADMIN_SECRET)
        .await;
    let body: Value = res.json();
    let merchant_id = body["merchant_id"].as_str().unwrap();
    let raw_key = body["api_key"].as_str().unwrap().to_string();

    let listed = server
        .get(&format!("/merchants/{merchant_id}/keys"))
        .add_header("X-Admin-Secret", TEST_ADMIN_SECRET)
        .await;
    listed.assert_status_ok();
    let text = listed.text();

    assert!(
        !text.contains(&raw_key),
        "key listing must not contain the secret"
    );
    let body: Value = listed.json();
    let entry = &body["keys"][0];
    assert_eq!(entry["active"], json!(true));
    assert!(entry["prefix"].as_str().unwrap().starts_with("sg_"));
    // The prefix identifies a key without being enough to use it.
    assert!(entry["prefix"].as_str().unwrap().len() < raw_key.len());
}

/// Key management is an operator action — it must not be reachable with a
/// merchant's own key, only the admin secret.
#[tokio::test]
async fn test_key_endpoints_require_the_admin_secret() {
    let server = test_server().await;
    let res = server
        .post("/merchants")
        .add_header("X-Admin-Secret", TEST_ADMIN_SECRET)
        .await;
    let body: Value = res.json();
    let merchant_id = body["merchant_id"].as_str().unwrap();
    let merchant_key = body["api_key"].as_str().unwrap();

    for (method, path) in [
        ("POST", format!("/merchants/{merchant_id}/keys")),
        ("GET", format!("/merchants/{merchant_id}/keys")),
        ("DELETE", format!("/merchants/{merchant_id}/keys/whatever")),
    ] {
        // No credential at all.
        let res = match method {
            "POST" => server.post(&path).await,
            "GET" => server.get(&path).await,
            _ => server.delete(&path).await,
        };
        res.assert_status(StatusCode::UNAUTHORIZED);

        // A valid merchant key is not sufficient either.
        let res = match method {
            "POST" => {
                server
                    .post(&path)
                    .add_header("Authorization", format!("Bearer {merchant_key}"))
                    .await
            }
            "GET" => {
                server
                    .get(&path)
                    .add_header("Authorization", format!("Bearer {merchant_key}"))
                    .await
            }
            _ => {
                server
                    .delete(&path)
                    .add_header("Authorization", format!("Bearer {merchant_key}"))
                    .await
            }
        };
        res.assert_status(StatusCode::UNAUTHORIZED);
    }
}

/// One merchant must not be able to revoke another's key.
#[tokio::test]
async fn test_revocation_is_scoped_to_the_owning_merchant() {
    let server = test_server().await;

    let a: Value = server
        .post("/merchants")
        .add_header("X-Admin-Secret", TEST_ADMIN_SECRET)
        .await
        .json();
    let b: Value = server
        .post("/merchants")
        .add_header("X-Admin-Secret", TEST_ADMIN_SECRET)
        .await
        .json();

    let b_id = b["merchant_id"].as_str().unwrap();
    let a_key_id = a["key_id"].as_str().unwrap();
    let a_key = a["api_key"].as_str().unwrap().to_string();

    // Give B a second key so the last-key guard is not what stops this.
    server
        .post(&format!("/merchants/{b_id}/keys"))
        .add_header("X-Admin-Secret", TEST_ADMIN_SECRET)
        .await
        .assert_status(StatusCode::CREATED);

    // Try to revoke A's key through B.
    let res = server
        .delete(&format!("/merchants/{b_id}/keys/{a_key_id}"))
        .add_header("X-Admin-Secret", TEST_ADMIN_SECRET)
        .await;
    res.assert_status(StatusCode::NOT_FOUND);

    // A's key is untouched.
    server
        .get("/payments")
        .add_header("Authorization", format!("Bearer {a_key}"))
        .await
        .assert_status_ok();
}

/// Key endpoints for a merchant that does not exist must 404, not silently
/// succeed against nothing.
#[tokio::test]
async fn test_key_endpoints_404_for_unknown_merchant() {
    let server = test_server().await;
    let res = server
        .get("/merchants/does-not-exist/keys")
        .add_header("X-Admin-Secret", TEST_ADMIN_SECRET)
        .await;
    res.assert_status(StatusCode::NOT_FOUND);
    assert_eq!(res.json::<Value>()["code"], "merchant_not_found");
}

// ── Scoped reads on GET /payments/:id (issues #67, #85) ──────────────────

/// Unauthenticated callers keep a way to poll for completion, but the response
/// must not identify the merchant or the sum involved. Payment ids travel
/// through logs, referrers and browser history, so this response is
/// effectively public.
#[tokio::test]
async fn test_public_payment_view_hides_merchant_and_amounts() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;
    let created: Value = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .json(&json!({ "amount": "1234.5678", "asset": "XLM" }))
        .await
        .json();
    let id = created["id"].as_str().unwrap();
    let merchant_id = created["merchant_id"].as_str().unwrap();

    let res = server.get(&format!("/payments/{id}")).await;
    res.assert_status_ok();
    let body: Value = res.json();

    // Still useful: you can tell whether it has been paid.
    assert_eq!(body["id"], json!(id));
    assert_eq!(body["status"], json!("pending"));
    assert!(body["expires_at"].is_string());

    // ...and nothing that enables cross-tenant reconnaissance.
    for leaked in [
        "merchant_id",
        "amount",
        "paid_amount",
        "tx_hash",
        "destination_address",
    ] {
        assert!(
            body.get(leaked).is_none(),
            "public view must not expose {leaked}: {body}"
        );
    }

    // Belt and braces: the merchant id must not appear anywhere in the bytes.
    assert!(
        !res.text().contains(merchant_id),
        "merchant id leaked into the public view"
    );
    assert!(
        !res.text().contains("1234.5678"),
        "amount leaked into the public view"
    );
}

/// The owning merchant still gets everything.
#[tokio::test]
async fn test_owner_sees_full_payment_detail() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;
    let id = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .json(&json!({ "amount": "10", "asset": "XLM" }))
        .await
        .json::<Value>()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let res = server
        .get(&format!("/payments/{id}"))
        .add_header("Authorization", format!("Bearer {key}"))
        .await;
    res.assert_status_ok();
    let body: Value = res.json();
    for field in [
        "merchant_id",
        "amount",
        "asset",
        "destination_address",
        "memo",
    ] {
        assert!(
            body.get(field).is_some(),
            "owner should see {field}: {body}"
        );
    }
}

/// Another merchant's key must get a 404, not a 403. A 403 would confirm the
/// payment exists and belongs to someone else, which is the cross-tenant
/// signal these issues are about.
#[tokio::test]
async fn test_other_merchants_key_cannot_read_a_payment() {
    let server = test_server().await;
    let owner_key = provision_merchant(&server).await;
    let stranger_key = provision_merchant(&server).await;

    let id = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {owner_key}"))
        .json(&json!({ "amount": "10", "asset": "XLM" }))
        .await
        .json::<Value>()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let res = server
        .get(&format!("/payments/{id}"))
        .add_header("Authorization", format!("Bearer {stranger_key}"))
        .await;
    res.assert_status(StatusCode::NOT_FOUND);
    assert_eq!(res.json::<Value>()["code"], "payment_not_found");

    // Identical to the response for an id that does not exist at all, so the
    // two cases are indistinguishable to a prober.
    let unknown = server
        .get("/payments/00000000-0000-4000-8000-000000000000")
        .add_header("Authorization", format!("Bearer {stranger_key}"))
        .await;
    assert_eq!(unknown.status_code(), res.status_code());
    assert_eq!(unknown.json::<Value>()["code"], res.json::<Value>()["code"]);
}

/// A supplied-but-invalid key is an error, not a silent downgrade to the
/// public view — otherwise a typo'd or revoked key looks like missing fields.
#[tokio::test]
async fn test_invalid_key_on_public_route_is_rejected() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;
    let id = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .json(&json!({ "amount": "10", "asset": "XLM" }))
        .await
        .json::<Value>()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let res = server
        .get(&format!("/payments/{id}"))
        .add_header("Authorization", "Bearer sg_deadbeef")
        .await;
    res.assert_status(StatusCode::UNAUTHORIZED);
    assert_eq!(res.json::<Value>()["code"], "unauthorized");
}

/// A revoked key must lose access to detail it previously had.
#[tokio::test]
async fn test_revoked_key_loses_access_to_payment_detail() {
    let server = test_server().await;
    let provisioned: Value = server
        .post("/merchants")
        .add_header("X-Admin-Secret", TEST_ADMIN_SECRET)
        .await
        .json();
    let merchant_id = provisioned["merchant_id"].as_str().unwrap();
    let old_key = provisioned["api_key"].as_str().unwrap().to_string();
    let old_key_id = provisioned["key_id"].as_str().unwrap();

    let id = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {old_key}"))
        .json(&json!({ "amount": "10", "asset": "XLM" }))
        .await
        .json::<Value>()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Issue a replacement so the last-key guard doesn't block revocation.
    server
        .post(&format!("/merchants/{merchant_id}/keys"))
        .add_header("X-Admin-Secret", TEST_ADMIN_SECRET)
        .await
        .assert_status(StatusCode::CREATED);
    server
        .delete(&format!("/merchants/{merchant_id}/keys/{old_key_id}"))
        .add_header("X-Admin-Secret", TEST_ADMIN_SECRET)
        .await
        .assert_status_ok();

    let res = server
        .get(&format!("/payments/{id}"))
        .add_header("Authorization", format!("Bearer {old_key}"))
        .await;
    res.assert_status(StatusCode::UNAUTHORIZED);
}

// ── API versioning (issue #121) ──────────────────────────────────────────

/// The versioned surface must be a complete, working mount — not a subset.
#[tokio::test]
async fn test_v1_routes_serve_the_full_api() {
    let server = test_server().await;

    // Provision through /v1.
    let res = server
        .post("/v1/merchants")
        .add_header("X-Admin-Secret", TEST_ADMIN_SECRET)
        .await;
    res.assert_status(StatusCode::CREATED);
    let body: Value = res.json();
    let key = body["api_key"].as_str().unwrap().to_string();
    let merchant_id = body["merchant_id"].as_str().unwrap().to_string();

    // Create, fetch and list through /v1.
    let created = server
        .post("/v1/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .json(&json!({ "amount": "10", "asset": "XLM" }))
        .await;
    created.assert_status(StatusCode::CREATED);
    let id = created.json::<Value>()["id"].as_str().unwrap().to_string();

    server
        .get(&format!("/v1/payments/{id}"))
        .add_header("Authorization", format!("Bearer {key}"))
        .await
        .assert_status_ok();
    server
        .get("/v1/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .await
        .assert_status_ok();
    server
        .get(&format!("/v1/payments/{id}/webhooks"))
        .add_header("Authorization", format!("Bearer {key}"))
        .await
        .assert_status_ok();
    server
        .get(&format!("/v1/merchants/{merchant_id}/keys"))
        .add_header("X-Admin-Secret", TEST_ADMIN_SECRET)
        .await
        .assert_status_ok();
}

/// Introducing versioning must not break existing integrators — that would be
/// the exact failure versioning exists to prevent. Unversioned paths keep
/// working, and say so via RFC 8594 / RFC 8288 headers rather than requiring
/// anyone to read release notes.
#[tokio::test]
async fn test_legacy_paths_still_work_and_advertise_their_successor() {
    let server = test_server().await;

    let res = server
        .post("/merchants")
        .add_header("X-Admin-Secret", TEST_ADMIN_SECRET)
        .await;
    res.assert_status(StatusCode::CREATED);

    assert_eq!(res.header("deprecation"), "true");
    let link = res.header("link");
    assert_eq!(
        link.to_str().unwrap(),
        "</v1/merchants>; rel=\"successor-version\"",
        "legacy responses must point at their /v1 equivalent"
    );
}

/// The canonical surface must not mark itself deprecated.
#[tokio::test]
async fn test_v1_responses_are_not_marked_deprecated() {
    let server = test_server().await;
    let res = server
        .post("/v1/merchants")
        .add_header("X-Admin-Secret", TEST_ADMIN_SECRET)
        .await;
    res.assert_status(StatusCode::CREATED);
    assert!(
        !res.headers().contains_key("deprecation"),
        "/v1 is canonical and must not advertise itself as deprecated"
    );
}

/// Operational endpoints are infrastructure, not contract. Versioning them
/// would break liveness probes and scrape configs on every API revision for
/// no benefit, so they stay where they are.
#[tokio::test]
async fn test_operational_endpoints_are_not_versioned() {
    let server = test_server().await;

    for path in ["/health", "/ready", "/metrics", "/dashboard"] {
        server.get(path).await.assert_status_ok();
    }
    for path in ["/v1/health", "/v1/ready", "/v1/metrics", "/v1/dashboard"] {
        server.get(path).await.assert_status(StatusCode::NOT_FOUND);
    }
}

/// Versioning must not open a hole around the auth layers.
#[tokio::test]
async fn test_v1_enforces_the_same_authorization() {
    let server = test_server().await;

    // Admin gate on merchant provisioning.
    server
        .post("/v1/merchants")
        .await
        .assert_status(StatusCode::UNAUTHORIZED);

    // Merchant auth on payments.
    server
        .get("/v1/payments")
        .await
        .assert_status(StatusCode::UNAUTHORIZED);
    server
        .post("/v1/payments")
        .json(&json!({ "amount": "10", "asset": "XLM" }))
        .await
        .assert_status(StatusCode::UNAUTHORIZED);
}

/// A payment created on one mount must be readable from the other — they are
/// the same API, not two parallel deployments.
#[tokio::test]
async fn test_both_mounts_share_the_same_data() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;

    let id = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .json(&json!({ "amount": "7", "asset": "XLM" }))
        .await
        .json::<Value>()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let via_v1 = server
        .get(&format!("/v1/payments/{id}"))
        .add_header("Authorization", format!("Bearer {key}"))
        .await;
    via_v1.assert_status_ok();
    assert_eq!(via_v1.json::<Value>()["id"], json!(id));
}
