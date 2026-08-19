//! Regression coverage for issue #305: state-changing operations must emit
//! a structured audit event carrying `audit=true`, an `action`, an `outcome`,
//! the acting merchant (or admin), a `source_ip`, and a `request_id`.
//!
//! `#[traced_test]` captures this test's `tracing` output into an in-memory,
//! per-test buffer; `logs_contain` searches the formatted event text.

use axum::http::StatusCode;
use axum_test::TestServer;
use serde_json::{json, Value};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::str::FromStr;
use std::sync::Arc;
use stellargate::{
    api,
    config::{Config, ListenerMode, WebhookPayloadDetail},
    db, AppState,
};
use tracing_test::traced_test;

const TEST_ADMIN_SECRET: &str = "test-admin-secret";

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
        webhook_retry_max_delay_ms: 60_000,
        allowed_webhook_schemes: vec!["https".into(), "http".into()],
        webhook_payload_detail: WebhookPayloadDetail::Minimal,
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
        rate_limit_requests_per_sec: 1000,
        db_pool_max_connections: 10,
        db_busy_timeout_ms: 5000,
        cors_allowed_origins: vec![],
        listener_mode: ListenerMode::Poll,
        webhook_allow_private_targets: false,
        admin_provisioning_secret: TEST_ADMIN_SECRET.into(),
        request_timeout_secs: 30,
        stream_idle_timeout_secs: 30,
        trusted_proxy_cidrs: vec![],
        max_payment_amount: Default::default(),
        min_payment_amount: Default::default(),
    }
}

async fn test_server() -> TestServer {
    let cfg = make_config();
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
    TestServer::new(router).unwrap()
}

/// Provisions a merchant and returns its API key.
async fn provision_merchant(server: &TestServer) -> String {
    let res = server
        .post("/merchants")
        .add_header("X-Admin-Secret", TEST_ADMIN_SECRET)
        .await;
    res.assert_status(StatusCode::CREATED);
    res.json::<Value>()["api_key"].as_str().unwrap().to_string()
}

/// Every field the README's "Audit events" schema promises must actually
/// appear on the `payment.create` event.
#[tokio::test]
#[traced_test]
async fn test_payment_create_emits_audit_event_with_full_schema() {
    let server = test_server().await;
    let key = provision_merchant(&server).await;

    let res = server
        .post("/payments")
        .add_header("Authorization", format!("Bearer {key}"))
        .json(&json!({ "amount": "10.00", "asset": "XLM" }))
        .await;
    res.assert_status(StatusCode::CREATED);

    assert!(logs_contain("audit"));
    assert!(logs_contain("action"));
    assert!(logs_contain("payment.create"));
    assert!(logs_contain("actor"));
    assert!(logs_contain("merchant"));
    assert!(logs_contain("outcome"));
    assert!(logs_contain("created"));
    assert!(logs_contain("payment_id"));
    assert!(logs_contain("amount"));
    assert!(logs_contain("asset"));
    assert!(logs_contain("source_ip"));
    assert!(logs_contain("request_id"));
}

/// Successful provisioning — the single most privileged operation — must be
/// logged, not only failures.
#[tokio::test]
#[traced_test]
async fn test_merchant_provision_emits_audit_event_on_success() {
    let server = test_server().await;
    let _ = provision_merchant(&server).await;

    assert!(logs_contain("audit"));
    assert!(logs_contain("merchant.provision"));
    assert!(logs_contain("actor"));
    assert!(logs_contain("admin"));
    assert!(logs_contain("outcome"));
    assert!(logs_contain("created"));
    assert!(logs_contain("source_ip"));
    assert!(logs_contain("request_id"));
}

/// A key revocation must carry a `source_ip`, which the original issue
/// called out as the one field still missing from an otherwise-logged event.
#[tokio::test]
#[traced_test]
async fn test_api_key_revoke_emits_audit_event_with_source_ip() {
    let server = test_server().await;
    let create_res = server
        .post("/merchants")
        .add_header("X-Admin-Secret", TEST_ADMIN_SECRET)
        .await;
    create_res.assert_status(StatusCode::CREATED);
    let body: Value = create_res.json();
    let merchant_id = body["merchant_id"].as_str().unwrap();
    let first_key_id = body["key_id"].as_str().unwrap().to_string();

    // Issue a second key so the first can be revoked without hitting
    // last_active_key.
    let issue_res = server
        .post(&format!("/merchants/{merchant_id}/keys"))
        .add_header("X-Admin-Secret", TEST_ADMIN_SECRET)
        .await;
    issue_res.assert_status(StatusCode::CREATED);

    let revoke_res = server
        .delete(&format!("/merchants/{merchant_id}/keys/{first_key_id}"))
        .add_header("X-Admin-Secret", TEST_ADMIN_SECRET)
        .await;
    revoke_res.assert_status(StatusCode::OK);

    assert!(logs_contain("audit"));
    assert!(logs_contain("api_key.revoke"));
    assert!(logs_contain("outcome"));
    assert!(logs_contain("revoked"));
    assert!(logs_contain("source_ip"));
    assert!(logs_contain("request_id"));
}
