//! Startup trustline check (issue #116).
//!
//! `horizon::check_trustlines` queries Horizon for the gateway account's
//! balances and surfaces any accepted asset the account has no trustline for —
//! such assets would otherwise mint unpayable intents. These tests drive it
//! against a mock Horizon endpoint.

use std::str::FromStr;
use std::sync::Arc;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use stellargate::{
    config::{AcceptedAsset, Config, ListenerMode},
    db, horizon, AppState,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const GATEWAY: &str = "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5";
const USDC_ISSUER: &str = "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN";

/// Build an `AppState` whose Horizon client points at `horizon_url` and which
/// accepts XLM plus USDC issued by `USDC_ISSUER`.
async fn make_state(horizon_url: String) -> Arc<AppState> {
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
            trusted_proxy_cidrs: vec![],
        },
        http: reqwest::Client::new(),
        webhook_http: reqwest::Client::new(),
        webhook_metrics: stellargate::metrics::WebhookMetrics::new(),
        auth_metrics: stellargate::metrics::AuthMetrics::new(),
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
