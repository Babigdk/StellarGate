//! Expiry sweeper: retiring pending payment intents that were never paid.
//!
//! Every intent is created with an `expires_at` timestamp (`created_at +
//! PAYMENT_TTL_SECS`). A background task periodically transitions any `pending`
//! intent past that deadline to the terminal `expired` status and fires a
//! `payment.expired` webhook, so the Horizon poller stops watching dead intents
//! and merchants get a definite signal that the intent will never settle.
//!
//! Expiry is purely time- and database-driven, so the sweeper runs even when no
//! Stellar gateway is configured (unlike the Horizon poller).

use crate::supervise::TaskExit;
use crate::{db, webhook, AppState};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{debug, info, warn};

/// The webhook event emitted when an intent is swept to `expired`.
const EXPIRED_EVENT: &str = "payment.expired";

/// Run one sweep: expire up to `expiry_batch_size` overdue pending intents and
/// fire each one's webhook. Returns how many intents were expired. Safe to call
/// repeatedly — an intent is transitioned at most once, and a backlog larger
/// than the batch drains over several sweeps instead of one long write lock.
pub async fn sweep_once(state: &Arc<AppState>) -> anyhow::Result<usize> {
    let expired = db::expire_overdue(&state.pool, state.config.expiry_batch_size).await?;
    for payment in &expired {
        info!(payment_id = %payment.id, "payment intent expired");
        webhook::dispatch(state, payment, EXPIRED_EVENT, None).await;
    }
    Ok(expired.len())
}

/// Background loop that sweeps expired intents on the configured poll interval
/// until the process shuts down.
pub async fn run_sweeper(state: Arc<AppState>, mut shutdown: watch::Receiver<bool>) -> TaskExit {
    let interval = Duration::from_secs(state.config.poll_interval_secs.max(1));
    info!(
        ttl_secs = state.config.payment_ttl_secs,
        interval_secs = state.config.poll_interval_secs,
        "expiry sweeper started"
    );

    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = shutdown.changed() => {
                info!("expiry sweeper shutting down");
                return TaskExit::ShutdownRequested;
            }
        }
        match sweep_once(&state).await {
            Ok(0) => debug!("sweep: nothing to expire"),
            Ok(n) => info!(expired = n, "sweep expired payments"),
            Err(e) => warn!(error = %e, "sweep cycle failed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AcceptedAsset, Config, ListenerMode};
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use std::str::FromStr;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_config(webhook_url_allowed: bool) -> Config {
        Config {
            port: 0,
            database_url: "sqlite::memory:".into(),
            network: "testnet".into(),
            horizon_url: String::new(),
            gateway_public: "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5".into(),
            accepted_assets: AcceptedAsset::default_list(),
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
            rate_limit_requests_per_sec: 1000,
            db_pool_max_connections: 5,
            db_busy_timeout_ms: 5000,
            cors_allowed_origins: vec![],
            listener_mode: ListenerMode::Poll,
            // Allow loopback targets so tests can dispatch to a wiremock server.
            webhook_allow_private_targets: webhook_url_allowed,
            admin_provisioning_secret: String::new(),
            request_timeout_secs: 30,
            trusted_proxy_cidrs: vec![],
            max_payment_amount: Default::default(),
            min_payment_amount: Default::default(),
        }
    }

    async fn memory_state(cfg: Config) -> AppState {
        let pool = SqlitePoolOptions::new()
            .connect_with(
                SqliteConnectOptions::from_str(&cfg.database_url)
                    .unwrap()
                    .create_if_missing(true),
            )
            .await
            .unwrap();
        db::migrate(&pool).await.unwrap();
        AppState {
            pool,
            config: cfg,
            http: reqwest::Client::new(),
            webhook_http: reqwest::Client::new(),
            webhook_metrics: crate::metrics::WebhookMetrics::new(),
            auth_metrics: crate::metrics::AuthMetrics::new(),
            horizon_metrics: crate::metrics::HorizonMetrics::new(),
            task_health: crate::TaskHealth::new(),
        }
    }

    /// End-to-end coverage for the `payment.expired` webhook: an overdue
    /// intent swept by `sweep_once` must fire a real, correctly-signed HTTP
    /// POST to the merchant's webhook endpoint (issue #156 — this path had no
    /// test beyond the sweeper's own DB-level unit tests).
    #[tokio::test]
    async fn sweep_fires_payment_expired_webhook_end_to_end() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/hook"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let state = Arc::new(memory_state(test_config(true)).await);
        let webhook_url = format!("{}/hook", server.uri());
        let payment = db::create_payment(
            &state.pool,
            db::NewPayment {
                id: "pay_expiring",
                merchant_id: "merchant1",
                destination_address: "GDESTINATION",
                memo: "EXPMEMO",
                amount: "10",
                asset: "XLM",
                asset_issuer: None,
                webhook_url: Some(&webhook_url),
                // Already overdue: sweep_once must expire it immediately.
                ttl_secs: -10,
            },
        )
        .await
        .unwrap();

        let expired_count = sweep_once(&state).await.unwrap();
        assert_eq!(expired_count, 1, "the overdue intent must be swept");

        let updated = db::get_payment(&state.pool, &payment.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(updated.status, "expired");

        let received = server.received_requests().await.unwrap();
        assert_eq!(received.len(), 1, "exactly one webhook POST must be sent");
        let req = &received[0];
        assert_eq!(
            req.headers.get("X-StellarGate-Event").unwrap(),
            EXPIRED_EVENT
        );

        let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
        assert_eq!(body["event"], EXPIRED_EVENT);
        assert_eq!(body["payment_id"], payment.id);
        assert_eq!(body["status"], "expired");

        // The signature is valid over the body the mock server actually received.
        let timestamp: i64 = req
            .headers
            .get("X-StellarGate-Timestamp")
            .unwrap()
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        let expected_sig = crate::webhook::sign(&state.config.webhook_secret, timestamp, &req.body);
        assert_eq!(
            req.headers
                .get("X-StellarGate-Signature")
                .unwrap()
                .to_str()
                .unwrap(),
            expected_sig
        );

        let deliveries = db::list_webhook_deliveries(&state.pool, &payment.id)
            .await
            .unwrap();
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].status, "delivered");
        assert_eq!(deliveries[0].event(), EXPIRED_EVENT);
    }

    /// A second sweep after the webhook already fired must not re-expire (and
    /// therefore not re-notify) the same intent.
    #[tokio::test]
    async fn sweep_is_idempotent_and_does_not_redeliver() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/hook"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let state = Arc::new(memory_state(test_config(true)).await);
        let webhook_url = format!("{}/hook", server.uri());
        db::create_payment(
            &state.pool,
            db::NewPayment {
                id: "pay_expiring_once",
                merchant_id: "merchant1",
                destination_address: "GDESTINATION",
                memo: "EXPMEMOONCE",
                amount: "10",
                asset: "XLM",
                asset_issuer: None,
                webhook_url: Some(&webhook_url),
                ttl_secs: -10,
            },
        )
        .await
        .unwrap();

        assert_eq!(sweep_once(&state).await.unwrap(), 1);
        assert_eq!(sweep_once(&state).await.unwrap(), 0);

        let received = server.received_requests().await.unwrap();
        assert_eq!(
            received.len(),
            1,
            "a second sweep must not re-fire the expiry webhook"
        );
    }
}
