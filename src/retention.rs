//! Retention worker.
//!
//! Two tables grow monotonically with traffic and have no natural bound:
//! `idempotency_keys` gains a row per guarded create, and `webhook_deliveries`
//! gains one per delivery attempt. Neither is ever removed, so on a
//! long-running deployment the disk is the only thing that eventually stops
//! them — and on the single-volume deployments this service targets, a full
//! disk takes the gateway down (issues #110, #111).
//!
//! This worker prunes both on an interval, in batches so no single statement
//! holds the write lock long enough to stall payment traffic.

use crate::supervise::TaskExit;
use crate::{db, AppState};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{debug, info, warn};

/// Upper bound on rows removed per table per cycle.
///
/// Without this, the first run against a large backlog would delete
/// indefinitely, monopolising the single writer. Whatever is left is picked up
/// next cycle, so a backlog drains over several passes instead of one long
/// stall.
const MAX_PER_CYCLE: u64 = 50_000;

pub async fn run_retention_worker(
    state: Arc<AppState>,
    mut shutdown: watch::Receiver<bool>,
) -> TaskExit {
    let interval = Duration::from_secs(state.config.retention_interval_secs.max(1));

    if state.config.webhook_delivery_retention_days <= 0
        && state.config.idempotency_retention_days <= 0
    {
        /* A legitimate configuration choice, not a fault — and now
        distinguishable from one (issue #317). */
        return TaskExit::DisabledByConfig(
            "both WEBHOOK_DELIVERY_RETENTION_DAYS and IDEMPOTENCY_RETENTION_DAYS are 0",
        );
    }

    info!(
        interval_secs = state.config.retention_interval_secs,
        webhook_delivery_retention_days = state.config.webhook_delivery_retention_days,
        idempotency_retention_days = state.config.idempotency_retention_days,
        "retention worker started"
    );

    loop {
        /* Wait first. Pruning is never urgent, and running it during startup
        would compete with the listeners replaying their cursor. */
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = shutdown.changed() => {
                info!("retention worker shutting down");
                return TaskExit::ShutdownRequested;
            }
        }

        match prune_once(&state).await {
            Ok((0, 0)) => debug!("retention: nothing to prune"),
            Ok((deliveries, keys)) => info!(
                webhook_deliveries = deliveries,
                idempotency_keys = keys,
                "retention pruned rows"
            ),
            Err(e) => warn!(error = %e, "retention cycle failed"),
        }
    }
}

/// Run one pruning cycle. Returns `(deliveries_removed, keys_removed)`.
///
/// A failure part-way through is not rolled back and does not need to be:
/// deleting expired rows is idempotent, so the next cycle simply continues
/// from wherever this one stopped.
pub async fn prune_once(state: &Arc<AppState>) -> anyhow::Result<(u64, u64)> {
    let mut deliveries = 0;
    if state.config.webhook_delivery_retention_days > 0 {
        deliveries = drain(MAX_PER_CYCLE, || {
            db::prune_webhook_deliveries(&state.pool, state.config.webhook_delivery_retention_days)
        })
        .await?;

        /* Aged-out failures nobody has acknowledged are kept rather than
        deleted (issue #319), so they are compacted instead: the payload is
        the bulk of the row and its only reader is redelivery, which is not
        something anyone does to a months-old failure. Counted separately from
        `deliveries` — these rows are retained, not removed. */
        let compacted = drain(MAX_PER_CYCLE, || {
            db::compact_stale_failed_deliveries(
                &state.pool,
                state.config.webhook_delivery_retention_days,
            )
        })
        .await?;
        if compacted > 0 {
            info!(
                compacted,
                "retention compacted unacknowledged failed deliveries to tombstones"
            );
        }
    }

    let mut keys = 0;
    if state.config.idempotency_retention_days > 0 {
        keys = drain(MAX_PER_CYCLE, || {
            db::prune_idempotency_keys(&state.pool, state.config.idempotency_retention_days)
        })
        .await?;
    }

    Ok((deliveries, keys))
}

/// Repeat a batched delete until it comes back short (nothing left to remove)
/// or the per-cycle cap is reached.
async fn drain<F, Fut>(cap: u64, mut batch: F) -> anyhow::Result<u64>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<u64>>,
{
    let mut total = 0;
    loop {
        let n = batch().await?;
        total += n;

        // A short batch means the table is drained.
        if n < db::PRUNE_BATCH as u64 || total >= cap {
            return Ok(total);
        }

        /* Yield between batches so a large backlog doesn't hold the writer
        back-to-back and starve payment writes. */
        tokio::task::yield_now().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AcceptedAsset, Config, ListenerMode};
    use sqlx::sqlite::SqlitePoolOptions;

    fn test_config(delivery_days: i64, idempotency_days: i64) -> Config {
        Config {
            port: 0,
            database_url: "sqlite::memory:".into(),
            network: "testnet".into(),
            horizon_url: String::new(),
            gateway_public: "UNCONFIGURED".into(),
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
            webhook_delivery_retention_days: delivery_days,
            idempotency_retention_days: idempotency_days,
            poll_interval_secs: 10,
            cursor_staleness_multiple: 3,
            payment_ttl_secs: 3600,
            expiry_batch_size: 1,
            rate_limit_requests_per_sec: 1000,
            db_pool_max_connections: 1,
            db_busy_timeout_ms: 5000,
            cors_allowed_origins: vec![],
            listener_mode: ListenerMode::Poll,
            webhook_allow_private_targets: false,
            admin_provisioning_secret: "admin".into(),
            request_timeout_secs: 30,
            stream_idle_timeout_secs: 30,
            trusted_proxy_cidrs: vec![],
            max_payment_amount: Default::default(),
            min_payment_amount: Default::default(),
        }
    }

    async fn state_with(cfg: Config) -> Arc<AppState> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        db::migrate(&pool).await.unwrap();
        Arc::new(AppState {
            pool,
            config: cfg,
            http: reqwest::Client::new(),
            webhook_http: reqwest::Client::new(),
            webhook_metrics: crate::metrics::WebhookMetrics::new(),
            auth_metrics: crate::metrics::AuthMetrics::new(),
            horizon_metrics: crate::metrics::HorizonMetrics::new(),
            task_health: crate::TaskHealth::new(),
        })
    }

    /// Insert one delivery row with an explicit age and acknowledgement state.
    async fn insert_delivery(
        state: &Arc<AppState>,
        id: &str,
        status: &str,
        age_days: i64,
        acknowledged: bool,
    ) {
        sqlx::query(
            "INSERT INTO webhook_deliveries
             (id, payment_id, url, payload, status, attempts, acknowledged_at, created_at)
             VALUES (?, 'p', 'https://e.example/h', '{\"event\":\"payment.completed\"}', ?, 1,
                     CASE WHEN ? THEN strftime('%Y-%m-%dT%H:%M:%SZ','now') ELSE NULL END,
                     strftime('%Y-%m-%dT%H:%M:%SZ','now', ?))",
        )
        .bind(id)
        .bind(status)
        .bind(acknowledged)
        .bind(format!("-{age_days} days"))
        .execute(&state.pool)
        .await
        .unwrap();
    }

    /// Aged deliveries go — *except* a terminal failure nobody has looked at.
    ///
    /// Deleting one of those destroys, on a timer, the only record that an
    /// event was permanently lost; the evidence for "we never received your
    /// webhook" expired precisely when it was most likely to be asked for
    /// (issue #319).
    #[tokio::test]
    async fn prunes_aged_deliveries_but_keeps_unacknowledged_failures() {
        let state = state_with(test_config(30, 7)).await;

        insert_delivery(&state, "old-delivered", "delivered", 60, false).await;
        insert_delivery(&state, "old-failed-unacked", "failed", 60, false).await;
        insert_delivery(&state, "old-failed-acked", "failed", 60, true).await;
        // Still owned by the redrive worker.
        insert_delivery(&state, "old-pending", "pending", 60, false).await;
        insert_delivery(&state, "new-delivered", "delivered", 1, false).await;

        let (deliveries, _) = prune_once(&state).await.unwrap();
        assert_eq!(
            deliveries, 2,
            "the delivered row and the acknowledged failure should go"
        );

        let left: Vec<String> = sqlx::query_scalar("SELECT id FROM webhook_deliveries ORDER BY id")
            .fetch_all(&state.pool)
            .await
            .unwrap();
        assert_eq!(
            left,
            vec!["new-delivered", "old-failed-unacked", "old-pending"]
        );
    }

    /// Exempting failures outright would trade one unbounded table for
    /// another, so a retained failure is compacted to a tombstone instead: the
    /// row survives, the payload does not.
    #[tokio::test]
    async fn retained_failures_are_compacted_to_tombstones() {
        let state = state_with(test_config(30, 7)).await;
        insert_delivery(&state, "old-failed", "failed", 60, false).await;
        insert_delivery(&state, "new-failed", "failed", 1, false).await;

        prune_once(&state).await.unwrap();

        let old: String = sqlx::query_scalar("SELECT payload FROM webhook_deliveries WHERE id = ?")
            .bind("old-failed")
            .fetch_one(&state.pool)
            .await
            .unwrap();
        assert_eq!(
            old, "",
            "an aged retained failure keeps the row, not the body"
        );

        let new: String = sqlx::query_scalar("SELECT payload FROM webhook_deliveries WHERE id = ?")
            .bind("new-failed")
            .fetch_one(&state.pool)
            .await
            .unwrap();
        assert!(
            new.contains("payment.completed"),
            "a failure inside the retention window is still redeliverable"
        );
    }

    /// Compaction must not rewrite the same row on every cycle.
    #[tokio::test]
    async fn compaction_is_idempotent() {
        let state = state_with(test_config(30, 7)).await;
        insert_delivery(&state, "old-failed", "failed", 60, false).await;

        let first = db::compact_stale_failed_deliveries(&state.pool, 30)
            .await
            .unwrap();
        let second = db::compact_stale_failed_deliveries(&state.pool, 30)
            .await
            .unwrap();

        assert_eq!(first, 1);
        assert_eq!(
            second, 0,
            "an already-compacted row must not be touched again"
        );
    }

    /// A pending delivery must never be pruned, however old: the redrive
    /// worker still owns it, and deleting it would silently drop a webhook the
    /// merchant is owed.
    #[tokio::test]
    async fn never_prunes_a_pending_delivery() {
        let state = state_with(test_config(1, 1)).await;
        sqlx::query(
            "INSERT INTO webhook_deliveries (id, payment_id, url, payload, status, created_at)
             VALUES ('stuck', 'p', 'https://e.example/h', '{}', 'pending',
                     strftime('%Y-%m-%dT%H:%M:%SZ','now','-3650 days'))",
        )
        .execute(&state.pool)
        .await
        .unwrap();

        prune_once(&state).await.unwrap();

        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM webhook_deliveries")
            .fetch_one(&state.pool)
            .await
            .unwrap();
        assert_eq!(n, 1, "a pending delivery must survive pruning");
    }

    /// Aged idempotency keys go; keys inside the retry window stay.
    #[tokio::test]
    async fn prunes_only_aged_idempotency_keys() {
        let state = state_with(test_config(30, 7)).await;

        for (key, age_days) in [("old", 30), ("fresh", 1)] {
            sqlx::query(
                "INSERT INTO idempotency_keys (merchant_id, idempotency_key, payment_id, created_at)
                 VALUES ('m', ?, 'p', strftime('%Y-%m-%dT%H:%M:%SZ','now', ?))",
            )
            .bind(key)
            .bind(format!("-{age_days} days"))
            .execute(&state.pool)
            .await
            .unwrap();
        }

        let (_, keys) = prune_once(&state).await.unwrap();
        assert_eq!(keys, 1);

        let left: Vec<String> = sqlx::query_scalar("SELECT idempotency_key FROM idempotency_keys")
            .fetch_all(&state.pool)
            .await
            .unwrap();
        assert_eq!(left, vec!["fresh"]);
    }

    /// A retention window of 0 means "keep forever" and must delete nothing.
    #[tokio::test]
    async fn zero_retention_disables_pruning() {
        let state = state_with(test_config(0, 0)).await;
        sqlx::query(
            "INSERT INTO webhook_deliveries (id, payment_id, url, payload, status, created_at)
             VALUES ('ancient', 'p', 'https://e.example/h', '{}', 'delivered',
                     strftime('%Y-%m-%dT%H:%M:%SZ','now','-3650 days'))",
        )
        .execute(&state.pool)
        .await
        .unwrap();

        assert_eq!(prune_once(&state).await.unwrap(), (0, 0));
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM webhook_deliveries")
            .fetch_one(&state.pool)
            .await
            .unwrap();
        assert_eq!(n, 1);
    }

    /// A backlog larger than one batch must drain across repeated statements
    /// rather than being cut off at the batch size.
    #[tokio::test]
    async fn drains_a_backlog_larger_than_one_batch() {
        let state = state_with(test_config(1, 1)).await;
        let total = db::PRUNE_BATCH + 137;

        for i in 0..total {
            sqlx::query(
                "INSERT INTO webhook_deliveries (id, payment_id, url, payload, status, created_at)
                 VALUES (?, 'p', 'https://e.example/h', '{}', 'delivered',
                         strftime('%Y-%m-%dT%H:%M:%SZ','now','-30 days'))",
            )
            .bind(format!("d{i}"))
            .execute(&state.pool)
            .await
            .unwrap();
        }

        let (deliveries, _) = prune_once(&state).await.unwrap();
        assert_eq!(deliveries, total as u64, "the whole backlog should drain");

        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM webhook_deliveries")
            .fetch_one(&state.pool)
            .await
            .unwrap();
        assert_eq!(n, 0);
    }
}
