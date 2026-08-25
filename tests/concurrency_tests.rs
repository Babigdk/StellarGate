//! Concurrency tests for stream + poller double-settle (issue #155).
//!
//! The Horizon stream listener and the interval poller both call
//! `reconcile_payment` independently.  When the same on-chain transaction
//! arrives via both paths at almost the same time — the common case in
//! `STELLAR_LISTENER_MODE=stream` — a naïve implementation would settle the
//! intent twice, fire two webhooks, and record two `webhook_deliveries` rows.
//!
//! The fix is a guarded `UPDATE … WHERE status IN ('pending','underpaid')`.
//! SQLite serialises concurrent writers, so exactly one UPDATE matches a row
//! still in a watchable state; the second UPDATE sees `rows_affected = 0` and
//! skips the webhook.
//!
//! These tests verify that guarantee end-to-end:
//!
//! 1. **double_concurrent_reconcile_settles_exactly_once** — two concurrent
//!    `reconcile_payment` calls for the same transaction resolve to a single
//!    `completed` status and a single `webhook_deliveries` row (i.e. a single
//!    webhook dispatch attempt).
//!
//! 2. **sequential_reconcile_is_idempotent** — a second `reconcile_payment`
//!    call for a payment that is already `completed` is a no-op (the
//!    tx_hash guard inside `reconcile_payment` catches this path, which
//!    covers poller re-scans after a stream settlement).

use std::sync::Arc;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::str::FromStr;
use stellargate::{
    config::{AcceptedAsset, Config, ListenerMode},
    db::{self, NewPayment},
    horizon::{reconcile_payment, HorizonPayment, TransactionRef},
    AppState,
};
use uuid::Uuid;
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, ResponseTemplate,
};

// ── helpers ──────────────────────────────────────────────────────────────────

/// Build a minimal in-memory SQLite pool with migrations applied.
///
/// The pool is deliberately multi-connection (`max_connections(5)`) because
/// these tests exist to prove the single-settlement guarantee under
/// *concurrent* reconciliation (issue #78) — that only means something if
/// concurrent tasks can genuinely land on different pooled connections. A
/// bare `sqlite::memory:` DSN gives each connection its own private,
/// unrelated database, so two "concurrent" writers could each own a private
/// copy of the row and the guarantee this suite exists to test would never
/// actually be exercised (issue #309). `cache=shared` (plus a unique name per
/// call, so parallel test binaries don't collide, and `min_connections(1)` to
/// keep the shared database alive for the pool's lifetime) makes every
/// connection in the pool talk to the same database, which is what makes
/// "concurrent" here mean what it says.
async fn memory_pool() -> db::Db {
    let dsn = format!("sqlite:file:{}?mode=memory&cache=shared", Uuid::new_v4());
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .min_connections(1)
        .connect_with(SqliteConnectOptions::from_str(&dsn).unwrap())
        .await
        .unwrap();
    db::migrate(&pool).await.unwrap();
    pool
}

/// Build an `AppState` wired to `pool` and pointing webhooks at `webhook_url`.
fn make_state(pool: db::Db, _webhook_url: Option<String>) -> Arc<AppState> {
    // Use a real XLM-only accepted assets list (no issuer to validate).
    let accepted_assets = vec![AcceptedAsset {
        code: "XLM".into(),
        issuer: None,
    }];

    Arc::new(AppState {
        pool,
        config: Config {
            port: 0,
            database_url: "sqlite::memory:".into(),
            network: "testnet".into(),
            horizon_url: "https://horizon.invalid".parse().unwrap(),
            // A real-looking Stellar strkey so Config::validate_addresses passes.
            gateway_public: "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5".into(),
            accepted_assets,
            webhook_secret: "a-very-long-and-secure-webhook-signing-secret-32-chars".into(),
            webhook_retry_attempts: 1,
            webhook_retry_delay_ms: 0,
            webhook_retry_max_delay_ms: 60_000,
            allowed_webhook_schemes: vec!["https".into(), "http".into()],
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
            listener_mode: ListenerMode::Stream,
            // Allow loopback targets so we can use wiremock's 127.0.0.1 server.
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

/// Create a pending payment intent and return its id, wiring `webhook_url`
/// into the row so webhook dispatch has a target.
async fn seed_pending_payment(pool: &db::Db, webhook_url: Option<&str>) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    db::create_payment(
        pool,
        NewPayment {
            id: &id,
            merchant_id: "test-merchant",
            destination_address: "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
            memo: "ABCD1234",
            amount: "10",
            asset: "XLM",
            asset_issuer: None,
            webhook_url,
            ttl_secs: 3600,
        },
    )
    .await
    .unwrap();
    id
}

/// A native-XLM Horizon payment that satisfies the seeded intent.
fn make_horizon_payment() -> HorizonPayment {
    HorizonPayment {
        kind: "payment".into(),
        amount: Some("10.0000000".into()),
        asset_type: Some("native".into()),
        asset_code: None,
        asset_issuer: None,
        to: Some("GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5".into()),
        transaction_hash: Some("CONCURRENT_TX_HASH_01".into()),
        transaction: Some(TransactionRef {
            memo: Some("ABCD1234".into()),
            memo_type: Some("text".into()),
            successful: Some(true),
        }),
        paging_token: Some("1".into()),
        created_at: None,
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// Two concurrent `reconcile_payment` calls for the same transaction must
/// settle the intent exactly once: `status = completed` and exactly one
/// `webhook_deliveries` row recorded.
///
/// This is the core regression test for the stream+poller double-settle race
/// described in issue #155.
#[tokio::test]
async fn double_concurrent_reconcile_settles_exactly_once() {
    // ── setup ──────────────────────────────────────────────────────────────
    // Spin up a local HTTP server that acts as the merchant webhook endpoint.
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/webhook"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock_server)
        .await;

    let webhook_url = format!("{}/webhook", mock_server.uri());
    let pool = memory_pool().await;
    let payment_id = seed_pending_payment(&pool, Some(&webhook_url)).await;
    let state = make_state(pool.clone(), Some(webhook_url));
    let hp = make_horizon_payment();

    // ── concurrent reconciliation ──────────────────────────────────────────
    // Simulate the stream and the poller both processing the same transaction
    // at the same time. tokio::join! runs both futures on the same thread by
    // default in a single-threaded test runtime, but SQLite's WAL serialises
    // the writes, so the outcome is deterministic regardless.
    let (r1, r2) = tokio::join!(
        reconcile_payment(&state, &hp),
        reconcile_payment(&state, &hp),
    );

    // Both calls must complete without error.
    let settled1 = r1.expect("first reconcile_payment call must not error");
    let settled2 = r2.expect("second reconcile_payment call must not error");

    // ── assertions ─────────────────────────────────────────────────────────
    // Exactly one of the two calls should have written a settlement; the other
    // should have been rejected by the status guard.
    assert_eq!(
        settled1 as u8 + settled2 as u8,
        1,
        "exactly one reconciliation should settle the intent; got settled1={settled1} settled2={settled2}"
    );

    // The payment must be in the `completed` state.
    let payment = db::get_payment(&pool, &payment_id)
        .await
        .unwrap()
        .expect("payment must still exist");
    assert_eq!(
        payment.status, "completed",
        "payment must be completed after concurrent reconciliation"
    );
    assert_eq!(
        payment.tx_hash.as_deref(),
        Some("CONCURRENT_TX_HASH_01"),
        "tx_hash must be recorded"
    );

    // Exactly one webhook delivery row must have been recorded (one dispatch,
    // not two). We give the async webhook dispatch a moment to complete.
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    let deliveries = db::list_webhook_deliveries(&pool, &payment_id)
        .await
        .unwrap();
    assert_eq!(
        deliveries.len(),
        1,
        "exactly one webhook delivery must be recorded; got {}",
        deliveries.len()
    );

    // The mock server must also have received exactly one POST.
    let received = mock_server.received_requests().await.unwrap();
    assert_eq!(
        received.len(),
        1,
        "exactly one HTTP webhook request must reach the mock server; got {}",
        received.len()
    );
}

/// A second `reconcile_payment` call for a transaction whose intent is already
/// `completed` must be a no-op: a completed intent is no longer watchable, so
/// `find_pending_by_memo` returns nothing and the record is ignored.
///
/// This covers the common poller-after-stream scenario: the stream settles the
/// intent, then the next poll cycle revisits the same Horizon record.
#[tokio::test]
async fn sequential_reconcile_is_idempotent() {
    // No real webhook target needed — we just want to confirm no duplicate
    // rows accumulate.
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/webhook"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock_server)
        .await;

    let webhook_url = format!("{}/webhook", mock_server.uri());
    let pool = memory_pool().await;
    let payment_id = seed_pending_payment(&pool, Some(&webhook_url)).await;
    let state = make_state(pool.clone(), Some(webhook_url));
    let hp = make_horizon_payment();

    // First call — should settle.
    let first = reconcile_payment(&state, &hp)
        .await
        .expect("first reconcile must not error");
    assert!(first, "first reconcile_payment call must settle the intent");

    // Second call for the same tx_hash — the completed intent is no longer
    // watchable, so this must be a no-op.
    let second = reconcile_payment(&state, &hp)
        .await
        .expect("second reconcile must not error");
    assert!(
        !second,
        "second reconcile_payment call for the same tx must be a no-op"
    );

    // Status is still completed and still exactly one delivery row.
    let payment = db::get_payment(&pool, &payment_id).await.unwrap().unwrap();
    assert_eq!(payment.status, "completed");

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    let deliveries = db::list_webhook_deliveries(&pool, &payment_id)
        .await
        .unwrap();
    assert_eq!(
        deliveries.len(),
        1,
        "idempotent re-reconcile must not produce a second webhook delivery row"
    );

    let received = mock_server.received_requests().await.unwrap();
    assert_eq!(
        received.len(),
        1,
        "idempotent re-reconcile must not send a second HTTP webhook; got {}",
        received.len()
    );
}

/// A native-XLM Horizon payment for the seeded intent (`memo = ABCD1234`) with
/// an explicit transaction hash and amount.
fn payment_with(tx_hash: &str, amount: &str) -> HorizonPayment {
    HorizonPayment {
        kind: "payment".into(),
        amount: Some(amount.into()),
        asset_type: Some("native".into()),
        asset_code: None,
        asset_issuer: None,
        to: Some("GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5".into()),
        transaction_hash: Some(tx_hash.into()),
        transaction: Some(TransactionRef {
            memo: Some("ABCD1234".into()),
            memo_type: Some("text".into()),
            successful: Some(true),
        }),
        paging_token: Some("1".into()),
        created_at: None,
    }
}

/// Production reconciliation must consume the same settlement decision as the
/// pure Horizon verdict tests. Exact, underpaid, and exact-top-up outcomes are
/// exercised by the tests above/below; this closes the remaining overpayment
/// branch through `reconcile_payment` itself (issue #225).
#[tokio::test]
async fn reconcile_payment_uses_shared_decision_for_overpayment() {
    let pool = memory_pool().await;
    let payment_id = seed_pending_payment(&pool, None).await;
    let state = make_state(pool.clone(), None);

    let overpayment = payment_with("TX_OVERPAID", "12.5000000");
    assert!(reconcile_payment(&state, &overpayment).await.unwrap());

    let payment = db::get_payment(&pool, &payment_id).await.unwrap().unwrap();
    assert_eq!(payment.status, "completed");
    assert_eq!(payment.tx_hash.as_deref(), Some("TX_OVERPAID"));
    assert_eq!(payment.paid_amount.as_deref(), Some("12.5"));
}

/// Re-processing any previously-seen transaction is a no-op regardless of the
/// order records arrive in — the cumulative ledger is the SUM over the
/// `processed_transactions` set, not the single most-recent `tx_hash`
/// (issue #119).
///
/// Reproduces the exact failure the old single-`tx_hash` dedup allowed: two
/// partial payments land, then the *first* one is re-seen. The old guard only
/// remembered the latest hash, so it would re-credit the earlier transaction.
#[tokio::test]
async fn reprocessing_past_transactions_never_double_credits() {
    let pool = memory_pool().await;
    // Seeded intent expects 10 XLM (see `seed_pending_payment`). No webhook URL,
    // so `settle` fires no outbound request.
    let payment_id = seed_pending_payment(&pool, None).await;
    let state = make_state(pool.clone(), None);

    let tx_a = payment_with("TX_A", "4.0000000");
    let tx_b = payment_with("TX_B", "3.0000000");
    let tx_c = payment_with("TX_C", "3.0000000");

    // Helper: assert the intent's persisted status and cumulative paid amount.
    async fn assert_state(pool: &db::Db, id: &str, status: &str, paid: &str) {
        let p = db::get_payment(pool, id).await.unwrap().unwrap();
        assert_eq!(p.status, status, "status");
        assert_eq!(p.paid_amount.as_deref(), Some(paid), "paid_amount");
    }

    // First partial: 4 of 10 — underpaid.
    reconcile_payment(&state, &tx_a).await.unwrap();
    assert_state(&pool, &payment_id, "underpaid", "4").await;

    // Second partial: +3 → 7 of 10 — still underpaid.
    reconcile_payment(&state, &tx_b).await.unwrap();
    assert_state(&pool, &payment_id, "underpaid", "7").await;

    // Re-seeing the FIRST transaction (the case the old dedup got wrong) must be
    // a no-op — not a re-credit to 11.
    assert!(!reconcile_payment(&state, &tx_a).await.unwrap());
    assert_state(&pool, &payment_id, "underpaid", "7").await;

    // Re-seeing the second, too, in the "wrong" order — still a no-op.
    assert!(!reconcile_payment(&state, &tx_b).await.unwrap());
    assert_state(&pool, &payment_id, "underpaid", "7").await;

    // A genuine third partial completes the intent exactly: 7 + 3 = 10.
    reconcile_payment(&state, &tx_c).await.unwrap();
    assert_state(&pool, &payment_id, "completed", "10").await;

    // Every past transaction re-seen after completion, in any order, is a no-op.
    for tx in [&tx_c, &tx_a, &tx_b] {
        assert!(!reconcile_payment(&state, tx).await.unwrap());
        assert_state(&pool, &payment_id, "completed", "10").await;
    }
}
