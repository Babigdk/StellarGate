//! Integration tests for `webhook::dispatch` against a mock HTTP server.
//!
//! Covers request construction (signature header), retry-then-success, and
//! exhausted-retries paths, asserting both the requests the mock server
//! received and the resulting `webhook_deliveries` row.

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use stellargate::{
    config::{AcceptedAsset, Config, ListenerMode},
    db,
    horizon::{self, HorizonPayment, TransactionRef},
    webhook, AppState,
};
use uuid::Uuid;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// A fresh, uniquely-named in-memory SQLite database with `cache=shared`, so
/// every connection the pool opens talks to the SAME database rather than
/// each getting its own private one, which a bare `sqlite::memory:` DSN
/// would do with this pool's default multi-connection size (issue #309).
fn shared_memory_dsn() -> String {
    format!("sqlite:file:{}?mode=memory&cache=shared", Uuid::new_v4())
}

fn make_config(webhook_secret: &str, retry_attempts: u32) -> Config {
    Config {
        port: 0,
        database_url: shared_memory_dsn(),
        network: "testnet".into(),
        horizon_url: String::new(),
        gateway_public: "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5".into(),
        accepted_assets: AcceptedAsset::default_list(),
        webhook_secret: webhook_secret.into(),
        webhook_retry_attempts: retry_attempts,
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
        cors_allowed_origins: vec![],
        listener_mode: ListenerMode::Poll,
        // These tests dispatch to a wiremock server on 127.0.0.1, which the
        // SSRF guard would otherwise block.
        webhook_allow_private_targets: true,
        rate_limit_requests_per_sec: 1000,
        db_pool_max_connections: 10,
        db_busy_timeout_ms: 5000,
        admin_provisioning_secret: String::new(),
        request_timeout_secs: 30,
        stream_idle_timeout_secs: 30,
        trusted_proxy_cidrs: vec![],
        max_payment_amount: Default::default(),
        min_payment_amount: Default::default(),
    }
}

async fn setup_state(cfg: Config) -> AppState {
    let pool = SqlitePoolOptions::new()
        // A shared-cache in-memory database is dropped once its last
        // connection closes — keep exactly one open for the pool's lifetime.
        .min_connections(1)
        .connect_with(SqliteConnectOptions::from_str(&cfg.database_url).unwrap())
        .await
        .unwrap();
    db::migrate(&pool).await.unwrap();
    AppState {
        pool,
        config: cfg,
        http: reqwest::Client::new(),
        webhook_http: reqwest::Client::new(),
        webhook_metrics: stellargate::metrics::WebhookMetrics::new(),
        auth_metrics: stellargate::metrics::AuthMetrics::new(),
        horizon_metrics: stellargate::metrics::HorizonMetrics::new(),
        task_health: stellargate::TaskHealth::new(),
    }
}

async fn create_test_payment(state: &AppState, webhook_url: &str) -> db::Payment {
    db::create_payment(
        &state.pool,
        db::NewPayment {
            id: "pay_test",
            merchant_id: "merchant1",
            destination_address: "GDESTINATION",
            memo: "MEMOTEST",
            amount: "10",
            asset: "XLM",
            asset_issuer: None,
            webhook_url: Some(webhook_url),
            ttl_secs: 3600,
        },
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn dispatch_delivers_successfully_with_valid_signature() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let cfg = make_config("test-secret", 3);
    let state = setup_state(cfg).await;
    let payment = create_test_payment(&state, &format!("{}/hook", server.uri())).await;

    webhook::dispatch(&state, &payment, "payment.completed", None).await;

    let received = server.received_requests().await.unwrap();
    assert_eq!(received.len(), 1);
    let req = &received[0];
    /* The signature now covers "{timestamp}.{body}", so verify using the
    timestamp the request advertises in its header. */
    let timestamp: i64 = req
        .headers
        .get("X-StellarGate-Timestamp")
        .expect("timestamp header must be present")
        .to_str()
        .unwrap()
        .parse()
        .expect("timestamp header must be an integer");
    let expected_sig = webhook::sign(&state.config.webhook_secret, timestamp, &req.body);
    assert_eq!(
        req.headers
            .get("X-StellarGate-Signature")
            .expect("signature header must be present")
            .to_str()
            .unwrap(),
        expected_sig
    );
    assert_eq!(
        req.headers.get("X-StellarGate-Event").unwrap(),
        "payment.completed"
    );

    let deliveries = db::list_webhook_deliveries(&state.pool, &payment.id)
        .await
        .unwrap();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].status, "delivered");
    assert_eq!(deliveries[0].attempts, 1);
}

#[tokio::test]
async fn dispatch_retries_on_5xx_then_succeeds() {
    let server = MockServer::start().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_responder = calls.clone();
    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(move |_req: &wiremock::Request| {
            let attempt = calls_for_responder.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                ResponseTemplate::new(500)
            } else {
                ResponseTemplate::new(200)
            }
        })
        .expect(2)
        .mount(&server)
        .await;

    let cfg = make_config("test-secret", 3);
    let state = setup_state(cfg).await;
    let payment = create_test_payment(&state, &format!("{}/hook", server.uri())).await;

    webhook::dispatch(&state, &payment, "payment.completed", None).await;

    assert_eq!(calls.load(Ordering::SeqCst), 2);

    let deliveries = db::list_webhook_deliveries(&state.pool, &payment.id)
        .await
        .unwrap();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].status, "delivered");
    assert_eq!(deliveries[0].attempts, 2);
}

#[tokio::test]
async fn dispatch_marks_failed_after_exhausting_retries() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(ResponseTemplate::new(500))
        .expect(3)
        .mount(&server)
        .await;

    let cfg = make_config("test-secret", 3);
    let state = setup_state(cfg).await;
    let payment = create_test_payment(&state, &format!("{}/hook", server.uri())).await;

    webhook::dispatch(&state, &payment, "payment.completed", None).await;

    let deliveries = db::list_webhook_deliveries(&state.pool, &payment.id)
        .await
        .unwrap();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].status, "failed");
    assert_eq!(deliveries[0].attempts, 3);
}

#[tokio::test]
async fn event_field_in_body_matches_header_and_is_covered_by_signature() {
    /* Security regression test for issue #160.
     *
     * The X-StellarGate-Event header is NOT covered by the HMAC signature.
     * Receivers MUST route on the `event` field inside the verified JSON body,
     * not on the header. This test asserts:
     *
     * 1. The signed body contains the `event` field.
     * 2. The header value mirrors the body's event field (i.e. they agree when
     *    the request has not been tampered with).
     * 3. The HMAC signature is computed over the body (which includes `event`),
     *    so altering the header would not invalidate the signature — confirming
     *    the header is informational only.
     */
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let cfg = make_config("test-secret", 1);
    let state = setup_state(cfg).await;
    let payment = create_test_payment(&state, &format!("{}/hook", server.uri())).await;

    webhook::dispatch(&state, &payment, "payment.completed", None).await;

    let received = server.received_requests().await.unwrap();
    assert_eq!(received.len(), 1);
    let req = &received[0];

    // 1. The header is present.
    let header_event = req
        .headers
        .get("X-StellarGate-Event")
        .expect("X-StellarGate-Event header must be present")
        .to_str()
        .unwrap();

    // 2. The body contains the `event` field.
    let body: serde_json::Value =
        serde_json::from_slice(&req.body).expect("body must be valid JSON");
    let body_event = body["event"]
        .as_str()
        .expect("body must contain an `event` field");

    // 3. Header and body agree (no tampering in this happy-path test).
    assert_eq!(
        header_event, body_event,
        "X-StellarGate-Event header must mirror the body event field"
    );
    assert_eq!(body_event, "payment.completed");

    // 4. The signature is valid over the body (which contains `event`),
    //    confirming the event type is authenticated through the body, not the header.
    let timestamp: i64 = req
        .headers
        .get("X-StellarGate-Timestamp")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    let expected_sig = webhook::sign(&state.config.webhook_secret, timestamp, &req.body);
    assert_eq!(
        req.headers
            .get("X-StellarGate-Signature")
            .unwrap()
            .to_str()
            .unwrap(),
        expected_sig,
        "signature must be valid over the body (which contains the event field)"
    );
}

/// A native-XLM Horizon payment record matching `create_test_payment`'s
/// default amount ("10") and destination ("GDESTINATION").
fn make_horizon_payment(tx_hash: &str) -> HorizonPayment {
    HorizonPayment {
        kind: "payment".into(),
        amount: Some("10.0000000".into()),
        asset_type: Some("native".into()),
        asset_code: None,
        asset_issuer: None,
        to: Some("GDESTINATION".into()),
        transaction_hash: Some(tx_hash.into()),
        transaction: Some(TransactionRef {
            memo: Some("MEMOTEST".into()),
            memo_type: Some("text".into()),
            successful: Some(true),
        }),
        paging_token: Some("1".into()),
        created_at: None,
    }
}

/// TODO.md Step 7: "settle not blocked by slow webhook".
///
/// `reconcile_payment` commits the payment's terminal status to the database
/// *before* it calls `webhook::dispatch`, so the intent is durably settled
/// even while the (slow) webhook HTTP call is still in flight. This test
/// proves that externally: it drives reconciliation against a webhook
/// endpoint that hangs for several seconds, and asserts the payment is
/// already `completed` — visible to any other reader of the database — well
/// before that delay elapses.
#[tokio::test]
async fn settle_not_blocked_by_slow_webhook() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(5)))
        .expect(1)
        .mount(&server)
        .await;

    let mut cfg = make_config("test-secret", 1);
    // Long enough that the client doesn't time out before the delayed
    // response arrives; the point of this test is that settlement doesn't
    // wait for it, not that the request itself fails.
    cfg.webhook_timeout_secs = 30;
    let state = Arc::new(setup_state(cfg).await);
    let payment = create_test_payment(&state, &format!("{}/hook", server.uri())).await;
    let hp = make_horizon_payment("SLOW_WEBHOOK_TX");

    let task_state = state.clone();
    let handle = tokio::spawn(async move { horizon::reconcile_payment(&task_state, &hp).await });

    // Give reconciliation enough time to settle the payment in the database,
    // but nowhere near enough for the 5s-delayed webhook response to land.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mid_flight = db::get_payment(&state.pool, &payment.id)
        .await
        .unwrap()
        .expect("payment must still exist");
    assert_eq!(
        mid_flight.status, "completed",
        "the payment must be settled well before the slow webhook responds"
    );
    assert!(
        !handle.is_finished(),
        "reconciliation should still be waiting on the slow webhook response"
    );

    let settled = handle
        .await
        .expect("reconcile task must not panic")
        .expect("reconcile_payment must not error");
    assert!(
        settled,
        "reconcile_payment must report that it settled the intent"
    );

    let received = server.received_requests().await.unwrap();
    assert_eq!(
        received.len(),
        1,
        "the slow webhook must still have been attempted"
    );
}

/// TODO.md Step 7: "failed/pending delivery is retried after restart".
///
/// Simulates a process that recorded a webhook delivery row and crashed
/// before ever attempting the HTTP POST (the row is left `pending`,
/// `attempts = 0`, exactly as `dispatch()` leaves it between insert and its
/// first send). A restarted process's redrive worker runs one pass
/// immediately on startup (`run_redrive_worker`) — this test calls the same
/// `redrive_once` it would call, and asserts the stuck delivery is picked up,
/// re-sent, and marked `delivered`.
#[tokio::test]
async fn pending_delivery_is_redriven_after_restart() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let mut cfg = make_config("test-secret", 3);
    // No need to wait out the eligibility grace window in a test.
    cfg.webhook_redrive_grace_secs = 0;
    let state = setup_state(cfg).await;
    let webhook_url = format!("{}/hook", server.uri());
    let payment = create_test_payment(&state, &webhook_url).await;

    let payload = webhook::build_payload(&payment, "payment.completed", None);
    db::save_webhook_delivery(
        &state.pool,
        "stuck-delivery",
        &payment.id,
        &webhook_url,
        &serde_json::to_string(&payload).unwrap(),
        "payment.completed",
    )
    .await
    .unwrap();

    let state = Arc::new(state);
    let redriven = webhook::redrive_once(&state).await;
    assert_eq!(
        redriven, 1,
        "the stuck delivery must be picked up for redrive"
    );

    let received = server.received_requests().await.unwrap();
    assert_eq!(
        received.len(),
        1,
        "the redrive worker must actually re-send the stuck delivery"
    );

    let delivery = db::get_webhook_delivery(&state.pool, "stuck-delivery")
        .await
        .unwrap()
        .expect("delivery row must still exist");
    assert_eq!(delivery.status, "delivered");
    assert_eq!(delivery.attempts, 1);
}

/// A delivery that already exhausted its inline retries (`status = failed`)
/// is still below the redrive attempt cap, so the worker gives it another
/// chance instead of abandoning it permanently after a transient outage.
#[tokio::test]
async fn failed_delivery_below_max_attempts_is_retried_by_redrive() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let mut cfg = make_config("test-secret", 3);
    cfg.webhook_redrive_grace_secs = 0;
    cfg.webhook_redrive_max_attempts = 8;
    let state = setup_state(cfg).await;
    let webhook_url = format!("{}/hook", server.uri());
    let payment = create_test_payment(&state, &webhook_url).await;

    let payload = webhook::build_payload(&payment, "payment.completed", None);
    db::save_webhook_delivery(
        &state.pool,
        "exhausted-delivery",
        &payment.id,
        &webhook_url,
        &serde_json::to_string(&payload).unwrap(),
        "payment.completed",
    )
    .await
    .unwrap();
    // Mark it failed with fewer attempts than the redrive cap, as dispatch()
    // would after exhausting its own (independently configured) retry count.
    db::update_webhook_delivery(&state.pool, "exhausted-delivery", "failed", 3)
        .await
        .unwrap();

    let state = Arc::new(state);
    let redriven = webhook::redrive_once(&state).await;
    assert_eq!(redriven, 1);

    let delivery = db::get_webhook_delivery(&state.pool, "exhausted-delivery")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivery.status, "delivered");
    assert_eq!(delivery.attempts, 4);
}

/// A delivery still within its grace window (i.e. a `dispatch()` call could
/// plausibly still be in flight for it) must not be touched by the redrive
/// worker — otherwise a slow-but-live delivery would be redriven concurrently
/// with its own inline attempt, double-sending the webhook.
#[tokio::test]
async fn redrive_skips_deliveries_still_within_grace_window() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;

    let mut cfg = make_config("test-secret", 3);
    cfg.webhook_redrive_grace_secs = 3600; // freshly-inserted rows stay ineligible
    let state = setup_state(cfg).await;
    let webhook_url = format!("{}/hook", server.uri());
    let payment = create_test_payment(&state, &webhook_url).await;

    let payload = webhook::build_payload(&payment, "payment.completed", None);
    db::save_webhook_delivery(
        &state.pool,
        "fresh-delivery",
        &payment.id,
        &webhook_url,
        &serde_json::to_string(&payload).unwrap(),
        "payment.completed",
    )
    .await
    .unwrap();

    let state = Arc::new(state);
    let redriven = webhook::redrive_once(&state).await;
    assert_eq!(redriven, 0, "a fresh delivery must not be redriven yet");

    let delivery = db::get_webhook_delivery(&state.pool, "fresh-delivery")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivery.status, "pending");
    assert_eq!(delivery.attempts, 0);
}

/// Once a delivery reaches the redrive attempt cap it is left `failed`
/// permanently rather than retried forever.
#[tokio::test]
async fn redrive_marks_delivery_failed_after_exhausting_max_attempts() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .mount(&server)
        .await;

    let mut cfg = make_config("test-secret", 3);
    cfg.webhook_redrive_grace_secs = 0;
    cfg.webhook_redrive_max_attempts = 5;
    let state = setup_state(cfg).await;
    let webhook_url = format!("{}/hook", server.uri());
    let payment = create_test_payment(&state, &webhook_url).await;

    let payload = webhook::build_payload(&payment, "payment.completed", None);
    db::save_webhook_delivery(
        &state.pool,
        "last-chance-delivery",
        &payment.id,
        &webhook_url,
        &serde_json::to_string(&payload).unwrap(),
        "payment.completed",
    )
    .await
    .unwrap();
    db::update_webhook_delivery(&state.pool, "last-chance-delivery", "failed", 4)
        .await
        .unwrap();

    let state = Arc::new(state);
    let redriven = webhook::redrive_once(&state).await;
    assert_eq!(redriven, 1);

    let delivery = db::get_webhook_delivery(&state.pool, "last-chance-delivery")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivery.status, "failed");
    assert_eq!(delivery.attempts, 5);
}

// ── Terminal-failure metric coverage (issues #319, #233) ─────────────────────

/// A webhook target that resolves into a blocked range is a *permanent*
/// failure — no retry will ever make it succeed — but it was not counted as
/// one. That left a whole class of terminal failure invisible to
/// `stellargate_webhook_deliveries_total{outcome="failed"}` and to any alert
/// built on it, which is precisely the alerting the dead-letter view relies on
/// to tell an operator that failures are happening at all.
#[tokio::test]
async fn ssrf_blocked_dispatch_is_counted_as_a_terminal_failure() {
    let mut cfg = make_config("secret", 3);
    // Turn the guard back on so a loopback target is rejected.
    cfg.webhook_allow_private_targets = false;
    let state = setup_state(cfg).await;

    // Resolves to loopback, so the SSRF guard rejects it before any request.
    let payment = create_test_payment(&state, "http://127.0.0.1:1/hook").await;
    webhook::dispatch(&state, &payment, "payment.completed", None).await;

    assert_eq!(
        state.webhook_metrics.failed(),
        1,
        "an SSRF-blocked delivery is terminal and must increment the failure counter"
    );

    let deliveries = db::list_webhook_deliveries(&state.pool, &payment.id)
        .await
        .unwrap();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].status, "failed");
}

/// The same gap existed on the redrive path.
#[tokio::test]
async fn ssrf_blocked_redrive_is_counted_as_a_terminal_failure() {
    let mut cfg = make_config("secret", 3);
    cfg.webhook_allow_private_targets = false;
    // The row is created moments before the redrive pass, so the default grace
    // window would (correctly) treat it as possibly still in flight.
    cfg.webhook_redrive_grace_secs = 0;
    let state = setup_state(cfg).await;

    let payment = create_test_payment(&state, "http://127.0.0.1:1/hook").await;
    db::save_webhook_delivery(
        &state.pool,
        "blocked-delivery",
        &payment.id,
        "http://127.0.0.1:1/hook",
        r#"{"event":"payment.completed"}"#,
        "payment.completed",
    )
    .await
    .unwrap();

    let state = Arc::new(state);
    assert_eq!(webhook::redrive_once(&state).await, 1);
    assert_eq!(
        state.webhook_metrics.failed(),
        1,
        "an SSRF-blocked redrive is terminal and must be counted"
    );
}
