//! First-run Horizon cursor baselining (issue #311).
//!
//! `starting_cursor` used to baseline a brand-new deployment at the account's
//! single most recent payment (`order=desc&limit=1`), adopting its paging
//! token as the floor for forward polling. That is correct only if no payment
//! relevant to this gateway predates it — which fails whenever:
//!
//! - the account is **reused** (a redeploy after losing the volume, a
//!   migration between hosts) and already has an open intent whose payment
//!   sits behind the account's newer, unrelated traffic, or
//! - a payment lands in the narrow **race window** between the baselining
//!   query and the first forward poll and happens to sort at or below the
//!   single-record baseline.
//!
//! Neither produces an error — the intent just stays `pending` until the
//! sweeper expires it. These tests drive `poll_once` against a mock Horizon
//! that reproduces both shapes and assert the payment is still reconciled.

use std::str::FromStr;
use std::sync::Arc;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use stellargate::{
    config::{AcceptedAsset, AmountLimit, Config, ListenerMode},
    db, horizon, AppState,
};
use wiremock::{Mock, MockServer, ResponseTemplate};

const GATEWAY: &str = "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5";

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
            accepted_assets: vec![AcceptedAsset {
                code: "XLM".into(),
                issuer: None,
            }],
            webhook_secret: "a-very-long-and-secure-webhook-signing-secret-32-chars".into(),
            webhook_retry_attempts: 1,
            webhook_retry_delay_ms: 0,
            webhook_retry_max_delay_ms: 60_000,
            allowed_webhook_schemes: vec!["https".into()],
            stream_idle_timeout_secs: 30,
            max_payment_amount: AmountLimit::default(),
            min_payment_amount: AmountLimit::default(),
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
        task_health: stellargate::TaskHealth::new(),
    })
}

/// A pending payment intent, inserted with its `created_at` backdated to
/// `token`'s timestamp (see [`ts`]) so it can sit arbitrarily far behind
/// other, newer Horizon activity — reproducing "this account already has
/// history" instead of "the intent was just created".
async fn create_backdated_pending(
    state: &AppState,
    memo: &str,
    amount: &str,
    token: u32,
) -> db::Payment {
    let payment = db::create_payment(
        &state.pool,
        db::NewPayment {
            id: &format!("pay_{memo}"),
            merchant_id: "merchant1",
            destination_address: GATEWAY,
            memo,
            amount,
            asset: "XLM",
            asset_issuer: None,
            webhook_url: None,
            ttl_secs: 365 * 24 * 3600, // long TTL: the sweeper must not race the test
        },
    )
    .await
    .unwrap();

    sqlx::query("UPDATE payments SET created_at = ? WHERE id = ?")
        .bind(ts(token))
        .bind(&payment.id)
        .execute(&state.pool)
        .await
        .unwrap();

    db::find_pending_by_memo(&state.pool, memo)
        .await
        .unwrap()
        .expect("payment must still be pending immediately after creation")
}

/// A stable, strictly-increasing-with-`token` RFC 3339 timestamp, so higher
/// tokens sort later both numerically and lexicographically — matching how
/// `created_at` comparisons work in `starting_cursor`.
fn ts(token: u32) -> String {
    let secs = token % 60;
    let mins = (token / 60) % 60;
    let hours = token / 3600;
    format!("2024-01-01T{hours:02}:{mins:02}:{secs:02}Z")
}

/// One synthetic Horizon payment record. Only `MATCH_MEMO` carries a memo
/// that matches a pending intent; every other token is noise with no memo, so
/// it can never accidentally settle anything.
fn record_json(token: u32, memo: Option<&str>) -> serde_json::Value {
    let mut tx = serde_json::json!({ "successful": true });
    if let Some(m) = memo {
        tx["memo"] = serde_json::json!(m);
        tx["memo_type"] = serde_json::json!("text");
    }
    serde_json::json!({
        "type": "payment",
        "amount": "5.0000000",
        "asset_type": "native",
        "to": GATEWAY,
        "transaction_hash": format!("TX{token}"),
        "transaction": tx,
        "paging_token": token.to_string(),
        "created_at": ts(token),
    })
}

/// Mount a mock Horizon `/payments` endpoint serving `total` synthetic
/// records (tokens `1..=total`, newest = `total`), paginating both
/// `order=desc` (backward, for baselining) and `order=asc` (forward, for the
/// catch-up poll) with a fixed page size, honoring the `cursor` query param
/// exactly as Horizon does. Token `matched_token`, if given, carries
/// `matched_memo`; every other record is memo-less noise.
async fn mount_synthetic_history(
    server: &MockServer,
    total: u32,
    page_size: u32,
    matched_token: Option<u32>,
    matched_memo: &'static str,
) {
    Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path(format!(
            "/accounts/{GATEWAY}/payments"
        )))
        .respond_with(move |req: &wiremock::Request| {
            let query: std::collections::HashMap<_, _> = req.url.query_pairs().collect();
            let desc = query.get("order").map(|o| o.as_ref()) == Some("desc");
            let cursor: Option<u32> = query.get("cursor").and_then(|c| c.parse().ok());

            let tokens: Vec<u32> = if desc {
                let start = cursor.unwrap_or(total + 1);
                let hi = start.saturating_sub(1);
                let lo = hi.saturating_sub(page_size).saturating_add(1).max(1);
                if hi == 0 {
                    vec![]
                } else {
                    (lo..=hi).rev().collect()
                }
            } else {
                let start = cursor.unwrap_or(0);
                ((start + 1)..=total).take(page_size as usize).collect()
            };

            let records: Vec<_> = tokens
                .iter()
                .map(|&t| record_json(t, (Some(t) == matched_token).then_some(matched_memo)))
                .collect();
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({ "_embedded": { "records": records } }))
        })
        .mount(server)
        .await;
}

/// **Reused account**: an open intent was created long before 400 unrelated,
/// newer payments landed on the same account (more than one `PAGE_LIMIT`
/// page's worth of noise). The naive "single most recent payment" baseline
/// would adopt a token far newer than the intent and never see its payment.
/// The multi-page backward walk must still find and settle it.
#[tokio::test]
async fn reused_account_old_open_intent_is_not_skipped() {
    const TOTAL: u32 = 450;
    const OLD_TOKEN: u32 = 30; // deep behind more than 2 pages of noise
    const MEMO: &str = "OLDMEMO1";

    let server = MockServer::start().await;
    mount_synthetic_history(&server, TOTAL, 200, Some(OLD_TOKEN), MEMO).await;

    let state = make_state(server.uri()).await;
    // Backdate the intent to just before the matched record's timestamp, so
    // a boundary baseline (stopping exactly at the intent's creation time)
    // would still have to include the payment itself.
    create_backdated_pending(&state, MEMO, "5", OLD_TOKEN.saturating_sub(1)).await;

    let settled = horizon::poll_once(&state).await.unwrap();
    assert_eq!(
        settled, 1,
        "the old intent's payment must be found and settled despite 400+ \
         newer, unrelated payments on the same account"
    );

    let payment = db::find_pending_by_memo(&state.pool, MEMO).await.unwrap();
    assert!(
        payment.is_none(),
        "the intent must no longer be pending once settled"
    );
}

/// **Startup race**: the intent's matching payment is not the account's
/// single most recent record — a couple of newer, unrelated payments exist
/// ahead of it, all within the very first backward page. The old exact
/// (`order=desc&limit=1`) baseline would have adopted a token past the
/// matched payment and silently skipped it forever.
#[tokio::test]
async fn payment_just_behind_the_tip_is_not_skipped() {
    const TOTAL: u32 = 10;
    const MATCHED_TOKEN: u32 = 8; // two newer, unrelated records exist above it
    const MEMO: &str = "RACEMEMO";

    let server = MockServer::start().await;
    mount_synthetic_history(&server, TOTAL, 200, Some(MATCHED_TOKEN), MEMO).await;

    let state = make_state(server.uri()).await;
    create_backdated_pending(&state, MEMO, "5", MATCHED_TOKEN.saturating_sub(1)).await;

    let settled = horizon::poll_once(&state).await.unwrap();
    assert_eq!(
        settled, 1,
        "a payment sitting just behind the tip must not be skipped by the \
         first-run baseline"
    );
}

/// A completely fresh account with no payment history at all still starts
/// from `"0"` and settles a payment that arrives immediately after boot —
/// the pre-existing fallback path must keep working unchanged.
#[tokio::test]
async fn fresh_account_with_no_history_still_settles_the_first_payment() {
    const MEMO: &str = "FRESH001";
    let server = MockServer::start().await;
    mount_synthetic_history(&server, 1, 200, Some(1), MEMO).await;

    let state = make_state(server.uri()).await;
    create_backdated_pending(&state, MEMO, "5", 1).await;

    let settled = horizon::poll_once(&state).await.unwrap();
    assert_eq!(settled, 1);
}
