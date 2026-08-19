//! Stellar Horizon integration: detecting and verifying on-chain payments.
//!
//! A background poller periodically asks Horizon for the most recent payments
//! into the gateway account, matches them against pending payment intents by
//! transaction memo, verifies the asset and amount, and transitions the intent
//! to a terminal or watchable state, firing a webhook in each case.
//!
//! ## Payment resolution policy
//!
//! | Scenario | DB status | Webhook event | Notes |
//! |---|---|---|---|
//! | Paid exactly the requested amount | `completed` | `payment.completed` | — |
//! | Paid **more** than requested | `completed` | `payment.overpaid` | `delta` = excess; merchant should refund |
//! | Paid **less** than requested | `underpaid` | `payment.underpaid` | `delta` = shortfall; intent stays watchable |
//! | Top-up brings total to exactly expected | `completed` | `payment.completed` | — |
//! | Top-up brings total above expected | `completed` | `payment.overpaid` | `delta` = cumulative excess |
//!
//! Once an intent reaches `completed`, it is removed from the watchlist.
//! Any subsequent on-chain payment to the same address and memo is silently
//! ignored — it will not trigger an additional webhook.
//!
//! Multiple follow-up (top-up) payments are supported per underpaid intent.
//! Every processed transaction is recorded in the `processed_transactions`
//! join table, and the cumulative received amount is the SUM over that set, so
//! re-seeing a transaction (on a later poll cycle, over the stream, or from a
//! concurrent reconciler) never double-counts and the ledger is independent of
//! the order records arrive in. The payment row's `tx_hash` still records the
//! most recent processed transaction for display.
//!
//! ## Finality
//!
//! A payment only settles an intent when its joined transaction reports
//! `successful: true`. Matching on type/destination/memo/asset/amount is not
//! sufficient for money movement — a failed, replaced, or reorg-orphaned
//! transaction can carry all the right fields yet move no funds. We therefore
//! require the `successful` flag explicitly (records are always fetched with
//! `join=transactions`, so it is present) rather than relying on the implicit
//! and undocumented behaviour that Horizon's payments-for-account endpoint
//! tends to surface only successful operations.
//!
//! The matching logic in [`verify`] is pure and unit-tested; the networked
//! functions wrap it with I/O.

use crate::supervise::TaskExit;
use crate::{db, money, webhook, AppState};
use futures_util::StreamExt;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::sync::watch;
use tracing::{debug, info, warn};

/// Key under which the last fully-processed Horizon paging token is stored in
/// the `kv_state` table, so polling resumes from it across restarts.
const PAYMENT_CURSOR_KEY: &str = "horizon_payment_cursor";

/// How many payment records to request per Horizon page while catching up.
const PAGE_LIMIT: u32 = 200;

/// A single payment operation as returned by Horizon, with the embedded
/// transaction (requested via `join=transactions`) so we can read its memo.
#[derive(Debug, Clone, Deserialize)]
pub struct HorizonPayment {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub amount: Option<String>,
    #[serde(default)]
    pub asset_type: Option<String>,
    #[serde(default)]
    pub asset_code: Option<String>,
    #[serde(default)]
    pub asset_issuer: Option<String>,
    #[serde(default)]
    pub to: Option<String>,
    #[serde(default)]
    pub transaction_hash: Option<String>,
    #[serde(default)]
    pub transaction: Option<TransactionRef>,
    /// Horizon's opaque paging cursor for this record. We persist the latest
    /// processed token so polling resumes from it instead of re-scanning.
    #[serde(default)]
    pub paging_token: Option<String>,
    /// RFC 3339 timestamp of the operation (ledger close time), used to
    /// measure how far behind the poller/stream cursor is running.
    #[serde(default)]
    pub created_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TransactionRef {
    #[serde(default)]
    pub memo: Option<String>,
    #[serde(default)]
    pub memo_type: Option<String>,
    /// Whether the enclosing transaction succeeded on-chain. Horizon populates
    /// this on every transaction record; we treat a missing value as *not*
    /// known-successful and refuse to settle against it (see
    /// [`HorizonPayment::is_successful`]).
    #[serde(default)]
    pub successful: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct PaymentsPage {
    #[serde(rename = "_embedded")]
    embedded: Embedded,
}

#[derive(Debug, Deserialize)]
struct Embedded {
    records: Vec<HorizonPayment>,
}

/// The gateway account as returned by Horizon's `/accounts/{id}` endpoint. We
/// only care about its balance lines, which double as its trustlines: a
/// non-native asset appears here only if the account trusts that issuer.
#[derive(Debug, Deserialize)]
struct AccountResponse {
    #[serde(default)]
    balances: Vec<AccountBalance>,
}

/// One balance / trustline line on a Stellar account.
#[derive(Debug, Clone, Deserialize)]
pub struct AccountBalance {
    #[serde(default)]
    pub asset_type: Option<String>,
    #[serde(default)]
    pub asset_code: Option<String>,
    #[serde(default)]
    pub asset_issuer: Option<String>,
}

/// The outcome of matching a Horizon payment against a pending intent.
#[derive(Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Cumulative paid amount equals the requested amount exactly.
    Completed {
        tx_hash: String,
        paid_amount: String,
    },
    /// Cumulative paid amount exceeds the requested amount.
    /// The intent is fulfilled; `delta` is the excess the merchant should refund.
    Overpaid {
        tx_hash: String,
        paid_amount: String,
    },
    /// Cumulative paid amount is still below the requested amount.
    /// The intent remains open; `delta` is the shortfall still owed.
    Underpaid {
        tx_hash: String,
        paid_amount: String,
    },
}

impl HorizonPayment {
    /// The transaction's memo, but only when Horizon reports it as `memo_type:
    /// "text"`.
    ///
    /// We generate text memos exclusively (see [`crate::api::payments`]).
    /// Stellar transactions can instead carry a `memo_id` (u64) or
    /// `memo_hash`/`memo_return` (32-byte) memo, and Horizon still populates
    /// the JSON `memo` field for those — as a decimal string or base64,
    /// respectively. A `memo_id` consisting only of digits could coincide
    /// with one of our hex memos as plain text, so the type must be checked;
    /// otherwise an unrelated `memo_id` payment could be mistaken for one of
    /// ours.
    fn memo(&self) -> Option<&str> {
        let t = self.transaction.as_ref()?;
        if t.memo_type.as_deref() != Some("text") {
            return None;
        }
        t.memo.as_deref()
    }

    /// Whether the transaction that carried this payment is known to have
    /// succeeded on-chain.
    ///
    /// Horizon's payments-for-account endpoint generally returns operations
    /// from successful transactions, but that is an implementation detail we
    /// must not rely on for money movement: a failed, replaced, or
    /// reorg-orphaned transaction must never settle an intent. We therefore
    /// require the joined transaction to explicitly report `successful: true`.
    /// A missing flag (e.g. a record fetched without `join=transactions`, or a
    /// future/altered payload) is treated as not-successful and rejected.
    fn is_successful(&self) -> bool {
        self.transaction.as_ref().and_then(|t| t.successful) == Some(true)
    }
}

/// Seconds elapsed between an RFC 3339 timestamp and now. Used to observe
/// cursor age (how stale the poller/stream cursor is) and settlement latency
/// (how long an intent took to settle). Returns `None` if `ts` doesn't parse.
fn elapsed_secs(ts: &str) -> Option<i64> {
    let then = OffsetDateTime::parse(ts, &Rfc3339).ok()?;
    Some((OffsetDateTime::now_utc() - then).whole_seconds())
}

/// Decide whether a Horizon payment satisfies a pending intent.
///
/// `already_paid_stroops` is the cumulative amount already received for this
/// intent (0 for a fresh `pending` payment, non-zero for an `underpaid` one).
///
/// Returns `None` when the payment is unrelated (wrong type, destination, memo,
/// or asset — including a credit payment whose issuer does not match the
/// issuer stored on the intent). When it matches, returns the verdict for the
/// cumulative total.
pub fn verify(
    payment: &db::Payment,
    hp: &HorizonPayment,
    already_paid_stroops: i64,
) -> Option<Verdict> {
    if hp.kind != "payment" {
        return None;
    }
    if hp.to.as_deref() != Some(payment.destination_address.as_str()) {
        return None;
    }
    if hp.memo() != Some(payment.memo.as_str()) {
        return None;
    }
    /* Only settle against a transaction Horizon reports as successful. Matching
    on type/destination/memo/asset/amount is not enough for money movement: a
    failed or reorg-orphaned transaction can carry all the right fields yet
    never have moved funds. See [`HorizonPayment::is_successful`] for the
    finality assumptions this encodes. */
    if !hp.is_successful() {
        return None;
    }

    /* Match the issuer this intent was priced in — not any allow-list entry
    that happens to share the code. Two `USDC` issuers used to both settle an
    intent that stored only the code (issue #222). */
    let asset_matches = match payment.asset_issuer.as_deref().filter(|s| !s.is_empty()) {
        None => {
            payment.asset.eq_ignore_ascii_case("XLM") && hp.asset_type.as_deref() == Some("native")
        }
        Some(issuer) => {
            hp.asset_code.as_deref() == Some(payment.asset.as_str())
                && hp.asset_issuer.as_deref() == Some(issuer)
        }
    };
    if !asset_matches {
        return None;
    }

    let raw_amount = hp.amount.as_deref()?;
    let new_paid = money::parse_stroops(raw_amount)?;
    let expected = money::parse_stroops(&payment.amount)?;
    let total_paid = already_paid_stroops + new_paid;
    let tx_hash = hp.transaction_hash.clone().unwrap_or_default();
    let paid_amount = money::stroops_to_string(total_paid);

    use std::cmp::Ordering;
    match total_paid.cmp(&expected) {
        Ordering::Equal => Some(Verdict::Completed {
            tx_hash,
            paid_amount,
        }),
        Ordering::Greater => Some(Verdict::Overpaid {
            tx_hash,
            paid_amount,
        }),
        Ordering::Less => Some(Verdict::Underpaid {
            tx_hash,
            paid_amount,
        }),
    }
}

/// A Horizon HTTP failure, distinguishing throttling (`429 Too Many Requests`
/// or `503 Service Unavailable`, carrying `Retry-After` when Horizon sends
/// one) from any other failure. Before this, `fetch_recent_payments` turned
/// every non-2xx response into an opaque `reqwest::Error` via
/// `error_for_status()`, discarding the status code and headers — the poller
/// had no way to tell "back off, and for how long" from an ordinary blip
/// (issue #313).
#[derive(Debug)]
pub struct HorizonHttpError {
    pub status: reqwest::StatusCode,
    pub retry_after: Option<Duration>,
    body: String,
}

impl std::fmt::Display for HorizonHttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Horizon returned {}: {}", self.status, self.body)
    }
}

impl std::error::Error for HorizonHttpError {}

impl HorizonHttpError {
    /// `429`/`503` mean "back off", not "something is broken" — every other
    /// non-2xx status is treated as an ordinary failure.
    pub fn is_rate_limited(&self) -> bool {
        matches!(
            self.status,
            reqwest::StatusCode::TOO_MANY_REQUESTS | reqwest::StatusCode::SERVICE_UNAVAILABLE
        )
    }
}

/// Parse `Retry-After` as delta-seconds (RFC 9110) — the form every rate
/// limiter this service talks to actually sends, including Horizon's. The
/// HTTP-date form is not handled: nothing observed in the wild sends it for
/// `429`/`503`, and misparsing a date as a much smaller or larger delay would
/// be worse than falling back to the exponential backoff below.
fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
}

/// Fetch the most recent payments into `account` from Horizon, newest first,
/// with their transactions joined so memos are available.
///
/// A non-2xx response is returned as a [`HorizonHttpError`] rather than
/// `error_for_status`'s opaque `reqwest::Error`, so a caller can tell a `429`/
/// `503` — and any `Retry-After` Horizon attached — apart from an ordinary
/// failure (issue #313).
pub async fn fetch_recent_payments(
    client: &reqwest::Client,
    horizon_url: &str,
    account: &str,
    cursor: &str,
    limit: u32,
) -> anyhow::Result<Vec<HorizonPayment>> {
    let url = format!(
        "{}/accounts/{}/payments?order=asc&cursor={}&limit={}&join=transactions",
        horizon_url.trim_end_matches('/'),
        account,
        cursor,
        limit
    );
    let resp = client
        .get(&url)
        .header("Accept", "application/json")
        .send()
        .await?;

    let status = resp.status();
    if !status.is_success() {
        let retry_after = parse_retry_after(resp.headers());
        let body = resp.text().await.unwrap_or_default();
        return Err(HorizonHttpError {
            status,
            retry_after,
            body,
        }
        .into());
    }

    let page: PaymentsPage = resp.json().await?;
    Ok(page.embedded.records)
}

/// Return the accepted assets the gateway account holds **no** trustline for.
///
/// Native XLM never needs a trustline, so it is always considered held. An
/// issued asset (`CODE:ISSUER`) is held only if the account has a balance line
/// with the matching `asset_code` and `asset_issuer`. Pure, so it is
/// unit-tested without any network.
pub fn missing_trustlines<'a>(
    accepted_assets: &'a [crate::config::AcceptedAsset],
    balances: &[AccountBalance],
) -> Vec<&'a crate::config::AcceptedAsset> {
    accepted_assets
        .iter()
        .filter(|asset| match asset.issuer.as_deref() {
            // Native asset — no trustline required.
            None => false,
            Some(issuer) => !balances.iter().any(|b| {
                b.asset_code.as_deref() == Some(asset.code.as_str())
                    && b.asset_issuer.as_deref() == Some(issuer)
            }),
        })
        .collect()
}

/// At startup, check that the gateway account holds a trustline for every
/// accepted non-native asset, and warn about any that are missing.
///
/// An accepted asset without a trustline mints unpayable intents: the gateway
/// advertises (say) USDC, a customer pays, and the payment bounces on-chain
/// because the account cannot receive it. Surfacing this at boot turns a silent
/// runtime failure into an actionable startup warning.
///
/// Best-effort by design: a Horizon error (unreachable, account not yet funded)
/// is returned to the caller to log, but must not abort boot — the account may
/// be provisioned shortly after start. Returns the list of accepted asset codes
/// that are missing a trustline (empty when all are present).
pub async fn check_trustlines(state: &Arc<AppState>) -> anyhow::Result<Vec<String>> {
    let url = format!(
        "{}/accounts/{}",
        state.config.horizon_url.trim_end_matches('/'),
        state.config.gateway_public,
    );
    let account: AccountResponse = state
        .http
        .get(&url)
        .header("Accept", "application/json")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let missing = missing_trustlines(&state.config.accepted_assets, &account.balances);
    for asset in &missing {
        warn!(
            asset = %asset.code,
            issuer = %asset.issuer.as_deref().unwrap_or(""),
            "gateway account has no trustline for an accepted asset; intents in \
             this asset will be unpayable until a trustline is established"
        );
    }
    Ok(missing.iter().map(|a| a.code.clone()).collect())
}

/// How many pages [`starting_cursor`] will walk backward, at most, while
/// searching for a baseline that covers every currently open intent (issue
/// #311). Bounds the worst case — an account with a large payment history and
/// an old open intent — to a fixed number of Horizon requests at boot rather
/// than an unbounded backward scan. `MAX_BASELINE_PAGES * PAGE_LIMIT` (5,000
/// records) is the same order of magnitude as [`MAX_PAGES_PER_CYCLE`]'s
/// per-cycle budget.
const MAX_BASELINE_PAGES: usize = 25;

/// Resolve the cursor this cycle should start paging from.
///
/// From the second run onward this simply resumes from the saved token. On
/// the very first run (no persisted cursor) it deliberately baselines with
/// overlap rather than adopting the account's single most recent payment
/// exactly, which used to skip two kinds of payment silently:
///
/// - **Reused account.** Pointing a fresh instance at an account that already
///   received payments — a redeploy after losing the volume, a migration
///   between hosts — would adopt whatever the account's newest payment
///   happened to be as the floor, hiding every payment for an intent created
///   before that point even though it is still open.
/// - **Startup race.** A payment that lands between Horizon answering the
///   baselining query and the first forward poll can sort at or below the
///   single-record baseline (e.g. read-replica lag on Horizon's side), and
///   was silently skipped.
///
/// Neither produces an error: the intent just stays `pending` until the
/// sweeper expires it, with no record connecting the customer's on-chain
/// payment to anything. Re-processing an already-settled transaction is a
/// no-op through `processed_transactions` (issue #78), so the cost of
/// over-scanning is a few wasted queries — cheap insurance against silently
/// under-scanning.
///
/// The baseline is chosen by paging backward (`order=desc`) from the tip
/// until the oldest record seen is older than every currently open intent's
/// `created_at` (a payment cannot be relevant to an intent it predates), or
/// until [`MAX_BASELINE_PAGES`] is reached, or until Horizon has no more
/// history. When nothing is currently open (a genuinely fresh deployment),
/// one page of backward overlap is taken anyway, purely to cover the startup
/// race. If the account has no payments at all, baselining starts from `"0"`
/// so the first payment that ever arrives is still captured.
async fn starting_cursor(state: &Arc<AppState>) -> anyhow::Result<String> {
    if let Some(cursor) = db::get_state(&state.pool, PAYMENT_CURSOR_KEY).await? {
        return Ok(cursor);
    }

    /* `list_pending` returns oldest-first, so the first row (if any) is the
    earliest `created_at` we must not scan past. A payment settling this
    intent cannot have landed before the intent existed. */
    let earliest_open = db::list_pending(&state.pool)
        .await?
        .into_iter()
        .next()
        .map(|p| p.created_at);

    let mut next_cursor: Option<String> = None;
    let mut skipped = 0usize;
    let mut pages = 0usize;

    let token = loop {
        let url = format!(
            "{}/accounts/{}/payments?order=desc&limit={}{}",
            state.config.horizon_url.trim_end_matches('/'),
            state.config.gateway_public,
            PAGE_LIMIT,
            next_cursor
                .as_ref()
                .map(|c| format!("&cursor={c}"))
                .unwrap_or_default(),
        );
        let page: PaymentsPage = state
            .http
            .get(&url)
            .header("Accept", "application/json")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        pages += 1;

        let Some(oldest) = page.embedded.records.last() else {
            /* Horizon has no more history behind us — we have walked all the
            way back to the account's first-ever payment (or it has none at
            all). Either way, "0" is the maximally safe baseline: nothing is
            skipped, at the cost of a full replay on the next forward poll. */
            return Ok("0".to_string());
        };
        skipped += page.embedded.records.len();
        let token = oldest.paging_token.clone().unwrap_or_default();
        next_cursor = Some(token.clone());

        let covers_every_open_intent = match &earliest_open {
            // Nothing open yet — one page of overlap is enough to cover the
            // startup race; there is no older business state to protect.
            None => true,
            /* Strictly older, not `<=`: the boundary record itself must be
            excluded from "covered", because the persisted cursor becomes the
            *exclusive* start of the next forward poll. Stopping at `<=` could
            land the cursor exactly on (or after) the earliest open intent's
            creation instant and exclude a payment that landed in the same
            instant — reintroducing the exact race this baselining exists to
            close. An unparseable or absent `created_at` is treated as "not
            proven older" so the walk keeps going rather than risk stopping
            too early. */
            Some(earliest) => oldest
                .created_at
                .as_deref()
                .map(|ts| ts < earliest.as_str())
                .unwrap_or(false),
        };

        if covers_every_open_intent {
            break token;
        }
        if pages >= MAX_BASELINE_PAGES {
            warn!(
                pages,
                skipped_records = skipped,
                "Horizon baseline walk hit its page cap before clearing every open \
                 intent's creation time; baselining with the overlap found so far"
            );
            break token;
        }
    };

    /* Persist immediately so a crash right after this scan still leaves us
    baselined rather than repeating a potentially multi-page walk next time. */
    db::set_state(&state.pool, PAYMENT_CURSOR_KEY, &token).await?;
    info!(
        cursor = %token,
        skipped_records = skipped,
        pages,
        "Horizon poller baselined with overlap"
    );
    Ok(token)
}

/// Requests a single `poll_once` call will issue before yielding back to the
/// caller, even if Horizon still has more pages. Without this, a backlog that
/// built up while Horizon was throttling (issue #313) would be drained in one
/// cycle by looping until caught up — reissuing exactly the request volume
/// that tripped the limit, immediately after it lifts. The cursor is
/// checkpointed after every page, so stopping early costs nothing: the next
/// cycle resumes exactly where this one stopped, `PAGE_LIMIT` records later
/// per additional cycle.
const MAX_PAGES_PER_CYCLE: usize = 25;

/// Run one poll cycle: page forward from the persisted cursor through every
/// payment that has landed since, settling any that satisfy a pending intent,
/// until caught up or [`MAX_PAGES_PER_CYCLE`] pages have been fetched. The
/// cursor is advanced (and persisted) only after a page's records have been
/// processed, so no record is ever skipped and a restart (or the next cycle,
/// if the page cap was hit) resumes exactly where it left off. Safe to call
/// repeatedly; re-seeing an already-settled record is a no-op (its intent is
/// no longer pending).
pub async fn poll_once(state: &Arc<AppState>) -> anyhow::Result<usize> {
    let mut cursor = starting_cursor(state).await?;
    let mut settled = 0;
    let mut pages = 0usize;

    loop {
        let page = fetch_recent_payments(
            &state.http,
            &state.config.horizon_url,
            &state.config.gateway_public,
            &cursor,
            PAGE_LIMIT,
        )
        .await?;
        let count = page.len();
        pages += 1;

        for hp in &page {
            if let Some(token) = &hp.paging_token {
                cursor = token.clone();
            }
            match reconcile_payment(state, hp).await {
                Ok(true) => settled += 1,
                Ok(false) => {}
                Err(e) => warn!(error = %e, "failed to reconcile polled payment"),
            }
        }

        if let Some(cursor_age_secs) = page
            .last()
            .and_then(|hp| hp.created_at.as_deref())
            .and_then(elapsed_secs)
        {
            info!(cursor_age_secs, "poller cursor advanced");
        }

        /* Checkpoint after the whole page is processed. If we crash mid-page the
        cursor still points at the last fully-processed page, and re-reading
        the unfinished page is harmless (settled intents are skipped). */
        db::set_state(&state.pool, PAYMENT_CURSOR_KEY, &cursor).await?;

        // A short page means Horizon has nothing newer — we're caught up.
        if count < PAGE_LIMIT as usize {
            break;
        }

        if pages >= MAX_PAGES_PER_CYCLE {
            debug!(
                pages,
                "poll cycle hit its per-cycle page cap; resuming next cycle"
            );
            break;
        }
    }

    /* A completed cycle is on-chain progress even with nothing to settle — the
    cursor advanced and the poller is alive. This heartbeat is what /ready's
    cursor-freshness check measures (issue #315). */
    state.task_health.note_success();

    Ok(settled)
}

/// Look up the pending intent matching this Horizon payment by memo, verify it,
/// and settle it if it matches. Returns `true` when an intent was settled.
///
/// This is intentionally `pub` so integration tests can drive concurrent
/// reconciliations to verify the single-settlement guarantee (issue #155).
pub async fn reconcile_payment(state: &Arc<AppState>, hp: &HorizonPayment) -> anyhow::Result<bool> {
    let memo = match hp.memo() {
        Some(m) => m,
        None => return Ok(false),
    };

    let payment = match db::find_pending_by_memo(&state.pool, memo).await? {
        Some(p) => p,
        None => return Ok(false),
    };

    let hp_hash = hp.transaction_hash.as_deref().unwrap_or("");

    /* The authoritative received-amount ledger is the SUM over every
    transaction already recorded for this intent — not the single most-recent
    `tx_hash`. Read it before recording this transaction so `verify` sees the
    prior total. */
    let already_paid_stroops = db::sum_processed_stroops(&state.pool, &payment.id).await?;

    /* Gate on a real, matching, on-chain payment before recording anything, so
    unrelated traffic never pollutes the ledger. `verify` returns `None` for
    anything that does not satisfy this intent (wrong type/destination/memo/
    asset, or an unparseable amount). */
    if verify(&payment, hp, already_paid_stroops).is_none() {
        return Ok(false);
    }

    /* Record this transaction idempotently. If it was already credited — seen
    on an earlier poll cycle, redelivered over the stream, or racing a
    concurrent reconciler — the insert is a no-op and we must not settle again.
    This makes re-processing any past transaction a no-op regardless of the
    order records arrive in (issue #119). */
    let new_stroops = hp
        .amount
        .as_deref()
        .and_then(money::parse_stroops)
        .unwrap_or(0);
    if !db::record_processed_tx(&state.pool, &payment.id, hp_hash, new_stroops).await? {
        return Ok(false);
    }

    /* Re-sum over the recorded set (now including this transaction) so the
    persisted `paid_amount` always reflects every processed transaction. */
    let total_stroops = db::sum_processed_stroops(&state.pool, &payment.id).await?;
    let expected_stroops = money::parse_stroops(&payment.amount).unwrap_or(0);
    let paid_amount = money::stroops_to_string(total_stroops);

    use std::cmp::Ordering;
    let (status, event, delta) = match total_stroops.cmp(&expected_stroops) {
        Ordering::Equal => ("completed", "payment.completed", None),
        Ordering::Greater => {
            let excess = money::stroops_to_string(total_stroops - expected_stroops);
            info!(
                payment_id = %payment.id,
                excess = %excess,
                "overpayment — intent completed, excess should be refunded"
            );
            ("completed", "payment.overpaid", Some(excess))
        }
        Ordering::Less => {
            let remaining = money::stroops_to_string(expected_stroops - total_stroops);
            warn!(
                payment_id = %payment.id,
                expected = %payment.amount,
                paid = %paid_amount,
                remaining = %remaining,
                "underpayment — intent remains open for a top-up"
            );
            ("underpaid", "payment.underpaid", Some(remaining))
        }
    };

    let did_settle = settle(
        state,
        &payment,
        status,
        hp_hash,
        &paid_amount,
        event,
        delta.as_deref(),
    )
    .await;
    Ok(did_settle)
}

/// Persist a terminal or intermediate status for `payment` and fire its webhook.
/// Returns `true` when the row was actually updated (i.e. a settlement was
/// committed); returns `false` when the status guard rejected the update because
/// a concurrent reconciler already settled the intent.
///
/// Callers must propagate this return value so `reconcile_payment` can report
/// accurately whether it settled an intent, which is what the concurrency test
/// asserts (issue #155).
async fn settle(
    state: &Arc<AppState>,
    payment: &db::Payment,
    status: &str,
    tx_hash: &str,
    paid_amount: &str,
    event: &str,
    delta: Option<&str>,
) -> bool {
    match db::update_payment_status(&state.pool, &payment.id, status, tx_hash, paid_amount).await {
        Err(e) => {
            warn!(payment_id = %payment.id, error = %e, "failed to update payment status");
            return false;
        }
        Ok(false) => {
            // A concurrent reconciler already settled this intent — skip the webhook.
            debug!(
                payment_id = %payment.id,
                "skipping duplicate settlement (status guard rejected update)"
            );
            return false;
        }
        Ok(true) => {}
    }
    let settlement_latency_secs = elapsed_secs(&payment.created_at);
    info!(
        payment_id = %payment.id,
        status,
        %tx_hash,
        ?settlement_latency_secs,
        "payment settled"
    );

    // Reflect the new state in the copy we hand to the webhook.
    let mut settled = payment.clone();
    settled.status = status.to_string();
    settled.tx_hash = Some(tx_hash.to_string());
    settled.paid_amount = Some(paid_amount.to_string());
    /* Webhook delivery is handled asynchronously by the webhook subsystem
    (recording here is non-blocking from reconciliation's point of view).

    Design note: dispatch() still delivers inline so the common case settles and
    notifies in one pass with no added latency. The redrive worker is a safety net
    on top of that for the crash case, not a replacement for it — rewriting dispatch
    to be record-only would be a bigger, riskier change than issue #156 asked for
    and would break the existing synchronous-delivery test coverage. */
    webhook::dispatch(state, &settled, event, delta).await;
    true
}

/// Floor and cap for the poller's failure backoff. Reuses the same
/// equal-jitter schedule (`webhook::retry_delay`) the webhook redrive worker
/// backs off with (issue #318) — the shape solves the same problem here:
/// growth so repeated failures stop hammering Horizon at the configured poll
/// interval, jitter so it doesn't retry in lockstep with itself every cycle.
const POLL_BACKOFF_BASE: Duration = Duration::from_secs(1);
const POLL_BACKOFF_MAX: Duration = Duration::from_secs(120);

/// Choose how long to wait before the poller's next attempt, given this
/// cycle's outcome (issue #313).
///
/// A `429`/`503` that carried `Retry-After` is honored exactly — Horizon told
/// us how long to wait, and second-guessing it with our own backoff could
/// still be too short. Every other failure (including a rate limit with no
/// `Retry-After`) falls back to the exponential-with-jitter schedule, keyed
/// on `consecutive_failures` so it keeps growing across repeated failures
/// instead of resetting each cycle.
fn next_poll_delay(consecutive_failures: u32, retry_after: Option<Duration>) -> Duration {
    retry_after.unwrap_or_else(|| {
        webhook::retry_delay(consecutive_failures, POLL_BACKOFF_BASE, POLL_BACKOFF_MAX)
    })
}

/// Background loop that polls Horizon on the configured interval until the
/// process shuts down. Idles (without polling) while no gateway is configured.
///
/// A failed cycle no longer waits out the fixed `POLL_INTERVAL_SECS` before
/// trying again at the same rate that likely caused it (issue #313): a `429`
/// backs off for at least `Retry-After`, and repeated failures of any kind
/// back off exponentially with jitter, both reset to the configured interval
/// by the next success.
pub async fn run_poller(state: Arc<AppState>, mut shutdown: watch::Receiver<bool>) -> TaskExit {
    if !state.config.gateway_configured() {
        /* Previously this parked on the shutdown signal purely so the
        supervisor would not read a deliberate idle as an unexpected return.
        Saying so explicitly is both clearer and cheaper: the supervisor now
        knows this is terminal-by-design, reports it once, and does not hold a
        task open for the life of the process to convey it (issue #317). */
        return TaskExit::DisabledByConfig("STELLAR_GATEWAY_PUBLIC is unconfigured");
    }

    let interval = Duration::from_secs(state.config.poll_interval_secs.max(1));
    info!(
        account = %state.config.gateway_public,
        interval_secs = state.config.poll_interval_secs,
        "Horizon poller started"
    );

    let mut consecutive_failures: u32 = 0;
    let mut next_delay = interval;

    loop {
        tokio::select! {
            _ = tokio::time::sleep(next_delay) => {}
            _ = shutdown.changed() => {
                info!("Horizon poller shutting down");
                return TaskExit::ShutdownRequested;
            }
        }

        match poll_once(&state).await {
            Ok(n) => {
                if n == 0 {
                    debug!("poll: nothing to settle");
                } else {
                    info!(settled = n, "poll cycle settled payments");
                }
                state.horizon_metrics.record_success();
                consecutive_failures = 0;
                next_delay = interval;
            }
            Err(e) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                let horizon_err = e.downcast_ref::<HorizonHttpError>();
                let rate_limited = horizon_err.is_some_and(|he| he.is_rate_limited());
                let retry_after = horizon_err.and_then(|he| he.retry_after);

                if rate_limited {
                    state.horizon_metrics.record_rate_limited();
                } else {
                    state.horizon_metrics.record_error();
                }

                next_delay = next_poll_delay(consecutive_failures, retry_after);
                warn!(
                    error = %e,
                    rate_limited,
                    retry_after_secs = retry_after.map(|d| d.as_secs()),
                    consecutive_failures,
                    next_delay_secs = next_delay.as_secs(),
                    "poll cycle failed"
                );
            }
        }
    }
}

/// One parsed Server-Sent-Events block: the fields of a single `\n\n`-delimited
/// event. See <https://html.spec.whatwg.org/multipage/server-sent-events.html>.
#[derive(Debug, Default, PartialEq, Eq)]
struct SseEvent {
    /// The `event:` name, if any (Horizon uses `open` for the greeting).
    event: Option<String>,
    /// The `id:` field — Horizon sets it to the paging token, which we reuse as
    /// the reconnect cursor so no payments are missed across a dropped stream.
    id: Option<String>,
    /// The concatenated `data:` lines.
    data: String,
}

/// Parse one SSE event block (the text between blank-line delimiters) into its
/// fields. Comment lines (`:`...) and unrecognised fields such as `retry:` are
/// ignored. Pure, so it is unit-tested without any network.
fn parse_sse_block(block: &str) -> SseEvent {
    let mut ev = SseEvent::default();
    let mut data_lines: Vec<&str> = Vec::new();
    for line in block.lines() {
        /* Per the spec a value has its single leading space (if present)
        stripped after the colon. */
        if let Some(rest) = line.strip_prefix("data:") {
            data_lines.push(rest.strip_prefix(' ').unwrap_or(rest));
        } else if let Some(rest) = line.strip_prefix("event:") {
            ev.event = Some(rest.strip_prefix(' ').unwrap_or(rest).to_string());
        } else if let Some(rest) = line.strip_prefix("id:") {
            ev.id = Some(rest.strip_prefix(' ').unwrap_or(rest).to_string());
        }
    }
    ev.data = data_lines.join("\n");
    ev
}

/// Background task that subscribes to Horizon's payment SSE stream for the
/// gateway account and settles matching intents as records arrive. Reconnects
/// automatically with exponential backoff, resuming from the last seen cursor
/// so no payments are missed across a dropped connection. Idles (without
/// connecting) while no gateway is configured.
pub async fn run_stream_listener(
    state: Arc<AppState>,
    mut shutdown: watch::Receiver<bool>,
) -> TaskExit {
    if !state.config.gateway_configured() {
        return TaskExit::DisabledByConfig("STELLAR_GATEWAY_PUBLIC is unconfigured");
    }

    info!(account = %state.config.gateway_public, "Horizon payment stream listener started");

    /* A dedicated client without the shared client's overall request timeout —
    the SSE connection is long-lived and must not be cut off mid-stream. */
    let client = match reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(30))
        .user_agent(concat!("StellarGate/", env!("CARGO_PKG_VERSION")))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            /* The case issue #317 calls out by name: this was a `warn!`
            followed by a permanent exit, recorded as an ordinary stop. Payment
            detection over the stream was simply gone, and nothing said so.
            It is a fault, not a configuration choice, so it is reported as one
            and the supervisor retries it. */
            return TaskExit::Fatal(format!("failed to build stream HTTP client: {e}"));
        }
    };

    let base_backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(30);
    let mut backoff = base_backoff;
    // Start at the live edge; subsequent reconnects resume from the last event.
    let mut cursor = "now".to_string();
    let idle_timeout = Duration::from_secs(state.config.stream_idle_timeout_secs);
    let mut first_connection = true;

    loop {
        if !first_connection {
            /* Every pass through the loop body after the first is a reconnect —
            whether the previous connection closed cleanly, errored, or (issue
            #312) went idle past `idle_timeout` with no error at all. A
            persistently-reconnecting stream is the alertable signal that a
            half-open connection keeps disabling live payment detection. */
            state.horizon_metrics.record_stream_reconnect();
        }
        first_connection = false;

        let cursor_before = cursor.clone();
        tokio::select! {
            result = stream_once(&state, &client, &mut cursor, idle_timeout) => {
                match result {
                    Ok(()) => debug!("Horizon stream closed by server; reconnecting"),
                    Err(e) => warn!(error = %e, "Horizon stream dropped; reconnecting"),
                }
            }
            _ = shutdown.changed() => {
                info!("Horizon stream listener shutting down");
                return TaskExit::ShutdownRequested;
            }
        }

        if cursor != cursor_before {
            backoff = base_backoff;
        }

        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = shutdown.changed() => {
                info!("Horizon stream listener shutting down");
                return TaskExit::ShutdownRequested;
            }
        }
        backoff = (backoff * 2).min(max_backoff);
    }
}

/// Open one SSE connection and process events until the stream ends, errors,
/// or goes `idle_timeout` without delivering a single byte. Advances `cursor`
/// to the latest event `id` so a reconnect resumes cleanly.
///
/// The dedicated stream client (built by [`run_stream_listener`]) carries no
/// overall request timeout — the connection is meant to live indefinitely —
/// so nothing else bounds a half-open socket that stops delivering bytes
/// without closing: a NAT or load balancer dropping idle state without
/// sending `RST`, or an upstream stall. Horizon sends periodic keep-alive
/// comment lines on its SSE endpoints, so an idle window with no bytes at all
/// is a reliable liveness signal (issue #312): every await on the next chunk
/// is itself bounded by `idle_timeout`, and running past it is reported as an
/// error so the caller's existing reconnect-with-backoff path picks it up
/// exactly as it would a dropped connection.
async fn stream_once(
    state: &Arc<AppState>,
    client: &reqwest::Client,
    cursor: &mut String,
    idle_timeout: Duration,
) -> anyhow::Result<()> {
    let url = format!(
        "{}/accounts/{}/payments?cursor={}&join=transactions",
        state.config.horizon_url.trim_end_matches('/'),
        state.config.gateway_public,
        cursor,
    );

    let resp = client
        .get(&url)
        .header("Accept", "text/event-stream")
        .send()
        .await?
        .error_for_status()?;

    let mut stream = resp.bytes_stream();
    /* Accumulate raw bytes (not lossily-decoded str) so multibyte characters
    split across chunk boundaries are never corrupted. */
    let mut buf: Vec<u8> = Vec::new();

    loop {
        let chunk = match tokio::time::timeout(idle_timeout, stream.next()).await {
            Err(_) => {
                return Err(anyhow::anyhow!(
                    "no data received on Horizon stream for {idle_timeout:?}; \
                     treating the connection as dead"
                ));
            }
            Ok(None) => break,
            Ok(Some(chunk)) => chunk?,
        };
        buf.extend_from_slice(&chunk);

        /* Dispatch every complete event (terminated by a blank line) in the
        buffer, leaving any partial trailing event for the next chunk. */
        while let Some(end) = find_event_end(&buf) {
            let block: Vec<u8> = buf.drain(..end).collect();
            let text = String::from_utf8_lossy(&block);
            handle_stream_event(state, &text, cursor).await;
        }
    }

    Ok(())
}

/// Find the byte index just past the first event delimiter in `buf`, i.e. the
/// number of leading bytes that form one complete event plus its terminator.
/// SSE events are separated by a blank line — `\n\n` (LF) or `\r\n\r\n` (CRLF).
/// Returns `None` if no event is complete yet.
fn find_event_end(buf: &[u8]) -> Option<usize> {
    let lf = buf.windows(2).position(|w| w == b"\n\n").map(|i| i + 2);
    let crlf = buf.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4);
    match (lf, crlf) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

/// Parse one streamed event block and, when it carries a payment record, run it
/// through the shared reconciliation path. Non-payment frames (Horizon's `open`
/// greeting, keep-alives) are ignored.
async fn handle_stream_event(state: &Arc<AppState>, block: &str, cursor: &mut String) {
    let ev = parse_sse_block(block);

    // Advance the reconnect cursor as soon as we learn a newer event id.
    if let Some(id) = ev.id {
        *cursor = id;
    }

    if ev.event.as_deref() == Some("open") || ev.data.is_empty() {
        return;
    }

    match serde_json::from_str::<HorizonPayment>(&ev.data) {
        Ok(hp) => {
            if let Some(cursor_age_secs) = hp.created_at.as_deref().and_then(elapsed_secs) {
                info!(cursor_age_secs, "stream cursor advanced");
            }
            /* Receiving a payment record means the stream is alive and the
            cursor moved — the same heartbeat /ready's freshness check uses. */
            state.task_health.note_success();
            if let Err(e) = reconcile_payment(state, &hp).await {
                warn!(error = %e, "failed to reconcile streamed payment");
            }
        }
        // The greeting payload (`"hello"`) and any non-record frames land here.
        Err(e) => debug!(error = %e, "ignoring non-payment stream frame"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending(asset: &str, amount: &str) -> db::Payment {
        let asset_issuer = match asset {
            "USDC" => Some("GUSDC".into()),
            _ => None,
        };
        db::Payment {
            id: "id-1".into(),
            merchant_id: "m".into(),
            destination_address: "GGATEWAY".into(),
            memo: "MEMO1234".into(),
            amount: amount.into(),
            asset: asset.into(),
            status: "pending".into(),
            webhook_url: None,
            tx_hash: None,
            paid_amount: None,
            created_at: "now".into(),
            updated_at: "now".into(),
            expires_at: "later".into(),
            asset_issuer,
        }
    }

    fn native_payment(amount: &str, memo: &str, to: &str) -> HorizonPayment {
        HorizonPayment {
            kind: "payment".into(),
            amount: Some(amount.into()),
            asset_type: Some("native".into()),
            asset_code: None,
            asset_issuer: None,
            to: Some(to.into()),
            transaction_hash: Some("TXHASH".into()),
            transaction: Some(TransactionRef {
                memo: Some(memo.into()),
                memo_type: Some("text".into()),
                successful: Some(true),
            }),
            paging_token: Some("1".into()),
            created_at: None,
        }
    }

    fn test_assets() -> Vec<crate::config::AcceptedAsset> {
        vec![
            crate::config::AcceptedAsset {
                code: "XLM".into(),
                issuer: None,
            },
            crate::config::AcceptedAsset {
                code: "USDC".into(),
                issuer: Some("GUSDC".into()),
            },
        ]
    }

    #[test]
    fn exact_xlm_payment_completes() {
        let p = pending("XLM", "10.00");
        let hp = native_payment("10.0000000", "MEMO1234", "GGATEWAY");
        assert_eq!(
            verify(&p, &hp, 0),
            Some(Verdict::Completed {
                tx_hash: "TXHASH".into(),
                paid_amount: "10".into(),
            })
        );
    }

    #[test]
    fn overpayment_yields_overpaid_verdict() {
        let p = pending("XLM", "10");
        let hp = native_payment("12.5", "MEMO1234", "GGATEWAY");
        assert_eq!(
            verify(&p, &hp, 0),
            Some(Verdict::Overpaid {
                tx_hash: "TXHASH".into(),
                paid_amount: "12.5".into(),
            })
        );
    }

    #[test]
    fn underpayment_yields_underpaid_verdict() {
        let p = pending("XLM", "10");
        let hp = native_payment("9.9999999", "MEMO1234", "GGATEWAY");
        assert_eq!(
            verify(&p, &hp, 0),
            Some(Verdict::Underpaid {
                tx_hash: "TXHASH".into(),
                paid_amount: "9.9999999".into(),
            })
        );
    }

    #[test]
    fn topup_completing_underpaid_intent() {
        // First payment: 3 of 5 XLM — underpaid.
        let p = pending("XLM", "5");
        let hp1 = native_payment("3.0000000", "MEMO1234", "GGATEWAY");
        assert!(matches!(
            verify(&p, &hp1, 0),
            Some(Verdict::Underpaid { .. })
        ));

        // Top-up: 2 XLM arrives; cumulative = 5 = expected — completes exactly.
        let hp2 = native_payment("2.0000000", "MEMO1234", "GGATEWAY");
        assert_eq!(
            verify(&p, &hp2, 30_000_000),
            Some(Verdict::Completed {
                tx_hash: "TXHASH".into(),
                paid_amount: "5".into(),
            })
        );
    }

    #[test]
    fn topup_overpaying_underpaid_intent() {
        // First payment: 3 of 5 XLM — underpaid.
        let p = pending("XLM", "5");
        // Top-up of 3 XLM; cumulative = 6 > 5 — overpaid.
        let hp = native_payment("3.0000000", "MEMO1234", "GGATEWAY");
        assert_eq!(
            verify(&p, &hp, 30_000_000),
            Some(Verdict::Overpaid {
                tx_hash: "TXHASH".into(),
                paid_amount: "6".into(),
            })
        );
    }

    #[test]
    fn wrong_memo_is_ignored() {
        let p = pending("XLM", "10");
        let hp = native_payment("10", "OTHER", "GGATEWAY");
        assert_eq!(verify(&p, &hp, 0), None);
    }

    /// A `memo_id`/`memo_hash`/`memo_return` transaction that happens to
    /// render the same characters as one of our hex memos must never be
    /// mistaken for a match — only `memo_type: "text"` counts.
    #[test]
    fn non_text_memo_type_is_ignored_even_if_value_matches() {
        let p = pending("XLM", "10");
        let mut hp = native_payment("10", "MEMO1234", "GGATEWAY");
        hp.transaction.as_mut().unwrap().memo_type = Some("id".into());
        assert_eq!(verify(&p, &hp, 0), None);
    }

    #[test]
    fn missing_memo_type_is_ignored() {
        let p = pending("XLM", "10");
        let mut hp = native_payment("10", "MEMO1234", "GGATEWAY");
        hp.transaction.as_mut().unwrap().memo_type = None;
        assert_eq!(verify(&p, &hp, 0), None);
    }

    #[test]
    fn wrong_destination_is_ignored() {
        let p = pending("XLM", "10");
        let hp = native_payment("10", "MEMO1234", "GSOMEONEELSE");
        assert_eq!(verify(&p, &hp, 0), None);
    }

    #[test]
    fn xlm_intent_rejects_usdc_payment() {
        let p = pending("XLM", "10");
        let mut hp = native_payment("10", "MEMO1234", "GGATEWAY");
        hp.asset_type = Some("credit_alphanum4".into());
        hp.asset_code = Some("USDC".into());
        hp.asset_issuer = Some("GUSDC".into());
        assert_eq!(verify(&p, &hp, 0), None);
    }

    #[test]
    fn usdc_payment_with_correct_issuer_completes() {
        let p = pending("USDC", "5");
        let hp = HorizonPayment {
            kind: "payment".into(),
            amount: Some("5.0".into()),
            asset_type: Some("credit_alphanum4".into()),
            asset_code: Some("USDC".into()),
            asset_issuer: Some("GUSDC".into()),
            to: Some("GGATEWAY".into()),
            transaction_hash: Some("TXHASH".into()),
            transaction: Some(TransactionRef {
                memo: Some("MEMO1234".into()),
                memo_type: Some("text".into()),
                successful: Some(true),
            }),
            paging_token: Some("1".into()),
            created_at: None,
        };
        assert!(matches!(
            verify(&p, &hp, 0),
            Some(Verdict::Completed { .. })
        ));
    }

    #[test]
    fn usdc_payment_with_wrong_issuer_is_ignored() {
        let p = pending("USDC", "5");
        let mut hp = HorizonPayment {
            kind: "payment".into(),
            amount: Some("5.0".into()),
            asset_type: Some("credit_alphanum4".into()),
            asset_code: Some("USDC".into()),
            asset_issuer: Some("GFAKEISSUER".into()),
            to: Some("GGATEWAY".into()),
            transaction_hash: Some("TXHASH".into()),
            transaction: Some(TransactionRef {
                memo: Some("MEMO1234".into()),
                memo_type: Some("text".into()),
                successful: Some(true),
            }),
            paging_token: Some("1".into()),
            created_at: None,
        };
        assert_eq!(verify(&p, &hp, 0), None);
        // Sanity: with the right issuer it would have matched.
        hp.asset_issuer = Some("GUSDC".into());
        assert!(verify(&p, &hp, 0).is_some());
    }

    #[test]
    fn native_payment_does_not_settle_usdc_intent_without_issuer() {
        /* `ACCEPTED_ASSETS=XLM,USDC` (no issuer) used to persist a USDC intent
        with `asset_issuer: None`, and `verify()` treated issuer-less as native
        — so 100 XLM settled a 100 USDC invoice (issue #221). */
        let mut p = pending("USDC", "100");
        p.asset_issuer = None;
        let hp = native_payment("100.0000000", "MEMO1234", "GGATEWAY");
        assert_eq!(verify(&p, &hp, 0), None);
        // A real USDC credit payment must not match either — there is no issuer
        // to pin the intent to.
        let credit = HorizonPayment {
            kind: "payment".into(),
            amount: Some("100.0".into()),
            asset_type: Some("credit_alphanum4".into()),
            asset_code: Some("USDC".into()),
            asset_issuer: Some("GUSDC".into()),
            to: Some("GGATEWAY".into()),
            transaction_hash: Some("TXHASH".into()),
            transaction: Some(TransactionRef {
                memo: Some("MEMO1234".into()),
                memo_type: Some("text".into()),
                successful: Some(true),
            }),
            paging_token: Some("1".into()),
            created_at: None,
        };
        assert_eq!(verify(&p, &credit, 0), None);
    }

    #[test]
    fn same_code_from_a_different_issuer_does_not_settle() {
        /* Intent priced in USDC from issuer A. A Horizon payment of USDC from
        issuer B must not settle it, even though both share a code (issue #222). */
        let mut p = pending("USDC", "5");
        p.asset_issuer = Some("GISSUER_A".into());
        let mut hp = HorizonPayment {
            kind: "payment".into(),
            amount: Some("5.0".into()),
            asset_type: Some("credit_alphanum4".into()),
            asset_code: Some("USDC".into()),
            asset_issuer: Some("GISSUER_B".into()),
            to: Some("GGATEWAY".into()),
            transaction_hash: Some("TXHASH".into()),
            transaction: Some(TransactionRef {
                memo: Some("MEMO1234".into()),
                memo_type: Some("text".into()),
                successful: Some(true),
            }),
            paging_token: Some("1".into()),
            created_at: None,
        };
        assert_eq!(verify(&p, &hp, 0), None);
        hp.asset_issuer = Some("GISSUER_A".into());
        assert!(verify(&p, &hp, 0).is_some());
    }

    #[test]
    fn non_payment_operation_is_ignored() {
        let p = pending("XLM", "10");
        let mut hp = native_payment("10", "MEMO1234", "GGATEWAY");
        hp.kind = "create_account".into();
        assert_eq!(verify(&p, &hp, 0), None);
    }

    /// A transaction Horizon reports as `successful: false` must never settle an
    /// intent, even when type/destination/memo/asset/amount all match.
    #[test]
    fn failed_transaction_is_ignored() {
        let p = pending("XLM", "10");
        let mut hp = native_payment("10.0000000", "MEMO1234", "GGATEWAY");
        hp.transaction.as_mut().unwrap().successful = Some(false);
        assert_eq!(verify(&p, &hp, 0), None);
        // Sanity: the same record with `successful: true` would have completed.
        hp.transaction.as_mut().unwrap().successful = Some(true);
        assert!(matches!(
            verify(&p, &hp, 0),
            Some(Verdict::Completed { .. })
        ));
    }

    /// A record whose joined transaction omits the `successful` flag is treated
    /// as not-known-successful and rejected — we never settle on an absent flag.
    #[test]
    fn missing_successful_flag_is_ignored() {
        let p = pending("XLM", "10");
        let mut hp = native_payment("10.0000000", "MEMO1234", "GGATEWAY");
        hp.transaction.as_mut().unwrap().successful = None;
        assert_eq!(verify(&p, &hp, 0), None);
    }

    fn native_balance() -> AccountBalance {
        AccountBalance {
            asset_type: Some("native".into()),
            asset_code: None,
            asset_issuer: None,
        }
    }

    fn issued_balance(code: &str, issuer: &str) -> AccountBalance {
        AccountBalance {
            asset_type: Some("credit_alphanum4".into()),
            asset_code: Some(code.into()),
            asset_issuer: Some(issuer.into()),
        }
    }

    #[test]
    fn missing_trustlines_flags_untrusted_issued_asset() {
        // Accepts XLM (native) and USDC:GUSDC, but the account holds only XLM.
        let assets = test_assets();
        let missing = missing_trustlines(&assets, &[native_balance()]);
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].code, "USDC");
    }

    #[test]
    fn missing_trustlines_none_when_all_assets_trusted() {
        let balances = [native_balance(), issued_balance("USDC", "GUSDC")];
        assert!(missing_trustlines(&test_assets(), &balances).is_empty());
    }

    #[test]
    fn missing_trustlines_requires_the_matching_issuer() {
        // Right code, wrong issuer — the trustline is still considered missing.
        let assets = test_assets();
        let balances = [issued_balance("USDC", "GWRONGISSUER")];
        let missing = missing_trustlines(&assets, &balances);
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].code, "USDC");
    }

    #[test]
    fn missing_trustlines_never_flags_native_xlm() {
        // An XLM-only gateway with no balance lines at all still needs no
        // trustline for its native asset.
        let assets = [crate::config::AcceptedAsset {
            code: "XLM".into(),
            issuer: None,
        }];
        assert!(missing_trustlines(&assets, &[]).is_empty());
    }

    #[test]
    fn parses_payment_sse_event() {
        let block = "id: 123456789\nevent: \ndata: {\"type\":\"payment\",\"amount\":\"10.0\"}";
        let ev = parse_sse_block(block);
        assert_eq!(ev.id.as_deref(), Some("123456789"));
        assert_eq!(ev.data, "{\"type\":\"payment\",\"amount\":\"10.0\"}");
    }

    #[test]
    fn parses_open_greeting_event() {
        let block = "retry: 1000\nevent: open\ndata: \"hello\"";
        let ev = parse_sse_block(block);
        assert_eq!(ev.event.as_deref(), Some("open"));
        assert_eq!(ev.data, "\"hello\"");
        // The greeting payload is not a payment record.
        assert!(serde_json::from_str::<HorizonPayment>(&ev.data).is_err());
    }

    #[test]
    fn joins_multiline_sse_data_and_ignores_comments() {
        let block = ": keep-alive\ndata: {\"type\":\ndata: \"payment\"}\nid: 99";
        let ev = parse_sse_block(block);
        assert_eq!(ev.data, "{\"type\":\n\"payment\"}");
        assert_eq!(ev.id.as_deref(), Some("99"));
    }

    #[test]
    fn streamed_payment_deserializes_into_verifiable_record() {
        /* A single Horizon payment record as pushed over SSE (note: a streamed
        record carries its memo inline under `transaction`, same as the page). */
        let data = r#"{
            "type": "payment",
            "amount": "10.0000000",
            "asset_type": "native",
            "to": "GGATEWAY",
            "transaction_hash": "abc",
            "transaction": { "memo": "MEMO1234", "memo_type": "text", "successful": true }
        }"#;
        let hp: HorizonPayment = serde_json::from_str(data).unwrap();
        let p = pending("XLM", "10.00");
        assert!(matches!(
            verify(&p, &hp, 0),
            Some(Verdict::Completed { .. })
        ));
    }

    #[test]
    fn find_event_end_detects_complete_events() {
        assert_eq!(find_event_end(b"data: x\n\nrest"), Some(9));
        assert_eq!(find_event_end(b"data: x\r\n\r\nrest"), Some(11));
        assert_eq!(find_event_end(b"data: partial\n"), None);
    }

    #[test]
    fn deserializes_horizon_payments_page() {
        let body = r#"{
            "_embedded": { "records": [
                {
                    "type": "payment",
                    "amount": "10.0000000",
                    "asset_type": "native",
                    "to": "GGATEWAY",
                    "transaction_hash": "abc",
                    "paging_token": "123456789-1",
                    "transaction": { "memo": "MEMO1234", "memo_type": "text" }
                }
            ]}
        }"#;
        let page: PaymentsPage = serde_json::from_str(body).unwrap();
        assert_eq!(page.embedded.records.len(), 1);
        assert_eq!(page.embedded.records[0].memo(), Some("MEMO1234"));
        assert_eq!(
            page.embedded.records[0].paging_token.as_deref(),
            Some("123456789-1")
        );
    }

    // ── Poller backoff (issue #313) ──────────────────────────────────────────

    #[test]
    fn retry_after_header_parses_delta_seconds() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::RETRY_AFTER, "30".parse().unwrap());
        assert_eq!(parse_retry_after(&headers), Some(Duration::from_secs(30)));
    }

    #[test]
    fn retry_after_header_absent_is_none() {
        assert_eq!(parse_retry_after(&reqwest::header::HeaderMap::new()), None);
    }

    /// The HTTP-date form is deliberately not parsed (see `parse_retry_after`'s
    /// doc comment) — an unrecognised value must fall back to `None`, not panic
    /// or misparse into a nonsense duration.
    #[test]
    fn retry_after_header_http_date_form_is_ignored() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::RETRY_AFTER,
            "Wed, 21 Oct 2026 07:28:00 GMT".parse().unwrap(),
        );
        assert_eq!(parse_retry_after(&headers), None);
    }

    #[test]
    fn is_rate_limited_true_for_429_and_503() {
        for status in [
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
        ] {
            let err = HorizonHttpError {
                status,
                retry_after: None,
                body: String::new(),
            };
            assert!(err.is_rate_limited(), "{status} must be rate_limited");
        }
    }

    #[test]
    fn is_rate_limited_false_for_an_ordinary_server_error() {
        let err = HorizonHttpError {
            status: reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            retry_after: None,
            body: String::new(),
        };
        assert!(!err.is_rate_limited());
    }

    /// A `Retry-After` from Horizon is honored exactly, not folded into or
    /// capped by the exponential schedule.
    #[test]
    fn next_delay_honors_retry_after_over_the_backoff_schedule() {
        assert_eq!(
            next_poll_delay(1, Some(Duration::from_secs(300))),
            Duration::from_secs(300),
            "a large Retry-After must not be clamped by POLL_BACKOFF_MAX"
        );
        assert_eq!(
            next_poll_delay(1, Some(Duration::from_millis(500))),
            Duration::from_millis(500),
            "a small Retry-After must still be honored, not raised to a floor"
        );
    }

    /// Without a `Retry-After`, repeated failures back off exponentially:
    /// equal jitter keeps each delay within `[ceiling/2, ceiling]`
    /// (`webhook::retry_delay`'s contract), and the ceiling for
    /// `consecutive_failures=10` has long since saturated at
    /// `POLL_BACKOFF_MAX` (`POLL_BACKOFF_BASE * 2^9` vastly exceeds it) — so
    /// unlike failure 1, whose ceiling is still `POLL_BACKOFF_BASE`, every
    /// draw for failure 10 must land in the top half of the max.
    #[test]
    fn next_delay_without_retry_after_grows_with_consecutive_failures() {
        let first = next_poll_delay(1, None);
        assert!(
            first <= POLL_BACKOFF_BASE,
            "failure 1's ceiling must still be POLL_BACKOFF_BASE, got {first:?}"
        );

        for _ in 0..20 {
            let tenth = next_poll_delay(10, None);
            assert!(
                tenth >= POLL_BACKOFF_MAX / 2 && tenth <= POLL_BACKOFF_MAX,
                "failure 10 must be within [MAX/2, MAX], got {tenth:?}"
            );
        }
    }
}
