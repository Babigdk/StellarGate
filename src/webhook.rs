//! Outbound webhook delivery.
//!
//! When a payment reaches a terminal state we POST a signed JSON event to the
//! merchant's `webhook_url`. To let receivers prove both authenticity and
//! freshness, every request carries two headers:
//!
//! - `X-StellarGate-Timestamp`: the Unix time (seconds) the event was signed.
//! - `X-StellarGate-Signature`: the hex HMAC-SHA256 of `"{timestamp}.{body}"`
//!   (Stripe-style), keyed with the shared `WEBHOOK_SECRET`.
//! - `X-StellarGate-Event`: a convenience copy of the `event` field from the
//!   body, included so receivers can route cheaply before parsing JSON.
//!   **This header is not covered by the HMAC signature.** It can be altered
//!   in transit without invalidating the signature. Receivers that make
//!   security-sensitive decisions MUST route on the `event` field inside the
//!   verified JSON body, not on this header.
//!
//! Binding the signature to the timestamp stops a captured request from being
//! replayed indefinitely: a receiver recomputes the signature over the same
//! `"{timestamp}.{body}"` string and rejects timestamps that fall outside a
//! small tolerance window. See the README "Verifying webhooks" section for the
//! verification recipe and recommended window.

use crate::supervise::TaskExit;
use crate::{db, AppState};
// `KeyInit` provides `new_from_slice`; it moved off `Mac` in hmac 0.13.
use hmac::{Hmac, KeyInit, Mac};
use serde_json::json;
use sha2::Sha256;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{watch, Semaphore};
use tracing::{debug, info, warn};
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;

/// Compute the hex-encoded HMAC-SHA256 signature for a webhook, binding it to
/// `timestamp` by signing the Stripe-style payload `"{timestamp}.{body}"`.
///
/// The `body` is the full JSON event payload produced by [`build_payload`],
/// which always includes an `"event"` field. Because the event type is part of
/// the signed body, receivers can rely on it being authentic after verifying
/// the signature — no separate signing of the `X-StellarGate-Event` header is
/// needed or performed.
///
/// Receivers must recompute the signature over the same `"{timestamp}.{body}"`
/// string and reject the request if `timestamp` is too far from their own clock
/// (see the README), which is what prevents replay of an old, valid signature.
pub fn sign(secret: &str, timestamp: i64, body: &[u8]) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts keys of any length");
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

/// Current Unix time in seconds, used as the webhook signing timestamp.
pub(crate) fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Build the JSON event payload for a payment in a terminal state.
///
/// `delta` carries the absolute difference between the requested and received
/// amounts, and is included in the payload for `payment.overpaid` (excess to
/// refund) and `payment.underpaid` (shortfall still owed) events.
///
/// Amounts are canonicalized to ensure consistent serialization: "10.00", "10.0",
/// and "10" all serialize as "10". This handles both new payments (already
/// canonicalized on write) and any legacy data.
pub fn build_payload(payment: &db::Payment, event: &str, delta: Option<&str>) -> serde_json::Value {
    // Canonicalize the requested amount
    let canonical_amount = crate::money::parse_stroops(&payment.amount)
        .map(crate::money::stroops_to_string)
        .unwrap_or_else(|| payment.amount.clone());

    // Canonicalize the received amount
    let canonical_paid_amount = payment
        .paid_amount
        .as_ref()
        .and_then(|pa| crate::money::parse_stroops(pa).map(crate::money::stroops_to_string));

    // Canonicalize delta if present (it's a price difference)
    let canonical_delta =
        delta.and_then(|d| crate::money::parse_stroops(d).map(crate::money::stroops_to_string));

    let mut payload = json!({
        "event": event,
        "payment_id": payment.id,
        "merchant_id": payment.merchant_id,
        "tx_hash": payment.tx_hash,
        "amount": canonical_amount,
        "paid_amount": canonical_paid_amount,
        "asset": payment.asset,
        "asset_issuer": payment.asset_issuer,
        "status": payment.status,
    });
    if let Some(d) = canonical_delta {
        payload["delta"] = json!(d);
    }
    payload
}

/// Dispatch a webhook for `payment` if it has a `webhook_url`, retrying on
/// failure per the configured policy. Each attempt and the final outcome are
/// recorded in the `webhook_deliveries` table. Errors are logged, never
/// propagated — a failed webhook must not roll back a confirmed payment.
///
/// The signed body (`"{timestamp}.{body}"`) already contains the `event` field,
/// so the event type is fully authenticated by the HMAC signature. The
/// `X-StellarGate-Event` header is also included as a routing convenience but
/// is **not** covered by the signature — receivers must not use it for
/// security-sensitive decisions.
///
/// `delta` is the absolute amount difference included in overpaid/underpaid
/// events; pass `None` for exact-payment events.
pub async fn dispatch(state: &AppState, payment: &db::Payment, event: &str, delta: Option<&str>) {
    let Some(url) = payment.webhook_url.clone() else {
        return;
    };

    let payload = build_payload(payment, event, delta);
    let body = match serde_json::to_vec(&payload) {
        Ok(b) => b,
        Err(e) => {
            warn!(payment_id = %payment.id, error = %e, "failed to serialize webhook payload");
            return;
        }
    };
    let timestamp = current_timestamp();
    let signature = sign(&state.config.webhook_secret, timestamp, &body);

    let delivery_id = Uuid::new_v4().to_string();
    if let Err(e) = db::save_webhook_delivery(
        &state.pool,
        &delivery_id,
        &payment.id,
        &url,
        &String::from_utf8_lossy(&body),
        event,
    )
    .await
    {
        warn!(error = %e, "failed to record webhook delivery");
    }

    let client = match safe_client(state, &url).await {
        Ok(c) => c,
        Err(e) => {
            warn!(payment_id = %payment.id, %url, error = %e, "webhook blocked by SSRF guard");
            /* This is a terminal failure and must be counted like one. It was
            not, so an entire class of permanent failure — a target that
            resolves into a blocked range — was invisible to
            `stellargate_webhook_deliveries_total{outcome="failed"}` and to any
            alert built on it (issues #319, #233). */
            state.webhook_metrics.record_failed();
            let _ = db::update_webhook_delivery(&state.pool, &delivery_id, "failed", 0).await;
            return;
        }
    };

    let attempts = state.config.webhook_retry_attempts.max(1);
    let base_delay = Duration::from_millis(state.config.webhook_retry_delay_ms);
    let max_delay = Duration::from_millis(state.config.webhook_retry_max_delay_ms);
    let start = Instant::now();

    for attempt in 1..=attempts {
        // Every attempt after the first is a retry — record it.
        if attempt > 1 {
            state.webhook_metrics.record_retry();
        }

        let result = client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("X-StellarGate-Signature", &signature)
            .header("X-StellarGate-Timestamp", timestamp.to_string())
            .header("X-StellarGate-Event", event)
            .body(body.clone())
            .send()
            .await;

        match result {
            Ok(resp) if resp.status().is_success() => {
                info!(payment_id = %payment.id, %url, attempt, "webhook delivered");
                state.webhook_metrics.record_delivered();
                state
                    .webhook_metrics
                    .record_latency_ms(start.elapsed().as_millis() as u64);
                let _ = db::update_webhook_delivery(
                    &state.pool,
                    &delivery_id,
                    "delivered",
                    attempt as i64,
                )
                .await;
                return;
            }
            Ok(resp) => {
                warn!(payment_id = %payment.id, status = %resp.status(), attempt, "webhook rejected");
            }
            Err(e) => {
                warn!(payment_id = %payment.id, error = %e, attempt, "webhook request failed");
            }
        }

        if attempt < attempts {
            tokio::time::sleep(retry_delay(attempt, base_delay, max_delay)).await;
        }
    }

    warn!(payment_id = %payment.id, %url, "webhook delivery exhausted all retries");
    state.webhook_metrics.record_failed();
    state
        .webhook_metrics
        .record_latency_ms(start.elapsed().as_millis() as u64);
    let _ = db::update_webhook_delivery(&state.pool, &delivery_id, "failed", attempts as i64).await;
}

/// How long to wait before inline retry `attempt + 1`, given the configured
/// base and cap.
///
/// Two properties, and the issue needs both (#318).
///
/// **Growth.** `base * 2^(attempt-1)`, capped at `max`. A constant delay meant
/// a receiver returning 503 for two minutes saw, per delivery, three attempts
/// at `t`, `t+5s`, `t+10s` — and across a settlement burst of N payments, `3N`
/// requests arriving in three tight clusters, precisely while the receiver was
/// least able to absorb them.
///
/// **Jitter**, because growth alone does not desynchronise anything. Deliveries
/// that fail together share an attempt number, so a purely exponential schedule
/// puts their next attempts at the same instant — the same lockstep, just
/// further apart.
///
/// This is *equal* jitter — uniform in `[ceiling/2, ceiling]` — rather than the
/// more common full jitter over `[0, ceiling]`. Full jitter can return a
/// near-zero delay, and this service already treats that as a misconfiguration:
/// `WEBHOOK_RETRY_DELAY_MS == 0` with retries enabled is rejected at boot
/// because "a zero delay causes retry bursts that hammer the target endpoint".
/// Equal jitter keeps a guaranteed floor under every retry while still spreading
/// a co-failing batch across half the window.
pub fn retry_delay(attempt: u32, base: Duration, max: Duration) -> Duration {
    /* `2^(attempt-1)`, saturating rather than wrapping: a large
    WEBHOOK_RETRY_ATTEMPTS must clamp to `max`, not overflow to a tiny delay. */
    let factor = 2u32.saturating_pow(attempt.saturating_sub(1));
    let ceiling = base.saturating_mul(factor).min(max);

    let ceiling_ms = ceiling.as_millis() as u64;
    if ceiling_ms == 0 {
        return Duration::ZERO;
    }
    let half = ceiling_ms / 2;
    Duration::from_millis(half + rand::random_range(0..=(ceiling_ms - half)))
}

/// Resolve and SSRF-check `url`, returning a client pinned to the validated
/// address. The client is built with the configured `WEBHOOK_TIMEOUT_SECS`
/// timeout so that each delivery attempt is independently bounded. Honors
/// `webhook_allow_private_targets` for local dev/tests that intentionally
/// target a loopback mock server.
pub(crate) async fn safe_client(state: &AppState, url: &str) -> anyhow::Result<reqwest::Client> {
    let target = crate::ssrf::validate(url, state.config.webhook_allow_private_targets).await?;
    Ok(crate::ssrf::pinned_client(
        &target,
        Duration::from_secs(state.config.webhook_timeout_secs),
    )?)
}

/// Re-attempt every webhook delivery left `pending` or `failed` by a process
/// that exited mid-delivery (or a receiver that was down when retries were
/// exhausted), bounded to `webhook_redrive_concurrency` requests in flight at
/// once. Returns how many deliveries were attempted.
///
/// This is the safety net behind `dispatch()`'s inline retry loop: dispatch
/// already delivers the common case synchronously, but a crash between
/// recording a delivery and reaching a terminal status otherwise leaves that
/// row stuck forever. Called once immediately on worker startup (see
/// [`run_redrive_worker`]) so a restart redrives without waiting a full tick.
pub async fn redrive_once(state: &Arc<AppState>) -> usize {
    let candidates = match db::list_redrivable_deliveries(
        &state.pool,
        state.config.webhook_redrive_max_attempts as i64,
        state.config.webhook_redrive_grace_secs,
        state.config.webhook_redrive_backoff_initial_secs,
        state.config.webhook_redrive_backoff_max_secs,
        state.config.webhook_redrive_jitter_secs,
    )
    .await
    {
        Ok(rows) => rows,
        Err(e) => {
            warn!(error = %e, "failed to list redrivable webhook deliveries");
            return 0;
        }
    };

    if candidates.is_empty() {
        return 0;
    }

    let semaphore = Arc::new(Semaphore::new(
        state.config.webhook_redrive_concurrency.max(1),
    ));
    let mut tasks = Vec::with_capacity(candidates.len());
    for delivery in candidates {
        let state = state.clone();
        let semaphore = semaphore.clone();
        tasks.push(tokio::spawn(async move {
            let _permit = semaphore
                .acquire_owned()
                .await
                .expect("semaphore is never closed");
            redrive_one(&state, delivery).await;
        }));
    }

    let attempted = tasks.len();
    for task in tasks {
        let _ = task.await;
    }
    attempted
}

/// Re-send one delivery's stored payload and record the outcome. Mirrors the
/// manual `POST /payments/:id/webhook_deliveries/:id/redeliver` path, but is
/// driven by the background worker instead of a merchant request, and re-signs
/// with a fresh timestamp on every attempt (the receiver's replay-tolerance
/// window is measured from when the request actually lands).
async fn redrive_one(state: &Arc<AppState>, delivery: db::WebhookDelivery) {
    let event = delivery.event();
    let attempt = delivery.attempts + 1;

    let client = match safe_client(state, &delivery.url).await {
        Ok(c) => c,
        Err(e) => {
            warn!(delivery_id = %delivery.id, url = %delivery.url, error = %e, "redrive blocked by SSRF guard");
            // Terminal, so counted — same gap as the inline path above.
            state.webhook_metrics.record_failed();
            let _ =
                db::update_webhook_delivery(&state.pool, &delivery.id, "failed", delivery.attempts)
                    .await;
            return;
        }
    };

    let body = delivery.payload.as_bytes();
    let timestamp = current_timestamp();
    let signature = sign(&state.config.webhook_secret, timestamp, body);
    let start = Instant::now();

    // Every redrive attempt is effectively a retry.
    state.webhook_metrics.record_retry();

    let result = client
        .post(&delivery.url)
        .header("Content-Type", "application/json")
        .header("X-StellarGate-Signature", &signature)
        .header("X-StellarGate-Timestamp", timestamp.to_string())
        .header("X-StellarGate-Event", &event)
        .body(body.to_vec())
        .send()
        .await;

    let outcome = match result {
        Ok(resp) if resp.status().is_success() => {
            info!(delivery_id = %delivery.id, %attempt, "webhook redriven successfully");
            state.webhook_metrics.record_delivered();
            state
                .webhook_metrics
                .record_latency_ms(start.elapsed().as_millis() as u64);
            "delivered"
        }
        Ok(resp) => {
            warn!(delivery_id = %delivery.id, status = %resp.status(), %attempt, "redrive attempt rejected");
            if attempt >= state.config.webhook_redrive_max_attempts as i64 {
                state.webhook_metrics.record_failed();
                state
                    .webhook_metrics
                    .record_latency_ms(start.elapsed().as_millis() as u64);
                "failed"
            } else {
                "pending"
            }
        }
        Err(e) => {
            warn!(delivery_id = %delivery.id, error = %e, %attempt, "redrive attempt failed");
            if attempt >= state.config.webhook_redrive_max_attempts as i64 {
                state.webhook_metrics.record_failed();
                state
                    .webhook_metrics
                    .record_latency_ms(start.elapsed().as_millis() as u64);
                "failed"
            } else {
                "pending"
            }
        }
    };

    let _ = db::update_webhook_delivery(&state.pool, &delivery.id, outcome, attempt).await;
}

/// Background loop that periodically redrives stuck webhook deliveries until
/// the process shuts down. Runs one pass immediately on startup — before the
/// first sleep — so a restart repairs any deliveries left `pending`/`failed`
/// by the previous process without waiting a full interval.
pub async fn run_redrive_worker(
    state: Arc<AppState>,
    mut shutdown: watch::Receiver<bool>,
) -> TaskExit {
    let interval = Duration::from_secs(state.config.webhook_redrive_interval_secs.max(1));
    info!(
        interval_secs = state.config.webhook_redrive_interval_secs,
        "webhook redrive worker started"
    );

    loop {
        match redrive_once(&state).await {
            0 => debug!("redrive: nothing to redrive"),
            n => info!(redriven = n, "redrive tick processed deliveries"),
        }

        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = shutdown.changed() => {
                info!("webhook redrive worker shutting down");
                return TaskExit::ShutdownRequested;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Inline retry backoff and jitter (issue #318) ─────────────────────────

    const BASE: Duration = Duration::from_millis(1_000);
    const MAX: Duration = Duration::from_millis(30_000);

    /// The ceiling doubles per attempt: a constant delay meant a settlement
    /// burst of N payments produced 3N requests in three tight clusters,
    /// arriving exactly while a restarting receiver was least able to take
    /// them.
    #[test]
    fn inline_retry_delay_grows_exponentially() {
        // Equal jitter puts every sample in [ceiling/2, ceiling], so the
        // *whole range* for attempt n sits at or above attempt n-1's ceiling
        // only once doubled — assert on the bounds rather than one sample.
        for (attempt, lo, hi) in [(1, 500, 1_000), (2, 1_000, 2_000), (3, 2_000, 4_000)] {
            for _ in 0..200 {
                let d = retry_delay(attempt, BASE, MAX).as_millis() as u64;
                assert!(
                    (lo..=hi).contains(&d),
                    "attempt {attempt} delay {d}ms outside [{lo}, {hi}]"
                );
            }
        }
    }

    #[test]
    fn inline_retry_delay_is_capped() {
        // Attempt 20 would be 1s * 2^19 ≈ 145 hours without the cap.
        for _ in 0..200 {
            assert!(retry_delay(20, BASE, MAX) <= MAX);
        }
    }

    /// Saturating arithmetic, not wrapping: an absurd attempt number must
    /// clamp to the cap rather than overflow into a tiny delay and reproduce
    /// the burst this issue is about.
    #[test]
    fn inline_retry_delay_does_not_overflow_to_a_small_value() {
        let d = retry_delay(u32::MAX, BASE, MAX);
        assert!(d >= MAX / 2 && d <= MAX, "got {d:?}");
    }

    /// The property the issue actually asks for: deliveries that fail at the
    /// same moment must not retry at the same moment. Without jitter every one
    /// of these samples would be identical.
    #[test]
    fn simultaneous_failures_do_not_retry_simultaneously() {
        let samples: std::collections::HashSet<u128> = (0..500)
            .map(|_| retry_delay(1, BASE, MAX).as_millis())
            .collect();
        assert!(
            samples.len() > 100,
            "500 deliveries failing together produced only {} distinct retry \
             delays — they would still arrive in lockstep",
            samples.len()
        );
    }

    /// Equal jitter, not full jitter. `WEBHOOK_RETRY_DELAY_MS == 0` is rejected
    /// at boot because "a zero delay causes retry bursts that hammer the target
    /// endpoint"; jitter that could return ~0 would reintroduce exactly that.
    #[test]
    fn jitter_never_collapses_the_delay_to_zero() {
        for _ in 0..500 {
            assert!(
                retry_delay(1, BASE, MAX) >= BASE / 2,
                "every retry keeps a floor of half the configured base delay"
            );
        }
    }

    /// A zero base (only reachable with retries disabled, which the config
    /// validator enforces) must not panic on the modulo.
    #[test]
    fn zero_base_delay_is_handled() {
        assert_eq!(
            retry_delay(1, Duration::ZERO, Duration::ZERO),
            Duration::ZERO
        );
    }

    #[test]
    fn signature_is_deterministic() {
        // Identical inputs always produce the same digest.
        let body = b"{\"event\":\"payment.success\"}";
        assert_eq!(
            sign("key", 1_700_000_000, body),
            sign("key", 1_700_000_000, body)
        );
    }

    #[test]
    fn signature_matches_known_vector() {
        /* Locks the Stripe-style signed payload format "{timestamp}.{body}".
        Independently reproducible:
          printf '1700000000.{"id":"evt_1"}' | openssl dgst -sha256 -hmac whsec_test */
        let sig = sign("whsec_test", 1_700_000_000, br#"{"id":"evt_1"}"#);
        assert_eq!(
            sig,
            "c89214b5b5da833daed6f0b8c5bb6bd58cea9022bd80ccc78230f3942d632925"
        );
    }

    #[test]
    fn signature_covers_timestamp() {
        /* The whole point of the timestamp: changing only it changes the
        signature, so an old signature cannot be replayed with a fresh time. */
        let body = b"{\"event\":\"payment.completed\"}";
        assert_ne!(
            sign("secret", 1_700_000_000, body),
            sign("secret", 1_700_000_001, body),
            "changing only the timestamp must change the signature"
        );
    }

    #[test]
    fn signature_changes_with_secret() {
        let body = b"{\"event\":\"payment.success\"}";
        assert_ne!(sign("secret-a", 1, body), sign("secret-b", 1, body));
    }

    #[test]
    fn build_payload_includes_event_in_signed_body() {
        // The `event` field must be present in the JSON body so that after
        // signature verification a receiver can read the authenticated event
        // type from the body rather than from the unsigned X-StellarGate-Event
        // header. This test locks that contract.
        let payment = db::Payment {
            id: "pay_1".into(),
            merchant_id: "merchant_1".into(),
            destination_address: "GDESTINATION".into(),
            memo: "ABCD1234".into(),
            amount: "10".into(),
            asset: "XLM".into(),
            status: "completed".into(),
            tx_hash: Some("txhash".into()),
            paid_amount: Some("10".into()),
            webhook_url: None,
            created_at: "2026-01-01T00:00:00".into(),
            updated_at: "2026-01-01T00:00:01".into(),
            expires_at: "2026-01-01T01:00:00".into(),
            asset_issuer: None,
        };

        for event in &[
            "payment.completed",
            "payment.overpaid",
            "payment.underpaid",
            "payment.expired",
        ] {
            let payload = build_payload(&payment, event, None);
            assert_eq!(
                payload["event"].as_str(),
                Some(*event),
                "build_payload must embed the event type in the JSON body \
                 so it is covered by the HMAC signature (event={event})"
            );

            // Confirm the serialised bytes contain the event string, i.e. that
            // it survives the round-trip through serde_json::to_vec used in dispatch().
            let body = serde_json::to_vec(&payload).unwrap();
            let body_str = String::from_utf8(body).unwrap();
            assert!(
                body_str.contains(event),
                "serialised body must contain the event string (event={event})"
            );
        }
    }
}
