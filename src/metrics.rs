//! In-process metrics: atomic counters and a latency histogram for webhook
//! delivery outcomes.
//!
//! All types are cheaply clonable (backed by `Arc`-wrapped atomics) so they
//! can be stored on `AppState` and shared across handlers and background tasks
//! without additional synchronisation.
//!
//! ## Exposition
//! `GET /metrics` returns a plain-text Prometheus-compatible snapshot so any
//! standard scraper can ingest the data with zero configuration.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Histogram buckets for webhook delivery latency (milliseconds).
/// Covers the range from sub-10 ms fast paths up to the 10 s default timeout.
const LATENCY_BUCKETS_MS: &[u64] = &[10, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000];

#[derive(Clone)]
pub struct WebhookMetrics {
    inner: Arc<WebhookMetricsInner>,
}

struct WebhookMetricsInner {
    /// Deliveries that reached the endpoint and received a 2xx response.
    delivered: AtomicU64,
    /// Deliveries that exhausted all retry attempts without a success.
    failed: AtomicU64,
    /// Individual retry attempts (i.e. attempts after the first try).
    retried: AtomicU64,
    /// Sum of all delivery latencies in milliseconds (for computing mean).
    latency_sum_ms: AtomicU64,
    /// Total completed delivery attempts (for mean denominator).
    latency_count: AtomicU64,
    /// Per-bucket counts. Index `i` corresponds to `LATENCY_BUCKETS_MS[i]`;
    /// the last slot is the `+Inf` bucket.
    latency_buckets: [AtomicU64; 10],
}

impl Default for WebhookMetricsInner {
    fn default() -> Self {
        Self {
            delivered: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            retried: AtomicU64::new(0),
            latency_sum_ms: AtomicU64::new(0),
            latency_count: AtomicU64::new(0),
            // 9 explicit buckets + 1 +Inf = 10 slots
            latency_buckets: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
        }
    }
}

impl WebhookMetrics {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(WebhookMetricsInner::default()),
        }
    }

    /// Record a successful delivery (2xx response received).
    pub fn record_delivered(&self) {
        self.inner.delivered.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a final delivery failure (all retries exhausted without success).
    pub fn record_failed(&self) {
        self.inner.failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one retry attempt (every attempt after the first try).
    pub fn record_retry(&self) {
        self.inner.retried.fetch_add(1, Ordering::Relaxed);
    }

    /// Record the end-to-end latency for one delivery, in milliseconds.
    ///
    /// Histogram buckets are cumulative: a 75 ms observation increments every
    /// bucket whose `le` bound is ≥ 75 (i.e. `le="100"`, `le="250"`, …
    /// `le="+Inf"`), matching the Prometheus exposition format.
    pub fn record_latency_ms(&self, ms: u64) {
        self.inner.latency_sum_ms.fetch_add(ms, Ordering::Relaxed);
        self.inner.latency_count.fetch_add(1, Ordering::Relaxed);
        for (i, &bound) in LATENCY_BUCKETS_MS.iter().enumerate() {
            if ms <= bound {
                self.inner.latency_buckets[i].fetch_add(1, Ordering::Relaxed);
            }
        }
        // +Inf bucket is always incremented.
        self.inner.latency_buckets[LATENCY_BUCKETS_MS.len()].fetch_add(1, Ordering::Relaxed);
    }

    // ── Snapshot accessors ────────────────────────────────────────────────

    pub fn delivered(&self) -> u64 {
        self.inner.delivered.load(Ordering::Relaxed)
    }
    pub fn failed(&self) -> u64 {
        self.inner.failed.load(Ordering::Relaxed)
    }
    pub fn retried(&self) -> u64 {
        self.inner.retried.load(Ordering::Relaxed)
    }
    pub fn latency_sum_ms(&self) -> u64 {
        self.inner.latency_sum_ms.load(Ordering::Relaxed)
    }
    pub fn latency_count(&self) -> u64 {
        self.inner.latency_count.load(Ordering::Relaxed)
    }
    pub fn latency_bucket(&self, i: usize) -> u64 {
        self.inner.latency_buckets[i].load(Ordering::Relaxed)
    }
}

impl Default for WebhookMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Outcome counters for `auth_middleware`, so credential-stuffing or
/// misconfigured-client traffic is visible in the `/metrics` scrape rather
/// than only in logs.
#[derive(Clone)]
pub struct AuthMetrics {
    inner: Arc<AuthMetricsInner>,
}

struct AuthMetricsInner {
    /// Requests that presented a valid API key.
    success: AtomicU64,
    /// Requests with no (or a malformed) `Authorization: Bearer` header.
    failure_missing_key: AtomicU64,
    /// Requests with a well-formed key that didn't match any merchant.
    failure_invalid_key: AtomicU64,
    /// Requests that failed the key lookup itself (database error).
    failure_internal_error: AtomicU64,
}

impl Default for AuthMetricsInner {
    fn default() -> Self {
        Self {
            success: AtomicU64::new(0),
            failure_missing_key: AtomicU64::new(0),
            failure_invalid_key: AtomicU64::new(0),
            failure_internal_error: AtomicU64::new(0),
        }
    }
}

impl AuthMetrics {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(AuthMetricsInner::default()),
        }
    }

    pub fn record_success(&self) {
        self.inner.success.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_failure_missing_key(&self) {
        self.inner
            .failure_missing_key
            .fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_failure_invalid_key(&self) {
        self.inner
            .failure_invalid_key
            .fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_failure_internal_error(&self) {
        self.inner
            .failure_internal_error
            .fetch_add(1, Ordering::Relaxed);
    }

    // ── Snapshot accessors ────────────────────────────────────────────────

    pub fn success(&self) -> u64 {
        self.inner.success.load(Ordering::Relaxed)
    }
    pub fn failure_missing_key(&self) -> u64 {
        self.inner.failure_missing_key.load(Ordering::Relaxed)
    }
    pub fn failure_invalid_key(&self) -> u64 {
        self.inner.failure_invalid_key.load(Ordering::Relaxed)
    }
    pub fn failure_internal_error(&self) -> u64 {
        self.inner.failure_internal_error.load(Ordering::Relaxed)
    }
}

impl Default for AuthMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Outcome counters for the Horizon poller's cycles, so throttling or
/// sustained failure is a queryable fact on the `/metrics` scrape rather than
/// only a `warn!` line indistinguishable from a one-off blip (issue #313).
#[derive(Clone)]
pub struct HorizonMetrics {
    inner: Arc<HorizonMetricsInner>,
}

struct HorizonMetricsInner {
    /// Cycles that completed without error (whether or not anything settled).
    success: AtomicU64,
    /// Cycles that failed on a `429`/`503` from Horizon.
    rate_limited: AtomicU64,
    /// Cycles that failed for any other reason.
    error: AtomicU64,
    /// Times the SSE stream listener reconnected — a closed connection, an
    /// HTTP error, or (issue #312) an idle timeout with no error at all. A
    /// persistently-reconnecting stream is the alertable signal that a
    /// half-open connection is repeatedly disabling live payment detection.
    stream_reconnects: AtomicU64,
}

impl Default for HorizonMetricsInner {
    fn default() -> Self {
        Self {
            success: AtomicU64::new(0),
            rate_limited: AtomicU64::new(0),
            error: AtomicU64::new(0),
            stream_reconnects: AtomicU64::new(0),
        }
    }
}

impl HorizonMetrics {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(HorizonMetricsInner::default()),
        }
    }

    pub fn record_success(&self) {
        self.inner.success.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_rate_limited(&self) {
        self.inner.rate_limited.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_error(&self) {
        self.inner.error.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_stream_reconnect(&self) {
        self.inner.stream_reconnects.fetch_add(1, Ordering::Relaxed);
    }

    // ── Snapshot accessors ────────────────────────────────────────────────

    pub fn success(&self) -> u64 {
        self.inner.success.load(Ordering::Relaxed)
    }
    pub fn rate_limited(&self) -> u64 {
        self.inner.rate_limited.load(Ordering::Relaxed)
    }
    pub fn error(&self) -> u64 {
        self.inner.error.load(Ordering::Relaxed)
    }
    pub fn stream_reconnects(&self) -> u64 {
        self.inner.stream_reconnects.load(Ordering::Relaxed)
    }
}

impl Default for HorizonMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-asset gateway trustline state, refreshed by every call to
/// `horizon::check_trustlines` — at boot and, since trustlines can be revoked
/// or `ACCEPTED_ASSETS` extended at any time after that, on the recurring
/// trustline-checker task as well.
///
/// A Horizon failure while checking must not read the same as a confirmed
/// absence: [`Self::record_check_failure`] only bumps `check_failures` and
/// leaves the per-asset map untouched, so a stale "missing" or "present"
/// entry survives an outage rather than being overwritten by a guess.
/// `last_success_unix` (0 until the first successful check) is how a scrape
/// tells "we have never confirmed this" apart from "confirmed and stale".
#[derive(Clone)]
pub struct TrustlineMetrics {
    inner: Arc<TrustlineMetricsInner>,
}

struct TrustlineMetricsInner {
    /// Asset code -> confirmed missing (`true`) or confirmed present
    /// (`false`). Only ever written by a successful check; a code absent from
    /// the map has simply never been confirmed either way.
    missing: Mutex<HashMap<String, bool>>,
    /// Checks that could not reach Horizon or got a non-2xx response.
    check_failures: AtomicU64,
    /// Unix timestamp of the last check that got a confirmed answer from
    /// Horizon; `0` means never.
    last_success_unix: AtomicI64,
}

impl Default for TrustlineMetricsInner {
    fn default() -> Self {
        Self {
            missing: Mutex::new(HashMap::new()),
            check_failures: AtomicU64::new(0),
            last_success_unix: AtomicI64::new(0),
        }
    }
}

impl TrustlineMetrics {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(TrustlineMetricsInner::default()),
        }
    }

    /// Record a successful check: `checked` is every non-native accepted
    /// asset the check evaluated, `missing` the subset with no trustline.
    /// Replaces the prior state for exactly the assets checked, so an asset
    /// removed from `ACCEPTED_ASSETS` between checks simply stops being
    /// reported rather than lingering at its last known value.
    pub fn record_check<'a>(&self, checked: impl IntoIterator<Item = &'a str>, missing: &[String]) {
        let mut map = self.inner.missing.lock().unwrap();
        map.clear();
        for code in checked {
            map.insert(code.to_string(), missing.iter().any(|m| m == code));
        }
        drop(map);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        self.inner.last_success_unix.store(now, Ordering::Relaxed);
    }

    pub fn record_check_failure(&self) {
        self.inner.check_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// `Some(true)` — confirmed missing. `Some(false)` — confirmed present.
    /// `None` — never confirmed either way (not yet checked, or dropped from
    /// `ACCEPTED_ASSETS`).
    pub fn is_missing(&self, code: &str) -> Option<bool> {
        self.inner.missing.lock().unwrap().get(code).copied()
    }

    pub fn check_failures(&self) -> u64 {
        self.inner.check_failures.load(Ordering::Relaxed)
    }

    pub fn last_success_unix(&self) -> i64 {
        self.inner.last_success_unix.load(Ordering::Relaxed)
    }

    /// Snapshot for rendering, sorted by asset code for deterministic output.
    pub fn snapshot(&self) -> Vec<(String, bool)> {
        let map = self.inner.missing.lock().unwrap();
        let mut out: Vec<_> = map.iter().map(|(k, v)| (k.clone(), *v)).collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }
}

impl Default for TrustlineMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// ── Prometheus text exposition ────────────────────────────────────────────────

/// Render webhook delivery, auth outcome, background-task, and Horizon poll
/// metrics as a Prometheus-compatible plain-text snapshot. Called by
/// `GET /metrics`.
pub fn render(
    webhook: &WebhookMetrics,
    auth: &AuthMetrics,
    tasks: &crate::TaskHealth,
    horizon: &HorizonMetrics,
    trustlines: &TrustlineMetrics,
) -> String {
    let mut out = String::with_capacity(1024);

    // stellargate_webhook_deliveries_total — counter vec by outcome
    out.push_str(
        "# HELP stellargate_webhook_deliveries_total Total webhook delivery attempts by outcome.\n",
    );
    out.push_str("# TYPE stellargate_webhook_deliveries_total counter\n");
    out.push_str(&format!(
        "stellargate_webhook_deliveries_total{{outcome=\"delivered\"}} {}\n",
        webhook.delivered()
    ));
    out.push_str(&format!(
        "stellargate_webhook_deliveries_total{{outcome=\"failed\"}} {}\n",
        webhook.failed()
    ));

    // stellargate_webhook_retries_total — counter
    out.push_str("# HELP stellargate_webhook_retries_total Total webhook retry attempts (excludes the first try).\n");
    out.push_str("# TYPE stellargate_webhook_retries_total counter\n");
    out.push_str(&format!(
        "stellargate_webhook_retries_total {}\n",
        webhook.retried()
    ));

    // stellargate_webhook_delivery_latency_ms — histogram
    out.push_str("# HELP stellargate_webhook_delivery_latency_ms End-to-end webhook delivery latency in milliseconds.\n");
    out.push_str("# TYPE stellargate_webhook_delivery_latency_ms histogram\n");
    for (i, &bound) in LATENCY_BUCKETS_MS.iter().enumerate() {
        out.push_str(&format!(
            "stellargate_webhook_delivery_latency_ms_bucket{{le=\"{}\"}} {}\n",
            bound,
            webhook.latency_bucket(i)
        ));
    }
    out.push_str(&format!(
        "stellargate_webhook_delivery_latency_ms_bucket{{le=\"+Inf\"}} {}\n",
        webhook.latency_bucket(LATENCY_BUCKETS_MS.len())
    ));
    out.push_str(&format!(
        "stellargate_webhook_delivery_latency_ms_sum {}\n",
        webhook.latency_sum_ms()
    ));
    out.push_str(&format!(
        "stellargate_webhook_delivery_latency_ms_count {}\n",
        webhook.latency_count()
    ));

    // stellargate_auth_attempts_total — counter vec by outcome/reason
    out.push_str(
        "# HELP stellargate_auth_attempts_total Total auth middleware decisions by outcome and reason.\n",
    );
    out.push_str("# TYPE stellargate_auth_attempts_total counter\n");
    out.push_str(&format!(
        "stellargate_auth_attempts_total{{outcome=\"success\"}} {}\n",
        auth.success()
    ));
    out.push_str(&format!(
        "stellargate_auth_attempts_total{{outcome=\"failure\",reason=\"missing_key\"}} {}\n",
        auth.failure_missing_key()
    ));
    out.push_str(&format!(
        "stellargate_auth_attempts_total{{outcome=\"failure\",reason=\"invalid_key\"}} {}\n",
        auth.failure_invalid_key()
    ));
    out.push_str(&format!(
        "stellargate_auth_attempts_total{{outcome=\"failure\",reason=\"internal_error\"}} {}\n",
        auth.failure_internal_error()
    ));

    // Background task counters (issue #316): a crash-looping worker must be
    // visible on the scrape, not only as a log line at shutdown.
    out.push_str(
        "# HELP stellargate_tasks_started_total Total background task starts (including restarts).\n",
    );
    out.push_str("# TYPE stellargate_tasks_started_total counter\n");
    out.push_str(&format!(
        "stellargate_tasks_started_total {}\n",
        tasks.started()
    ));
    out.push_str("# HELP stellargate_tasks_stopped_total Total background task clean stops.\n");
    out.push_str("# TYPE stellargate_tasks_stopped_total counter\n");
    out.push_str(&format!(
        "stellargate_tasks_stopped_total {}\n",
        tasks.stopped()
    ));
    out.push_str("# HELP stellargate_tasks_failed_total Total background task panics.\n");
    out.push_str("# TYPE stellargate_tasks_failed_total counter\n");
    out.push_str(&format!(
        "stellargate_tasks_failed_total {}\n",
        tasks.failed()
    ));
    out.push_str(
        "# HELP stellargate_task_restarts_total Supervisor restarts of a background task after panic or unexpected return.\n",
    );
    out.push_str("# TYPE stellargate_task_restarts_total counter\n");
    let snaps = tasks.snapshot();
    for snap in &snaps {
        out.push_str(&format!(
            "stellargate_task_restarts_total{{task=\"{}\"}} {}\n",
            snap.name, snap.restarts
        ));
    }
    out.push_str(
        "# HELP stellargate_task_running Whether the named background task is currently running (1) or not (0).\n",
    );
    out.push_str("# TYPE stellargate_task_running gauge\n");
    for snap in &snaps {
        out.push_str(&format!(
            "stellargate_task_running{{task=\"{}\"}} {}\n",
            snap.name,
            if snap.running { 1 } else { 0 }
        ));
    }
    out.push_str(
        "# HELP stellargate_task_consecutive_failures Consecutive panics of a background task since it last ran stably.\n",
    );
    out.push_str("# TYPE stellargate_task_consecutive_failures gauge\n");
    for snap in &snaps {
        out.push_str(&format!(
            "stellargate_task_consecutive_failures{{task=\"{}\"}} {}\n",
            snap.name, snap.consecutive_failures
        ));
    }

    /* Expected-versus-live (issue #317). The raw counters could not answer
    "how many workers should be running, and how many are?": `stopped` was
    overloaded across clean shutdown, configuration-disabled exit and fault, so
    `started - stopped - failed` was not a live count and there was nothing to
    compare it against. These two gauges are that comparison, and
    `expected` already excludes deliberately-disabled workers. Alert on
    `stellargate_tasks_live < stellargate_tasks_expected`. */
    out.push_str(
        "# HELP stellargate_tasks_expected Background workers this deployment expects to be running, excluding any disabled by configuration.\n",
    );
    out.push_str("# TYPE stellargate_tasks_expected gauge\n");
    out.push_str(&format!(
        "stellargate_tasks_expected {}\n",
        tasks.expected_tasks()
    ));
    out.push_str("# HELP stellargate_tasks_live Expected background workers currently running.\n");
    out.push_str("# TYPE stellargate_tasks_live gauge\n");
    out.push_str(&format!("stellargate_tasks_live {}\n", tasks.live_tasks()));

    /* Separates "switched off on purpose" from "not running", which
    `stellargate_task_running` alone reports identically. */
    out.push_str(
        "# HELP stellargate_task_disabled Whether the named background task exited because configuration gave it nothing to do (1) or not (0).\n",
    );
    out.push_str("# TYPE stellargate_task_disabled gauge\n");
    for snap in &snaps {
        out.push_str(&format!(
            "stellargate_task_disabled{{task=\"{}\"}} {}\n",
            snap.name,
            if snap.disabled_reason.is_some() { 1 } else { 0 }
        ));
    }

    // stellargate_horizon_poll_cycles_total — counter vec by outcome (#313)
    out.push_str(
        "# HELP stellargate_horizon_poll_cycles_total Total Horizon poll cycles by outcome.\n",
    );
    out.push_str("# TYPE stellargate_horizon_poll_cycles_total counter\n");
    out.push_str(&format!(
        "stellargate_horizon_poll_cycles_total{{outcome=\"success\"}} {}\n",
        horizon.success()
    ));
    out.push_str(&format!(
        "stellargate_horizon_poll_cycles_total{{outcome=\"rate_limited\"}} {}\n",
        horizon.rate_limited()
    ));
    out.push_str(&format!(
        "stellargate_horizon_poll_cycles_total{{outcome=\"error\"}} {}\n",
        horizon.error()
    ));

    // stellargate_horizon_stream_reconnects_total — counter (#312)
    out.push_str(
        "# HELP stellargate_horizon_stream_reconnects_total Total times the Horizon SSE stream listener reconnected.\n",
    );
    out.push_str("# TYPE stellargate_horizon_stream_reconnects_total counter\n");
    out.push_str(&format!(
        "stellargate_horizon_stream_reconnects_total {}\n",
        horizon.stream_reconnects()
    ));

    /* Reuses TaskHealth's last-success timestamp rather than tracking a
    second one: `note_success()` is already called at the end of every
    successful `poll_once` (and by the stream listener), so it is already the
    authoritative "on-chain detection last made progress" instant that
    /ready's cursor-freshness check reads. */
    out.push_str(
        "# HELP stellargate_horizon_last_successful_poll_timestamp_seconds Unix timestamp of the last successful Horizon poll or stream event.\n",
    );
    out.push_str("# TYPE stellargate_horizon_last_successful_poll_timestamp_seconds gauge\n");
    out.push_str(&format!(
        "stellargate_horizon_last_successful_poll_timestamp_seconds {}\n",
        tasks.last_success_unix()
    ));

    // stellargate_missing_trustlines — gauge vec by asset (this issue)
    out.push_str(
        "# HELP stellargate_missing_trustlines Whether the gateway account is currently confirmed to have no trustline for an accepted asset (1) or confirmed to have one (0). An asset is absent from this metric until the first successful trustline check evaluates it.\n",
    );
    out.push_str("# TYPE stellargate_missing_trustlines gauge\n");
    for (asset, missing) in trustlines.snapshot() {
        out.push_str(&format!(
            "stellargate_missing_trustlines{{asset=\"{asset}\"}} {}\n",
            if missing { 1 } else { 0 }
        ));
    }

    out.push_str(
        "# HELP stellargate_trustline_check_failures_total Total trustline checks that could not reach Horizon or got a non-2xx response. Does not affect stellargate_missing_trustlines, which only reflects confirmed answers.\n",
    );
    out.push_str("# TYPE stellargate_trustline_check_failures_total counter\n");
    out.push_str(&format!(
        "stellargate_trustline_check_failures_total {}\n",
        trustlines.check_failures()
    ));

    out.push_str(
        "# HELP stellargate_trustline_check_last_success_timestamp_seconds Unix timestamp of the last trustline check that got a confirmed answer from Horizon. 0 means never — treat stellargate_missing_trustlines as unknown, not confirmed, until this is nonzero.\n",
    );
    out.push_str("# TYPE stellargate_trustline_check_last_success_timestamp_seconds gauge\n");
    out.push_str(&format!(
        "stellargate_trustline_check_last_success_timestamp_seconds {}\n",
        trustlines.last_success_unix()
    ));

    out
}

#[cfg(test)]
mod trustline_metrics_tests {
    use super::TrustlineMetrics;

    #[test]
    fn unchecked_asset_is_unknown() {
        let m = TrustlineMetrics::new();
        assert_eq!(m.is_missing("USDC"), None);
        assert_eq!(m.last_success_unix(), 0);
    }

    #[test]
    fn a_successful_check_records_present_and_missing() {
        let m = TrustlineMetrics::new();
        m.record_check(["USDC", "EURC"], &["USDC".to_string()]);
        assert_eq!(m.is_missing("USDC"), Some(true));
        assert_eq!(m.is_missing("EURC"), Some(false));
        assert!(m.last_success_unix() > 0);
    }

    #[test]
    fn a_failed_check_does_not_overwrite_prior_state() {
        let m = TrustlineMetrics::new();
        m.record_check(["USDC"], &["USDC".to_string()]);
        let ts = m.last_success_unix();
        m.record_check_failure();
        assert_eq!(
            m.is_missing("USDC"),
            Some(true),
            "prior confirmed state survives a Horizon failure"
        );
        assert_eq!(
            m.last_success_unix(),
            ts,
            "failure must not bump the success timestamp"
        );
        assert_eq!(m.check_failures(), 1);
    }

    #[test]
    fn a_later_check_drops_assets_no_longer_checked() {
        let m = TrustlineMetrics::new();
        m.record_check(["USDC", "EURC"], &["USDC".to_string()]);
        m.record_check(["EURC"], &[]);
        assert_eq!(
            m.is_missing("USDC"),
            None,
            "asset removed from ACCEPTED_ASSETS stops being reported"
        );
        assert_eq!(m.is_missing("EURC"), Some(false));
    }

    #[test]
    fn snapshot_is_sorted_by_asset_code() {
        let m = TrustlineMetrics::new();
        m.record_check(["USDC", "EURC", "AAA"], &["USDC".to_string()]);
        let codes: Vec<_> = m.snapshot().into_iter().map(|(c, _)| c).collect();
        assert_eq!(
            codes,
            vec!["AAA".to_string(), "EURC".to_string(), "USDC".to_string()]
        );
    }
}
