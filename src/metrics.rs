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
    /// Age, in seconds, of the most recently processed Horizon payment record
    /// — the same value `poll_once` and `handle_stream_event` already compute
    /// via `elapsed_secs` and previously only logged. This is the sharpest
    /// signal of whether payment detection is falling behind, and was
    /// unavailable to alerting until exported here (missing-metrics issue).
    cursor_age_secs: AtomicI64,
}

impl Default for HorizonMetricsInner {
    fn default() -> Self {
        Self {
            success: AtomicU64::new(0),
            rate_limited: AtomicU64::new(0),
            error: AtomicU64::new(0),
            stream_reconnects: AtomicU64::new(0),
            cursor_age_secs: AtomicI64::new(0),
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

    /// Record the age (in seconds) of the most recently processed Horizon
    /// payment record. Called from `poll_once` after each page and from
    /// `handle_stream_event` for each streamed record, alongside the
    /// existing `info!(cursor_age_secs, ...)` log lines.
    pub fn record_cursor_age_secs(&self, secs: i64) {
        self.inner.cursor_age_secs.store(secs, Ordering::Relaxed);
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
    pub fn cursor_age_secs(&self) -> i64 {
        self.inner.cursor_age_secs.load(Ordering::Relaxed)
    }
}

impl Default for HorizonMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Histogram buckets for HTTP request latency (milliseconds). Roughly the
/// Prometheus client library defaults (5ms .. 10s), rendered as seconds in
/// the exposition to match the `_seconds` metric name convention.
const HTTP_LATENCY_BUCKETS_MS: &[u64] =
    &[5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000];

/// One route+method's latency distribution.
#[derive(Default, Clone)]
struct RouteLatency {
    sum_ms: u64,
    count: u64,
    /// Cumulative per-bucket counts; one slot past
    /// `HTTP_LATENCY_BUCKETS_MS` for `+Inf`.
    buckets: Vec<u64>,
}

impl RouteLatency {
    fn record(&mut self, elapsed_ms: u64) {
        if self.buckets.is_empty() {
            self.buckets = vec![0; HTTP_LATENCY_BUCKETS_MS.len() + 1];
        }
        self.sum_ms += elapsed_ms;
        self.count += 1;
        for (i, &bound) in HTTP_LATENCY_BUCKETS_MS.iter().enumerate() {
            if elapsed_ms <= bound {
                self.buckets[i] += 1;
            }
        }
        *self.buckets.last_mut().unwrap() += 1;
    }
}

/// HTTP request counters and a latency histogram, labelled by the matched
/// route pattern (e.g. `/v1/payments/:id`) and method — never the raw URI or
/// a path parameter — so cardinality stays bounded by the fixed route table
/// regardless of how many distinct payment or merchant ids are requested.
///
/// A request that hit no route at all (a genuine 404 on an unmapped path) is
/// labelled `<unmatched>` rather than the raw path, for the same reason.
#[derive(Clone)]
pub struct HttpMetrics {
    inner: Arc<Mutex<HttpMetricsInner>>,
}

#[derive(Default)]
struct HttpMetricsInner {
    /// (method, route, status) -> count.
    requests: HashMap<(String, String, u16), u64>,
    /// (method, route) -> latency distribution.
    latency: HashMap<(String, String), RouteLatency>,
}

impl HttpMetrics {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HttpMetricsInner::default())),
        }
    }

    /// Record one completed HTTP request.
    pub fn record(&self, method: &str, route: &str, status: u16, elapsed_ms: u64) {
        let mut inner = self.inner.lock().unwrap();
        *inner
            .requests
            .entry((method.to_string(), route.to_string(), status))
            .or_insert(0) += 1;
        inner
            .latency
            .entry((method.to_string(), route.to_string()))
            .or_default()
            .record(elapsed_ms);
    }

    /// Snapshot of request counts, sorted for deterministic exposition.
    fn requests_snapshot(&self) -> Vec<(String, String, u16, u64)> {
        let inner = self.inner.lock().unwrap();
        let mut rows: Vec<_> = inner
            .requests
            .iter()
            .map(|((method, route, status), count)| {
                (method.clone(), route.clone(), *status, *count)
            })
            .collect();
        rows.sort_unstable_by(|a, b| (a.0.as_str(), a.1.as_str(), a.2).cmp(&(&b.0, &b.1, b.2)));
        rows
    }

    /// Snapshot of latency distributions, sorted for deterministic exposition.
    fn latency_snapshot(&self) -> Vec<(String, String, RouteLatency)> {
        let inner = self.inner.lock().unwrap();
        let mut rows: Vec<_> = inner
            .latency
            .iter()
            .map(|((method, route), lat)| (method.clone(), route.clone(), lat.clone()))
            .collect();
        rows.sort_unstable_by(|a, b| (a.0.as_str(), a.1.as_str()).cmp(&(&b.0, &b.1)));
        rows
    }
}

impl Default for HttpMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Histogram buckets for payment settlement latency (seconds): creation to
/// the poller/stream settling the intent. Spans a few seconds (a fast
/// on-chain confirmation) up to a couple of hours (a slow top-up on an
/// underpaid intent).
const SETTLEMENT_LATENCY_BUCKETS_SECS: &[u64] =
    &[1, 5, 15, 30, 60, 120, 300, 600, 1_800, 3_600, 7_200];

/// Payment lifecycle counters and settlement-latency histogram, so payment
/// creation and settlement throughput/latency are queryable facts on
/// `/metrics` rather than only visible via `GET /payments` polling or log
/// lines (missing-metrics issue).
#[derive(Clone)]
pub struct PaymentMetrics {
    inner: Arc<PaymentMetricsInner>,
}

struct PaymentMetricsInner {
    created: AtomicU64,
    completed: AtomicU64,
    overpaid: AtomicU64,
    underpaid: AtomicU64,
    expired: AtomicU64,
    settlement_latency_sum_secs: AtomicU64,
    settlement_latency_count: AtomicU64,
    settlement_latency_buckets: [AtomicU64; 12],
}

impl Default for PaymentMetricsInner {
    fn default() -> Self {
        Self {
            created: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            overpaid: AtomicU64::new(0),
            underpaid: AtomicU64::new(0),
            expired: AtomicU64::new(0),
            settlement_latency_sum_secs: AtomicU64::new(0),
            settlement_latency_count: AtomicU64::new(0),
            settlement_latency_buckets: Default::default(),
        }
    }
}

impl PaymentMetrics {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(PaymentMetricsInner::default()),
        }
    }

    /// Record a new payment intent (`POST /payments` minting a fresh id, not
    /// an idempotent replay of an existing one).
    pub fn record_created(&self) {
        self.inner.created.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a payment intent swept to `expired` by the sweeper.
    pub fn record_expired(&self) {
        self.inner.expired.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a settlement outcome (`completed`, `overpaid`, or `underpaid`)
    /// and, when known, the elapsed time since the intent was created.
    /// `underpaid` is an intermediate rather than terminal state, but is
    /// still counted here — it is the reconciler's verdict for this
    /// transaction, distinct from the intent's eventual final status.
    pub fn record_settlement(&self, status: &str, latency_secs: Option<i64>) {
        let counter = match status {
            "completed" => &self.inner.completed,
            "overpaid" => &self.inner.overpaid,
            "underpaid" => &self.inner.underpaid,
            _ => return,
        };
        counter.fetch_add(1, Ordering::Relaxed);

        if let Some(secs) = latency_secs {
            // A negative value (clock skew) has no meaningful bucket; floor at 0.
            let secs = secs.max(0) as u64;
            self.inner
                .settlement_latency_sum_secs
                .fetch_add(secs, Ordering::Relaxed);
            self.inner
                .settlement_latency_count
                .fetch_add(1, Ordering::Relaxed);
            for (i, &bound) in SETTLEMENT_LATENCY_BUCKETS_SECS.iter().enumerate() {
                if secs <= bound {
                    self.inner.settlement_latency_buckets[i].fetch_add(1, Ordering::Relaxed);
                }
            }
            self.inner.settlement_latency_buckets[SETTLEMENT_LATENCY_BUCKETS_SECS.len()]
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    // ── Snapshot accessors ────────────────────────────────────────────────

    pub fn created(&self) -> u64 {
        self.inner.created.load(Ordering::Relaxed)
    }
    pub fn completed(&self) -> u64 {
        self.inner.completed.load(Ordering::Relaxed)
    }
    pub fn overpaid(&self) -> u64 {
        self.inner.overpaid.load(Ordering::Relaxed)
    }
    pub fn underpaid(&self) -> u64 {
        self.inner.underpaid.load(Ordering::Relaxed)
    }
    pub fn expired(&self) -> u64 {
        self.inner.expired.load(Ordering::Relaxed)
    }
    pub fn settlement_latency_sum_secs(&self) -> u64 {
        self.inner
            .settlement_latency_sum_secs
            .load(Ordering::Relaxed)
    }
    pub fn settlement_latency_count(&self) -> u64 {
        self.inner.settlement_latency_count.load(Ordering::Relaxed)
    }
    pub fn settlement_latency_bucket(&self, i: usize) -> u64 {
        self.inner.settlement_latency_buckets[i].load(Ordering::Relaxed)
    }
}

impl Default for PaymentMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Database pool and on-disk file-size metrics, gathered fresh on each
/// `/metrics` scrape (issue: missing DB metrics). Cheap: pool state is two
/// atomic reads and file sizes are `stat()` calls on at most three files.
pub struct DbSnapshot {
    pub pool_size: u32,
    pub pool_idle: u32,
    pub pool_max: u32,
    /// Bytes of the main database file. `None` for an in-memory database.
    pub main_bytes: Option<u64>,
    /// Bytes of the `-wal` file. `None` if absent (e.g. no writes yet, or
    /// in-memory) — not rendered as a zero series in that case.
    pub wal_bytes: Option<u64>,
    /// Bytes of the `-shm` shared-memory index file. Same absence semantics
    /// as `wal_bytes`.
    pub shm_bytes: Option<u64>,
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

/// Render webhook delivery, auth outcome, background-task, Horizon poll, HTTP
/// traffic, payment lifecycle, and database metrics as a Prometheus-compatible
/// plain-text snapshot. Called by `GET /metrics`.
#[allow(clippy::too_many_arguments)]
pub fn render(
    webhook: &WebhookMetrics,
    auth: &AuthMetrics,
    tasks: &crate::TaskHealth,
    horizon: &HorizonMetrics,
    http: &HttpMetrics,
    payments: &PaymentMetrics,
    db: &DbSnapshot,
) -> String {
    let mut out = String::with_capacity(2048);

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

    /* stellargate_horizon_cursor_age_seconds — gauge. Previously computed
    correctly in both poll_once and handle_stream_event and only ever logged
    (`info!(cursor_age_secs, ...)`); this is the same value, exported so
    payment-detection lag is alertable rather than something an operator has
    to grep logs for. */
    out.push_str(
        "# HELP stellargate_horizon_cursor_age_seconds Age, in seconds, of the most recently processed Horizon payment record.\n",
    );
    out.push_str("# TYPE stellargate_horizon_cursor_age_seconds gauge\n");
    out.push_str(&format!(
        "stellargate_horizon_cursor_age_seconds {}\n",
        horizon.cursor_age_secs()
    ));

    // stellargate_http_requests_total — counter vec by method/route/status.
    // Labelled by the matched route pattern, never the raw URI, so
    // cardinality is bounded by the fixed route table regardless of how many
    // distinct payment/merchant ids are requested.
    out.push_str(
        "# HELP stellargate_http_requests_total Total HTTP requests by matched route, method, and status.\n",
    );
    out.push_str("# TYPE stellargate_http_requests_total counter\n");
    for (method, route, status, count) in http.requests_snapshot() {
        out.push_str(&format!(
            "stellargate_http_requests_total{{method=\"{method}\",route=\"{route}\",status=\"{status}\"}} {count}\n"
        ));
    }

    // stellargate_http_request_duration_seconds — histogram vec by method/route.
    out.push_str(
        "# HELP stellargate_http_request_duration_seconds HTTP request latency by matched route and method.\n",
    );
    out.push_str("# TYPE stellargate_http_request_duration_seconds histogram\n");
    for (method, route, lat) in http.latency_snapshot() {
        for (i, &bound_ms) in HTTP_LATENCY_BUCKETS_MS.iter().enumerate() {
            out.push_str(&format!(
                "stellargate_http_request_duration_seconds_bucket{{method=\"{method}\",route=\"{route}\",le=\"{}\"}} {}\n",
                bound_ms as f64 / 1000.0,
                lat.buckets[i]
            ));
        }
        out.push_str(&format!(
            "stellargate_http_request_duration_seconds_bucket{{method=\"{method}\",route=\"{route}\",le=\"+Inf\"}} {}\n",
            lat.buckets[HTTP_LATENCY_BUCKETS_MS.len()]
        ));
        out.push_str(&format!(
            "stellargate_http_request_duration_seconds_sum{{method=\"{method}\",route=\"{route}\"}} {}\n",
            lat.sum_ms as f64 / 1000.0
        ));
        out.push_str(&format!(
            "stellargate_http_request_duration_seconds_count{{method=\"{method}\",route=\"{route}\"}} {}\n",
            lat.count
        ));
    }

    // stellargate_payments_total — counter vec by lifecycle status.
    out.push_str("# HELP stellargate_payments_total Total payments by lifecycle status.\n");
    out.push_str("# TYPE stellargate_payments_total counter\n");
    out.push_str(&format!(
        "stellargate_payments_total{{status=\"created\"}} {}\n",
        payments.created()
    ));
    out.push_str(&format!(
        "stellargate_payments_total{{status=\"completed\"}} {}\n",
        payments.completed()
    ));
    out.push_str(&format!(
        "stellargate_payments_total{{status=\"overpaid\"}} {}\n",
        payments.overpaid()
    ));
    out.push_str(&format!(
        "stellargate_payments_total{{status=\"underpaid\"}} {}\n",
        payments.underpaid()
    ));
    out.push_str(&format!(
        "stellargate_payments_total{{status=\"expired\"}} {}\n",
        payments.expired()
    ));

    // stellargate_payment_settlement_latency_seconds — histogram: creation to
    // the poller/stream settling (or partially settling) the intent.
    out.push_str(
        "# HELP stellargate_payment_settlement_latency_seconds Time from payment creation to a settlement outcome (completed, overpaid, or underpaid).\n",
    );
    out.push_str("# TYPE stellargate_payment_settlement_latency_seconds histogram\n");
    for (i, &bound) in SETTLEMENT_LATENCY_BUCKETS_SECS.iter().enumerate() {
        out.push_str(&format!(
            "stellargate_payment_settlement_latency_seconds_bucket{{le=\"{bound}\"}} {}\n",
            payments.settlement_latency_bucket(i)
        ));
    }
    out.push_str(&format!(
        "stellargate_payment_settlement_latency_seconds_bucket{{le=\"+Inf\"}} {}\n",
        payments.settlement_latency_bucket(SETTLEMENT_LATENCY_BUCKETS_SECS.len())
    ));
    out.push_str(&format!(
        "stellargate_payment_settlement_latency_seconds_sum {}\n",
        payments.settlement_latency_sum_secs()
    ));
    out.push_str(&format!(
        "stellargate_payment_settlement_latency_seconds_count {}\n",
        payments.settlement_latency_count()
    ));

    // stellargate_db_pool_connections — gauge vec by state (issue: missing DB metrics).
    out.push_str(
        "# HELP stellargate_db_pool_connections Current SQLite connection pool size by state.\n",
    );
    out.push_str("# TYPE stellargate_db_pool_connections gauge\n");
    out.push_str(&format!(
        "stellargate_db_pool_connections{{state=\"idle\"}} {}\n",
        db.pool_idle
    ));
    out.push_str(&format!(
        "stellargate_db_pool_connections{{state=\"in_use\"}} {}\n",
        db.pool_size.saturating_sub(db.pool_idle)
    ));
    out.push_str(
        "# HELP stellargate_db_pool_max_connections Configured maximum SQLite connection pool size.\n",
    );
    out.push_str("# TYPE stellargate_db_pool_max_connections gauge\n");
    out.push_str(&format!(
        "stellargate_db_pool_max_connections {}\n",
        db.pool_max
    ));

    // stellargate_db_file_size_bytes — gauge vec by file. Omitted entirely
    // (no series) for an in-memory database or a file that hasn't been
    // created yet, rather than rendered as a misleading zero.
    out.push_str(
        "# HELP stellargate_db_file_size_bytes On-disk size of the SQLite database files, in bytes.\n",
    );
    out.push_str("# TYPE stellargate_db_file_size_bytes gauge\n");
    for (file, bytes) in [
        ("main", db.main_bytes),
        ("wal", db.wal_bytes),
        ("shm", db.shm_bytes),
    ] {
        if let Some(bytes) = bytes {
            out.push_str(&format!(
                "stellargate_db_file_size_bytes{{file=\"{file}\"}} {bytes}\n"
            ));
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_db_snapshot() -> DbSnapshot {
        DbSnapshot {
            pool_size: 3,
            pool_idle: 1,
            pool_max: 10,
            main_bytes: None,
            wal_bytes: None,
            shm_bytes: None,
        }
    }

    fn render_all(
        webhook: &WebhookMetrics,
        auth: &AuthMetrics,
        tasks: &crate::TaskHealth,
        horizon: &HorizonMetrics,
        http: &HttpMetrics,
        payments: &PaymentMetrics,
        db: &DbSnapshot,
    ) -> String {
        render(webhook, auth, tasks, horizon, http, payments, db)
    }

    // ── HttpMetrics ──────────────────────────────────────────────────────

    #[test]
    fn http_metrics_labels_are_bounded_by_route_not_raw_path() {
        let http = HttpMetrics::new();
        http.record("GET", "/v1/payments/:id", 200, 5);
        http.record("GET", "/v1/payments/:id", 200, 15);
        http.record("GET", "/v1/payments/:id", 404, 3);

        let rendered = render_all(
            &WebhookMetrics::new(),
            &AuthMetrics::new(),
            &crate::TaskHealth::new(),
            &HorizonMetrics::new(),
            &http,
            &PaymentMetrics::new(),
            &empty_db_snapshot(),
        );

        assert!(
            rendered.contains(
                "stellargate_http_requests_total{method=\"GET\",route=\"/v1/payments/:id\",status=\"200\"} 2"
            ),
            "got:\n{rendered}"
        );
        assert!(
            rendered.contains(
                "stellargate_http_requests_total{method=\"GET\",route=\"/v1/payments/:id\",status=\"404\"} 1"
            ),
            "got:\n{rendered}"
        );
        assert!(
            rendered.contains(
                "stellargate_http_request_duration_seconds_count{method=\"GET\",route=\"/v1/payments/:id\"} 3"
            ),
            "the latency histogram must aggregate over the same bounded route \
             label as the counter:\n{rendered}"
        );
        assert!(
            rendered.contains("stellargate_http_request_duration_seconds_sum{method=\"GET\",route=\"/v1/payments/:id\"} 0.023"),
            "sum must be in seconds, not milliseconds:\n{rendered}"
        );
    }

    #[test]
    fn http_latency_histogram_buckets_are_cumulative() {
        let http = HttpMetrics::new();
        http.record("GET", "/health", 200, 3); // falls in every bucket
        http.record("GET", "/health", 200, 600); // falls in buckets >= 1000ms

        let rendered = render_all(
            &WebhookMetrics::new(),
            &AuthMetrics::new(),
            &crate::TaskHealth::new(),
            &HorizonMetrics::new(),
            &http,
            &PaymentMetrics::new(),
            &empty_db_snapshot(),
        );

        assert!(rendered.contains(
            "stellargate_http_request_duration_seconds_bucket{method=\"GET\",route=\"/health\",le=\"0.005\"} 1"
        ));
        assert!(rendered.contains(
            "stellargate_http_request_duration_seconds_bucket{method=\"GET\",route=\"/health\",le=\"1\"} 2"
        ));
        assert!(rendered.contains(
            "stellargate_http_request_duration_seconds_bucket{method=\"GET\",route=\"/health\",le=\"+Inf\"} 2"
        ));
    }

    // ── PaymentMetrics ───────────────────────────────────────────────────

    #[test]
    fn payment_metrics_count_by_lifecycle_status() {
        let payments = PaymentMetrics::new();
        payments.record_created();
        payments.record_created();
        payments.record_settlement("completed", Some(42));
        payments.record_settlement("underpaid", Some(10));
        payments.record_expired();

        assert_eq!(payments.created(), 2);
        assert_eq!(payments.completed(), 1);
        assert_eq!(payments.underpaid(), 1);
        assert_eq!(payments.expired(), 1);
        assert_eq!(payments.settlement_latency_count(), 2);
        assert_eq!(payments.settlement_latency_sum_secs(), 52);
    }

    #[test]
    fn payment_settlement_clock_skew_floors_at_zero_rather_than_wrapping() {
        let payments = PaymentMetrics::new();
        payments.record_settlement("completed", Some(-5));
        // A negative latency must not wrap around as a huge u64 via `as u64`.
        assert_eq!(payments.settlement_latency_sum_secs(), 0);
        assert_eq!(payments.settlement_latency_count(), 1);
    }

    #[test]
    fn payment_metrics_are_exported_on_render() {
        let payments = PaymentMetrics::new();
        payments.record_created();
        payments.record_settlement("overpaid", Some(90));

        let rendered = render_all(
            &WebhookMetrics::new(),
            &AuthMetrics::new(),
            &crate::TaskHealth::new(),
            &HorizonMetrics::new(),
            &HttpMetrics::new(),
            &payments,
            &empty_db_snapshot(),
        );

        assert!(rendered.contains("stellargate_payments_total{status=\"created\"} 1"));
        assert!(rendered.contains("stellargate_payments_total{status=\"overpaid\"} 1"));
        assert!(rendered.contains("stellargate_payment_settlement_latency_seconds_count 1"));
        assert!(rendered.contains("stellargate_payment_settlement_latency_seconds_sum 90"));
    }

    // ── HorizonMetrics cursor age ────────────────────────────────────────

    #[test]
    fn horizon_cursor_age_is_a_gauge_not_only_a_log_line() {
        let horizon = HorizonMetrics::new();
        assert_eq!(horizon.cursor_age_secs(), 0);
        horizon.record_cursor_age_secs(37);
        assert_eq!(horizon.cursor_age_secs(), 37);

        let rendered = render_all(
            &WebhookMetrics::new(),
            &AuthMetrics::new(),
            &crate::TaskHealth::new(),
            &horizon,
            &HttpMetrics::new(),
            &PaymentMetrics::new(),
            &empty_db_snapshot(),
        );
        assert!(
            rendered.contains("# TYPE stellargate_horizon_cursor_age_seconds gauge"),
            "got:\n{rendered}"
        );
        assert!(
            rendered.contains("stellargate_horizon_cursor_age_seconds 37"),
            "got:\n{rendered}"
        );
    }

    // ── DbSnapshot ───────────────────────────────────────────────────────

    #[test]
    fn db_pool_metrics_are_exported() {
        let db = DbSnapshot {
            pool_size: 4,
            pool_idle: 3,
            pool_max: 10,
            main_bytes: None,
            wal_bytes: None,
            shm_bytes: None,
        };
        let rendered = render_all(
            &WebhookMetrics::new(),
            &AuthMetrics::new(),
            &crate::TaskHealth::new(),
            &HorizonMetrics::new(),
            &HttpMetrics::new(),
            &PaymentMetrics::new(),
            &db,
        );
        assert!(rendered.contains("stellargate_db_pool_connections{state=\"idle\"} 3"));
        assert!(rendered.contains("stellargate_db_pool_connections{state=\"in_use\"} 1"));
        assert!(rendered.contains("stellargate_db_pool_max_connections 10"));
    }

    #[test]
    fn db_file_size_series_are_omitted_when_absent() {
        let rendered = render_all(
            &WebhookMetrics::new(),
            &AuthMetrics::new(),
            &crate::TaskHealth::new(),
            &HorizonMetrics::new(),
            &HttpMetrics::new(),
            &PaymentMetrics::new(),
            &empty_db_snapshot(),
        );
        assert!(
            !rendered.contains("stellargate_db_file_size_bytes{"),
            "an in-memory database must render no file-size series, not a \
             misleading zero:\n{rendered}"
        );
    }

    #[test]
    fn db_file_size_series_are_rendered_when_present() {
        let db = DbSnapshot {
            pool_size: 1,
            pool_idle: 1,
            pool_max: 10,
            main_bytes: Some(4096),
            wal_bytes: Some(128),
            shm_bytes: None,
        };
        let rendered = render_all(
            &WebhookMetrics::new(),
            &AuthMetrics::new(),
            &crate::TaskHealth::new(),
            &HorizonMetrics::new(),
            &HttpMetrics::new(),
            &PaymentMetrics::new(),
            &db,
        );
        assert!(rendered.contains("stellargate_db_file_size_bytes{file=\"main\"} 4096"));
        assert!(rendered.contains("stellargate_db_file_size_bytes{file=\"wal\"} 128"));
        assert!(!rendered.contains("stellargate_db_file_size_bytes{file=\"shm\"}"));
    }
}
