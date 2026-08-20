pub mod api;
pub mod config;
pub mod db;
pub mod expiry;
pub mod horizon;
pub mod metrics;
pub mod money;
pub mod retention;
pub mod ssrf;
pub mod strkey;
pub mod supervise;
pub mod webhook;

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Consecutive panics at or above this mark a required task as crash-looping.
/// `/health` fails while any required task is crash-looping, even if the
/// supervisor has already spawned a replacement (issue #316).
pub const CRASH_LOOP_THRESHOLD: u32 = 3;

/// Snapshot of one background task, used to render `/metrics`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskSnapshot {
    pub name: &'static str,
    pub running: bool,
    pub restarts: u64,
    pub consecutive_failures: u32,
    /// Set when the task exited because configuration gave it nothing to do.
    /// Distinguishes "switched off on purpose" from "not running", which the
    /// counters alone could not (issue #317).
    pub disabled_reason: Option<&'static str>,
}

/// Tracks background task health: per-task liveness for `/health`, the last
/// successful on-chain progress for `/ready`'s cursor-freshness check, and
/// started/stopped/failure/restart counts for monitoring and alerting.
#[derive(Clone)]
pub struct TaskHealth {
    inner: Arc<TaskHealthInner>,
}

struct TaskHealthInner {
    /// Count of task starts.
    started: AtomicU64,
    /// Count of task stops.
    stopped: AtomicU64,
    /// Count of task panics/failures.
    failed: AtomicU64,
    /// Per-task liveness, keyed by the name passed to [`TaskHealth::task_started`].
    running: Mutex<HashMap<&'static str, bool>>,
    /// Supervisor restarts after a panic or unexpected return, keyed by task name.
    restarts: Mutex<HashMap<&'static str, u64>>,
    /// Consecutive panics since the last stable run, keyed by task name.
    consecutive_failures: Mutex<HashMap<&'static str, u32>>,
    /// Tasks that exited because configuration gave them nothing to do, with
    /// the reason. Distinct from "stopped" on purpose (issue #317): a disabled
    /// worker is a deployment choice, not a fault, so it must not fail
    /// `/health` — while a worker that stopped for any *other* reason still
    /// must.
    disabled: Mutex<HashMap<&'static str, &'static str>>,
    /// Task names that must be running for `/health` to pass. Declared by the
    /// process that spawns the tasks (main.rs), so "expected" is a deployment
    /// decision rather than something the probe has to guess.
    required: Mutex<Vec<&'static str>>,
    /// Unix seconds of the last successful poll cycle or stream event; `0`
    /// means never. Drives `/ready`'s cursor-freshness check (issue #315).
    last_success_unix: AtomicI64,
}

impl Default for TaskHealthInner {
    fn default() -> Self {
        Self {
            started: AtomicU64::new(0),
            stopped: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            running: Mutex::new(HashMap::new()),
            restarts: Mutex::new(HashMap::new()),
            consecutive_failures: Mutex::new(HashMap::new()),
            disabled: Mutex::new(HashMap::new()),
            required: Mutex::new(Vec::new()),
            last_success_unix: AtomicI64::new(0),
        }
    }
}

impl TaskHealth {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(TaskHealthInner::default()),
        }
    }

    /// Declare that the named task must keep running for the service to be
    /// healthy. `/health` returns `503` while any required task is not running
    /// (issue #315). Call before the task is spawned.
    pub fn require(&self, name: &'static str) {
        self.inner.required.lock().unwrap().push(name);
    }

    pub fn task_started(&self, name: &'static str) {
        self.inner.started.fetch_add(1, Ordering::Relaxed);
        self.inner.running.lock().unwrap().insert(name, true);
    }

    pub fn task_stopped(&self, name: &'static str) {
        self.inner.stopped.fetch_add(1, Ordering::Relaxed);
        self.inner.running.lock().unwrap().insert(name, false);
    }

    pub fn task_failed(&self, name: &'static str) {
        self.inner.failed.fetch_add(1, Ordering::Relaxed);
        // A failed task is by definition not running; reflect that in the
        // liveness map even if `task_stopped` never ran for it.
        self.inner.running.lock().unwrap().insert(name, false);
        let mut consecutive = self.inner.consecutive_failures.lock().unwrap();
        *consecutive.entry(name).or_insert(0) += 1;
    }

    /// Record that the supervisor is about to spawn a replacement after a
    /// panic or unexpected return. Distinct from [`task_started`]: a restart
    /// is the event operators alert on (issue #316).
    pub fn task_restarted(&self, name: &'static str) {
        let mut restarts = self.inner.restarts.lock().unwrap();
        *restarts.entry(name).or_insert(0) += 1;
    }

    /// Record that a task exited because configuration gave it nothing to do.
    ///
    /// Terminal and deliberate, so it is *not* a fault: the task is marked not
    /// running, but excluded from [`dead_required_tasks`](Self::dead_required_tasks)
    /// so `/health` does not report a worker that was switched off on purpose
    /// as a failure (issue #317).
    pub fn task_disabled(&self, name: &'static str, reason: &'static str) {
        self.inner.stopped.fetch_add(1, Ordering::Relaxed);
        self.inner.running.lock().unwrap().insert(name, false);
        self.inner.disabled.lock().unwrap().insert(name, reason);
    }

    /// Whether a task exited via [`task_disabled`](Self::task_disabled), and why.
    pub fn disabled_reason(&self, name: &'static str) -> Option<&'static str> {
        self.inner.disabled.lock().unwrap().get(name).copied()
    }

    /// How many workers this deployment expects to be running: the required
    /// set, minus any that configuration has deliberately disabled.
    ///
    /// The counters alone could never answer "how many workers should be
    /// running, and how many are?" — `stopped` was overloaded across clean
    /// shutdown, configuration-disabled exit and fault, so even once exposed
    /// the arithmetic would have been wrong (issue #317).
    pub fn expected_tasks(&self) -> usize {
        let required = self.inner.required.lock().unwrap();
        let disabled = self.inner.disabled.lock().unwrap();
        required
            .iter()
            .filter(|name| !disabled.contains_key(*name))
            .count()
    }

    /// How many of the expected workers are actually running right now.
    /// Compare against [`expected_tasks`](Self::expected_tasks).
    pub fn live_tasks(&self) -> usize {
        let running = self.inner.running.lock().unwrap();
        let required = self.inner.required.lock().unwrap();
        let disabled = self.inner.disabled.lock().unwrap();
        required
            .iter()
            .filter(|name| !disabled.contains_key(*name))
            .filter(|name| running.get(*name) == Some(&true))
            .count()
    }

    /// The inner task has been running without panicking long enough to treat
    /// as stable. Clears the consecutive-failure streak so a one-off panic
    /// later does not keep `/health` red forever.
    pub fn note_stable(&self, name: &'static str) {
        self.inner
            .consecutive_failures
            .lock()
            .unwrap()
            .insert(name, 0);
    }

    pub fn started(&self) -> u64 {
        self.inner.started.load(Ordering::Relaxed)
    }

    pub fn stopped(&self) -> u64 {
        self.inner.stopped.load(Ordering::Relaxed)
    }

    pub fn failed(&self) -> u64 {
        self.inner.failed.load(Ordering::Relaxed)
    }

    pub fn restarts(&self, name: &'static str) -> u64 {
        self.inner
            .restarts
            .lock()
            .unwrap()
            .get(name)
            .copied()
            .unwrap_or(0)
    }

    pub fn consecutive_failures(&self, name: &'static str) -> u32 {
        self.inner
            .consecutive_failures
            .lock()
            .unwrap()
            .get(name)
            .copied()
            .unwrap_or(0)
    }

    /// Names of required tasks that are not currently running. Empty when the
    /// service is healthy; drives `/health`.
    pub fn dead_required_tasks(&self) -> Vec<&'static str> {
        let running = self.inner.running.lock().unwrap();
        let required = self.inner.required.lock().unwrap();
        let disabled = self.inner.disabled.lock().unwrap();
        required
            .iter()
            .copied()
            // A worker switched off by configuration is not dead; it was never
            // meant to be running (issue #317).
            .filter(|name| !disabled.contains_key(name))
            .filter(|name| running.get(name) != Some(&true))
            .collect()
    }

    /// Required tasks whose consecutive panics have reached
    /// [`CRASH_LOOP_THRESHOLD`]. Empty when none are crash-looping.
    pub fn crash_looping_required_tasks(&self) -> Vec<&'static str> {
        let consecutive = self.inner.consecutive_failures.lock().unwrap();
        let required = self.inner.required.lock().unwrap();
        required
            .iter()
            .copied()
            .filter(|name| consecutive.get(name).copied().unwrap_or(0) >= CRASH_LOOP_THRESHOLD)
            .collect()
    }

    /// Per-task snapshot for Prometheus exposition. Includes every required
    /// name plus any other name the supervisor has touched.
    pub fn snapshot(&self) -> Vec<TaskSnapshot> {
        let running = self.inner.running.lock().unwrap();
        let restarts = self.inner.restarts.lock().unwrap();
        let consecutive = self.inner.consecutive_failures.lock().unwrap();
        let required = self.inner.required.lock().unwrap();
        let disabled = self.inner.disabled.lock().unwrap();

        let mut names: Vec<&'static str> = required.clone();
        for name in running
            .keys()
            .chain(restarts.keys())
            .chain(consecutive.keys())
            .chain(disabled.keys())
        {
            if !names.contains(name) {
                names.push(*name);
            }
        }
        names.sort_unstable();
        names
            .into_iter()
            .map(|name| TaskSnapshot {
                name,
                running: running.get(name).copied().unwrap_or(false),
                restarts: restarts.get(name).copied().unwrap_or(0),
                consecutive_failures: consecutive.get(name).copied().unwrap_or(0),
                disabled_reason: disabled.get(name).copied(),
            })
            .collect()
    }

    /// Record successful on-chain progress — a completed poll cycle or a
    /// received stream event. This is the heartbeat `/ready` freshness-checks.
    pub fn note_success(&self) {
        self.set_last_success_unix(unix_now_secs());
    }

    /// Overwrite the last-success timestamp directly. Used by tests to
    /// simulate a stale cursor without waiting out a poll interval.
    pub fn set_last_success_unix(&self, unix_secs: i64) {
        self.inner
            .last_success_unix
            .store(unix_secs, Ordering::Relaxed);
    }

    /// Seconds since the last successful poll/stream event. A never-updated
    /// timestamp (`0`) reads as maximally stale; a clock before the epoch
    /// saturates at `0` rather than going negative.
    pub fn last_success_age_secs(&self) -> i64 {
        unix_now_secs().saturating_sub(self.inner.last_success_unix.load(Ordering::Relaxed))
    }

    /// The raw Unix timestamp `note_success` last recorded (`0` if it never
    /// has). Exposed as a gauge on `/metrics` (issue #313).
    pub fn last_success_unix(&self) -> i64 {
        self.inner.last_success_unix.load(Ordering::Relaxed)
    }
}

/// Current Unix time in whole seconds.
fn unix_now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

impl Default for TaskHealth {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared application state handed to every request handler and the background
/// Horizon poller. Cloning is cheap — the pool and HTTP client are internally
/// reference-counted.
pub struct AppState {
    pub pool: db::Db,
    pub config: config::Config,
    pub http: reqwest::Client,
    pub webhook_http: reqwest::Client,
    /// Webhook delivery metrics: delivered/failed/retried counts and a latency
    /// histogram. Exposed via `GET /metrics` so operators can see delivery
    /// success rate, retry volume, and failure spikes at a glance.
    pub webhook_metrics: metrics::WebhookMetrics,
    /// Auth middleware outcome counters: success/failure (by reason) counts.
    /// Exposed via `GET /metrics` so credential-stuffing or misconfigured
    /// clients are visible without grepping logs.
    pub auth_metrics: metrics::AuthMetrics,
    /// Horizon poll cycle outcome counters: success/rate_limited/error.
    /// Exposed via `GET /metrics` so sustained throttling is a queryable fact
    /// rather than indistinguishable `warn!` lines (issue #313).
    pub horizon_metrics: metrics::HorizonMetrics,
    /// Per-asset gateway trustline state, refreshed at boot and on a
    /// recurring interval thereafter (trustlines can be revoked, or an asset
    /// added to `ACCEPTED_ASSETS`, at any time after boot). Exposed via
    /// `GET /metrics` and consulted by `POST /payments` to reject intents in
    /// an asset currently confirmed unpayable.
    pub trustline_metrics: metrics::TrustlineMetrics,
    /// Background task health: per-task liveness (drives `/health`), the last
    /// successful on-chain progress (drives `/ready`'s cursor-freshness check),
    /// and started/stopped/failure/restart counts for monitoring and alerting.
    pub task_health: TaskHealth,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_required_tasks_is_healthy() {
        let health = TaskHealth::new();
        assert!(health.dead_required_tasks().is_empty());
    }

    #[test]
    fn running_required_task_is_healthy() {
        let health = TaskHealth::new();
        health.require("poller");
        health.task_started("poller");
        assert!(health.dead_required_tasks().is_empty());
    }

    #[test]
    fn stopped_required_task_is_dead() {
        let health = TaskHealth::new();
        health.require("poller");
        health.task_started("poller");
        health.task_stopped("poller");
        assert_eq!(health.dead_required_tasks(), vec!["poller"]);
    }

    #[test]
    fn never_started_required_task_is_dead() {
        let health = TaskHealth::new();
        health.require("redrive");
        assert_eq!(health.dead_required_tasks(), vec!["redrive"]);
    }

    #[test]
    fn unrequired_stopped_task_does_not_fail_health() {
        // A task that is not required (e.g. the stream on a poll-only
        // deployment, or the poller without a configured gateway) stopping is
        // by design and must not fail /health.
        let health = TaskHealth::new();
        health.require("sweeper");
        health.task_started("sweeper");
        health.task_started("poller");
        health.task_stopped("poller");
        assert!(health.dead_required_tasks().is_empty());
    }

    #[test]
    fn failed_task_is_not_running() {
        // task_failed marks the task dead even if task_stopped never ran.
        let health = TaskHealth::new();
        health.require("poller");
        health.task_started("poller");
        health.task_failed("poller");
        assert_eq!(health.dead_required_tasks(), vec!["poller"]);
        assert_eq!(health.failed(), 1);
        assert_eq!(health.consecutive_failures("poller"), 1);
    }

    #[test]
    fn three_consecutive_failures_are_crash_looping() {
        let health = TaskHealth::new();
        health.require("poller");
        health.task_started("poller");
        for _ in 0..CRASH_LOOP_THRESHOLD {
            health.task_failed("poller");
            health.task_restarted("poller");
            health.task_started("poller");
        }
        assert_eq!(health.crash_looping_required_tasks(), vec!["poller"]);
        assert_eq!(health.restarts("poller"), CRASH_LOOP_THRESHOLD as u64);
        // Currently running again — dead list is empty, crash-loop list is not.
        assert!(health.dead_required_tasks().is_empty());
    }

    #[test]
    fn note_stable_clears_crash_loop() {
        let health = TaskHealth::new();
        health.require("poller");
        for _ in 0..CRASH_LOOP_THRESHOLD {
            health.task_failed("poller");
        }
        assert_eq!(health.crash_looping_required_tasks(), vec!["poller"]);
        health.note_stable("poller");
        assert!(health.crash_looping_required_tasks().is_empty());
        assert_eq!(health.consecutive_failures("poller"), 0);
    }

    #[test]
    fn last_success_age_is_zero_after_note_success() {
        let health = TaskHealth::new();
        health.note_success();
        assert!(health.last_success_age_secs() <= 1);
    }

    #[test]
    fn never_succeeded_is_maximally_stale() {
        let health = TaskHealth::new();
        assert!(health.last_success_age_secs() > 1_000_000_000);
    }

    #[test]
    fn stale_timestamp_reports_large_age() {
        let health = TaskHealth::new();
        health.set_last_success_unix(unix_now_secs() - 120);
        assert!(health.last_success_age_secs() >= 119);
    }
}
