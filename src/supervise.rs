//! Supervisor for long-running background tasks.
//!
//! Each worker is spawned as a child task. A panic (or unexpected return) is
//! logged and counted immediately, then the worker is restarted with bounded
//! exponential backoff. The supervisor itself does not panic, so a crash in
//! the poller no longer silently ends payment detection for the life of the
//! process (issue #316).

use crate::TaskHealth;
use std::future::Future;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{error, info};

/// Why a background worker's future returned.
///
/// Previously every worker returned `()`, so the supervisor could only see
/// *that* one had stopped, never *why* (issue #317). Several workers return
/// permanently on a startup condition, and each of those looked exactly like a
/// clean shutdown: `run_stream_listener` failing to build its HTTP client
/// logged a `warn!` and exited for the life of the process, and
/// `run_retention_worker` exiting because both retention windows were `0`
/// recorded the same thing — one a fault, the other a deliberate
/// configuration choice, and nothing could tell them apart.
///
/// Making the reason explicit is what lets the supervisor act on it: restart a
/// fault, leave a disabled worker alone, and keep quiet on an ordinary
/// shutdown.
#[derive(Debug)]
pub enum TaskExit {
    /// The shutdown signal fired. Ordinary and expected; not restarted, not
    /// logged as a problem.
    ShutdownRequested,
    /// Configuration says this worker has nothing to do — an unconfigured
    /// gateway wallet, both retention windows set to `0`. **Terminal**: it is
    /// reported once at boot and never restarted, because restarting it would
    /// change nothing and would spin against the same configuration forever.
    DisabledByConfig(&'static str),
    /// The worker hit something it could not continue past. **Restartable**,
    /// and logged at `error` with the task name, because unlike the two above
    /// this one means the service is not doing a job it is supposed to be
    /// doing.
    Fatal(String),
}

/// Backoff (and stability) knobs for [`supervise_with`]. Production uses
/// [`Backoff::default`]: 1s doubling to 60s, stable after 5s without a panic.
#[derive(Debug, Clone, Copy)]
pub struct Backoff {
    pub initial: Duration,
    pub max: Duration,
    /// How long a replacement must run before consecutive panics are cleared.
    pub stability: Duration,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            initial: Duration::from_secs(1),
            max: Duration::from_secs(60),
            stability: Duration::from_secs(5),
        }
    }
}

/// Supervise `make` until `shutdown` is true. Uses [`Backoff::default`].
pub fn supervise<F, Fut>(
    health: TaskHealth,
    name: &'static str,
    shutdown: watch::Receiver<bool>,
    make: F,
) -> JoinHandle<()>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = TaskExit> + Send + 'static,
{
    supervise_with(health, name, shutdown, make, Backoff::default())
}

/// Like [`supervise`], with explicit backoff — used by tests so a panic-and-
/// resume cycle does not wait on the production 1s floor.
pub fn supervise_with<F, Fut>(
    health: TaskHealth,
    name: &'static str,
    mut shutdown: watch::Receiver<bool>,
    mut make: F,
    backoff: Backoff,
) -> JoinHandle<()>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = TaskExit> + Send + 'static,
{
    tokio::spawn(async move {
        let mut delay = backoff.initial;
        loop {
            if *shutdown.borrow() {
                health.task_stopped(name);
                return;
            }

            health.task_started(name);
            let mut child = tokio::spawn(make());
            let mut marked_stable = false;

            // One join of the child. A stability timer running alongside it
            // clears the consecutive-panic streak once the replacement has
            // lived long enough to not be a crash-loop.
            let join = loop {
                tokio::select! {
                    biased;
                    _ = shutdown.changed() => {
                        let _ = child.await;
                        health.task_stopped(name);
                        return;
                    }
                    _ = tokio::time::sleep(backoff.stability), if !marked_stable => {
                        health.note_stable(name);
                        marked_stable = true;
                    }
                    join = &mut child => break join,
                }
            };

            if *shutdown.borrow() {
                health.task_stopped(name);
                return;
            }

            match join {
                Ok(TaskExit::ShutdownRequested) => {
                    /* The worker saw the shutdown signal before this loop did.
                    Ordinary; nothing to report and nothing to restart. */
                    health.task_stopped(name);
                    return;
                }
                Ok(TaskExit::DisabledByConfig(reason)) => {
                    /* Terminal by design. Restarting would spin against the
                    same configuration forever, and `/health` must not report a
                    deliberately-disabled worker as dead — so it is recorded as
                    disabled rather than merely stopped, and said once. */
                    info!(task = name, %reason, "background task disabled by configuration");
                    health.task_disabled(name, reason);
                    return;
                }
                Ok(TaskExit::Fatal(reason)) => {
                    /* The case this issue is really about: a worker that gave
                    up on a fault used to be indistinguishable from a clean
                    stop, and was logged at `warn` if at all. */
                    error!(
                        task = name,
                        %reason,
                        "background task exited on a fatal error; restarting"
                    );
                    health.task_stopped(name);
                }
                Err(e) if e.is_panic() => {
                    health.task_failed(name);
                    error!(task = name, "background task panicked; restarting");
                }
                Err(_) => {
                    // Cancelled — treat as shutdown.
                    health.task_stopped(name);
                    return;
                }
            }

            health.task_restarted(name);

            tokio::select! {
                biased;
                _ = shutdown.changed() => {
                    health.task_stopped(name);
                    return;
                }
                _ = tokio::time::sleep(delay) => {}
            }

            delay = delay.saturating_mul(2).min(backoff.max);
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TaskHealth;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    fn fast_backoff() -> Backoff {
        Backoff {
            initial: Duration::from_millis(5),
            max: Duration::from_millis(20),
            stability: Duration::from_millis(30),
        }
    }

    #[tokio::test]
    async fn panicking_task_is_restarted() {
        let health = TaskHealth::new();
        health.require("probe");
        let (tx, rx) = watch::channel(false);
        let runs = Arc::new(AtomicU64::new(0));
        let runs_inner = runs.clone();
        let shutdown_for_child = rx.clone();

        let handle = supervise_with(
            health.clone(),
            "probe",
            rx,
            move || {
                let runs = runs_inner.clone();
                let mut shutdown = shutdown_for_child.clone();
                async move {
                    let n = runs.fetch_add(1, Ordering::SeqCst);
                    if n == 0 {
                        panic!("deliberate test panic");
                    }
                    let _ = shutdown.changed().await;
                    TaskExit::ShutdownRequested
                }
            },
            fast_backoff(),
        );

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if runs.load(Ordering::SeqCst) >= 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("task did not resume after panic");

        assert!(
            health.failed() >= 1,
            "panic must be counted when it happens, not at shutdown"
        );
        assert_eq!(health.restarts("probe"), 1);
        assert!(
            health.dead_required_tasks().is_empty(),
            "replacement must be marked running: {:?}",
            health.dead_required_tasks()
        );

        let _ = tx.send(true);
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("supervisor did not stop")
            .expect("supervisor task panicked");
    }

    // ── Exit reasons (issue #317) ────────────────────────────────────────────

    /// Spawn a supervisor whose worker returns `exit` on its first run and then
    /// parks until shutdown, and return the handle plus a run counter.
    fn supervise_returning(
        health: TaskHealth,
        rx: watch::Receiver<bool>,
        mut exits: Vec<TaskExit>,
    ) -> (JoinHandle<()>, Arc<AtomicU64>) {
        let runs = Arc::new(AtomicU64::new(0));
        let runs_inner = runs.clone();
        let shutdown_for_child = rx.clone();
        exits.reverse();
        let exits = Arc::new(std::sync::Mutex::new(exits));

        let handle = supervise_with(
            health,
            "probe",
            rx,
            move || {
                let runs = runs_inner.clone();
                let exits = exits.clone();
                let mut shutdown = shutdown_for_child.clone();
                async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    let next = exits.lock().unwrap().pop();
                    match next {
                        Some(exit) => exit,
                        None => {
                            let _ = shutdown.changed().await;
                            TaskExit::ShutdownRequested
                        }
                    }
                }
            },
            fast_backoff(),
        );
        (handle, runs)
    }

    /// `DisabledByConfig` is terminal. Restarting would spin against the same
    /// configuration forever, and — critically — a worker switched off on
    /// purpose must not read as dead on `/health`.
    #[tokio::test]
    async fn disabled_by_config_is_terminal_and_not_a_failure() {
        let health = TaskHealth::new();
        health.require("probe");
        let (tx, rx) = watch::channel(false);

        let (handle, runs) = supervise_returning(
            health.clone(),
            rx,
            vec![TaskExit::DisabledByConfig("nothing to do")],
        );

        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("supervisor should return immediately on a disabled exit")
            .unwrap();

        assert_eq!(runs.load(Ordering::SeqCst), 1, "must not be restarted");
        assert_eq!(health.disabled_reason("probe"), Some("nothing to do"));
        assert!(
            health.dead_required_tasks().is_empty(),
            "a deliberately-disabled worker is not a dead one"
        );
        assert_eq!(health.failed(), 0, "disabled is not a fault");
        assert_eq!(
            health.expected_tasks(),
            0,
            "a disabled worker is no longer expected to be running"
        );
        let _ = tx.send(true);
    }

    /// `Fatal` is restartable and — unlike before, when this path was a `warn!`
    /// and a permanent exit — is reported as an error naming the task.
    #[tokio::test]
    async fn fatal_exit_is_restarted() {
        let health = TaskHealth::new();
        health.require("probe");
        let (tx, rx) = watch::channel(false);

        let (handle, runs) = supervise_returning(
            health.clone(),
            rx,
            vec![TaskExit::Fatal("could not build client".into())],
        );

        tokio::time::timeout(Duration::from_secs(2), async {
            while runs.load(Ordering::SeqCst) < 2 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("a fatal exit must be restarted, not left dead");

        assert_eq!(health.restarts("probe"), 1);
        assert!(
            health.disabled_reason("probe").is_none(),
            "a fault must not be recorded as a configuration choice"
        );
        assert_eq!(
            health.expected_tasks(),
            1,
            "a faulting worker is still expected to be running"
        );

        let _ = tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
    }

    /// The ordinary case: no restart, no error.
    #[tokio::test]
    async fn shutdown_requested_stops_without_restarting() {
        let health = TaskHealth::new();
        health.require("probe");
        let (tx, rx) = watch::channel(false);

        let (handle, runs) =
            supervise_returning(health.clone(), rx, vec![TaskExit::ShutdownRequested]);

        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("supervisor should return on a shutdown exit")
            .unwrap();

        assert_eq!(runs.load(Ordering::SeqCst), 1, "must not be restarted");
        assert_eq!(health.restarts("probe"), 0);
        assert_eq!(health.failed(), 0);
        let _ = tx.send(true);
    }

    /// The question the counters could never answer: how many workers should
    /// be running, and how many are?
    #[tokio::test]
    async fn expected_and_live_counts_track_a_disabled_worker() {
        let health = TaskHealth::new();
        health.require("alpha");
        health.require("beta");

        health.task_started("alpha");
        health.task_started("beta");
        assert_eq!((health.expected_tasks(), health.live_tasks()), (2, 2));

        // Switched off by configuration: expected falls with it, so the
        // deployment does not read as permanently degraded.
        health.task_disabled("beta", "disabled by config");
        assert_eq!((health.expected_tasks(), health.live_tasks()), (1, 1));

        // A genuine death shows up as a shortfall.
        health.task_failed("alpha");
        assert_eq!((health.expected_tasks(), health.live_tasks()), (1, 0));
    }

    #[tokio::test]
    async fn crash_loop_is_visible_while_restarting() {
        let health = TaskHealth::new();
        health.require("probe");
        let (tx, rx) = watch::channel(false);
        let shutdown_for_child = rx.clone();

        let handle = supervise_with(
            health.clone(),
            "probe",
            rx,
            move || {
                let shutdown = shutdown_for_child.clone();
                async move {
                    if *shutdown.borrow() {
                        return TaskExit::ShutdownRequested;
                    }
                    panic!("always boom");
                }
            },
            fast_backoff(),
        );

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if !health.crash_looping_required_tasks().is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("crash-loop was never recorded");

        assert_eq!(health.crash_looping_required_tasks(), vec!["probe"]);
        assert!(health.failed() >= crate::CRASH_LOOP_THRESHOLD as u64);
        assert!(health.restarts("probe") >= 1);

        let _ = tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
    }
}
