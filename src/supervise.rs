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
use tracing::{error, warn};

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
    Fut: Future<Output = ()> + Send + 'static,
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
    Fut: Future<Output = ()> + Send + 'static,
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
                Ok(()) => {
                    warn!(
                        task = name,
                        "background task returned unexpectedly; restarting"
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
                        return;
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
