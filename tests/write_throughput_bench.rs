//! Measured write-throughput ceiling for the single-writer SQLite setup
//! (issue #321). Not run in CI — `#[ignore]`d, and deliberately not part of
//! `cargo test --locked`'s default set, since it measures wall-clock
//! performance rather than correctness and its number is only meaningful
//! relative to the hardware it runs on.
//!
//! Run it directly to reproduce a number:
//!
//! ```text
//! cargo test --release --test write_throughput_bench -- --ignored --nocapture
//! ```
//!
//! Methodology: a file-backed (not `:memory:` — issue #309 means an
//! in-memory pool with more than one connection is actually N separate
//! databases) SQLite pool opened with the exact PRAGMAs `main.rs::open_pool`
//! uses in production (WAL, `synchronous = NORMAL`, a busy timeout), and a
//! fixed pool size matching `DB_POOL_MAX_CONNECTIONS`'s documented default.
//! `CONCURRENCY` tasks each insert payments back-to-back for `DURATION`,
//! contending for SQLite's single writer exactly as concurrent request
//! handlers would. This isolates the write path itself from HTTP, auth, and
//! JSON overhead, which is the part issue #321 is actually about — every
//! writer this service has (payment creation, settlement, webhook delivery
//! bookkeeping, `last_used_at`) goes through the same single-writer lock this
//! measures.

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use stellargate::db::{self, NewPayment};

const CONCURRENCY: usize = 16;
const DURATION: Duration = Duration::from_secs(10);

#[tokio::test]
#[ignore]
async fn measures_sustained_payment_creation_throughput() {
    let dir = std::env::temp_dir().join(format!("stellargate-bench-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("bench.db");
    let database_url = format!("sqlite://{}", db_path.display());

    // Mirrors main.rs::open_pool exactly, so this measures the production
    // configuration rather than a more (or less) favorable one.
    let opts = SqliteConnectOptions::from_str(&database_url)
        .unwrap()
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .busy_timeout(Duration::from_millis(5000));
    let pool = SqlitePoolOptions::new()
        .max_connections(10) // DB_POOL_MAX_CONNECTIONS default
        .connect_with(opts)
        .await
        .unwrap();
    db::migrate(&pool).await.unwrap();

    let completed = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let mut tasks = Vec::with_capacity(CONCURRENCY);
    for worker in 0..CONCURRENCY {
        let pool = pool.clone();
        let completed = completed.clone();
        let errors = errors.clone();
        tasks.push(tokio::spawn(async move {
            let mut i: u64 = 0;
            while start.elapsed() < DURATION {
                let id = uuid::Uuid::new_v4().to_string();
                // Unique per (worker, i): memo is UNIQUE NOT NULL.
                let memo = format!("B{worker:02}{i:06}");
                let res = db::create_payment(
                    &pool,
                    NewPayment {
                        id: &id,
                        merchant_id: "bench-merchant",
                        destination_address:
                            "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
                        memo: &memo,
                        amount: "10.00",
                        asset: "XLM",
                        asset_issuer: None,
                        webhook_url: None,
                        ttl_secs: 3600,
                    },
                )
                .await;
                match res {
                    Ok(_) => {
                        completed.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                i += 1;
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    let elapsed = start.elapsed().as_secs_f64();
    let n = completed.load(Ordering::Relaxed);
    let errs = errors.load(Ordering::Relaxed);
    println!(
        "payments/sec = {:.1}  (completed={n}, errors={errs}, concurrency={CONCURRENCY}, elapsed={elapsed:.2}s)",
        n as f64 / elapsed
    );

    let _ = std::fs::remove_dir_all(&dir);
}
