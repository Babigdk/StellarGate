//! Proves the shared-cache in-memory SQLite fixture used across this suite
//! (issue #309) is genuinely shared across pooled connections — not merely
//! passing by connection-reuse luck the way a bare `sqlite::memory:` DSN did.
//!
//! `sqlite::memory:` gives every connection its own **private** database:
//! two connections opened against that DSN never see each other's writes,
//! even in the same process. A pool configured with more than one connection
//! (which is the default — and what `tests/concurrency_tests.rs` needs to
//! exercise real concurrent writers) could silently hand out a second,
//! unrelated database mid-test. `sqlite:file:<name>?mode=memory&cache=shared`
//! fixes this: every connection that opens the same named in-memory database
//! shares it, as long as at least one connection stays open (hence
//! `min_connections(1)` everywhere this DSN is used — SQLite drops a
//! shared-cache in-memory database once every connection to it closes).

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::Connection;
use std::str::FromStr;
use stellargate::db;

/// Two connections drawn from a shared-cache pool must see each other's
/// writes: a row inserted through one connection is immediately visible
/// through the other.
#[tokio::test]
async fn shared_cache_dsn_is_genuinely_shared_across_pooled_connections() {
    let dsn = format!(
        "sqlite:file:{}?mode=memory&cache=shared",
        uuid::Uuid::new_v4()
    );
    let pool = SqlitePoolOptions::new()
        .max_connections(2)
        .min_connections(1)
        .connect_with(SqliteConnectOptions::from_str(&dsn).unwrap())
        .await
        .unwrap();
    db::migrate(&pool).await.unwrap();

    // Acquire two connections and hold both open at once, so they are
    // provably distinct — not the pool handing back the same connection
    // twice.
    let mut conn_a = pool.acquire().await.unwrap();
    let mut conn_b = pool.acquire().await.unwrap();

    sqlx::query("INSERT INTO kv_state (key, value) VALUES (?, ?)")
        .bind("shared_test_key")
        .bind("written_via_connection_a")
        .execute(&mut *conn_a)
        .await
        .unwrap();

    let seen_on_b: String = sqlx::query_scalar("SELECT value FROM kv_state WHERE key = ?")
        .bind("shared_test_key")
        .fetch_one(&mut *conn_b)
        .await
        .unwrap();

    assert_eq!(
        seen_on_b, "written_via_connection_a",
        "a write on one pooled connection must be visible on another — the \
         database is shared, not per-connection"
    );
}

/// The footgun this fixture replaces, pinned so it can't silently come back:
/// two connections opened directly (bypassing the pool) against a bare
/// `sqlite::memory:` DSN do NOT share data — each gets its own private
/// database. This is exactly what let a multi-connection pool built on that
/// DSN pass its tests by connection-reuse luck rather than by construction.
#[tokio::test]
async fn bare_memory_dsn_does_not_share_data_across_connections() {
    let mut conn_a = sqlx::SqliteConnection::connect("sqlite::memory:")
        .await
        .unwrap();
    let mut conn_b = sqlx::SqliteConnection::connect("sqlite::memory:")
        .await
        .unwrap();

    sqlx::query("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .execute(&mut conn_a)
        .await
        .unwrap();

    // The same statement against connection B fails only if the table
    // genuinely doesn't exist there — proving the two connections are
    // talking to two different databases.
    let result = sqlx::query("SELECT * FROM t").execute(&mut conn_b).await;
    assert!(
        result.is_err(),
        "a bare sqlite::memory: DSN must NOT share schema/data across \
         connections — if this starts passing, SQLite's default behavior \
         has changed and the whole premise of issue #309 needs revisiting"
    );
}
