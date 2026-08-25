//! Asserts a freshly `db::migrate`d database matches the checked-in schema
//! snapshot at `tests/schema_snapshot.sql` (issue #308).
//!
//! `db::migrate` (in `src/db.rs`) is the only schema definition that
//! actually runs — the `migrations/*.sql` directory that used to sit
//! alongside it was never read at runtime and had drifted to the point of
//! missing whole tables the running code depends on. Rather than maintain a
//! second, hand-synchronised definition, this test makes the *running*
//! schema self-verifying: any change to `db::migrate` that isn't reflected
//! in `tests/schema_snapshot.sql` fails CI here instead of drifting
//! silently, the same failure mode `migrations/` had.
//!
//! To update the snapshot after an intentional schema change, run this test
//! with `--nocapture` — on a mismatch it prints the freshly generated
//! snapshot text so it can be pasted directly into `tests/schema_snapshot.sql`.

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::str::FromStr;
use stellargate::db;

const SNAPSHOT: &str = include_str!("schema_snapshot.sql");

/// Every `CREATE TABLE` / `CREATE INDEX` statement SQLite stores for the
/// schema `db::migrate` produces on a fresh database, in the same
/// `(type, name)` order the snapshot file uses. SQLite strips `IF NOT
/// EXISTS` when it stores DDL, so this is the schema's true, canonical text
/// — comparing it directly is what makes this test self-verifying rather
/// than another hand-maintained copy that can itself drift.
async fn current_schema_statements() -> Vec<String> {
    let pool = SqlitePoolOptions::new()
        .connect_with(SqliteConnectOptions::from_str("sqlite::memory:").unwrap())
        .await
        .unwrap();
    db::migrate(&pool).await.unwrap();

    let rows: Vec<(String,)> =
        sqlx::query_as("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY type, name")
            .fetch_all(&pool)
            .await
            .unwrap();

    rows.into_iter().map(|(sql,)| sql).collect()
}

/// Parse the checked-in snapshot file into the same shape: one entry per
/// statement, split on a line containing only `;`. Leading `--`-comment
/// lines (the file's header) are dropped.
fn snapshot_statements() -> Vec<String> {
    let mut body: String = SNAPSHOT
        .lines()
        .skip_while(|line| line.starts_with("--") || line.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    // Guarantee a trailing "\n;\n" so the final statement splits the same
    // way as every other one — `.lines()` above already stripped the file's
    // trailing newline, so without this the last entry would keep its `;`
    // glued on instead of being consumed as a separator.
    body.push('\n');
    body.split("\n;\n")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

#[tokio::test]
async fn migrated_schema_matches_the_checked_in_snapshot() {
    let current = current_schema_statements().await;
    let expected = snapshot_statements();

    if current == expected {
        return;
    }

    let mut message = String::from(
        "The schema db::migrate produces no longer matches tests/schema_snapshot.sql.\n\
         If this change was intentional, replace the snapshot file's statements with \
         the freshly generated ones below (each already ends with a lone `;` line):\n\n",
    );
    for stmt in &current {
        message.push_str(stmt);
        message.push_str("\n;\n");
    }

    let missing: Vec<_> = expected.iter().filter(|s| !current.contains(s)).collect();
    let added: Vec<_> = current.iter().filter(|s| !expected.contains(s)).collect();
    if !missing.is_empty() {
        message.push_str(&format!(
            "\nIn the snapshot but NOT in the live schema ({} statement(s)):\n",
            missing.len()
        ));
        for stmt in missing {
            message.push_str(&format!("  - {}\n", stmt.lines().next().unwrap_or(stmt)));
        }
    }
    if !added.is_empty() {
        message.push_str(&format!(
            "\nIn the live schema but NOT in the snapshot ({} statement(s)):\n",
            added.len()
        ));
        for stmt in added {
            message.push_str(&format!("  + {}\n", stmt.lines().next().unwrap_or(stmt)));
        }
    }

    panic!("{message}");
}
