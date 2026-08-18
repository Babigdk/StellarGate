use anyhow::Result;
use sqlx::{Pool, Row, Sqlite};

pub type Db = Pool<Sqlite>;

/// Normalize a raw SQLite timestamp to strict RFC 3339 UTC with a Z suffix.
///
/// Handles both legacy rows (`"2026-04-29 15:00:00"` / `"2026-04-29T15:00:00"`)
/// and already-correct rows (`"2026-04-29T15:00:00Z"`). Any value that doesn't
/// look like a 19-character datetime is returned unchanged so we never silently
/// corrupt unexpected data.
fn normalize_ts(raw: &str) -> String {
    let s = raw.trim();
    // Already has an explicit offset/Z — nothing to do.
    if s.ends_with('Z') || s.contains('+') {
        return s.to_string();
    }
    // Replace the space separator with T if present, then append Z.
    if s.len() == 19 {
        let with_t = s.replacen(' ', "T", 1);
        return format!("{with_t}Z");
    }
    s.to_string()
}

/// `LIKE` pattern every stored timestamp must match: strict RFC 3339 UTC with
/// a `Z` suffix and no fractional seconds, e.g. `2026-04-29T15:00:00Z`. `_`
/// matches exactly one character, so this pins the length and the position of
/// every separator without needing per-digit character classes SQLite's
/// dialect of `LIKE` cannot express.
///
/// Backing every timestamp `CHECK` constraint below (issue #314): every write
/// path already produces exactly this format via `strftime('%Y-%m-%dT%H:%M:%SZ',
/// ...)`, so this makes that a guarantee SQLite enforces rather than a
/// convention a future write path could silently break — which is exactly how
/// `expires_at` ended up compared as a lexical string against rows in the
/// legacy `"YYYY-MM-DD HH:MM:SS"` form (no `T`, no `Z`), which sorts *before*
/// every compliant timestamp and so reads as permanently expired.
///
/// Applies only to newly created tables: `CREATE TABLE IF NOT EXISTS` does not
/// retroactively add a constraint to a table that already exists, so an
/// upgrade of a running deployment does not gain this guarantee for rows
/// already on disk — the startup normalisation below is what repairs those.
const TS_PATTERN: &str = "____-__-__T__:__:__Z";

pub async fn migrate(pool: &Db) -> Result<()> {
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS payments (
            id TEXT PRIMARY KEY,
            merchant_id TEXT NOT NULL DEFAULT 'anonymous',
            destination_address TEXT NOT NULL,
            memo TEXT NOT NULL UNIQUE,
            amount TEXT NOT NULL,
            asset TEXT NOT NULL DEFAULT 'XLM',
            asset_issuer TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            webhook_url TEXT,
            tx_hash TEXT,
            paid_amount TEXT,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
                CHECK (created_at LIKE '{TS_PATTERN}'),
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
                CHECK (updated_at LIKE '{TS_PATTERN}'),
            expires_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now','+1 hour'))
                CHECK (expires_at LIKE '{TS_PATTERN}')
        )",
    ))
    .execute(pool)
    .await?;

    /* Bring pre-existing payment tables up to schema. New databases already have
    `expires_at` from the CREATE TABLE above; older ones need it added in
    place. SQLite rejects a non-constant DEFAULT on ALTER ... ADD COLUMN, so we
    add it nullable and backfill below. */
    let has_expires_at: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pragma_table_info('payments') WHERE name = 'expires_at'",
    )
    .fetch_one(pool)
    .await?;
    if has_expires_at == 0 {
        sqlx::query("ALTER TABLE payments ADD COLUMN expires_at TEXT")
            .execute(pool)
            .await?;
    }
    /* Backfill any row without an expiry (legacy rows, or rows inserted in the
    brief window before the column existed). `created_at + 1h` mirrors the
    default TTL; SQLite's date functions accept the stored RFC 3339 `Z` form. */
    sqlx::query(
        "UPDATE payments
            SET expires_at = strftime('%Y-%m-%dT%H:%M:%SZ', created_at, '+1 hour')
          WHERE expires_at IS NULL",
    )
    .execute(pool)
    .await?;

    /* Pin each intent to the issuer it was priced in. Rows written before this
    column existed only stored the asset *code*; `backfill_asset_issuers` fills
    them from the current allow-list after config loads (issue #222). */
    let has_asset_issuer: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pragma_table_info('payments') WHERE name = 'asset_issuer'",
    )
    .fetch_one(pool)
    .await?;
    if has_asset_issuer == 0 {
        sqlx::query("ALTER TABLE payments ADD COLUMN asset_issuer TEXT")
            .execute(pool)
            .await?;
    }

    sqlx::query("CREATE INDEX IF NOT EXISTS idx_payments_memo ON payments(memo)")
        .execute(pool)
        .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_payments_status ON payments(status)")
        .execute(pool)
        .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_payments_created_id ON payments(created_at DESC, id DESC)",
    )
    .execute(pool)
    .await?;

    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS webhook_deliveries (
            id TEXT PRIMARY KEY,
            payment_id TEXT NOT NULL,
            url TEXT NOT NULL,
            payload TEXT NOT NULL,
            event_type TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            last_attempt TEXT CHECK (last_attempt IS NULL OR last_attempt LIKE '{TS_PATTERN}'),
            acknowledged_at TEXT CHECK (acknowledged_at IS NULL OR acknowledged_at LIKE '{TS_PATTERN}'),
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
                CHECK (created_at LIKE '{TS_PATTERN}')
        )",
    ))
    .execute(pool)
    .await?;

    /* Bring pre-existing delivery tables up to schema. `event_type` records
    which event the payload represents so a redelivery can echo the original
    `X-StellarGate-Event` header instead of guessing. Rows written before this
    column existed stay NULL; readers fall back to the `event` field inside the
    stored payload. */
    let has_event_type: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pragma_table_info('webhook_deliveries') WHERE name = 'event_type'",
    )
    .fetch_one(pool)
    .await?;
    if has_event_type == 0 {
        sqlx::query("ALTER TABLE webhook_deliveries ADD COLUMN event_type TEXT")
            .execute(pool)
            .await?;
    }

    /* `acknowledged_at` records that somebody has seen a terminal failure and
    acted on it — set by the bulk requeue/acknowledge endpoint. It exists so
    retention can distinguish "this failure was dealt with" from "nobody has
    looked at this yet", and refuse to delete the latter (issue #319). Rows
    that predate the column are NULL, i.e. unacknowledged, which is the safe
    reading: we do not know that anyone saw them. */
    let has_acknowledged_at: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pragma_table_info('webhook_deliveries') WHERE name = 'acknowledged_at'",
    )
    .fetch_one(pool)
    .await?;
    if has_acknowledged_at == 0 {
        sqlx::query("ALTER TABLE webhook_deliveries ADD COLUMN acknowledged_at TEXT")
            .execute(pool)
            .await?;
    }

    /* Durable key/value state — used by the Horizon poller to persist its
    paging cursor so it resumes exactly where it left off across restarts. */
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS kv_state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
                CHECK (updated_at LIKE '{TS_PATTERN}')
        )",
    ))
    .execute(pool)
    .await?;

    /* Merchants are provisioned via POST /merchants. The raw API key is never
    stored; only its SHA-256 hex digest is persisted so a DB breach does not
    expose live credentials. */
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS merchants (
            id TEXT PRIMARY KEY,
            api_key_hash TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
                CHECK (created_at LIKE '{TS_PATTERN}')
        )",
    ))
    .execute(pool)
    .await?;

    /* API keys, one row per credential rather than one per merchant, so a key
    can be rotated (issue a second, revoke the first) and revoked individually
    without disturbing the merchant record.

    Only the SHA-256 digest is stored; `prefix` keeps the first few characters
    of the raw key so an operator can tell two keys apart in a list without the
    secret being recoverable. `revoked_at` is a tombstone rather than a delete
    so an audit trail survives revocation. */
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS api_keys (
            id TEXT PRIMARY KEY,
            merchant_id TEXT NOT NULL,
            key_hash TEXT NOT NULL UNIQUE,
            prefix TEXT NOT NULL,
            label TEXT,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
                CHECK (created_at LIKE '{TS_PATTERN}'),
            last_used_at TEXT CHECK (last_used_at IS NULL OR last_used_at LIKE '{TS_PATTERN}'),
            revoked_at TEXT CHECK (revoked_at IS NULL OR revoked_at LIKE '{TS_PATTERN}')
        )",
    ))
    .execute(pool)
    .await?;

    /* Authentication looks a key up by hash on every request, so this index is
    load-bearing rather than an optimisation. */
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash)")
        .execute(pool)
        .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_api_keys_merchant ON api_keys(merchant_id)")
        .execute(pool)
        .await?;

    /* Carry pre-existing single-key merchants across. Their raw key is not
    recoverable, but the hash is all authentication needs, so keys issued
    before this table existed keep working. The prefix is unknown for those
    rows — mark them rather than inventing one. */
    sqlx::query(
        "INSERT OR IGNORE INTO api_keys (id, merchant_id, key_hash, prefix, label, created_at)
         SELECT lower(hex(randomblob(16))), id, api_key_hash, 'legacy', 'migrated', created_at
           FROM merchants
          WHERE api_key_hash IS NOT NULL AND api_key_hash <> ''",
    )
    .execute(pool)
    .await?;

    /* `webhook_deliveries` is queried by payment_id on every delivery listing
    and by the redrive worker; without this it is a full scan (issue #112). */
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_payment
         ON webhook_deliveries(payment_id)",
    )
    .execute(pool)
    .await?;

    /* Idempotency keys for payment creation. A key is unique per merchant and
    maps to the payment id minted for the first request that used it, so a
    client retrying after a network blip gets the original payment back
    instead of a duplicate intent. */
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS idempotency_keys (
            merchant_id TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            payment_id TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
                CHECK (created_at LIKE '{TS_PATTERN}'),
            PRIMARY KEY (merchant_id, idempotency_key)
        )",
    ))
    .execute(pool)
    .await?;

    /* Every on-chain transaction we credit to an intent, one row per
    (payment_id, tx_hash). The cumulative received amount for an intent is the
    SUM of `amount_stroops` over its rows, so re-seeing a transaction (on a
    later poll cycle, over the stream, or from a concurrent reconciler) is an
    idempotent no-op instead of a double-credit. `amount_stroops` is the
    integer stroop value so SUM is exact. */
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS processed_transactions (
            payment_id TEXT NOT NULL,
            tx_hash TEXT NOT NULL,
            amount_stroops INTEGER NOT NULL,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
                CHECK (created_at LIKE '{TS_PATTERN}'),
            PRIMARY KEY (payment_id, tx_hash)
        )",
    ))
    .execute(pool)
    .await?;

    /* Backfill from legacy rows that recorded only the most-recent `tx_hash`
    and a cumulative `paid_amount`, so upgrading preserves the received-amount
    ledger for intents that are still in flight. Idempotent via ON CONFLICT, so
    it is safe to run on every startup. */
    let legacy = sqlx::query(
        "SELECT id, tx_hash, paid_amount FROM payments
         WHERE tx_hash IS NOT NULL AND tx_hash <> '' AND paid_amount IS NOT NULL",
    )
    .fetch_all(pool)
    .await?;
    for row in &legacy {
        let id: String = row.get("id");
        let tx_hash: String = row.get("tx_hash");
        let paid_amount: String = row.get("paid_amount");
        if let Some(stroops) = crate::money::parse_stroops(&paid_amount) {
            sqlx::query(
                "INSERT INTO processed_transactions (payment_id, tx_hash, amount_stroops)
                 VALUES (?, ?, ?)
                 ON CONFLICT(payment_id, tx_hash) DO NOTHING",
            )
            .bind(&id)
            .bind(&tx_hash)
            .bind(stroops)
            .execute(pool)
            .await?;
        }
    }

    /* Normalise legacy rows that were written by the old datetime('now') default,
    which produced "YYYY-MM-DD HH:MM:SS" (space, no Z). Safe to run on every
    startup — the WHERE clause skips rows that are already RFC 3339.

    `expires_at` is included for the same reason as the others (issue #314):
    left in the legacy space-separated form, it sorts *before* every compliant
    "…T…Z" timestamp — 'T' (0x54) > ' ' (0x20) — so `expires_at > strftime(...)`
    in list_pending/expire_overdue/find_pending_by_memo reads such a row as
    already expired. It would never surface as a detectable payment again and
    would be swept on the very next expiry cycle. */
    for tbl_col in [
        ("payments", "created_at"),
        ("payments", "updated_at"),
        ("payments", "expires_at"),
        ("webhook_deliveries", "created_at"),
    ] {
        let sql = format!(
            "UPDATE {} SET {col} = replace({col}, ' ', 'T') || 'Z' WHERE {col} NOT LIKE '%T%'",
            tbl_col.0,
            col = tbl_col.1
        );
        sqlx::query(&sql).execute(pool).await?;
    }

    Ok(())
}

/// Fill `asset_issuer` on rows that only stored a code, using the current
/// allow-list. Duplicate codes are rejected at boot, so each code maps to at
/// most one issuer. Native assets stay NULL.
pub async fn backfill_asset_issuers(
    pool: &Db,
    accepted: &[crate::config::AcceptedAsset],
) -> Result<()> {
    for asset in accepted {
        let Some(issuer) = asset.issuer.as_deref() else {
            continue;
        };
        sqlx::query(
            "UPDATE payments
                SET asset_issuer = ?
              WHERE upper(asset) = upper(?)
                AND (asset_issuer IS NULL OR asset_issuer = '')",
        )
        .bind(issuer)
        .bind(&asset.code)
        .execute(pool)
        .await?;
    }
    Ok(())
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Payment {
    pub id: String,
    pub merchant_id: String,
    pub destination_address: String,
    pub memo: String,
    pub amount: String,
    pub asset: String,
    pub status: String,
    pub webhook_url: Option<String>,
    pub tx_hash: Option<String>,
    pub paid_amount: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    /// When this intent stops being `pending` and is swept to `expired`.
    pub expires_at: String,
    /// Issuer account for a credit asset; `None` for native XLM. Settlement
    /// matches this issuer, not any allow-list entry that shares the code
    /// (issue #222).
    pub asset_issuer: Option<String>,
}

fn row_to_payment(row: &sqlx::sqlite::SqliteRow) -> Payment {
    Payment {
        id: row.get("id"),
        merchant_id: row.get("merchant_id"),
        destination_address: row.get("destination_address"),
        memo: row.get("memo"),
        amount: row.get("amount"),
        asset: row.get("asset"),
        status: row.get("status"),
        webhook_url: row.get("webhook_url"),
        tx_hash: row.get("tx_hash"),
        paid_amount: row.get("paid_amount"),
        created_at: normalize_ts(&row.get::<String, _>("created_at")),
        updated_at: normalize_ts(&row.get::<String, _>("updated_at")),
        expires_at: normalize_ts(&row.get::<String, _>("expires_at")),
        asset_issuer: row.get("asset_issuer"),
    }
}

/// Fields needed to insert a new payment intent.
pub struct NewPayment<'a> {
    pub id: &'a str,
    pub merchant_id: &'a str,
    pub destination_address: &'a str,
    pub memo: &'a str,
    pub amount: &'a str,
    pub asset: &'a str,
    /// Issuer for `asset`; `None` for native XLM.
    pub asset_issuer: Option<&'a str>,
    pub webhook_url: Option<&'a str>,
    /// Seconds from now until the intent expires. The expiry timestamp is
    /// computed by SQLite at insert time as `now + ttl_secs`.
    pub ttl_secs: i64,
}

pub async fn create_payment(pool: &Db, new: NewPayment<'_>) -> Result<Payment> {
    /* Canonicalize the amount: parse to stroops, then convert back to the
    canonical string representation. This ensures "10.00", "10.0", and "10"
    all serialize identically, eliminating spurious string-based comparisons
    across create/get/webhook responses. */
    let stroops =
        crate::money::parse_stroops(new.amount).ok_or_else(|| anyhow::anyhow!("Invalid amount"))?;
    let canonical_amount = crate::money::stroops_to_string(stroops);

    /* Compute the expiry as `now + ttl_secs` in SQLite so it shares the exact
    clock and RFC 3339 format as created_at. */
    let ttl_modifier = format!("{:+} seconds", new.ttl_secs);
    sqlx::query(
        "INSERT INTO payments (id, merchant_id, destination_address, memo, amount, asset, asset_issuer, webhook_url, expires_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%SZ','now',?))",
    )
    .bind(new.id)
    .bind(new.merchant_id)
    .bind(new.destination_address)
    .bind(new.memo)
    .bind(&canonical_amount)
    .bind(new.asset)
    .bind(new.asset_issuer)
    .bind(new.webhook_url)
    .bind(&ttl_modifier)
    .execute(pool)
    .await?;

    get_payment(pool, new.id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Payment not found after insert"))
}

/// Look up the payment id previously minted for `(merchant_id, key)`, if any.
pub async fn find_payment_id_by_idempotency_key(
    pool: &Db,
    merchant_id: &str,
    key: &str,
) -> Result<Option<String>> {
    let id: Option<String> = sqlx::query_scalar(
        "SELECT payment_id FROM idempotency_keys WHERE merchant_id = ? AND idempotency_key = ?",
    )
    .bind(merchant_id)
    .bind(key)
    .fetch_optional(pool)
    .await?;
    Ok(id)
}

/// Record the payment id minted for `(merchant_id, key)`. If the key already
/// exists (e.g. a concurrent request won the race), the existing mapping is left
/// untouched and the winning payment id is returned; otherwise `payment_id` is
/// stored and returned.
pub async fn save_idempotency_key(
    pool: &Db,
    merchant_id: &str,
    key: &str,
    payment_id: &str,
) -> Result<String> {
    sqlx::query(
        "INSERT INTO idempotency_keys (merchant_id, idempotency_key, payment_id)
         VALUES (?, ?, ?)
         ON CONFLICT(merchant_id, idempotency_key) DO NOTHING",
    )
    .bind(merchant_id)
    .bind(key)
    .bind(payment_id)
    .execute(pool)
    .await?;

    // Re-read so a concurrent insert that won the race returns the canonical id.
    let stored = find_payment_id_by_idempotency_key(pool, merchant_id, key)
        .await?
        .unwrap_or_else(|| payment_id.to_string());
    Ok(stored)
}

pub async fn get_payment(pool: &Db, id: &str) -> Result<Option<Payment>> {
    let row = sqlx::query(
        "SELECT id, merchant_id, destination_address, memo, amount, asset, asset_issuer, status,
                webhook_url, tx_hash, paid_amount, created_at, updated_at, expires_at
         FROM payments WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(row_to_payment))
}

/// Offset variant of `list_payments_keyset`. Rows are ordered by
/// `(created_at DESC, id DESC)` — exactly the keyset ordering — so a
/// `next_cursor` minted from this page resumes in cursor mode without
/// skipping or repeating rows. `created_at` is whole-second, so ties are
/// common; leaving their order to SQLite lets offset pages repeat or skip
/// rows and would make the migration cursor diverge from the keyset scan.
/// Offset-paginated page of a merchant's payments. Does **not** compute a row
/// count — see [`count_payments`] (issue #320). SQLite has no cached row
/// count, so a `COUNT(*)` here would scan every matching row on every list
/// request (including the first page) purely to fill a `total` field most
/// callers never read; keeping it a separate, opt-in query means the default
/// list path never pays for it.
pub async fn list_payments(
    pool: &Db,
    merchant_id: &str,
    status: Option<&str>,
    limit: i64,
    offset: i64,
) -> Result<Vec<Payment>> {
    let rows = if let Some(s) = status {
        sqlx::query(
            "SELECT id, merchant_id, destination_address, memo, amount, asset, asset_issuer, status,
                    webhook_url, tx_hash, paid_amount, created_at, updated_at, expires_at
             FROM payments WHERE merchant_id = ? AND status = ? ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?",
        )
        .bind(merchant_id)
        .bind(s)
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query(
            "SELECT id, merchant_id, destination_address, memo, amount, asset, asset_issuer, status,
                    webhook_url, tx_hash, paid_amount, created_at, updated_at, expires_at
             FROM payments WHERE merchant_id = ? ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?",
        )
        .bind(merchant_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await?
    };

    Ok(rows.iter().map(row_to_payment).collect())
}

/// Count a merchant's payments matching an optional status filter. Split out
/// from [`list_payments`] so the default `GET /payments` path never pays for
/// a full-table `COUNT(*)` — this only runs when a caller explicitly asks for
/// `total` via `?include_total=true` (issue #320).
pub async fn count_payments(pool: &Db, merchant_id: &str, status: Option<&str>) -> Result<i64> {
    let total = if let Some(s) = status {
        sqlx::query_scalar("SELECT COUNT(*) FROM payments WHERE merchant_id = ? AND status = ?")
            .bind(merchant_id)
            .bind(s)
            .fetch_one(pool)
            .await?
    } else {
        sqlx::query_scalar("SELECT COUNT(*) FROM payments WHERE merchant_id = ?")
            .bind(merchant_id)
            .fetch_one(pool)
            .await?
    };
    Ok(total)
}

pub async fn list_payments_keyset(
    pool: &Db,
    merchant_id: &str,
    status: Option<&str>,
    limit: i64,
    cursor: Option<(&str, &str)>,
) -> Result<Vec<Payment>> {
    let rows = match (status, cursor) {
        (None, None) => {
            sqlx::query(
                "SELECT id, merchant_id, destination_address, memo, amount, asset, asset_issuer, status,
                    webhook_url, tx_hash, paid_amount, created_at, updated_at, expires_at
             FROM payments WHERE merchant_id = ? ORDER BY created_at DESC, id DESC LIMIT ?",
            )
            .bind(merchant_id)
            .bind(limit)
            .fetch_all(pool)
            .await?
        }

        (None, Some((ts, cid))) => {
            sqlx::query(
                "SELECT id, merchant_id, destination_address, memo, amount, asset, asset_issuer, status,
                    webhook_url, tx_hash, paid_amount, created_at, updated_at, expires_at
             FROM payments
             WHERE merchant_id = ? AND (created_at < ? OR (created_at = ? AND id < ?))
             ORDER BY created_at DESC, id DESC LIMIT ?",
            )
            .bind(merchant_id)
            .bind(ts)
            .bind(ts)
            .bind(cid)
            .bind(limit)
            .fetch_all(pool)
            .await?
        }

        (Some(s), None) => {
            sqlx::query(
                "SELECT id, merchant_id, destination_address, memo, amount, asset, asset_issuer, status,
                    webhook_url, tx_hash, paid_amount, created_at, updated_at, expires_at
             FROM payments WHERE merchant_id = ? AND status = ? ORDER BY created_at DESC, id DESC LIMIT ?",
            )
            .bind(merchant_id)
            .bind(s)
            .bind(limit)
            .fetch_all(pool)
            .await?
        }

        (Some(s), Some((ts, cid))) => {
            sqlx::query(
                "SELECT id, merchant_id, destination_address, memo, amount, asset, asset_issuer, status,
                    webhook_url, tx_hash, paid_amount, created_at, updated_at, expires_at
             FROM payments
             WHERE merchant_id = ? AND status = ? AND (created_at < ? OR (created_at = ? AND id < ?))
             ORDER BY created_at DESC, id DESC LIMIT ?",
            )
            .bind(merchant_id)
            .bind(s)
            .bind(ts)
            .bind(ts)
            .bind(cid)
            .bind(limit)
            .fetch_all(pool)
            .await?
        }
    };

    Ok(rows.iter().map(row_to_payment).collect())
}

/// All payments still awaiting confirmation or top-up, oldest first. Rows whose
/// TTL has elapsed are excluded even if the sweeper hasn't transitioned them
/// yet, so an overdue intent is never polled.
pub async fn list_pending(pool: &Db) -> Result<Vec<Payment>> {
    let rows = sqlx::query(
        "SELECT id, merchant_id, destination_address, memo, amount, asset, asset_issuer, status,
                webhook_url, tx_hash, paid_amount, created_at, updated_at, expires_at
         FROM payments
         WHERE status IN ('pending', 'underpaid')
           AND expires_at > strftime('%Y-%m-%dT%H:%M:%SZ','now')
         ORDER BY created_at ASC",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(row_to_payment).collect())
}

/// Transition up to `batch` watchable payments whose TTL has elapsed to
/// `expired`, returning the rows that were swept so the caller can fire
/// `payment.expired` webhooks.
///
/// The whole batch is transitioned in a single `UPDATE … RETURNING` — one
/// round-trip instead of one guarded `UPDATE` per intent (issue #323). The
/// `WHERE … status IN ('pending','underpaid')` guard remains what makes a
/// concurrent settlement win the race: the subquery and update run under one
/// write lock, so a payment that settles in between is never selected here
/// (if the settlement committed first) and a payment this statement sweeps is
/// rejected by the settlement's own guard (issue #155) — never double-reported.
/// `RETURNING` yields exactly the rows this statement actually transitioned.
///
/// `batch` bounds each statement, so a large backlog drains over several
/// sweeps instead of one long write lock.
pub async fn expire_overdue(pool: &Db, batch: i64) -> Result<Vec<Payment>> {
    let rows = sqlx::query(
        "UPDATE payments
            SET status = 'expired',
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now')
          WHERE id IN (
              SELECT id FROM payments
               WHERE status IN ('pending', 'underpaid')
                 AND expires_at <= strftime('%Y-%m-%dT%H:%M:%SZ','now')
               ORDER BY created_at ASC
               LIMIT ?
          )
          RETURNING id, merchant_id, destination_address, memo, amount, asset,
                    asset_issuer, status, webhook_url, tx_hash, paid_amount,
                    created_at, updated_at, expires_at",
    )
    .bind(batch)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(row_to_payment).collect())
}

pub async fn find_pending_by_memo(pool: &Db, memo: &str) -> Result<Option<Payment>> {
    let row = sqlx::query(
        "SELECT id, merchant_id, destination_address, memo, amount, asset, asset_issuer, status,
                webhook_url, tx_hash, paid_amount, created_at, updated_at, expires_at
         FROM payments
         WHERE memo = ?
           AND status IN ('pending', 'underpaid')
           AND expires_at > strftime('%Y-%m-%dT%H:%M:%SZ','now')",
    )
    .bind(memo)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(row_to_payment))
}

/// Transition a payment to a new status, returning `true` when the row was
/// actually updated.
///
/// The `WHERE … AND status IN ('pending', 'underpaid')` guard is the key to
/// single-settlement under concurrent reconciliation (issue #155): SQLite's
/// serialized write path ensures that exactly one of two racing UPDATE
/// statements will match a row still in a watchable state. The loser sees
/// `rows_affected() == 0` and knows it must skip the webhook.
pub async fn update_payment_status(
    pool: &Db,
    id: &str,
    status: &str,
    tx_hash: &str,
    paid_amount: &str,
) -> Result<bool> {
    let result = sqlx::query(
        "UPDATE payments
            SET status = ?, tx_hash = ?, paid_amount = ?,
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now')
          WHERE id = ?
            AND status IN ('pending', 'underpaid')",
    )
    .bind(status)
    .bind(tx_hash)
    .bind(paid_amount)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Record that transaction `tx_hash`, worth `amount_stroops`, has been credited
/// to intent `payment_id`. Returns `true` when this is the first time the
/// transaction was recorded for the intent, and `false` when it was already
/// present (a re-seen record on a later poll cycle, over the stream, or from a
/// concurrent reconciler).
///
/// The `(payment_id, tx_hash)` primary key plus `ON CONFLICT DO NOTHING` makes
/// this the atomic dedup point: SQLite serialises writers, so exactly one of
/// two racing inserts for the same transaction observes `rows_affected() == 1`.
pub async fn record_processed_tx(
    pool: &Db,
    payment_id: &str,
    tx_hash: &str,
    amount_stroops: i64,
) -> Result<bool> {
    let result = sqlx::query(
        "INSERT INTO processed_transactions (payment_id, tx_hash, amount_stroops)
         VALUES (?, ?, ?)
         ON CONFLICT(payment_id, tx_hash) DO NOTHING",
    )
    .bind(payment_id)
    .bind(tx_hash)
    .bind(amount_stroops)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Sum of every transaction recorded against `payment_id`, in stroops. This is
/// the authoritative received-amount ledger for an intent — independent of how
/// many transactions arrived, or the order they were seen in.
pub async fn sum_processed_stroops(pool: &Db, payment_id: &str) -> Result<i64> {
    let total: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(amount_stroops), 0) FROM processed_transactions WHERE payment_id = ?",
    )
    .bind(payment_id)
    .fetch_one(pool)
    .await?;
    Ok(total)
}

/// Read a value from the durable key/value state table, if present.
pub async fn get_state(pool: &Db, key: &str) -> Result<Option<String>> {
    let value: Option<String> = sqlx::query_scalar("SELECT value FROM kv_state WHERE key = ?")
        .bind(key)
        .fetch_optional(pool)
        .await?;
    Ok(value)
}

/// Insert or update a value in the durable key/value state table.
pub async fn set_state(pool: &Db, key: &str, value: &str) -> Result<()> {
    sqlx::query(
        "INSERT INTO kv_state (key, value, updated_at)
         VALUES (?, ?, strftime('%Y-%m-%dT%H:%M:%SZ','now'))
         ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            updated_at = excluded.updated_at",
    )
    .bind(key)
    .bind(value)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn memo_exists(pool: &Db, memo: &str) -> Result<bool> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM payments WHERE memo = ?")
        .bind(memo)
        .fetch_one(pool)
        .await?;
    Ok(count > 0)
}

/// Record an outbound webhook delivery. `event_type` is the event name the
/// payload carries (e.g. `payment.underpaid`); it is persisted so a later
/// redelivery can reproduce the original `X-StellarGate-Event` header.
pub async fn save_webhook_delivery(
    pool: &Db,
    id: &str,
    payment_id: &str,
    url: &str,
    payload: &str,
    event_type: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO webhook_deliveries (id, payment_id, url, payload, event_type) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(id)
    .bind(payment_id)
    .bind(url)
    .bind(payload)
    .bind(event_type)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn update_webhook_delivery(
    pool: &Db,
    id: &str,
    status: &str,
    attempts: i64,
) -> Result<()> {
    sqlx::query(
        "UPDATE webhook_deliveries SET status = ?, attempts = ?, last_attempt = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ?",
    )
    .bind(status)
    .bind(attempts)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct WebhookDelivery {
    pub id: String,
    pub payment_id: String,
    pub url: String,
    pub payload: String,
    /// The event this payload represents. `None` only for rows written before
    /// the column existed — use [`WebhookDelivery::event`] to read it.
    pub event_type: Option<String>,
    pub status: String,
    pub attempts: i64,
    pub last_attempt: Option<String>,
    /// When somebody acted on this delivery — requeued it, or explicitly
    /// acknowledged it. `None` means nobody has looked at it yet, which is
    /// what keeps a terminal failure exempt from retention (issue #319).
    pub acknowledged_at: Option<String>,
    pub created_at: String,
}

/// Event name used when a legacy row has no `event_type` and its payload can't
/// be parsed. Every payload this gateway has ever written carries an `event`
/// field, so this is a last resort rather than an expected path.
const FALLBACK_EVENT: &str = "payment.completed";

impl WebhookDelivery {
    /// The event name to report for this delivery, falling back to the `event`
    /// field of the stored payload for rows written before `event_type`
    /// existed. Used to reproduce the original `X-StellarGate-Event` header on
    /// redelivery so the header can never contradict the body.
    pub fn event(&self) -> String {
        if let Some(event) = &self.event_type {
            return event.clone();
        }
        serde_json::from_str::<serde_json::Value>(&self.payload)
            .ok()
            .and_then(|v| v.get("event")?.as_str().map(str::to_string))
            .unwrap_or_else(|| FALLBACK_EVENT.to_string())
    }
}

fn row_to_webhook_delivery(row: &sqlx::sqlite::SqliteRow) -> WebhookDelivery {
    WebhookDelivery {
        id: row.get("id"),
        payment_id: row.get("payment_id"),
        url: row.get("url"),
        payload: row.get("payload"),
        event_type: row.get("event_type"),
        status: row.get("status"),
        attempts: row.get("attempts"),
        last_attempt: row.get("last_attempt"),
        acknowledged_at: row.get("acknowledged_at"),
        created_at: normalize_ts(&row.get::<String, _>("created_at")),
    }
}

/// Columns every delivery read selects, in the order `row_to_webhook_delivery`
/// expects. Kept in one place so adding a column cannot leave one query behind.
const DELIVERY_COLUMNS: &str = "id, payment_id, url, payload, event_type, status, attempts, \
                                last_attempt, acknowledged_at, created_at";

/// Deliveries eligible for the background redrive worker: not yet delivered,
/// under the attempt cap, and idle long enough that no in-flight `dispatch()`
/// call for the same row can still be running.
///
/// A delivery row's status only changes at the *end* of a `dispatch()` call
/// (success, final failure, or an SSRF rejection); while an attempt is still
/// in progress the row stays `pending` with no signal that work is under way.
/// `grace_secs` is the idle window past `last_attempt` (or `created_at` for a
/// row never attempted) that a row must clear before being considered stuck
/// rather than merely in flight — callers must size it comfortably above the
/// worst-case inline delivery time so this worker never races a live
/// `dispatch()` for the same row. It is also the hard floor under the
/// exponential backoff below, so a row is never touched sooner than this
/// regardless of `attempts`.
///
/// A row that has failed at least once (`attempts > 0`) additionally has to
/// clear an exponential backoff — `backoff_initial_secs * 2^(attempts-1)`,
/// capped at `backoff_max_secs` — before it is considered eligible again.
/// A row with `attempts == 0` (left behind by a crash between insert and its
/// first send, not a delivery failure) is exempt from this backoff and is
/// gated by `grace_secs` alone.
///
/// `jitter_secs` adds a per-row random offset in `[0, jitter_secs]` on top of
/// whichever window applies, and is what actually decorrelates a co-failing
/// batch (issue #318). The exponential backoff alone does not: rows that failed
/// together share an `attempts` value and a near-identical `last_attempt`, so
/// `initial * 2^(attempts-1)` resolves to the same instant for every one of
/// them, and this query — which computes eligibility in SQL from `last_attempt`
/// — re-clusters the batch on every pass. `RANDOM()` is evaluated per row per
/// statement, so each pass admits a different random subset and a batch that
/// failed together spreads over several intervals instead of moving as one
/// block. Pass `0` to disable.
pub async fn list_redrivable_deliveries(
    pool: &Db,
    max_attempts: i64,
    grace_secs: i64,
    backoff_initial_secs: i64,
    backoff_max_secs: i64,
    jitter_secs: i64,
) -> Result<Vec<WebhookDelivery>> {
    /* `ABS(RANDOM()) % (n+1)` yields [0, n]. Guarded on `jitter_secs > 0`:
    `% 1` is a constant 0, and a zero modulus is a runtime error in SQLite. */
    let rows = sqlx::query(&format!(
        "SELECT {DELIVERY_COLUMNS}
         FROM webhook_deliveries
         WHERE status IN ('pending', 'failed')
           AND attempts < ?
           AND datetime(COALESCE(last_attempt, created_at), '+' || (
                 CASE WHEN attempts = 0 THEN ?
                      ELSE MAX(?, MIN(? * (1 << MIN(attempts - 1, 32)), ?))
                 END
                 + CASE WHEN ? > 0 THEN ABS(RANDOM()) % (? + 1) ELSE 0 END
               ) || ' seconds') <= datetime('now')
         ORDER BY created_at ASC",
    )
    .bind(max_attempts)
    .bind(grace_secs)
    .bind(grace_secs)
    .bind(backoff_initial_secs)
    .bind(backoff_max_secs)
    .bind(jitter_secs)
    .bind(jitter_secs)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(row_to_webhook_delivery).collect())
}

/// Get all webhook deliveries for a payment, ordered by created_at descending.
pub async fn list_webhook_deliveries(pool: &Db, payment_id: &str) -> Result<Vec<WebhookDelivery>> {
    let rows = sqlx::query(&format!(
        "SELECT {DELIVERY_COLUMNS}
         FROM webhook_deliveries WHERE payment_id = ? ORDER BY created_at DESC",
    ))
    .bind(payment_id)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(row_to_webhook_delivery).collect())
}

/// Get a page of webhook deliveries for a payment with keyset (cursor)
/// pagination, sharing the contracts used by `GET /payments`.
///
/// Rows are ordered by `(created_at DESC, id DESC)` — the same ordering and
/// tie-break as the payments listing — so a `next_cursor` encoded from any
/// page resumes exactly after its last row and never re-reads or skips the
/// whole-second `created_at` tie group that ends the page. An optional
/// `status` filter narrows to deliveries in that state (`pending`,
/// `delivered`, or `failed`).
pub async fn list_webhook_deliveries_keyset(
    pool: &Db,
    payment_id: &str,
    status: Option<&str>,
    limit: i64,
    cursor: Option<(&str, &str)>,
) -> Result<Vec<WebhookDelivery>> {
    let rows = match (status, cursor) {
        (None, None) => {
            sqlx::query(&format!(
                "SELECT {DELIVERY_COLUMNS}
                 FROM webhook_deliveries WHERE payment_id = ?
                 ORDER BY created_at DESC, id DESC LIMIT ?",
            ))
            .bind(payment_id)
            .bind(limit)
            .fetch_all(pool)
            .await?
        }

        (None, Some((ts, cid))) => {
            sqlx::query(&format!(
                "SELECT {DELIVERY_COLUMNS}
                 FROM webhook_deliveries
                 WHERE payment_id = ? AND (created_at < ? OR (created_at = ? AND id < ?))
                 ORDER BY created_at DESC, id DESC LIMIT ?",
            ))
            .bind(payment_id)
            .bind(ts)
            .bind(ts)
            .bind(cid)
            .bind(limit)
            .fetch_all(pool)
            .await?
        }

        (Some(s), None) => {
            sqlx::query(&format!(
                "SELECT {DELIVERY_COLUMNS}
                 FROM webhook_deliveries WHERE payment_id = ? AND status = ?
                 ORDER BY created_at DESC, id DESC LIMIT ?",
            ))
            .bind(payment_id)
            .bind(s)
            .bind(limit)
            .fetch_all(pool)
            .await?
        }

        (Some(s), Some((ts, cid))) => {
            sqlx::query(&format!(
                "SELECT {DELIVERY_COLUMNS}
                 FROM webhook_deliveries
                 WHERE payment_id = ? AND status = ?
                   AND (created_at < ? OR (created_at = ? AND id < ?))
                 ORDER BY created_at DESC, id DESC LIMIT ?",
            ))
            .bind(payment_id)
            .bind(s)
            .bind(ts)
            .bind(ts)
            .bind(cid)
            .bind(limit)
            .fetch_all(pool)
            .await?
        }
    };

    Ok(rows.iter().map(row_to_webhook_delivery).collect())
}

/// List a *merchant's* deliveries across every one of their payments, filtered
/// by status and paginated with the same keyset cursor the payments list uses
/// (issue #319).
///
/// This is the query the dead-letter view is built on. Answering "a merchant
/// says they are missing events" previously required already knowing which
/// payment to look at, which is backwards — the payment id is exactly what the
/// person asking does not have. The only way to get the answer was to query
/// SQLite directly on the production volume.
///
/// Scoping is a join to `payments`, not a filter the caller supplies, so a
/// merchant can never read another tenant's deliveries.
pub async fn list_deliveries_for_merchant(
    pool: &Db,
    merchant_id: &str,
    status: &str,
    limit: i64,
    cursor: Option<(&str, &str)>,
) -> Result<Vec<WebhookDelivery>> {
    let mut sql = String::from(
        "SELECT d.id, d.payment_id, d.url, d.payload, d.event_type, d.status, d.attempts, \
                d.last_attempt, d.acknowledged_at, d.created_at
           FROM webhook_deliveries d
           JOIN payments p ON p.id = d.payment_id
          WHERE p.merchant_id = ? AND d.status = ?",
    );
    if cursor.is_some() {
        sql.push_str(" AND (d.created_at < ? OR (d.created_at = ? AND d.id < ?))");
    }
    sql.push_str(" ORDER BY d.created_at DESC, d.id DESC LIMIT ?");

    let mut query = sqlx::query(&sql).bind(merchant_id).bind(status);
    if let Some((ts, id)) = cursor {
        query = query.bind(ts).bind(ts).bind(id);
    }
    let rows = query.bind(limit).fetch_all(pool).await?;

    Ok(rows.iter().map(row_to_webhook_delivery).collect())
}

/// Requeue a merchant's failed deliveries so the redrive worker retries them,
/// and mark them acknowledged. Returns how many rows were affected.
///
/// `ids` empty means every failed delivery this merchant has.
pub async fn requeue_failed_deliveries(
    pool: &Db,
    merchant_id: &str,
    ids: &[String],
) -> Result<u64> {
    let mut sql = String::from(
        "UPDATE webhook_deliveries
            SET status = 'pending',
                attempts = 0,
                acknowledged_at = strftime('%Y-%m-%dT%H:%M:%SZ','now')
          WHERE status = 'failed'
            AND payment_id IN (SELECT id FROM payments WHERE merchant_id = ?)",
    );
    if !ids.is_empty() {
        sql.push_str(" AND id IN (");
        sql.push_str(&vec!["?"; ids.len()].join(","));
        sql.push(')');
    }

    let mut query = sqlx::query(&sql).bind(merchant_id);
    for id in ids {
        query = query.bind(id);
    }
    Ok(query.execute(pool).await?.rows_affected())
}

/// Get a specific webhook delivery by id.
pub async fn get_webhook_delivery(pool: &Db, id: &str) -> Result<Option<WebhookDelivery>> {
    let row = sqlx::query(&format!(
        "SELECT {DELIVERY_COLUMNS} FROM webhook_deliveries WHERE id = ?",
    ))
    .bind(id)
    .fetch_optional(pool)
    .await?;

    Ok(row.as_ref().map(row_to_webhook_delivery))
}

/// Probe database connectivity. Returns `Ok(())` if the pool can execute a
/// trivial query, or `Err` if the database is unreachable.
pub async fn ping(pool: &Db) -> Result<()> {
    sqlx::query_scalar::<_, i64>("SELECT 1")
        .fetch_one(pool)
        .await?;
    Ok(())
}

/* ---------------------------------------------------------------------------
Merchant API-key management
--------------------------------------------------------------------------- */

/// Hash a raw API key with SHA-256, returning the hex digest.
/// This is the only representation stored in the database.
fn hash_api_key(raw: &str) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(raw.as_bytes()))
}

/// Create a merchant and its first API key, atomically — a merchant that
/// exists with no way to authenticate would be unusable and un-fixable through
/// the API.
///
/// The raw key must be shown to the caller once; it is not recoverable.
///
/// `merchants.api_key_hash` is written only to satisfy the column's NOT NULL
/// constraint on databases created before `api_keys` existed. Nothing reads it
/// any more — authentication goes through `api_keys` so that rotation and
/// revocation work — and it is not maintained as keys change.
pub async fn create_merchant(pool: &Db, id: &str, raw_key: &str, prefix: &str) -> Result<String> {
    let mut tx = pool.begin().await?;

    sqlx::query("INSERT INTO merchants (id, api_key_hash) VALUES (?, ?)")
        .bind(id)
        .bind(hash_api_key(raw_key))
        .execute(&mut *tx)
        .await?;

    let key_id = uuid::Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO api_keys (id, merchant_id, key_hash, prefix, label)
         VALUES (?, ?, ?, ?, 'initial')",
    )
    .bind(&key_id)
    .bind(id)
    .bind(hash_api_key(raw_key))
    .bind(prefix)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(key_id)
}

/* ---------------------------------------------------------------------------
Retention
--------------------------------------------------------------------------- */

/// Rows removed per statement. Deleting in batches keeps each write lock short:
/// SQLite has a single writer, so one unbounded `DELETE` over a large table
/// would stall every payment write until it finished.
pub const PRUNE_BATCH: i64 = 500;

/// Delete one batch of idempotency keys older than `retention_days`.
///
/// A key only has to outlive the window in which a client might retry the
/// create it guarded. Past that it is dead weight, and the table has no other
/// bound (issue #110).
///
/// Returns how many rows went; the caller loops until a batch comes back
/// short.
pub async fn prune_idempotency_keys(pool: &Db, retention_days: i64) -> Result<u64> {
    let cutoff = format!("-{retention_days} days");
    let n = sqlx::query(
        "DELETE FROM idempotency_keys
          WHERE rowid IN (
              SELECT rowid FROM idempotency_keys
               WHERE created_at < strftime('%Y-%m-%dT%H:%M:%SZ','now',?)
               LIMIT ?
          )",
    )
    .bind(&cutoff)
    .bind(PRUNE_BATCH)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(n)
}

/// Delete one batch of webhook deliveries that have finished and aged out.
///
/// A `pending` row is still owned by the redrive worker — pruning it would
/// silently drop a delivery that was going to be retried. The worker marks
/// rows `failed` once attempts are exhausted, so nothing stays exempt forever
/// (issue #111).
///
/// An **unacknowledged `failed`** row is also exempt. Deleting one destroys the
/// only record that an event was permanently lost, on a timer, whether or not
/// anybody looked at it — so the evidence for "we never received your webhook"
/// expired exactly when it was most likely to be asked for (issue #319).
/// Acknowledging or requeueing a delivery clears the exemption, and
/// [`compact_stale_failed_deliveries`] keeps the retained rows from costing
/// what a full delivery row costs.
pub async fn prune_webhook_deliveries(pool: &Db, retention_days: i64) -> Result<u64> {
    let cutoff = format!("-{retention_days} days");
    let n = sqlx::query(
        "DELETE FROM webhook_deliveries
          WHERE rowid IN (
              SELECT rowid FROM webhook_deliveries
               WHERE (status = 'delivered'
                      OR (status = 'failed' AND acknowledged_at IS NOT NULL))
                 AND created_at < strftime('%Y-%m-%dT%H:%M:%SZ','now',?)
               LIMIT ?
          )",
    )
    .bind(&cutoff)
    .bind(PRUNE_BATCH)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(n)
}

/// Drop the stored payload of aged-out, unacknowledged `failed` deliveries,
/// leaving a compact tombstone.
///
/// Exempting terminal failures from retention outright would trade one
/// unbounded table for another, which is the problem retention was added to
/// solve (issues #110, #111). `payload` is by far the largest column, and its
/// only consumer is redelivery — which is not something anyone does to a
/// months-old failure. Clearing it keeps the row (and therefore the answer to
/// "did this event ever get through?") at a few hundred bytes, indefinitely.
///
/// The `payload <> ''` guard makes this idempotent: a row is compacted once,
/// not rewritten on every cycle.
pub async fn compact_stale_failed_deliveries(pool: &Db, retention_days: i64) -> Result<u64> {
    let cutoff = format!("-{retention_days} days");
    let n = sqlx::query(
        "UPDATE webhook_deliveries
            SET payload = ''
          WHERE rowid IN (
              SELECT rowid FROM webhook_deliveries
               WHERE status = 'failed'
                 AND acknowledged_at IS NULL
                 AND payload <> ''
                 AND created_at < strftime('%Y-%m-%dT%H:%M:%SZ','now',?)
               LIMIT ?
          )",
    )
    .bind(&cutoff)
    .bind(PRUNE_BATCH)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(n)
}

/// A key as exposed to an operator. Never carries the secret — only a
/// prefix, so two keys can be told apart in a list.
#[derive(Debug, Clone)]
pub struct ApiKeyInfo {
    pub id: String,
    pub prefix: String,
    pub label: Option<String>,
    pub created_at: String,
    pub last_used_at: Option<String>,
    pub revoked_at: Option<String>,
}

/// Characters of the raw key kept in `prefix` for display.
const KEY_PREFIX_LEN: usize = 12;

/// Mint a new API key: 256 bits from the OS CSPRNG, hex-encoded behind an
/// `sg_` marker.
///
/// Deliberately not a UUID. A v4 UUID carries only 122 random bits and spends
/// 6 of them encoding its version and variant, which is fine for an identifier
/// and wrong for a bearer credential. The `sg_` marker makes a leaked key
/// recognisable in logs and lets secret scanners match on it.
///
/// Returns `(raw_key, prefix)`. The raw key is shown once and never stored.
pub fn generate_api_key() -> (String, String) {
    use rand::RngCore;
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    let raw = format!("sg_{}", hex::encode(bytes));
    let prefix = raw.chars().take(KEY_PREFIX_LEN).collect();
    (raw, prefix)
}

/// Store a new API key for a merchant. Only the digest is persisted.
pub async fn create_api_key(
    pool: &Db,
    merchant_id: &str,
    raw_key: &str,
    prefix: &str,
    label: Option<&str>,
) -> Result<String> {
    let id = uuid::Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO api_keys (id, merchant_id, key_hash, prefix, label)
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(&id)
    .bind(merchant_id)
    .bind(hash_api_key(raw_key))
    .bind(prefix)
    .bind(label)
    .execute(pool)
    .await?;
    Ok(id)
}

/// Resolve a raw API key to its merchant, rejecting revoked keys.
///
/// Also refreshes `last_used_at`, but at most once a minute per key: this runs
/// on every authenticated request, and SQLite takes a write lock for each
/// update, so touching it unconditionally would put a write in the path of
/// every read. Minute granularity is plenty to spot a key that has gone quiet
/// or one that is being used when it should not be.
pub async fn find_merchant_by_key(pool: &Db, raw_key: &str) -> Result<Option<String>> {
    let hash = hash_api_key(raw_key);

    let row: Option<(String, String)> = sqlx::query_as(
        "SELECT id, merchant_id FROM api_keys WHERE key_hash = ? AND revoked_at IS NULL",
    )
    .bind(&hash)
    .fetch_optional(pool)
    .await?;

    let Some((key_id, merchant_id)) = row else {
        return Ok(None);
    };

    sqlx::query(
        "UPDATE api_keys
            SET last_used_at = strftime('%Y-%m-%dT%H:%M:%SZ','now')
          WHERE id = ?
            AND (last_used_at IS NULL
                 OR last_used_at < strftime('%Y-%m-%dT%H:%M:%SZ','now','-60 seconds'))",
    )
    .bind(&key_id)
    .execute(pool)
    .await?;

    Ok(Some(merchant_id))
}

/// Every key issued to a merchant, newest first, including revoked ones so the
/// history stays visible.
pub async fn list_api_keys(pool: &Db, merchant_id: &str) -> Result<Vec<ApiKeyInfo>> {
    /// (id, prefix, label, created_at, last_used_at, revoked_at) as selected below.
    type KeyRow = (
        String,
        String,
        Option<String>,
        String,
        Option<String>,
        Option<String>,
    );

    let rows: Vec<KeyRow> = sqlx::query_as(
        "SELECT id, prefix, label, created_at, last_used_at, revoked_at
               FROM api_keys WHERE merchant_id = ? ORDER BY created_at DESC, id DESC",
    )
    .bind(merchant_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(
            |(id, prefix, label, created_at, last_used_at, revoked_at)| ApiKeyInfo {
                id,
                prefix,
                label,
                created_at,
                last_used_at,
                revoked_at,
            },
        )
        .collect())
}

/// Revoke a key. Scoped by merchant so one merchant cannot revoke another's.
///
/// Returns `Ok(false)` when no such live key exists — either it was never
/// there, belongs to someone else, or is already revoked. Revoking twice is
/// not an error worth surfacing, but "no key was revoked" is.
pub async fn revoke_api_key(pool: &Db, merchant_id: &str, key_id: &str) -> Result<bool> {
    let affected = sqlx::query(
        "UPDATE api_keys
            SET revoked_at = strftime('%Y-%m-%dT%H:%M:%SZ','now')
          WHERE id = ? AND merchant_id = ? AND revoked_at IS NULL",
    )
    .bind(key_id)
    .bind(merchant_id)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(affected > 0)
}

/// Number of usable keys a merchant has, so the API can refuse to revoke the
/// last one and lock them out.
pub async fn count_active_api_keys(pool: &Db, merchant_id: &str) -> Result<i64> {
    let n: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM api_keys WHERE merchant_id = ? AND revoked_at IS NULL",
    )
    .bind(merchant_id)
    .fetch_one(pool)
    .await?;
    Ok(n)
}

/// Whether a merchant exists, so key endpoints can 404 rather than silently
/// operating on nothing.
pub async fn merchant_exists(pool: &Db, merchant_id: &str) -> Result<bool> {
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM merchants WHERE id = ?")
        .bind(merchant_id)
        .fetch_one(pool)
        .await?;
    Ok(n > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn memory_db() -> Db {
        let pool = SqlitePoolOptions::new()
            .connect("sqlite::memory:")
            .await
            .unwrap();
        migrate(&pool).await.unwrap();
        pool
    }

    /// An upgrade must not lock out merchants whose keys predate the
    /// `api_keys` table. This simulates a pre-upgrade database — the old
    /// `merchants.api_key_hash` schema with no `api_keys` table — runs the
    /// migration over it, and checks the original key still authenticates.
    ///
    /// Getting this wrong would revoke every live credential on deploy, with
    /// no way to recover them: the raw keys are unrecoverable by design.
    #[tokio::test]
    async fn legacy_single_key_merchants_survive_the_api_keys_migration() {
        let pool = SqlitePoolOptions::new()
            .connect("sqlite::memory:")
            .await
            .unwrap();

        // The schema as it existed before api_keys.
        sqlx::query(
            "CREATE TABLE merchants (
                id TEXT PRIMARY KEY,
                api_key_hash TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        let legacy_key = "3f1c9a7e-0b2d-4e5f-8a6b-7c8d9e0f1a2b"; // an old UUID key
        sqlx::query("INSERT INTO merchants (id, api_key_hash) VALUES (?, ?)")
            .bind("legacy-merchant")
            .bind(hash_api_key(legacy_key))
            .execute(&pool)
            .await
            .unwrap();

        migrate(&pool).await.unwrap();

        assert_eq!(
            find_merchant_by_key(&pool, legacy_key).await.unwrap(),
            Some("legacy-merchant".to_string()),
            "a key issued before the api_keys table must keep working"
        );

        // It is now a first-class key: listable, and revocable once replaced.
        let keys = list_api_keys(&pool, "legacy-merchant").await.unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].prefix, "legacy");
        assert!(keys[0].revoked_at.is_none());
    }

    /// Revoking a key must take effect immediately for authentication.
    #[tokio::test]
    async fn revoked_keys_stop_authenticating() {
        let pool = memory_db().await;
        let (raw, prefix) = generate_api_key();
        let key_id = create_merchant(&pool, "m1", &raw, &prefix).await.unwrap();

        assert_eq!(
            find_merchant_by_key(&pool, &raw).await.unwrap(),
            Some("m1".to_string())
        );

        assert!(revoke_api_key(&pool, "m1", &key_id).await.unwrap());
        assert_eq!(find_merchant_by_key(&pool, &raw).await.unwrap(), None);

        // Revoking again reports that nothing was revoked.
        assert!(!revoke_api_key(&pool, "m1", &key_id).await.unwrap());
    }

    /// Generated keys must be unique and carry full entropy.
    #[tokio::test]
    async fn generated_keys_are_unique_and_prefixed() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..500 {
            let (raw, prefix) = generate_api_key();
            assert!(raw.starts_with("sg_"));
            assert_eq!(raw.len(), 67);
            assert!(raw.starts_with(&prefix));
            assert!(seen.insert(raw), "generated a duplicate key");
        }
    }

    fn new_payment<'a>(id: &'a str, memo: &'a str, ttl_secs: i64) -> NewPayment<'a> {
        NewPayment {
            id,
            merchant_id: "m",
            destination_address: "GGATEWAY",
            memo,
            amount: "10",
            asset: "XLM",
            asset_issuer: None,
            webhook_url: None,
            ttl_secs,
        }
    }

    #[tokio::test]
    async fn create_sets_expiry_from_ttl() {
        let pool = memory_db().await;
        // A one-hour TTL lands the expiry strictly in the future...
        let live = create_payment(&pool, new_payment("a", "MEMOA", 3600))
            .await
            .unwrap();
        assert!(live.expires_at > live.created_at);
        // ...while a negative TTL produces an already-overdue expiry.
        let dead = create_payment(&pool, new_payment("b", "MEMOB", -10))
            .await
            .unwrap();
        assert!(dead.expires_at < dead.created_at);
    }

    #[tokio::test]
    async fn create_persists_asset_issuer() {
        let pool = memory_db().await;
        let usdc = create_payment(
            &pool,
            NewPayment {
                id: "usdc",
                merchant_id: "m",
                destination_address: "GGATEWAY",
                memo: "MEMOUSDC",
                amount: "5",
                asset: "USDC",
                asset_issuer: Some("GUSDC"),
                webhook_url: None,
                ttl_secs: 3600,
            },
        )
        .await
        .unwrap();
        assert_eq!(usdc.asset_issuer.as_deref(), Some("GUSDC"));

        let xlm = create_payment(&pool, new_payment("xlm", "MEMOXLM", 3600))
            .await
            .unwrap();
        assert_eq!(xlm.asset_issuer, None);
    }

    #[tokio::test]
    async fn backfill_fills_null_issuer_from_allow_list() {
        let pool = memory_db().await;
        sqlx::query(
            "INSERT INTO payments
                (id, merchant_id, destination_address, memo, amount, asset, status, expires_at)
             VALUES ('legacy', 'm', 'GGATEWAY', 'MEMOLEG', '5', 'USDC', 'pending',
                     strftime('%Y-%m-%dT%H:%M:%SZ','now','+1 hour'))",
        )
        .execute(&pool)
        .await
        .unwrap();

        backfill_asset_issuers(
            &pool,
            &[crate::config::AcceptedAsset {
                code: "USDC".into(),
                issuer: Some("GUSDC".into()),
            }],
        )
        .await
        .unwrap();

        let p = get_payment(&pool, "legacy").await.unwrap().unwrap();
        assert_eq!(p.asset_issuer.as_deref(), Some("GUSDC"));

        // Already-pinned rows are left alone.
        backfill_asset_issuers(
            &pool,
            &[crate::config::AcceptedAsset {
                code: "USDC".into(),
                issuer: Some("GOTHER".into()),
            }],
        )
        .await
        .unwrap();
        let p = get_payment(&pool, "legacy").await.unwrap().unwrap();
        assert_eq!(p.asset_issuer.as_deref(), Some("GUSDC"));
    }

    #[tokio::test]
    async fn list_pending_excludes_overdue_even_before_sweep() {
        let pool = memory_db().await;
        create_payment(&pool, new_payment("live", "MEMOL", 3600))
            .await
            .unwrap();
        create_payment(&pool, new_payment("dead", "MEMOD", -10))
            .await
            .unwrap();

        let pending = list_pending(&pool).await.unwrap();
        let ids: Vec<&str> = pending.iter().map(|p| p.id.as_str()).collect();
        assert_eq!(ids, vec!["live"]);
    }

    // ── Legacy timestamp format (issue #314) ─────────────────────────────────

    /// A row whose `expires_at` was written in the legacy
    /// `"YYYY-MM-DD HH:MM:SS"` form (space separator, no `Z`) — as produced by
    /// the old `datetime('now')` default — must not be misread as already
    /// expired just because it sorts lexically before an RFC 3339 `"…T…Z"`
    /// string, even when the date it encodes is far in the future.
    ///
    /// Bypasses the `expires_at` `CHECK` constraint via
    /// `PRAGMA ignore_check_constraints`, on a single held connection so the
    /// pragma and the write land on the same session — exactly how a
    /// pre-#314 row would already exist on disk before an upgrade, since the
    /// constraint is not retroactive for a table that already existed.
    async fn seed_legacy_format_expiry(pool: &Db, id: &str, memo: &str, legacy_expires_at: &str) {
        let mut conn = pool.acquire().await.unwrap();
        sqlx::query("PRAGMA ignore_check_constraints = 1")
            .execute(&mut *conn)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO payments
                (id, merchant_id, destination_address, memo, amount, asset, status, expires_at)
             VALUES (?, 'm', 'GGATEWAY', ?, '10', 'XLM', 'pending', ?)",
        )
        .bind(id)
        .bind(memo)
        .bind(legacy_expires_at)
        .execute(&mut *conn)
        .await
        .unwrap();
        sqlx::query("PRAGMA ignore_check_constraints = 0")
            .execute(&mut *conn)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn legacy_format_expiry_far_in_the_future_is_fixed_by_normalisation_and_then_findable() {
        let pool = memory_db().await;
        // 5 minutes from now, same calendar day in the overwhelming majority
        // of runs — deliberately *not* a different year or day, since a
        // different leading date digit would make the row compare greater
        // regardless of the separator and wouldn't reproduce the bug at all.
        // The reproduction needs an otherwise-identical date-time prefix so
        // the single differing byte — ' ' (0x20) vs 'T' (0x54) — is what
        // decides the (wrong) comparison outcome, exactly as issue #314
        // describes.
        let soon = time::OffsetDateTime::now_utc() + time::Duration::minutes(5);
        let legacy_expires_at = format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            soon.year(),
            u8::from(soon.month()),
            soon.day(),
            soon.hour(),
            soon.minute(),
            soon.second()
        );
        seed_legacy_format_expiry(&pool, "legacy", "MEMOLEGACY", &legacy_expires_at).await;

        // Before normalisation: the bug this issue describes. A row that will
        // not expire for 70+ years is invisible to both detection paths.
        assert!(
            list_pending(&pool).await.unwrap().is_empty(),
            "legacy-format row must reproduce the bug before normalisation runs"
        );
        assert!(find_pending_by_memo(&pool, "MEMOLEGACY")
            .await
            .unwrap()
            .is_none());

        // migrate() is re-run on every startup and is what applies the
        // normalisation UPDATE — simulating the next process start.
        migrate(&pool).await.unwrap();

        let pending = list_pending(&pool).await.unwrap();
        assert_eq!(
            pending.len(),
            1,
            "row must be detectable after normalisation"
        );
        assert_eq!(pending[0].id, "legacy");
        assert!(pending[0].expires_at.contains('T') && pending[0].expires_at.ends_with('Z'));

        let found = find_pending_by_memo(&pool, "MEMOLEGACY").await.unwrap();
        assert_eq!(found.map(|p| p.id), Some("legacy".to_string()));
    }

    /// The companion case: a legacy-format row whose encoded date has already
    /// passed must still be swept as overdue after normalisation — the fix
    /// must not accidentally make every legacy row look perpetually fresh.
    #[tokio::test]
    async fn legacy_format_expiry_in_the_past_is_still_expired_after_normalisation() {
        let pool = memory_db().await;
        seed_legacy_format_expiry(&pool, "legacy-dead", "MEMODEAD", "2020-01-01 00:00:00").await;

        migrate(&pool).await.unwrap();

        assert!(list_pending(&pool).await.unwrap().is_empty());
        let expired = expire_overdue(&pool, 10).await.unwrap();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].id, "legacy-dead");
        assert_eq!(expired[0].status, "expired");
    }

    /// The `CHECK` constraint itself: on a freshly created table (this test's
    /// `:memory:` database always is one), a write that does not conform to
    /// the RFC 3339 `Z` format must fail loudly at insert time rather than
    /// being silently accepted and only caught by the next normalisation pass.
    #[tokio::test]
    async fn malformed_expires_at_is_rejected_at_insert_time_on_a_fresh_table() {
        let pool = memory_db().await;
        let result = sqlx::query(
            "INSERT INTO payments
                (id, merchant_id, destination_address, memo, amount, asset, status, expires_at)
             VALUES ('bad', 'm', 'GGATEWAY', 'MEMOBAD', '10', 'XLM', 'pending', '2026-04-29 15:00:00')",
        )
        .execute(&pool)
        .await;
        assert!(
            result.is_err(),
            "a space-separated, non-Z timestamp must violate the CHECK constraint"
        );
    }

    #[tokio::test]
    async fn underpaid_payment_remains_findable_for_topup() {
        let pool = memory_db().await;
        create_payment(&pool, new_payment("partial", "MEMOP", 3600))
            .await
            .unwrap();
        update_payment_status(&pool, "partial", "underpaid", "TX1", "3")
            .await
            .unwrap();

        let found = find_pending_by_memo(&pool, "MEMOP").await.unwrap().unwrap();
        assert_eq!(found.id, "partial");
        assert_eq!(found.status, "underpaid");
        assert_eq!(found.paid_amount.as_deref(), Some("3"));
    }

    #[tokio::test]
    async fn overdue_underpaid_payment_expires() {
        let pool = memory_db().await;
        create_payment(&pool, new_payment("partial-dead", "MEMOX", -10))
            .await
            .unwrap();
        update_payment_status(&pool, "partial-dead", "underpaid", "TX1", "3")
            .await
            .unwrap();

        assert!(find_pending_by_memo(&pool, "MEMOX")
            .await
            .unwrap()
            .is_none());

        let expired = expire_overdue(&pool, 1).await.unwrap();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].id, "partial-dead");
        assert_eq!(expired[0].status, "expired");
    }

    #[tokio::test]
    async fn expire_overdue_transitions_and_is_idempotent() {
        let pool = memory_db().await;
        create_payment(&pool, new_payment("live", "MEMOL", 3600))
            .await
            .unwrap();
        create_payment(&pool, new_payment("dead", "MEMOD", -10))
            .await
            .unwrap();

        // First sweep expires exactly the overdue intent and returns it.
        let expired = expire_overdue(&pool, 10).await.unwrap();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].id, "dead");
        assert_eq!(expired[0].status, "expired");

        // GET /payments/:id reflects the expired status.
        let fetched = get_payment(&pool, "dead").await.unwrap().unwrap();
        assert_eq!(fetched.status, "expired");
        // The live intent is untouched.
        assert_eq!(
            get_payment(&pool, "live").await.unwrap().unwrap().status,
            "pending"
        );

        // A second sweep is a no-op — nothing is double-reported.
        assert_eq!(expire_overdue(&pool, 10).await.unwrap().len(), 0);
    }

    /// A backlog larger than a single batch must drain across repeated sweeps
    /// rather than being cut off at the batch size, and each sweep must report
    /// exactly the rows it transitioned (issue #323).
    #[tokio::test]
    async fn expire_overdue_drains_large_backlog_across_batches() {
        let pool = memory_db().await;
        let batch = 7;
        let total = PRUNE_BATCH + 137;
        for i in 0..total {
            create_payment(
                &pool,
                new_payment(&format!("backlog{i}"), &format!("MEMOB{i}"), -10),
            )
            .await
            .unwrap();
        }

        let mut swept = 0i64;
        while swept < total {
            let expired = expire_overdue(&pool, batch).await.unwrap();
            assert!(
                expired.len() <= batch as usize,
                "a sweep may not exceed its batch"
            );
            for payment in &expired {
                assert_eq!(payment.status, "expired");
            }
            swept += expired.len() as i64;
        }

        assert_eq!(swept, total, "the whole backlog must eventually drain");

        // Nothing is left over for the next sweep; nothing was double-counted.
        assert_eq!(expire_overdue(&pool, batch).await.unwrap().len(), 0);
    }

    /// A payment settled concurrently with the sweep must not be expired (and
    /// thus cannot be reported as expired) — the settlement's status guard wins
    /// the race even though the row was selected as overdue first (issue #323).
    #[tokio::test]
    async fn concurrent_settlement_beats_overdue_sweep() {
        let pool = memory_db().await;
        create_payment(&pool, new_payment("racer", "MEMOR", -10))
            .await
            .unwrap();
        create_payment(&pool, new_payment("swept", "MEMOS", -10))
            .await
            .unwrap();

        // "Concurrent" settlement: flip the racer out from under the sweep
        // between selection and update. `update_payment_status` applies the
        // same `status IN ('pending','underpaid')` guard as real settlement.
        assert!(
            update_payment_status(&pool, "racer", "completed", "TX1", "10")
                .await
                .unwrap()
        );

        let expired = expire_overdue(&pool, 10).await.unwrap();
        assert_eq!(
            expired.len(),
            1,
            "only the unsettled overdue intent may expire"
        );
        assert_eq!(expired[0].id, "swept");

        let racer = get_payment(&pool, "racer").await.unwrap().unwrap();
        assert_eq!(
            racer.status, "completed",
            "settlement must win over the sweep"
        );
        let swept = get_payment(&pool, "swept").await.unwrap().unwrap();
        assert_eq!(swept.status, "expired");
    }

    #[tokio::test]
    async fn list_redrivable_deliveries_excludes_delivered_and_over_cap() {
        let pool = memory_db().await;
        create_payment(&pool, new_payment("p1", "MEMOR1", 3600))
            .await
            .unwrap();

        save_webhook_delivery(
            &pool,
            "delivered",
            "p1",
            "http://x",
            "{}",
            "payment.completed",
        )
        .await
        .unwrap();
        update_webhook_delivery(&pool, "delivered", "delivered", 1)
            .await
            .unwrap();

        save_webhook_delivery(
            &pool,
            "over-cap",
            "p1",
            "http://x",
            "{}",
            "payment.completed",
        )
        .await
        .unwrap();
        update_webhook_delivery(&pool, "over-cap", "failed", 8)
            .await
            .unwrap();

        save_webhook_delivery(
            &pool,
            "eligible",
            "p1",
            "http://x",
            "{}",
            "payment.completed",
        )
        .await
        .unwrap();

        let candidates = list_redrivable_deliveries(&pool, 8, 0, 0, 0, 0)
            .await
            .unwrap();
        let ids: Vec<&str> = candidates.iter().map(|d| d.id.as_str()).collect();
        assert_eq!(
            ids,
            vec!["eligible"],
            "only the pending row under the attempt cap must be redrivable"
        );
    }

    #[tokio::test]
    async fn record_processed_tx_is_idempotent_and_sums_over_the_set() {
        let pool = memory_db().await;
        create_payment(&pool, new_payment("p", "MEMOTX", 3600))
            .await
            .unwrap();

        // First time a transaction is seen it is recorded and counted.
        assert!(record_processed_tx(&pool, "p", "TX_A", 40_000_000)
            .await
            .unwrap());
        assert_eq!(sum_processed_stroops(&pool, "p").await.unwrap(), 40_000_000);

        // Re-seeing the same transaction is a no-op — no double credit.
        assert!(!record_processed_tx(&pool, "p", "TX_A", 40_000_000)
            .await
            .unwrap());
        assert_eq!(sum_processed_stroops(&pool, "p").await.unwrap(), 40_000_000);

        // A distinct transaction adds to the running total.
        assert!(record_processed_tx(&pool, "p", "TX_B", 30_000_000)
            .await
            .unwrap());
        assert_eq!(sum_processed_stroops(&pool, "p").await.unwrap(), 70_000_000);

        // Re-seeing an *earlier* transaction after a later one is still a no-op,
        // regardless of order (issue #119).
        assert!(!record_processed_tx(&pool, "p", "TX_A", 40_000_000)
            .await
            .unwrap());
        assert_eq!(sum_processed_stroops(&pool, "p").await.unwrap(), 70_000_000);

        // Rows are scoped per intent.
        assert_eq!(sum_processed_stroops(&pool, "other").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn migrate_backfills_processed_transactions_from_legacy_rows() {
        let pool = memory_db().await;
        create_payment(&pool, new_payment("legacy", "MEMOLEG", 3600))
            .await
            .unwrap();
        // Simulate a pre-#119 underpaid intent: only the latest tx_hash and the
        // cumulative paid_amount were persisted.
        update_payment_status(&pool, "legacy", "underpaid", "TX_OLD", "3")
            .await
            .unwrap();
        // The join table is empty until a backfill runs.
        assert_eq!(sum_processed_stroops(&pool, "legacy").await.unwrap(), 0);

        // A subsequent migrate() (as on the next startup) backfills the ledger.
        migrate(&pool).await.unwrap();
        assert_eq!(
            sum_processed_stroops(&pool, "legacy").await.unwrap(),
            30_000_000
        );
        // And it is idempotent across restarts.
        migrate(&pool).await.unwrap();
        assert_eq!(
            sum_processed_stroops(&pool, "legacy").await.unwrap(),
            30_000_000
        );
    }

    #[tokio::test]
    async fn list_redrivable_deliveries_respects_grace_window() {
        let pool = memory_db().await;
        create_payment(&pool, new_payment("p1", "MEMOR2", 3600))
            .await
            .unwrap();
        save_webhook_delivery(&pool, "fresh", "p1", "http://x", "{}", "payment.completed")
            .await
            .unwrap();

        // Freshly inserted, so a large grace window makes it ineligible...
        assert!(list_redrivable_deliveries(&pool, 8, 3600, 0, 0, 0)
            .await
            .unwrap()
            .is_empty());
        // ...while a zero grace window makes it immediately eligible.
        assert_eq!(
            list_redrivable_deliveries(&pool, 8, 0, 0, 0, 0)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    /// The redrive half of issue #318.
    ///
    /// Exponential backoff does not desynchronise a batch that failed
    /// together: those rows share an `attempts` value and a near-identical
    /// `last_attempt`, so their next-attempt times coincide and this query
    /// hands the worker the whole cluster on one pass — which is precisely the
    /// stampede the backoff was supposed to prevent.
    ///
    /// With jitter, each pass admits a random subset instead. 200 co-failing
    /// rows and a 100-second window: the chance of all 200 clearing a random
    /// `[0,100]` offset at once is nil, so a full batch means jitter is not
    /// being applied.
    #[tokio::test]
    async fn jitter_desynchronises_a_batch_that_failed_together() {
        let pool = memory_db().await;
        create_payment(&pool, new_payment("p1", "MEMOJIT", 3600))
            .await
            .unwrap();

        // 200 deliveries, all created and failed at the same instant.
        for i in 0..200 {
            let id = format!("sync-{i}");
            save_webhook_delivery(&pool, &id, "p1", "http://x", "{}", "payment.completed")
                .await
                .unwrap();
        }

        // No jitter: every row clears the zero grace window, so the worker
        // takes the entire cluster in one pass — the behaviour being fixed.
        let unjittered = list_redrivable_deliveries(&pool, 8, 0, 0, 0, 0)
            .await
            .unwrap();
        assert_eq!(
            unjittered.len(),
            200,
            "without jitter a co-failing batch moves as one block"
        );

        // With jitter, each row waits a random extra [0, 100] seconds.
        let jittered = list_redrivable_deliveries(&pool, 8, 0, 0, 0, 100)
            .await
            .unwrap();
        assert!(
            jittered.len() < 200,
            "jitter must spread the batch across passes, but all {} rows were \
             returned at once",
            jittered.len()
        );
    }

    /// Jitter must only ever *delay* a row, never pull it forward past the
    /// grace window that keeps the worker off a live `dispatch()`.
    #[tokio::test]
    async fn jitter_never_shortens_the_grace_window() {
        let pool = memory_db().await;
        create_payment(&pool, new_payment("p1", "MEMOJI2", 3600))
            .await
            .unwrap();
        save_webhook_delivery(&pool, "fresh", "p1", "http://x", "{}", "payment.completed")
            .await
            .unwrap();

        for _ in 0..50 {
            assert!(
                list_redrivable_deliveries(&pool, 8, 3600, 0, 0, 300)
                    .await
                    .unwrap()
                    .is_empty(),
                "a row inside its grace window must stay ineligible regardless \
                 of the jitter draw"
            );
        }
    }

    #[tokio::test]
    async fn list_redrivable_deliveries_exempts_never_attempted_rows_from_backoff() {
        let pool = memory_db().await;
        create_payment(&pool, new_payment("p1", "MEMOR3", 3600))
            .await
            .unwrap();
        save_webhook_delivery(
            &pool,
            "crashed",
            "p1",
            "http://x",
            "{}",
            "payment.completed",
        )
        .await
        .unwrap();

        // attempts == 0 (never sent) is gated by grace_secs alone, not the
        // exponential backoff, even with a huge backoff floor configured.
        assert_eq!(
            list_redrivable_deliveries(&pool, 8, 0, 3600, 3600, 0)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn list_redrivable_deliveries_backs_off_exponentially_after_a_failure() {
        let pool = memory_db().await;
        create_payment(&pool, new_payment("p1", "MEMOR4", 3600))
            .await
            .unwrap();
        save_webhook_delivery(&pool, "flaky", "p1", "http://x", "{}", "payment.completed")
            .await
            .unwrap();
        update_webhook_delivery(&pool, "flaky", "failed", 1)
            .await
            .unwrap();

        // One failed attempt (attempts=1): backoff = initial * 2^0 = initial.
        // A huge initial delay makes it ineligible even with grace_secs=0.
        assert!(
            list_redrivable_deliveries(&pool, 8, 0, 3600, 3600, 0)
                .await
                .unwrap()
                .is_empty(),
            "a row with a recent failure must wait out the backoff delay"
        );
        // grace_secs is a floor under the backoff: even with backoff disabled
        // (initial=max=0), a large grace_secs still holds the row back.
        assert!(
            list_redrivable_deliveries(&pool, 8, 3600, 0, 0, 0)
                .await
                .unwrap()
                .is_empty(),
            "grace_secs must floor eligibility even when backoff computes to 0"
        );
    }
}
