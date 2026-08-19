-- Checked-in snapshot of the schema `db::migrate` (src/db.rs) produces on a
-- fresh database. `tests/schema_snapshot_test.rs` asserts a freshly migrated
-- database matches this file exactly, so a change to the schema that isn't
-- reflected here fails CI instead of drifting silently (issue #308).
--
-- Regenerate after an intentional schema change:
--   1. Update `db::migrate` in src/db.rs.
--   2. Run `cargo test --test schema_snapshot_test -- --nocapture` and copy
--      its failure output to refresh this file.
--   3. Review the diff like any other schema change.
--
-- Each statement below is exactly the `sql` column SQLite stores in
-- `sqlite_master` for it (SQLite strips `IF NOT EXISTS` when storing DDL),
-- ordered by `(type, name)`, terminated by a line containing only `;`.

CREATE INDEX idx_api_keys_hash ON api_keys(key_hash)
;
CREATE INDEX idx_api_keys_merchant ON api_keys(merchant_id)
;
CREATE INDEX idx_payments_created_id ON payments(created_at DESC, id DESC)
;
CREATE INDEX idx_payments_memo ON payments(memo)
;
CREATE INDEX idx_payments_status ON payments(status)
;
CREATE INDEX idx_webhook_deliveries_payment
         ON webhook_deliveries(payment_id)
;
CREATE TABLE api_keys (
            id TEXT PRIMARY KEY,
            merchant_id TEXT NOT NULL,
            key_hash TEXT NOT NULL UNIQUE,
            prefix TEXT NOT NULL,
            label TEXT,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
                CHECK (created_at LIKE '____-__-__T__:__:__Z'),
            last_used_at TEXT CHECK (last_used_at IS NULL OR last_used_at LIKE '____-__-__T__:__:__Z'),
            revoked_at TEXT CHECK (revoked_at IS NULL OR revoked_at LIKE '____-__-__T__:__:__Z')
        )
;
CREATE TABLE idempotency_keys (
            merchant_id TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            payment_id TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
                CHECK (created_at LIKE '____-__-__T__:__:__Z'),
            PRIMARY KEY (merchant_id, idempotency_key)
        )
;
CREATE TABLE kv_state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
                CHECK (updated_at LIKE '____-__-__T__:__:__Z')
        )
;
CREATE TABLE merchants (
            id TEXT PRIMARY KEY,
            api_key_hash TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
                CHECK (created_at LIKE '____-__-__T__:__:__Z')
        )
;
CREATE TABLE payments (
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
                CHECK (created_at LIKE '____-__-__T__:__:__Z'),
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
                CHECK (updated_at LIKE '____-__-__T__:__:__Z'),
            expires_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now','+1 hour'))
                CHECK (expires_at LIKE '____-__-__T__:__:__Z')
        )
;
CREATE TABLE processed_transactions (
            payment_id TEXT NOT NULL,
            tx_hash TEXT NOT NULL,
            amount_stroops INTEGER NOT NULL,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
                CHECK (created_at LIKE '____-__-__T__:__:__Z'),
            PRIMARY KEY (payment_id, tx_hash)
        )
;
CREATE TABLE webhook_deliveries (
            id TEXT PRIMARY KEY,
            payment_id TEXT NOT NULL,
            url TEXT NOT NULL,
            payload TEXT NOT NULL,
            event_type TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            last_attempt TEXT CHECK (last_attempt IS NULL OR last_attempt LIKE '____-__-__T__:__:__Z'),
            acknowledged_at TEXT CHECK (acknowledged_at IS NULL OR acknowledged_at LIKE '____-__-__T__:__:__Z'),
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
                CHECK (created_at LIKE '____-__-__T__:__:__Z')
        )
;
