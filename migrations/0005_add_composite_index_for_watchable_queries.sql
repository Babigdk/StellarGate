-- Add a partial composite index to optimize frequently-executed watchable-status queries.
-- The index covers status and expires_at columns filtered to the "pending" and "underpaid"
-- statuses. This significantly improves performance of list_pending(), expire_overdue(),
-- and find_pending_by_memo() queries that run every 10 seconds perpetually (issue #270).
--
-- Partial indexes (WHERE clause) are more efficient than single-column indexes for
-- queries on these watchable statuses, as SQLite can use this index to satisfy both
-- the status IN ('pending', 'underpaid') and expires_at comparisons in one step.
--
-- Runtime schema is applied by db::migrate, not this file. Keep this in sync
-- with the CREATE INDEX in src/db.rs.

CREATE INDEX IF NOT EXISTS idx_payments_status_expires_at ON payments(status, expires_at)
 WHERE status IN ('pending', 'underpaid');
