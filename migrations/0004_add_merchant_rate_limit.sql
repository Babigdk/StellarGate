-- Per-merchant rate-limit override. The API rate limiter previously keyed
-- purely on bucket + client IP, so a merchant's quota was a property of
-- where it connected from rather than who it was. NULL means "use the
-- configured default"; an operator can set a row-level override via
-- POST /merchants or PUT /merchants/:id/rate-limit.
--
-- Runtime schema is applied by db::migrate, not this file. Keep this in sync
-- with the ALTER in src/db.rs.

ALTER TABLE merchants ADD COLUMN rate_limit_per_sec INTEGER;
