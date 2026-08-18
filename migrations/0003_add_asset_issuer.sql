-- Persist the issuer each intent was priced in. Stellar asset codes are not
-- unique across issuers; matching only on code let a payment from issuer B
-- settle an intent priced in issuer A's USDC (issue #222).
--
-- Runtime schema is applied by db::migrate, not this file. Keep this in sync
-- with the ALTER in src/db.rs. Existing rows are backfilled from ACCEPTED_ASSETS
-- by db::backfill_asset_issuers at boot.

ALTER TABLE payments ADD COLUMN asset_issuer TEXT;
