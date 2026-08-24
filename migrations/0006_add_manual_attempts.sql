-- Manual redeliveries must not share the automatic redrive budget (issue #235).
-- The redrive worker continues to gate on `attempts` alone; POST .../redeliver
-- increments `manual_attempts` without touching `attempts` or `last_attempt`.
ALTER TABLE webhook_deliveries ADD COLUMN manual_attempts INTEGER NOT NULL DEFAULT 0;
