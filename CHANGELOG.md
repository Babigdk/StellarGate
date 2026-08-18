# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **`X-RateLimit-*` response headers.** Every response now carries
  `X-RateLimit-Limit`, `X-RateLimit-Remaining` and `X-RateLimit-Reset` for the
  bucket it fell into, so a client can pace itself before being throttled
  instead of discovering the limit by hitting it. All four rate-limit headers
  (including `Retry-After`) are listed in `Access-Control-Expose-Headers` — the
  CORS spec hides everything outside its safelist, and `Retry-After` is not on
  it, so a browser client could previously see the `429` but none of the
  headers explaining it. The bucket/quota model is now documented per route in
  the README and in `openapi.yaml` (issue #327).

### Fixed

- **`openapi.yaml` declares its security schemes.** The spec had no
  `components.securitySchemes` block and no `security` key on any operation, so
  every route read as unauthenticated — a client generated from it exposed no
  way to supply an API key, sent none, and got `401` on every call, leaving the
  integrator's first impression that the API was broken rather than that the
  spec was incomplete. It also misrepresented the security posture to anyone
  reviewing the contract. `bearerAuth` (merchant API key) and `adminSecret`
  (`X-Admin-Secret`) are now defined, `bearerAuth` is attached to every
  protected payment operation, `/health` declares `security: []` explicitly,
  and each protected operation documents its `401` shape.
  `GET /payments/{id}` is genuinely tri-modal, so its optional-auth behaviour
  is expressed as `[{}, {bearerAuth: []}]` with a `PublicPaymentView` schema
  for the anonymous projection, rather than flattened to a single requirement
  (issue #325).

- **Background-task supervisor.** A panic in the poller, stream listener,
  sweeper, retention worker, or webhook redrive used to end that task for the
  life of the process while HTTP and `/health` kept serving. Each worker is
  now supervised: panics are logged and counted when they happen, the task is
  restarted with bounded exponential backoff, crash-loops fail `/health`, and
  start/stop/fail/restart counters are exported on `/metrics` (issue #316).

### Added

- **API versioning.** Public routes are now served under `/v1` alongside a
  documented deprecation policy. Unversioned paths keep working and return
  `Deprecation` and `Link: rel="successor-version"` headers pointing at their
  `/v1` equivalent — shipping versioning by breaking every existing caller at
  once would be exactly the failure versioning exists to prevent. Operational
  endpoints (`/health`, `/ready`, `/metrics`, `/dashboard`) are deliberately
  unversioned: they are infrastructure, not contract (issue #121).
- **API key lifecycle management.** Keys are now 256-bit tokens from the OS
  CSPRNG prefixed `sg_`, replacing UUIDv4 — a v4 UUID carries 122 random bits
  and spends 6 encoding version/variant, which is fine for an identifier and
  wrong for a bearer credential. A merchant can hold several keys, so rotation
  is issue-then-revoke with an overlap window rather than a replace-in-place
  that would leave no valid key. `POST/GET /merchants/:id/keys` and
  `DELETE /merchants/:id/keys/:key_id` cover issue, list and revoke; revocation
  is a tombstone so the audit trail survives it, and revoking a merchant's last
  active key is refused. Keys issued before this change keep working — the
  migration carries them into the new table (issues #74, #81).
- **Data retention worker.** `idempotency_keys` and `webhook_deliveries` grew
  monotonically with no bound, so on a long-running deployment the disk was the
  only thing that stopped them — and these deployments run on a single volume,
  where a full disk takes the gateway down. A background worker now prunes both
  on an interval, configurable via `RETENTION_INTERVAL_SECS`,
  `WEBHOOK_DELIVERY_RETENTION_DAYS` and `IDEMPOTENCY_RETENTION_DAYS` (`0`
  disables either). A `pending` delivery is never pruned regardless of age —
  the redrive worker still owns it. Deletes are batched so no single statement
  holds SQLite's write lock long enough to stall payment traffic
  (issues #110, #111).
- Index on `webhook_deliveries(payment_id)`; delivery listings and the redrive
  worker were doing a full scan (issue #112).
- Operator dashboard at `/dashboard` — payments list with status filtering and
  cursor pagination, payment detail, webhook delivery history with one-click
  redelivery, and a live health indicator. Built as dependency-free HTML/CSS/JS
  compiled into the binary, so there is no build step and no separate deploy.
- Deployment stack under `deploy/` — Docker Compose (app + Caddy for automatic
  TLS), an Oracle Cloud bootstrap script, and a systemd unit — plus a
  production runbook (`DEPLOYMENT.md`) covering first deploy, secrets, backups,
  upgrades, rollback, alerting signals, and scaling limits. The gateway is not
  published on a host port; Caddy is the only route in.
- `.dockerignore`, cutting the Docker build context from ~7 GB to a few hundred
  kilobytes. Without it every image build shipped the whole `target/` directory.
- Repository furniture: issue and pull request templates, Dependabot
  configuration, `.editorconfig`, `.gitattributes`, and a pinned
  `rust-toolchain.toml`.
- `ALLOWED_WEBHOOK_SCHEMES` documented in `.env.example`.

### Changed

- Minimum supported Rust version is now **1.88**, declared consistently in
  `Cargo.toml`, the CI matrix, the Dockerfile, and `rust-toolchain.toml`. The
  previously declared 1.75 was unreachable — `time` requires 1.88 and `url`'s
  `icu_*` chain requires 1.86.
- `main.rs` startup wiring collapsed into `spawn_task`/`join_task` helpers,
  removing four near-identical spawn blocks and a macro that existed only to
  work around the same repetition. Behaviour unchanged.
- README rewritten against the actual API surface.
- **TLS switched from native-tls to rustls.** Both `sqlx` and `reqwest` now
  use `rustls`-based feature flags, eliminating the system OpenSSL runtime
  dependency and simplifying static/musl builds.
- **Listener mode validation tightened.** An invalid `STELLAR_LISTENER_MODE`
  value now fails fast at boot with a clear error instead of defaulting
  silently to `stream`.
- **Placeholder secrets rejected at boot.** Known placeholder values from
  `.env.example` (e.g., `default-secret`, `your_webhook_signing_secret`) are
  now detected and rejected during startup with a clear error to prevent
  accidental production use of weak credentials.

### Security

- **`GET /payments/:id` no longer discloses cross-tenant detail.** The endpoint
  was fully public and returned `merchant_id`, amounts, the destination address
  and `tx_hash` for any id — and payment ids travel through logs, referrers and
  browser history, so anyone who came across one could identify the merchant
  and the sum involved. It now returns a minimal projection (`id`, `status`,
  `expires_at`) to unauthenticated callers and the full record only to the
  owning merchant. Another merchant's key gets `404`, identical to an unknown
  id, so the response cannot be used to confirm a payment exists
  (issues #67, #85).

  **Breaking:** clients that read amounts or `merchant_id` from this endpoint
  without authenticating must now send the merchant's API key.

### Fixed

- **`GET /payments` offset pages now order rows exactly like cursor pages.**
  The offset query sorted by `created_at DESC` alone while the keyset query
  broke whole-second `created_at` ties on `id DESC`, so a `next_cursor` minted
  from an offset page silently skipped the rest of the tie group when handed
  to the cursor branch. The offset query now orders by
  `(created_at DESC, id DESC)` — the same ordering and the same index — and a
  short offset page returns `null` instead of a dangling cursor. The migration
  path from offset to cursor pagination is documented in the README; offset
  mode is marked deprecated (issues #328, #269).
- **Expiry sweeping now batches transitions.** `expire_overdue` previously
  issued one guarded `UPDATE` per overdue intent, costing N round-trips and N
  write-lock acquisitions per sweep — a real burden on the single SQLite
  writer after an outage leaves a large backlog overdue at once. It now
  transitions a bounded batch in a single `UPDATE … RETURNING`, so each sweep
  is one write sized by `EXPIRY_BATCH_SIZE` (default `500`) and the backlog
  drains over several sweeps. The `status IN ('pending','underpaid')` guard and
  the "only rows actually transitioned produce a webhook" property are
  preserved (issue #323).
- **The build.** `main` did not compile. An unclosed block in
  `rate_limit_middleware` plus a reversion to the pre-`moka` `Mutex` API, a
  duplicated struct field and an unterminated character literal in `config.rs`,
  and a dropped `elapsed_secs` helper whose three call sites remained.
- `Cargo.lock` disagreed with `Cargo.toml`, so every `--locked` CI step failed.
  Resolved by removing the unused `url` dependency — the code uses
  `reqwest::Url`, a re-export.
- HTTPS is again enforced for `webhook_url` on the public network. The rule had
  been replaced by the configurable scheme allow-list in a commit that never
  compiled, leaving its test failing; both gates now apply, so a permissive
  `ALLOWED_WEBHOOK_SCHEMES` cannot downgrade mainnet delivery to plaintext.
- Supply-chain CI, red on every push and weekly cron: bumped `event-listener`
  to the patched 5.4.2 (RUSTSEC-2026-0221) and allowed the `ISC` and
  `CDLA-Permissive-2.0` licences the rustls stack brings in. Dropped the now-
  unused `OpenSSL` licence allowance so it cannot return unnoticed.
- The Docker healthcheck invoked `curl`, which the runtime image did not
  install — containers reported unhealthy while serving traffic normally.

## [0.1.0] - 2026-07-29

Initial development release: payment intents, Horizon SSE and polling
listeners, payment verification, signed webhooks with retry and redrive,
multi-merchant API keys, intent expiry, SSRF protection, rate limiting, and
Prometheus metrics.

[Unreleased]: https://github.com/StellarGateLabs/StellarGate/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/StellarGateLabs/StellarGate/releases/tag/v0.1.0
