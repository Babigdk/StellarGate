# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Log rotation and resource limits on both compose stacks.** Neither
  `docker-compose.yml` nor `deploy/docker-compose.prod.yml` capped container
  memory or CPU, and the quickstart file had no log rotation at all — an
  unbounded `json-file` log driver grows until the disk holding
  `stellargate_data` fills up, which stops SQLite writes and, with them,
  payment processing. Both files now set `logging.options` (10 MB × 5) and
  `deploy.resources.limits`/`reservations` sized against the baseline Oracle
  Always Free shape (1 OCPU / 6 GB); the quickstart file also gained the
  `stop_grace_period: 35s` that `deploy/docker-compose.prod.yml` already had.
  Sizing rationale is in a new "Resource limits" section of DEPLOYMENT.md.
- **Deployment-tunable limits that were compile-time constants.** Request
  body size, rate-limiter capacity/TTL, list-endpoint pagination bounds,
  shutdown grace period, the Horizon poller's page size, and the retention
  worker's batch size and per-cycle cap were all `const`s baked into the
  binary, even though each is a deployment-shaped decision (proxy fanout
  behind the rate limiter, an orchestrator's termination grace period, a
  Horizon poll cycle's duration) rather than a design invariant. They are now
  `Config` fields — `MAX_BODY_BYTES`, `RATE_LIMITER_MAX_KEYS`,
  `RATE_LIMITER_IDLE_TTL_SECS`, `PAGINATION_DEFAULT_LIMIT`,
  `PAGINATION_MAX_LIMIT`, `SHUTDOWN_GRACE_SECS`, `HORIZON_PAGE_LIMIT`,
  `DB_PRUNE_BATCH_SIZE`, `RETENTION_MAX_ROWS_PER_CYCLE` — validated at boot,
  documented in `.env.example`, with the previous constants kept as their
  defaults so behaviour is unchanged out of the box. `SHUTDOWN_GRACE_SECS`'s
  relationship to an orchestrator's own termination grace period (Kubernetes'
  `terminationGracePeriodSeconds`, Docker's `stop_grace_period`) is documented
  in a new "Shutdown grace" section of DEPLOYMENT.md, and
  `deploy/docker-compose.prod.yml` now sets `stop_grace_period` to clear the
  app's default drain budget instead of undercutting it (issue #279).
- **Audit events for every state-changing operation.** Authentication
  outcomes and key issuance/revocation were already logged with
  `merchant_id`/`source_ip`, but payment creation, webhook redelivery
  (single and bulk), and merchant provisioning logged nothing — and
  provisioning, the one action that mints a credential, logged only its
  *failures*. Each now emits a structured `tracing` event carrying a stable
  `audit = true` marker plus `action`, `actor` (`merchant`/`admin`),
  `merchant_id`, `source_ip`, `request_id`, `outcome`, and the affected
  resource id, documented in a new "Audit events" section of the README.
  `client_ip_key` (the fail-closed IP attribution already used for rate
  limiting and auth logs) is now reusable from any handler via
  `client_ip_key_from_parts`, so every audit event uses the same source
  attribution as everything else (issue #305).
- **Webhook payload minimisation, and a startup warning for plaintext
  delivery.** `build_payload` put `merchant_id` and full financial detail
  (`amount`, `paid_amount`, `asset`, `asset_issuer`, `tx_hash`, `delta`) in
  every webhook body. HMAC signing proves authenticity, not confidentiality,
  and on any network other than `public`, `ALLOWED_WEBHOOK_SCHEMES` may still
  permit plain `http` — so that detail could transit in cleartext, and
  `merchant_id` in particular adds nothing for the legitimate recipient
  (it's *their* id) while making an intercepted payload immediately
  attributable. `WEBHOOK_PAYLOAD_DETAIL` (default `minimal`) now sends only
  `event`, `payment_id`, `status`, and `updated_at`; the full payload is
  available by setting it to `full`, and a receiver that needs the omitted
  fields can call `GET /v1/payments/:id` with its API key instead. Separately,
  boot now logs a `warn` whenever `ALLOWED_WEBHOOK_SCHEMES` includes `http`,
  on every network, not just when it would be unsafe — so enabling plaintext
  delivery is never a silent choice. Documented in a new "Webhook Payload
  Exposure" section of SECURITY.md and the README's "Payload detail"
  subsection, with a migration note for existing `full`-payload receivers
  (issue #306).

### Fixed

- **`RATE_LIMIT_REQUESTS_PER_SEC=0` no longer boots into the most aggressive
  limit the system can apply.** `Config::validate_timing` rejects
  `POLL_INTERVAL_SECS=0`, `PAYMENT_TTL_SECS=0`, `WEBHOOK_RETRY_ATTEMPTS=0`,
  `WEBHOOK_RETRY_DELAY_MS=0`, and `REQUEST_TIMEOUT_SECS=0` at boot, but
  `RATE_LIMIT_REQUESTS_PER_SEC=0` was missed — it passed validation and was
  then silently clamped up to `1` request/sec by `RateLimitState::new`. An
  operator setting `0` almost always means "disable rate limiting" or "I
  haven't configured this yet"; either way they got the tightest possible
  limit instead, with no warning at boot, which looks like an outage. Boot
  now refuses `RATE_LIMIT_REQUESTS_PER_SEC=0` with the same explanatory style
  as its siblings, and the `.max(1)` clamp is gone — the effective per-IP
  rate now always equals the configured one (issue #276).

- **An SSRF-blocked webhook delivery was retried by the redrive worker
  forever, double-counting the failure metric on every pass.** Both
  `dispatch()` and `redrive_one()` correctly marked a delivery blocked by the
  SSRF guard `status = "failed"`, but left `attempts` at its prior value
  (usually `0`). `list_redrivable_deliveries` selects on `status IN
  ('pending', 'failed') AND attempts < max_attempts`, so `status = "failed"`
  alone never removed the row from consideration — only `attempts` reaching
  the redrive cap does. The background worker picked the same blocked
  delivery back up on every subsequent pass, re-ran the (still-blocked) SSRF
  check, and incremented `stellargate_webhook_deliveries_total{outcome
  ="failed"}` again each time, forever. Both blocked branches now record
  `attempts` at the configured redrive cap, which is what actually makes the
  row terminal. New tests in `tests/webhook_dispatch_tests.rs` pin the
  invariant directly — a second `redrive_once` pass past the grace window
  attempts nothing, and a blocked `dispatch()` delivery never appears in
  `list_redrivable_deliveries` at all — rather than asserting specific column
  values, so they survive a future change of representation.

- **Test pools now use a genuinely shared in-memory SQLite database.** Every
  test suite built its pool from the DSN `sqlite::memory:` while allowing
  more than one pooled connection (the default). A bare `sqlite::memory:`
  DSN gives each connection its own **private** database, so which data a
  query saw depended on which pooled connection it happened to get — the
  suite passed by connection-reuse luck, not by construction.
  `tests/concurrency_tests.rs` was the sharpest instance: it exists to prove
  the single-settlement guarantee under *concurrent* reconciliation (issue
  #78), which only means something if concurrent tasks can land on genuinely
  different pooled connections talking to the *same* database. Every test
  pool now connects with `sqlite:file:<random-name>?mode=memory&cache=shared`
  plus `min_connections(1)` (so the shared database survives for the pool's
  lifetime), and a new `tests/db_shared_memory_tests.rs` proves both that the
  fixture is genuinely shared and that the old bare-DSN form is not, so the
  footgun can't be silently reintroduced (issue #309).

- **Issuer-less non-native assets fail at boot.** `ACCEPTED_ASSETS=XLM,USDC`
  (forgetting `:ISSUER`) used to parse as an issuer-less USDC entry, and
  `verify()` treated that shape as native XLM — a customer could settle a USDC
  invoice by sending XLM. Boot now refuses any non-`XLM` entry without an
  issuer, and a native payment cannot settle a USDC intent even if that
  misconfiguration is constructed by hand (issue #221).

- **Background tasks report *why* they exited.** `spawn_task` counted a start,
  and counted a stop when the future returned — with no way to tell "returned
  because shutdown was signalled" from "returned early because something went
  wrong". Several workers return permanently on a startup condition, and each
  looked exactly like a clean shutdown: `run_retention_worker` exiting because
  both retention windows are `0` (a deployment choice) recorded the same thing
  as `run_stream_listener` exiting because its HTTP client would not build (a
  `warn!` followed by a permanent end to stream-based payment detection).
  Workers now return an explicit `TaskExit` — `ShutdownRequested`,
  `DisabledByConfig` or `Fatal` — and the supervisor acts on it: a fatal exit is
  logged at **`error`** naming the task and restarted, a config-disabled exit is
  reported once at boot and is terminal, and neither is confused with an
  ordinary stop (issue #317).

- **`GET /payments` no longer runs a full `COUNT(*)` by default.** The offset
  branch issued a second query — `SELECT COUNT(*) FROM payments WHERE
  merchant_id = ?` (plus `AND status = ?` when filtering) — on every call,
  purely to fill `total`. SQLite has no cached row count, so this scanned
  every matching row every time, including the first page, for a field most
  clients never read (they render "next page" from `next_cursor` alone).
  `total` is now computed only when the request sets `?include_total=true`,
  and is entirely absent from the response — not `null` — otherwise. Keyset
  (cursor) pagination is unaffected; it has never returned `total` and
  remains the recommended approach (issue #320).

### Added

- **Explicit WAL checkpoint/growth tuning, a measured write-throughput
  ceiling, and a documented decision on Postgres.** `wal_autocheckpoint` and
  `journal_size_limit` are now set explicitly at pool-open time instead of
  left at SQLite's compiled-in defaults — the latter caps on-disk `-wal` file
  growth even when a long-lived reader defers `PASSIVE` checkpointing
  indefinitely. `tests/write_throughput_bench.rs` (`--ignored`, not part of
  CI) measures the single-writer path directly and DEPLOYMENT.md now states
  the result plus why a Postgres backend isn't in this change: `db::migrate`
  is a hand-written, unversioned schema (issue #268 is the prerequisite for a
  second backend to track in lockstep without drifting) (issue #321).

- **Expected-versus-live worker counts on `/health` and `/metrics`.** After boot
  there was no way to answer "how many workers should be running, and how many
  are?" — the information existed, but `stopped` was overloaded across three
  different meanings, so the arithmetic would have been wrong even once exposed.
  `/health` now carries a `tasks` object (`expected`, `live`, `disabled` with
  reasons) and `/metrics` exports `stellargate_tasks_expected`,
  `stellargate_tasks_live` and `stellargate_task_disabled`. `expected` excludes
  deliberately-disabled workers, so a poll-only or retention-disabled deployment
  does not read as permanently degraded, and
  `stellargate_tasks_live < stellargate_tasks_expected` is a usable alert
  (issues #317, #282, #103).

- **`X-RateLimit-*` response headers.** Every response now carries
  `X-RateLimit-Limit`, `X-RateLimit-Remaining` and `X-RateLimit-Reset` for the
  bucket it fell into, so a client can pace itself before being throttled
  instead of discovering the limit by hitting it. All four rate-limit headers
  (including `Retry-After`) are listed in `Access-Control-Expose-Headers` — the
  CORS spec hides everything outside its safelist, and `Retry-After` is not on
  it, so a browser client could previously see the `429` but none of the
  headers explaining it. The bucket/quota model is now documented per route in
  the README and in `openapi.yaml` (issue #327).
- **Dead-letter view for webhook deliveries.** Once a delivery exhausted its
  attempts it was marked `failed` and left there, findable only by knowing the
  payment id and calling `GET /payments/:id/webhooks` — backwards, since the
  reason to go looking is normally "a merchant says they are missing events"
  and a payment id is exactly what the person asking does not have. Answering
  it meant querying SQLite directly on the production volume, and a merchant
  could not self-serve at all. `GET /v1/payments/webhooks?status=failed` now
  lists a merchant's deliveries across every payment, cursor-paginated with the
  same conventions as `GET /payments` and scoped by a join rather than a
  caller-supplied filter (issue #319).
- **Bulk webhook recovery.** `POST /v1/payments/webhooks/redeliver` requeues
  failed deliveries — all of them, or up to 100 named ids — so a merchant who
  has fixed their receiver can recover what they missed. It sends nothing
  itself: rows go back to `pending` with `attempts = 0` and are retried by the
  redrive worker, whose concurrency limit and backoff already bound the
  outbound rate, so a bulk requeue cannot exhaust the redrive budget or
  stampede a receiver that has only just come back up (issues #319, #235).

### Changed

- **Unacknowledged terminal webhook failures survive retention.** A `failed`
  delivery was deleted after `WEBHOOK_DELIVERY_RETENTION_DAYS`, so the evidence
  that an event was permanently lost expired on a timer whether or not anyone
  had looked at it — precisely when it was most likely to be asked for. Such a
  row is now exempt until it is acknowledged (requeueing acknowledges it). To
  avoid trading one unbounded table for another, a retained failure is
  **compacted** once past the window: the row stays, its `payload` is cleared.
  The payload is the largest column and its only consumer is redelivery, which
  is not something anyone does to a months-old failure, so the record stays
  queryable indefinitely at a few hundred bytes (issue #319).

### Fixed

- **SSRF-blocked webhook deliveries are counted as terminal failures.** A
  target that resolves into a blocked range can never succeed, but neither the
  inline dispatch path nor the redrive path incremented
  `stellargate_webhook_deliveries_total{outcome="failed"}` — leaving an entire
  class of permanent failure invisible to the counter and to any alert built on
  it (issues #319, #233).
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

- **`GET /payments/:id/webhooks` now paginates like the payments listing.**
  The endpoint previously serialised every delivery row for a payment with no
  `LIMIT`, so a payment with unbounded delivery activity (see issue #233) grew
  the response without bound. It now supports a `status` filter
  (`pending`/`delivered`/`failed`), a `limit` (default 20, max 100), and keyset
  `cursor` pagination with `next_cursor` in the same contract as
  `GET /payments` (issue #326).
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
