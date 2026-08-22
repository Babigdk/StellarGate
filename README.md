
# StellarGate

[![CI](https://github.com/StellarGateLabs/StellarGate/actions/workflows/ci.yml/badge.svg)](https://github.com/StellarGateLabs/StellarGate/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.88%2B-orange.svg)](https://www.rust-lang.org)

A payment gateway API built on [Stellar](https://stellar.org) for accepting, verifying, and settling payments in XLM, USDC, and any other Stellar asset you configure.

> Think Stripe — but settlement happens on the Stellar network instead of through banks.

---

## Table of Contents

- [Overview](#overview)
- [How It Works](#how-it-works)
- [Features](#features)
- [Architecture](#architecture)
- [Getting Started](#getting-started)
- [Dashboard](#dashboard)
- [Configuration](#configuration)
  - [Trustlines](#trustlines)
- [Rate Limiting](#rate-limiting)
- [API Reference](#api-reference)
  - [Versioning](#versioning)
  - [API Key Lifecycle](#post-merchantsidkeys)
- [Payment Resolution Policy](#payment-resolution-policy)
- [Webhooks](#webhooks)
- [Security Model](#security-model)
- [Observability](#observability)
- [Deployment](#deployment)
- [Database Migrations](#database-migrations)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

StellarGate turns Stellar payments into a conventional REST API. A merchant creates a **payment intent** and receives a destination address plus a unique memo. The payer sends funds from any Stellar wallet. StellarGate watches the chain, matches the incoming transaction to the intent, settles it, and delivers a signed webhook to the merchant's application.

The gateway is **non-custodial in the strictest sense**: it never holds a secret key, never signs, and never submits a Stellar transaction. It only *observes* the configured gateway account for incoming payments. Refunds and payouts remain the merchant's responsibility.

## How It Works

```
┌─────────────┐   1. POST /payments      ┌──────────────┐
│ Merchant    │ ───────────────────────► │ StellarGate  │
│ Application │ ◄─────────────────────── │              │
└─────────────┘   address + memo + id    └──────┬───────┘
       │                                        │
       │ 2. show payment details                │ 3. watch Horizon
       ▼                                        │    (SSE stream + poller)
┌─────────────┐   pays address w/ memo   ┌──────▼───────┐
│   Payer's   │ ───────────────────────► │ Stellar      │
│   Wallet    │                          │ Network      │
└─────────────┘                          └──────┬───────┘
                                                │
┌─────────────┐   5. signed webhook      ┌──────▼───────┐
│ Merchant    │ ◄─────────────────────── │ 4. verify    │
│ Application │    payment.completed     │    & settle  │
└─────────────┘                          └──────────────┘
```

A payment is matched on three independent attributes — **memo**, **destination**, and **asset** — and only then is the amount compared. Transactions that fail on-chain are ignored.

## Features

| Capability | Status | Notes |
|---|---|---|
| Payment intents | ✅ | Create, fetch, list with filtering |
| Multi-merchant | ✅ | API-key auth; every payment scoped to a `merchant_id` |
| Real-time settlement | ✅ | Horizon SSE stream, with an interval poller as reconciler |
| Payment verification | ✅ | Memo + destination + asset + exact stroop amount |
| Over/underpayment handling | ✅ | Distinct statuses and events; underpaid intents accept a top-up |
| Intent expiry | ✅ | Configurable TTL with a `payment.expired` event |
| Signed webhooks | ✅ | Timestamped HMAC-SHA256, replay-resistant |
| Webhook redrive | ✅ | Background worker recovers deliveries lost to a crash |
| Delivery inspection | ✅ | List attempts and manually redeliver |
| Idempotent creates | ✅ | Via the `Idempotency-Key` header |
| Cursor pagination | ✅ | Keyset pagination, stable at any depth |
| SSRF protection | ✅ | Webhook targets resolved and filtered, re-checked on every send |
| Rate limiting | ✅ | Per-IP, per-route-bucket |
| API key lifecycle | ✅ | CSPRNG keys, rotation with overlap, instant revocation |
| Data retention | ✅ | Background pruning of aged delivery rows and idempotency keys |
| API versioning | ✅ | `/v1` prefix with a documented deprecation policy |
| Prometheus metrics | ✅ | `GET /metrics` |
| Dashboard UI | ✅ | Served at `/dashboard`; no build step or separate deploy |

## Architecture

```
src/
├── main.rs        Entry point: boot, background task spawn, graceful shutdown
├── lib.rs         Shared AppState and task-health tracking
├── config.rs      Environment parsing and validation (fails fast on bad input)
├── db.rs          SQLite persistence via sqlx
├── money.rs       Stroop-exact amount parsing and canonical serialization
├── strkey.rs      Stellar address (strkey) validation
├── ssrf.rs        Webhook target resolution and private-range filtering
├── horizon.rs     Horizon SSE listener, interval poller, payment verification
├── expiry.rs      Background sweeper for overdue pending intents
├── retention.rs   Background pruning of aged rows (bounds table growth)
├── metrics.rs     Prometheus counters and histograms
├── webhook.rs     Signed dispatch and the background redrive worker
└── api/
    ├── mod.rs     Router, auth, rate limiting, CORS, timeouts, dashboard, 404 fallback
    └── payments.rs  Payment and webhook-delivery handlers

static/            Dashboard assets, compiled into the binary via include_str!
migrations/        Versioned SQL, applied automatically on startup
tests/             Integration tests (API, concurrency, rate limits, webhooks, trustlines)
```

**Amounts are handled in stroops** (1 XLM = 10,000,000 stroops) as integers throughout. Floating-point arithmetic is never used for money. Values are canonicalized on write and on serialization, so `"10.00"`, `"10.0"`, and `"10"` are stored and returned identically.

**Two independent listeners** run concurrently. The SSE stream gives near-real-time settlement; the interval poller re-scans from a persisted cursor and acts as a reconciler for anything missed during a reconnect. Both converge on the same idempotent settlement path, so a payment observed twice settles once.

### Tech Stack

| Layer | Choice |
|---|---|
| Language | Rust (2021 edition, 1.88+) |
| HTTP | [axum](https://github.com/tokio-rs/axum) + [tower-http](https://github.com/tower-rs/tower-http) |
| Database | SQLite via [sqlx](https://github.com/launchbadge/sqlx) (WAL mode) |
| Async runtime | [tokio](https://tokio.rs) |
| TLS | rustls (no OpenSSL dependency) |
| Chain access | [Stellar Horizon API](https://developers.stellar.org/api) |

---

## Getting Started

### Prerequisites

- **Rust 1.88 or newer** — [install via rustup](https://rustup.rs)
- A Stellar account public key to receive payments (testnet keys: [Stellar Laboratory](https://laboratory.stellar.org/#account-creator))

### Install and Run

```bash
git clone https://github.com/StellarGateLabs/StellarGate.git
cd StellarGate

cp .env.example .env
# Edit .env — at minimum set STELLAR_GATEWAY_PUBLIC, WEBHOOK_SECRET,
# and ADMIN_PROVISIONING_SECRET

cargo run
```

The API listens on `http://localhost:3000` by default.

### Docker

The fastest path if you'd rather not install Rust:

```bash
cp .env.example .env   # edit as above
docker compose up --build
```

The SQLite database lives in a named volume (`stellargate_data`) and survives container restarts. `docker compose down` stops the stack while preserving that volume.

### Verify the Installation

```bash
# 1. Liveness
curl http://localhost:3000/health
# {"status":"ok"}

# 2. Provision a merchant (requires ADMIN_PROVISIONING_SECRET)
curl -X POST http://localhost:3000/merchants \
  -H "X-Admin-Secret: $ADMIN_PROVISIONING_SECRET"
# {"merchant_id":"...","api_key":"..."}

# 3. Create a payment intent
curl -X POST http://localhost:3000/payments \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"amount":"10","asset":"XLM"}'
```

---

## Dashboard

A read-and-operate dashboard is served directly by the gateway at
**`http://localhost:3000/dashboard`** — no separate process, build step, or
deploy. Sign in with any merchant API key.

| View | What it does |
|---|---|
| Payments | Table of the merchant's payments, filterable by status, paged with the keyset cursor |
| Payment detail | Full record — amounts, memo, destination, transaction hash, timestamps |
| Webhook deliveries | Every attempt for a payment, with a one-click **Redeliver** |
| Health | Live `/ready` indicator, polled every 30s |

**How it's built.** The page is plain HTML, CSS, and dependency-free
JavaScript, compiled into the binary with `include_str!`. There is no npm, no
bundler, and no `node_modules`: the deployable artifact stays a single Rust
binary, and the dashboard cannot drift out of sync with the API it ships
alongside. It is also a plain client of the documented REST API — it uses no
private endpoints, so anything it displays you can fetch yourself.

**Security.**

- The static shell is unauthenticated because it contains no data. Every figure
  on the page is fetched by your browser from the same authenticated endpoints
  documented below, using the key you supply.
- The key is held in your browser (`sessionStorage`, or `localStorage` if you
  tick *keep me signed in*) and sent as a bearer token. It is never stored
  server-side and never logged.
- Responses carry a strict `Content-Security-Policy` (`default-src 'none'`,
  no `unsafe-inline`) plus `X-Content-Type-Options: nosniff`, so the page an
  operator pastes a key into cannot load third-party script or be framed.
- All API-supplied values are inserted via `textContent`, never as markup —
  `webhook_url` and `memo` are merchant-controlled and would otherwise be a
  stored-XSS vector.

> The dashboard shows only what the signed-in merchant's key can already reach.
> It exposes no admin capability — merchant provisioning stays on
> `POST /merchants` behind `ADMIN_PROVISIONING_SECRET`. If you expose the
> gateway publicly, put `/dashboard` behind your own network controls as well.

---

## Configuration

All configuration is via environment variables, read once at startup. **Invalid values abort boot** rather than silently falling back to a default — a typo in an asset issuer or listener mode is a startup failure, not a runtime surprise.

### Core

| Variable | Description | Default |
|---|---|---|
| `PORT` | HTTP listen port | `3000` |
| `DATABASE_URL` | sqlx connection string (not a file path) | `sqlite:stellargate.db` |
| `STELLAR_NETWORK` | `testnet` or `public` | `testnet` |
| `STELLAR_HORIZON_URL` | Horizon endpoint | testnet Horizon |
| `STELLAR_GATEWAY_PUBLIC` | Gateway wallet public key (`G…`), validated as a strkey at startup. The listener stays idle until this is set. | — |
| `ACCEPTED_ASSETS` | Comma-separated. Only native XLM may be written as a bare `CODE`. Every other asset is `CODE:ISSUER` (`USDC:GA…`). A typo like `ACCEPTED_ASSETS=XLM,USDC` used to treat native XLM as settling USDC intents; boot now refuses it (issue #221). Duplicate codes are also refused (issue #222). Each issuer is strkey-validated. Adding an asset is config-only — but see [Trustlines](#trustlines). | `XLM,USDC:<testnet issuer>` |
| `REQUEST_TIMEOUT_SECS` | Whole-request timeout; exceeding it returns `408` | `30` |
| `MAX_PAYMENT_AMOUNT` | Maximum amount `POST /payments` accepts, in the asset's own units. A bare number (`100000`) applies to every asset; `CODE:AMOUNT` (`USDC:50000`) pins a bound to one asset specifically and always wins over the default; mix both with commas (`100000,USDC:50000`). Exceeding it returns `400 amount_out_of_range` naming the configured limit — distinct from `invalid_amount`, which means the value itself is malformed. Unset means no bound beyond `i64` overflow in `parse_stroops` (issue #310). | unset |
| `MIN_PAYMENT_AMOUNT` | Minimum amount, configured the same way as `MAX_PAYMENT_AMOUNT`. Boot refuses a configuration where an asset's effective minimum exceeds its effective maximum. | unset |

### Trustlines

**Every non-native asset in `ACCEPTED_ASSETS` needs a trustline on the gateway
account.** This is a Stellar rule, not a StellarGate one: an account cannot hold
an asset it does not trust, so a payment in an untrusted asset **fails on-chain**
before the gateway ever sees it. Nothing in the API can rescue it — the intent
sits `pending` until it expires while the payer's transaction is rejected.

XLM is native and never needs one. `ACCEPTED_ASSETS` defaults to including USDC,
so a fresh account will be missing that trustline.

StellarGate checks at startup and names anything missing:

```
WARN gateway account has no trustline for an accepted asset;
     intents in this asset will be unpayable  asset=USDC issuer=GBBD47IF…
INFO accepted assets with no trustline on the gateway account  missing=["USDC"]
```

It is a warning, not a boot failure — accepting XLM only is perfectly valid, so
refusing to start would be wrong. **Read the first lines of the log after your
first deploy.**

**The check doesn't stop at boot.** A trustline can be removed at any time
after startup — by the account operator, or by an issuer revoking
authorization — and an asset can be added to `ACCEPTED_ASSETS` on a redeploy.
A background `trustline_checker` task re-runs the same check on the retention
worker's cadence (`RETENTION_INTERVAL_SECS`, default 1 hour) for as long as
the gateway wallet is configured, so drift after boot is caught rather than
only ever logged once.

Current trustline state is on `GET /metrics`, so it's alertable rather than
grep-only:

```
# a confirmed-missing trustline
stellargate_missing_trustlines{asset="USDC"} 1

# how many checks have failed to reach Horizon at all — distinct from a
# confirmed-absent trustline, which only ever comes from a check that
# actually got an answer
stellargate_trustline_check_failures_total 0

# unix timestamp of the last check that got a confirmed answer; 0 means
# never — treat stellargate_missing_trustlines as unknown, not confirmed,
# until this is nonzero
stellargate_trustline_check_last_success_timestamp_seconds 1732000000
```

A Horizon outage during a check only increments
`stellargate_trustline_check_failures_total`; it leaves the last confirmed
`stellargate_missing_trustlines` value alone rather than guessing, so "Horizon
is unreachable" and "trustline confirmed absent" never look the same on the
scrape.

Once a trustline is confirmed missing, `POST /payments` for that asset returns
`503 trustline_missing` instead of minting an intent that can only bounce
on-chain. An asset that has never been checked yet (or whose last check
errored) is **not** rejected on that basis alone — an unreachable Horizon
must not also fail every payment creation for assets it simply hasn't had a
chance to confirm.

To check what the account currently trusts directly against Horizon:

```bash
curl -s "https://horizon-testnet.stellar.org/accounts/$STELLAR_GATEWAY_PUBLIC" \
  | jq '.balances[] | {asset: (.asset_code // "XLM"), issuer: .asset_issuer}'
```

Adding one is a `changeTrust` operation signed by the gateway account's **secret
key** — done once, from a wallet or script you control, never by StellarGate,
which holds no secret key and cannot do it for you. Any Stellar wallet
(Lobstr, Freighter) can add a trustline, or use the SDK:

```python
from stellar_sdk import Keypair, Server, TransactionBuilder, Network, Asset

kp = Keypair.from_secret("S...")            # gateway account secret
server = Server("https://horizon-testnet.stellar.org")
tx = (TransactionBuilder(
        source_account=server.load_account(kp.public_key),
        network_passphrase=Network.TESTNET_NETWORK_PASSPHRASE,
        base_fee=100)
      .append_change_trust_op(asset=Asset("USDC", "GBBD47IF…"))
      .set_timeout(90).build())
tx.sign(kp)
server.submit_transaction(tx)
```

Each trustline locks **0.5 XLM** of the account's base reserve, so keep enough
XLM free to cover one per asset.

> The issuer must match `ACCEPTED_ASSETS` exactly. `USDC` from the wrong issuer
> is a different asset entirely, and a trustline to it will not make payments in
> the configured one succeed.

### Settlement

| Variable | Description | Default |
|---|---|---|
| `STELLAR_LISTENER_MODE` | `stream` (SSE + poller reconciler) or `poll` (interval only) | `stream` |
| `POLL_INTERVAL_SECS` | How often the poller reconciles | `10` |
| `CURSOR_STALENESS_MULTIPLE` | Multiplier on `POLL_INTERVAL_SECS` that may elapse without a successful poll/stream event before `/ready` reports the detection cursor stale (`503`). A healthy poller cycles on the poll interval, so this only trips when the poller died or the stream wedged. | `3` |
| `PAYMENT_TTL_SECS` | How long an intent stays `pending` before expiring, from `created_at` | `3600` |
| `EXPIRY_BATCH_SIZE` | Maximum overdue intents the expiry sweeper transitions per sweep | `500` |

Intents are expired in bounded batches (`EXPIRY_BATCH_SIZE`) so a large
backlog drains over several sweeps instead of one long write lock — SQLite has
a single writer.

A poll cycle that fails no longer retries at the fixed `POLL_INTERVAL_SECS`
cadence that may have caused it. A `429`/`503` from Horizon backs off for at
least the `Retry-After` it sends; any other failure, or a rate limit with no
`Retry-After`, backs off exponentially with jitter (1s up to 120s), reset to
`POLL_INTERVAL_SECS` by the next successful cycle. Each cycle's catch-up loop
is also capped at 25 pages (5,000 records), so a backlog built up while
throttled cannot immediately re-trip the same limit — it drains over
subsequent cycles instead. See `stellargate_horizon_poll_cycles_total` under
[Observability](#observability) to track this.

**First-run cursor baselining.** The very first time the poller runs (no
`horizon_payment_cursor` persisted yet), it does not scan the gateway
account's entire payment history — that would replay everything on every
fresh deployment. Nor does it naively adopt the account's single most recent
payment as the floor: that is only safe if no payment relevant to this
gateway predates it, which fails for a **reused account** (a redeploy after
losing the database volume, a migration between hosts, where the account
already has payment history from before this instance ever started) and for
a **startup race** (a payment landing between the baselining query and the
first forward poll, which can sort at or below a single-record baseline on
read-replica lag). Neither failure produces an error — the intent just stays
`pending` until it expires, with no record connecting the customer's on-chain
payment to anything.

Instead, the poller pages backward (`order=desc`) from the tip until the
oldest record it has seen is older than every currently open (`pending` /
`underpaid`) intent's `created_at` — a payment cannot be relevant to an
intent it predates — deliberately over-scanning rather than under-scanning.
Re-processing an already-settled transaction is a no-op via
`processed_transactions`, so the cost of the overlap is a few extra Horizon
requests at boot. The walk is capped at 25 pages (5,000 records); if it hits
the cap before clearing every open intent's creation time, it baselines with
whatever overlap it found and logs a warning. When nothing is currently open,
one page of backward overlap is still taken, purely to cover the startup
race. An account with no payment history at all baselines at `"0"`, so its
first-ever payment is captured. The chosen baseline and the number of records
skipped are logged at `info` on every first run.

### Webhooks

| Variable | Description | Default |
|---|---|---|
| `WEBHOOK_SECRET` | HMAC-SHA256 signing secret. Must be **≥ 32 characters**; known placeholder values are rejected at boot. | — |
| `ALLOWED_WEBHOOK_SCHEMES` | Comma-separated URL schemes accepted for `webhook_url`. HTTPS is enforced on `public` regardless of this value. | `https` |
| `WEBHOOK_RETRY_ATTEMPTS` | Inline delivery attempts | `3` |
| `WEBHOOK_RETRY_DELAY_MS` | **Base** delay between inline retries — the first step of an exponential, jittered schedule, not a fixed interval | `5000` |
| `WEBHOOK_RETRY_MAX_DELAY_MS` | Ceiling on one inline retry delay. Must be `≥` `WEBHOOK_RETRY_DELAY_MS`. | `60000` |
| `WEBHOOK_TIMEOUT_SECS` | Per-attempt timeout; each retry is bounded independently | `10` |
| `WEBHOOK_ALLOW_PRIVATE_TARGETS` | Bypasses the SSRF private-range check. **Development and tests only.** | `false` |

#### Retry schedule

Inline retries back off exponentially and are jittered. Both halves matter, and
for different reasons.

**Backoff**, because a constant delay meant a receiver returning `503` for two
minutes produced — per delivery — three attempts at `t`, `t+5s`, `t+10s`. Across
a settlement burst of N payments that is `3N` requests arriving in three tight
clusters, precisely when the receiver is least able to absorb them.

**Jitter**, because backoff alone desynchronises nothing. Deliveries that failed
together share an attempt number, so a purely exponential schedule puts their
next attempts at the same instant — the same lockstep, just spaced further
apart.

The delay before retry *n* is drawn uniformly from
`[ceiling/2, ceiling]` where `ceiling = min(WEBHOOK_RETRY_DELAY_MS × 2^(n−1),
WEBHOOK_RETRY_MAX_DELAY_MS)`. That is **equal** jitter rather than the more
common full jitter over `[0, ceiling]`: full jitter can return a near-zero
delay, and this service already rejects `WEBHOOK_RETRY_DELAY_MS=0` at boot
because a zero delay causes exactly the retry bursts being avoided here. Equal
jitter keeps a guaranteed floor under every retry while still spreading a
co-failing batch across half the window.

`WEBHOOK_REDRIVE_GRACE_SECS` is validated at boot against this schedule, so a
grace window too short to clear the worst-case inline delivery — which would let
the redrive worker send a delivery whose inline dispatch is still running — is
rejected rather than discovered in production.

### Webhook Redrive Worker

Recovers deliveries left `pending`/`failed` by a process that exited mid-send or a receiver that was down when inline retries ran out. Its first pass runs immediately at startup, so a restart redrives without waiting a full interval.

| Variable | Description | Default |
|---|---|---|
| `WEBHOOK_REDRIVE_INTERVAL_SECS` | Scan frequency | `30` |
| `WEBHOOK_REDRIVE_CONCURRENCY` | Max redrive requests in flight | `4` |
| `WEBHOOK_REDRIVE_MAX_ATTEMPTS` | Total attempts (inline + redrive) before a delivery is left permanently `failed` | `8` |
| `WEBHOOK_REDRIVE_GRACE_SECS` | Idle time required before the worker touches a row, so it never races an in-flight inline delivery. Also the floor under the backoff. | `60` |
| `WEBHOOK_REDRIVE_BACKOFF_INITIAL_SECS` | Exponential backoff base: `initial × 2^(attempts−1)`. A row never attempted is exempt and gated only by the grace window. `0` disables growth. | `30` |
| `WEBHOOK_REDRIVE_BACKOFF_MAX_SECS` | Backoff ceiling. Must be `≥` the initial value. | `900` |
| `WEBHOOK_REDRIVE_JITTER_SECS` | Random extra delay (0–N seconds, drawn per row) on top of the window above. `0` disables. | `30` |

The jitter is what actually decorrelates a batch. Rows that failed together
share an `attempts` value and a near-identical `last_attempt`, so
`initial × 2^(attempts−1)` resolves to the same instant for all of them and the
worker — which computes eligibility in SQL — would hand itself the whole cluster
on every pass. The offset is drawn per row per statement, so each pass admits a
different random subset and the batch spreads over several intervals. It only
ever delays a row, never pulls one forward past the grace window.

### Retention

Two tables grow with traffic and have no natural bound — `idempotency_keys`
gains a row per guarded create, `webhook_deliveries` one per delivery attempt.
A background worker prunes both. On the single-volume deployments this service
targets, an unbounded table eventually fills the disk and takes the gateway
down.

| Variable | Description | Default |
|---|---|---|
| `RETENTION_INTERVAL_SECS` | How often the worker prunes | `3600` |
| `WEBHOOK_DELIVERY_RETENTION_DAYS` | Days to keep **terminal** (`delivered`/`failed`) delivery rows. `0` keeps them forever. | `30` |
| `IDEMPOTENCY_RETENTION_DAYS` | Days to keep idempotency keys — they only need to outlive the window in which a client might retry. `0` keeps them forever. | `7` |

> A `pending` delivery is **never** pruned regardless of age: the redrive worker
> still owns it, and deleting it would silently drop a webhook the merchant is
> owed. The worker marks rows `failed` once attempts are exhausted, so nothing
> stays exempt forever.

Deletes run in batches of 500 with a per-cycle cap. SQLite has a single writer,
so one unbounded `DELETE` over a large table would stall every payment write
until it finished; a backlog drains over several cycles instead.

### Security and Limits

| Variable | Description | Default |
|---|---|---|
| `ADMIN_PROVISIONING_SECRET` | Required via `X-Admin-Secret` to call `POST /merchants`. Unset disables provisioning entirely (always `401`). | _(unset — disabled)_ |
| `CORS_ALLOWED_ORIGINS` | Comma-separated origins. **Required** on `public`; omitting on testnet falls back to permissive with a warning. | _(unset)_ |
| `RATE_LIMIT_REQUESTS_PER_SEC` | Base per-IP limit. Write routes get this rate; read-only routes get 5×. Must be `> 0` — boot fails otherwise; there is no "disabled" value. | `10` |
| `TRUSTED_PROXY_CIDRS` | Comma-separated CIDR blocks whose `X-Forwarded-For`/`X-Real-IP` headers are honored for rate-limit bucketing and auth-log attribution. Every other peer is attributed by its own address and its headers are ignored — the safe default. | _(unset — headers ignored)_ |
| `DB_POOL_MAX_CONNECTIONS` | SQLite pool size. WAL allows one writer plus many readers. | `10` |
| `DB_BUSY_TIMEOUT_MS` | Lock-acquisition wait before erroring. Must be `> 0` under concurrent load. | `5000` |

---

## Rate Limiting

Every request is assigned to a **bucket**, and each bucket is limited
independently per client IP — so provisioning a merchant can never eat into a
client's payment quota, or vice versa. The client IP is resolved per
`TRUSTED_PROXY_CIDRS` above.

| Bucket | Routes | Quota |
|---|---|---|
| `payments` | `POST /payments` | `RATE_LIMIT_REQUESTS_PER_SEC` × 1 |
| `merchants` | `POST /merchants` | `RATE_LIMIT_REQUESTS_PER_SEC` × 1 |
| `redeliver` | `POST /payments/:id/webhooks/:delivery_id/redeliver` | `RATE_LIMIT_REQUESTS_PER_SEC` × 1 |
| `default` | everything else, including all other `GET` routes | `RATE_LIMIT_REQUESTS_PER_SEC` × 5 |

`/health`, `/ready` and `/metrics` are **not** in any bucket — they are exempt
from rate limiting entirely, not merely given a generous one. They are cheap,
come from a trusted orchestrator rather than the public internet, and their
whole purpose is to stay answerable when the system is under stress; sharing a
bucket with merchant traffic would mean a load spike that trips the limiter
could also turn an orchestrator's own liveness probe into a `429` — see
DEPLOYMENT.md for the failure mode this avoids.

Write and sensitive routes get the base rate; read-only traffic gets a more
generous allowance so ordinary polling is not throttled. Redelivery is bucketed
by *shape*, not by path — the URL carries payment and delivery ids, and keying
on those would let every id mint its own limiter entry, which is both an
unbounded map and a trivially bypassed limit.

### Response headers

**Every** response carries the current state of its bucket, so a client can pace
itself before being throttled rather than discovering the limit by hitting it:

| Header | Meaning |
|---|---|
| `X-RateLimit-Limit` | The bucket's effective quota, i.e. the multiplier is already applied |
| `X-RateLimit-Remaining` | Requests still available in this bucket right now |
| `X-RateLimit-Reset` | Delta-seconds until the bucket is back to **full** capacity |

A `429` additionally carries `Retry-After`, in delta-seconds, derived from the
limiter's own state — `governor` knows exactly when the next request would be
permitted, and this is that value rounded up with a floor of `1`. `Retry-After`
is time until a **single** request is permitted; `X-RateLimit-Reset` is time
until the bucket is full, so `Reset` is never the smaller of the two.

`Reset` is a delta rather than an epoch timestamp so that a client with a skewed
clock still gets a usable answer.

All four headers are listed in `Access-Control-Expose-Headers`. The CORS spec
hides every response header outside its safelist unless it is named there, and
`Retry-After` is not on that safelist — a self-pacing contract a browser cannot
read would be the same as no contract at all.

> **Note on precision.** Quotas are per-second, so a single cell replenishes in
> `1 / RATE_LIMIT_REQUESTS_PER_SEC` seconds — always under a second. Since
> `Retry-After` is an integer number of seconds (RFC 9110), it currently reports
> `1` at every configured rate. It is derived rather than hard-coded so that it
> stays correct if the quota shape changes; for pacing *now*, use
> `X-RateLimit-Remaining`.

---

## API Reference

### Versioning

The API is versioned by path prefix. **`/v1` is canonical** — use it for new
integrations:

```
POST /v1/payments
GET  /v1/payments/:id
POST /v1/merchants
```

Unversioned paths (`/payments`, `/merchants`) still work and serve the same
data, so nothing breaks today. They respond with headers pointing at their
replacement:

```
Deprecation: true
Link: </v1/payments>; rel="successor-version"
```

Operational endpoints — `/health`, `/ready`, `/metrics`, `/dashboard` and `/` —
are **not** versioned. They are infrastructure rather than contract; moving a
liveness probe with every API revision would break probes and scrape configs
for no benefit.

#### Deprecation policy

| Change | How it ships |
|---|---|
| Adding a field, endpoint, or optional parameter | Within the current version. Treat unknown fields as ignorable. |
| Changing or removing a field, changing a status code or error `code` | A new version prefix (`/v2`) |
| Security fixes that must apply to existing callers | Within the current version, documented in [CHANGELOG.md](CHANGELOG.md) as breaking |

That last row is a deliberate exception rather than an oversight: a data-exposure
fix that only applied to callers who opted into a new version would leave the
exposure in place for everyone who did not.

When a version is retired it will carry a `Sunset` header (RFC 8594) with the
removal date, announced in the changelog and release notes first. **No `Sunset`
date is currently set** for the unversioned paths — they emit `Deprecation`
only, because a sunset header is a commitment and none has been made.

---

### Authentication

| Scheme | Header | Used by |
|---|---|---|
| Merchant API key | `Authorization: Bearer <api_key>` | `POST /payments`, `GET /payments`, webhook delivery routes |
| Admin secret | `X-Admin-Secret: <secret>` | `POST /merchants` |

`GET /payments/:id` is reachable without a key so a checkout page can poll it directly, but **what it returns depends on who is asking** — an unauthenticated caller gets a minimal projection with no merchant or financial detail. See the endpoint below.

Both schemes are declared in [`openapi.yaml`](openapi.yaml) as `bearerAuth` and
`adminSecret`, and each operation carries its own `security` requirement, so a
generated SDK exposes a way to supply credentials and sends them without manual
modification. `GET /payments/:id` declares `[{}, {bearerAuth: []}]` — the
OpenAPI spelling of "auth is optional but changes the response".

### Error Envelope

Every error response uses the same shape:

```json
{
  "error": "A human-readable explanation",
  "code": "stable_machine_readable_code"
}
```

The `code` field is stable across releases and is what you should branch on.

**Request bodies are closed.** Every JSON body is validated against exactly the
fields its endpoint accepts, and anything else is rejected with `400`
`unknown_field` naming the offending field. A typo is not quietly dropped:

```bash
curl -X POST http://localhost:3000/v1/payments \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"amount": "100", "assset": "USDC"}'
```

```json
{
  "error": "invalid request body: Failed to deserialize the JSON body into the target type: unknown field `assset`, expected one of `amount`, `asset`, `webhook_url`",
  "code": "unknown_field"
}
```

This matters most where a field has a default. `asset` defaults to `XLM` when
absent, so before this the request above created a **100 XLM** intent and
returned `201` describing it — the single transposed character was recoverable
only by reading the response back carefully. `merchant_id` was the other sharp
edge: earlier revisions of `openapi.yaml` advertised it, but the handler has
always taken the merchant from the API key, so a client that sent it believed
it was choosing the tenant and was not.

| Code | HTTP | Meaning |
|---|---|---|
| `unauthorized` | `401` | Missing/invalid API key or admin secret |
| `invalid_request` | `400` | Malformed JSON or a deserialization failure |
| `unknown_field` | `400` | Request body contained a field the endpoint does not accept |
| `unsupported_media_type` | `415` | `Content-Type` is not `application/json` |
| `unsupported_asset` | `400` | Asset is not in `ACCEPTED_ASSETS` |
| `invalid_amount` | `400` | Not a positive decimal with ≤ 7 decimal places |
| `invalid_webhook_url` | `400` | Malformed, disallowed scheme, over 2048 chars, or SSRF-rejected |
| `invalid_status` | `400` | `status` filter is not a recognized value |
| `invalid_cursor` | `400` | `cursor` could not be decoded |
| `payment_not_found` | `404` | No such payment, or it belongs to another merchant |
| `merchant_not_found` | `404` | No merchant with that id |
| `key_not_found` | `404` | No active key with that id for this merchant |
| `last_active_key` | `400` | Refused: would revoke a merchant's only usable key |
| `invalid_label` | `400` | Key label exceeds 100 characters |
| `delivery_not_found` | `404` | No such delivery for that payment |
| `webhook_target_blocked` | `400` | Redelivery target rejected by the SSRF guard |
| `webhook_delivery_failed` | `502` | Receiver returned a non-success response |
| `rate_limit_exceeded` | `429` | Per-IP bucket limit exceeded |
| `idempotency_conflict` | `500` | Concurrent creates raced on one idempotency key |
| `not_found` | `404` | No matching route |
| `method_not_allowed` | `405` | Path exists, but not for this HTTP method |
| `request_timeout` | `408` | Request exceeded `REQUEST_TIMEOUT_SECS` |
| `internal_error` | `500` | Unexpected server-side failure |

`405`, `413`, and `408` are otherwise generated by axum and `tower_http`
themselves — before this, they carried an empty body rather than the envelope
above, so a client parsing the documented contract got a JSON decode failure
instead of an error it could act on. A response-rewriting middleware
(`json_error_envelope_middleware` in `src/api/mod.rs`) fills in the envelope
for these three cases wherever nothing downstream has already answered with
one.

---

### `POST /merchants`

Provision a merchant and return its API key. **Admin only** — requires `X-Admin-Secret`. There is no self-service signup; this is meant to be run by whoever operates the gateway.

```bash
curl -X POST http://localhost:3000/merchants \
  -H "X-Admin-Secret: $ADMIN_PROVISIONING_SECRET"
```

**`201 Created`**

```json
{
  "merchant_id": "a1b2c3d4-...",
  "api_key": "sg_ec5759103e27f...",
  "key_id": "d15f5a1a-..."
}
```

> ⚠️ `api_key` is returned **once**, in plaintext, and is never recoverable. Only a hash is stored. Save it immediately.

Keys are 256-bit tokens from the OS CSPRNG, prefixed `sg_` so they are
recognisable in logs and matchable by secret scanners. Use `key_id` to revoke
this key later.

---

### `POST /merchants/:id/keys`

Issue an **additional** key for a merchant — this is how rotation works. Admin only.

```bash
curl -X POST http://localhost:3000/merchants/$MERCHANT_ID/keys \
  -H "X-Admin-Secret: $ADMIN_PROVISIONING_SECRET" \
  -H "Content-Type: application/json" \
  -d '{"label": "rotation-2026-08"}'
```

**`201 Created`** — `{ "key_id": "…", "api_key": "sg_…", "prefix": "sg_6c85bd46e", "label": "rotation-2026-08" }`

> Rotation is **issue-then-revoke**, not replace-in-place. The new key is live
> immediately while the old one keeps working, so a merchant can deploy the new
> credential and only then retire the old one — there is never a window with no
> valid key. `label` is optional, purely for your own bookkeeping.

---

### `GET /merchants/:id/keys`

List a merchant's keys, including revoked ones so the history stays visible.
Admin only.

**`200 OK`**

```json
{
  "merchant_id": "abee3b99-…",
  "keys": [
    {
      "key_id": "e150ccc8-…",
      "prefix": "sg_6c85bd46e",
      "label": "rotation-2026-08",
      "created_at": "2026-08-11T15:45:29Z",
      "last_used_at": "2026-08-11T15:45:29Z",
      "revoked_at": null,
      "active": true
    }
  ]
}
```

Metadata only — the secret is unrecoverable by design, so this endpoint cannot
leak a usable credential. `prefix` exists so you can tell keys apart when
deciding which to revoke. `last_used_at` is refreshed at most once a minute per
key: it runs on every authenticated request, and SQLite takes a write lock per
update, so touching it unconditionally would put a write in the path of every
read.

---

### `DELETE /merchants/:id/keys/:key_id`

Revoke a key. Effective immediately — the next request using it gets `401`.
Admin only.

**`200 OK`** — `{ "key_id": "…", "revoked": true }`

> Refuses with `last_active_key` if it would revoke a merchant's **only** active
> key. This API has no self-service recovery, so that would turn a routine
> revocation into an incident. Issue a replacement first.

Revocation is a tombstone, not a delete, so the audit trail survives it. Keys
are scoped to their merchant: one merchant's key id cannot be revoked through
another's path.

---

### `POST /payments`

Create a payment intent. Requires a merchant API key; the merchant is taken from the key, not the request body.

**Request**

```json
{
  "amount": "10.00",
  "asset": "XLM",
  "webhook_url": "https://yourapp.com/webhooks/stellar"
}
```

| Field | Type | Required | Constraints |
|---|---|---|---|
| `amount` | string | ✅ | Positive decimal, ≤ 7 decimal places |
| `asset` | string | ❌ | Must be in `ACCEPTED_ASSETS`. Defaults to `XLM`. |
| `webhook_url` | string | ❌ | ≤ 2048 chars; scheme must be allowed; HTTPS required on `public`; SSRF-checked |

Any other field is rejected with `400` `unknown_field` — see [Error
Envelope](#error-envelope). In particular there is no `merchant_id` field: the
merchant comes from the API key and cannot be overridden by the body.

| Header | Required | Description |
|---|---|---|
| `Content-Type: application/json` | ✅ | Anything else returns `415` |
| `Authorization: Bearer <key>` | ✅ | Merchant API key |
| `Idempotency-Key` | ❌ | Client-chosen key, scoped per merchant. Reuse returns the original intent with `200 OK` instead of creating a duplicate. |

**`201 Created`** (or **`200 OK`** on an idempotency-key hit)

```json
{
  "id": "a1b2c3d4-...",
  "destination_address": "GBBD47IF6LWK7P7...",
  "memo": "A1B2C3D4",
  "amount": "10",
  "asset": "XLM",
  "asset_issuer": null,
  "status": "pending",
  "created_at": "2026-04-29T15:00:00Z",
  "expires_at": "2026-04-29T16:00:00Z"
}
```

> The payer must send **exactly** `amount` of `asset` to `destination_address` with `memo` set as a **text memo**. The intent expires at `expires_at` if unpaid.

---

### `GET /payments/:id`

Fetch a payment's current state. Reachable with or without a key, but the
response differs.

**Without a credential** — a minimal projection, enough to poll for completion:

```json
{
  "id": "a1b2c3d4-...",
  "status": "pending",
  "expires_at": "2026-04-29T16:00:00Z"
}
```

**With the owning merchant's key** — the full record:

```bash
curl http://localhost:3000/payments/$ID -H "Authorization: Bearer $API_KEY"
```

```json
{
  "id": "a1b2c3d4-...",
  "merchant_id": "your-merchant-id",
  "destination_address": "GBBD47IF6LWK7P7...",
  "memo": "A1B2C3D4",
  "amount": "10",
  "asset": "XLM",
  "asset_issuer": null,
  "status": "pending",
  "tx_hash": null,
  "paid_amount": null,
  "created_at": "2026-04-29T15:00:00Z",
  "updated_at": "2026-04-29T15:00:00Z",
  "expires_at": "2026-04-29T16:00:00Z"
}
```

| Caller | Response |
|---|---|
| No `Authorization` header | `200` — minimal projection above |
| Owning merchant's key | `200` — full record |
| **Another merchant's key** | `404 payment_not_found` |
| Invalid or revoked key | `401 unauthorized` |

> Another merchant's key gets a `404`, identical to an id that does not exist.
> A `403` would confirm the payment is real and belongs to someone else, which
> is precisely the cross-tenant signal this is meant to withhold.
>
> An invalid key is an error rather than a silent fall back to the public view,
> so a typo'd or revoked credential says so instead of looking like missing
> fields.

The public projection omits `merchant_id`, every amount, `tx_hash` and the
destination address by design. Payment ids travel through logs, referrers and
browser history, so treat anything on that response as effectively public.

**Status values**

| Status | Meaning |
|---|---|
| `pending` | Awaiting payment |
| `completed` | Fully paid (includes overpayment) |
| `underpaid` | Partially paid; still watched for a top-up |
| `expired` | TTL elapsed before payment arrived; no longer watched |

---

### `GET /payments`

List the authenticated merchant's payments, newest first. Supports **cursor**
(recommended) and **offset** (legacy) pagination.

| Param | Description | Default |
|---|---|---|
| `status` | Filter by `pending`, `completed`, `underpaid`, or `expired` | all |
| `limit` | Page size, 1–100 | `20` |
| `cursor` | Keyset cursor from a previous `next_cursor` | — |
| `offset` | Rows to skip (legacy; prefer `cursor`). Capped at `10000` — above that, `400 invalid_offset`. | `0` |
| `include_total` | Offset mode only. Compute and return `total`. | `false` |

**`200 OK`** — cursor mode (no `cursor` parameter on the first request)

```json
{
  "payments": [ { "id": "...", "status": "pending" } ],
  "limit": 20,
  "next_cursor": "3230..."
}
```

**`200 OK`** — offset mode (no `cursor` parameter, `offset` set)

```json
{
  "payments": [ { "id": "...", "status": "pending" } ],
  "limit": 20,
  "offset": 0,
  "next_cursor": "3230..."
}
```

Both modes order rows identically (`created_at DESC`, then `id DESC` to break
the whole-second `created_at` ties), so a `next_cursor` returned by an offset
page resumes cleanly in cursor mode. `next_cursor` is `null` on the final page
of either mode. Offset mode additionally returns `offset`.

> **`total` is opt-in (`?include_total=true`), not sent by default.** SQLite
> has no cached row count, so computing `total` is a full `COUNT(*)` scan over
> every matching row — on every list request, including the first page,
> regardless of how deep into the results the caller actually looks. Most
> clients render "next page" affordances from `next_cursor` alone and never
> read `total`, so the default path no longer pays for it. Ask for it
> explicitly when you need it:
>
> ```json
> {
>   "payments": [ { "id": "...", "status": "pending" } ],
>   "total": 42,
>   "limit": 20,
>   "offset": 0,
>   "next_cursor": "3230..."
> }
> ```
>
> `total` is entirely absent from the response (not `null`) when not
> requested, so a client can tell "not computed" apart from "computed as
> zero." Cursor mode has never returned `total` and `include_total` has no
> effect there.

> **Migration path.** Switch to cursor pagination by sending `cursor` instead
> of `offset`. Start with a first request that carries **no** `cursor` and
> **no** `offset`, then continue with the previous response's `next_cursor` on
> each subsequent request. The `next_cursor` an offset response returns is a
> courtesy for this migration — you may use it as the *first* cursor, but once
> you do you must stay in cursor mode; offset and cursor pagination cannot be
> mixed within one scan. Offset mode is retained for backward compatibility
> and is deprecated: like any offset paging, it can skip or repeat rows if
> data changes mid-scan.
>
> **`offset` is capped at `10000`.** SQLite implements `OFFSET` by producing
> and discarding every skipped row, so cost grows with depth; a request past
> the cap returns `400 invalid_offset` instead of paying for the scan. Cursor
> pagination has no such limit — it stays `O(log n)` regardless of depth — so
> this is another reason to prefer it over deep offset paging.

---

### `GET /payments/:id/webhooks`

List delivery attempts for a payment, newest first. Requires the owning merchant's API key.

| Query param | Description | Default |
|---|---|---|
| `status` | Filter by delivery status: `pending`, `delivered`, or `failed` | — |
| `limit` | Page size (clamped to `1..=100`) | `20` |
| `cursor` | Keyset cursor from a previous `next_cursor` | — |

`next_cursor` is `null` on the final page. To page through the history, start with a
request that carries **no** `cursor`, then pass the previous response's `next_cursor`
on each subsequent request.

**`200 OK`**

```json
{
  "payment_id": "a1b2c3d4-...",
  "deliveries": [
    {
      "id": "d1e2f3...",
      "url": "https://yourapp.com/webhooks/stellar",
      "event": "payment.completed",
      "status": "delivered",
      "attempts": 1,
      "last_attempt": "2026-04-29T15:04:00Z",
      "created_at": "2026-04-29T15:03:59Z"
    }
  ],
  "limit": 20,
  "next_cursor": "3230..."
}
```

### `POST /payments/:id/webhooks/:delivery_id/redeliver`

Manually re-send a delivery. The stored payload and event type are replayed verbatim with a **fresh** timestamp and signature. The SSRF guard re-runs against the target.

---

### `GET /payments/webhooks`

The **dead-letter view**: every delivery for the authenticated merchant, across
all of their payments. Defaults to `status=failed`.

This is the endpoint to reach for when a merchant says they are missing events,
because that question arrives *without* a payment id —
`GET /payments/:id/webhooks` can only answer it if you already know where to
look.

| Query | Default | Notes |
|---|---|---|
| `status` | `failed` | One of `failed`, `pending`, `delivered` |
| `limit` | `20` | 1–100 |
| `cursor` | — | Opaque keyset cursor, same convention as `GET /payments` |

```bash
curl "http://localhost:3000/v1/payments/webhooks?status=failed&limit=50" \
  -H "Authorization: Bearer $API_KEY"
```

**`200 OK`**

```json
{
  "deliveries": [
    {
      "id": "d1e2f3...",
      "payment_id": "a1b2c3d4-...",
      "url": "https://yourapp.com/webhooks/stellar",
      "event": "payment.completed",
      "status": "failed",
      "attempts": 8,
      "last_attempt": "2026-04-29T15:04:00Z",
      "acknowledged_at": null,
      "created_at": "2026-04-29T15:03:59Z"
    }
  ],
  "status": "failed",
  "limit": 50,
  "next_cursor": "3230..."
}
```

Scoping is a join to `payments`, not a filter you supply, so this can never
return another merchant's deliveries. The signed `payload` is omitted — a
listing is for triage, not replay.

---

### `POST /payments/webhooks/redeliver`

Bulk recovery after you have fixed your receiver. Requeues failed deliveries so
the background redrive worker retries them.

```bash
# Everything that failed
curl -X POST http://localhost:3000/v1/payments/webhooks/redeliver \
  -H "Authorization: Bearer $API_KEY"

# Or just specific ones (max 100 per request)
curl -X POST http://localhost:3000/v1/payments/webhooks/redeliver \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"delivery_ids": ["d1e2f3...", "d4e5f6..."]}'
```

**`200 OK`** — `{ "requeued": 42, "detail": "..." }`

> This endpoint **sends nothing itself**. It resets matching rows to `pending`
> with `attempts = 0` and hands them to the redrive worker, whose
> `WEBHOOK_REDRIVE_CONCURRENCY` and exponential backoff already bound the
> outbound rate. Requeueing ten thousand deliveries therefore costs one `UPDATE`
> and cannot stampede a receiver that has only just come back up.

Requeueing also **acknowledges** the delivery — see below.

### Retention of failed deliveries

A terminal failure is the evidence for "we never received your webhook", and
that question usually arrives long after the fact. So a `failed` delivery that
nobody has acknowledged is **exempt from
`WEBHOOK_DELIVERY_RETENTION_DAYS`** and is not deleted on a timer.

To keep that from trading one unbounded table for another, a retained failure is
**compacted** once it ages past the window: the row survives, its stored
`payload` is cleared. The payload's only consumer is redelivery, which is not
something anyone does to a months-old failure, and it is by far the largest
column — so the record of what was lost stays queryable indefinitely at a few
hundred bytes.

Requeueing via the endpoint above sets `acknowledged_at`, which returns the row
to ordinary retention.

Terminal failures are also counted in
`stellargate_webhook_deliveries_total{outcome="failed"}` — **including** the
SSRF-blocked path, which previously incremented nothing, leaving a whole class
of permanent failure invisible to alerts.

---

### `GET /health`

Liveness probe — cheap, and fails only on conditions a restart would fix. Returns `200 OK` while the process is running **and** every expected background task (poller, stream, sweeper, retention, redrive) is running. A task that died — a panic, or a poller that exited at startup — returns `503` naming the dead task, so a process whose payment detection is gone never looks healthy forever.

```json
{
  "status": "ok",
  "tasks": { "expected": 5, "live": 5, "disabled": [] }
}
```

```json
{
  "status": "unavailable",
  "reason": "background task(s) not running: poller",
  "tasks": { "expected": 5, "live": 4, "disabled": [] }
}
```

`tasks` answers "how many workers should be running, and how many are?" — a
question the process could not previously answer at all. **`expected` already
excludes workers that configuration has deliberately switched off**, so a
poll-only deployment (no stream listener) or one with both retention windows set
to `0` does not read as permanently degraded:

```json
{
  "status": "ok",
  "tasks": {
    "expected": 4,
    "live": 4,
    "disabled": [
      { "task": "retention", "reason": "both WEBHOOK_DELIVERY_RETENTION_DAYS and IDEMPOTENCY_RETENTION_DAYS are 0" }
    ]
  }
}
```

#### Why a worker stopped

Each worker returns an explicit reason rather than leaving the supervisor to
infer one, and the three are handled differently:

| Exit | Restarted? | Logged at | `/health` |
|---|---|---|---|
| Shutdown requested | no | `info` | n/a — the process is going away |
| Disabled by configuration | **no** — terminal, reported once at boot | `info` | listed under `disabled`; **not** a failure |
| Fatal error | **yes**, with bounded backoff | **`error`**, naming the task | counts as not running |

The distinction is load-bearing. Retention exiting because both windows are `0`
is a deployment choice; the stream listener exiting because its HTTP client
would not build is a fault that silently ends stream-based payment detection.
Both used to be recorded identically as "stopped", so the counters could not
separate them and neither could anyone reading them.

### `GET /ready`

Readiness probe. Runs `SELECT 1` against the database, probes Horizon (3 s timeout), and — once a gateway is configured — requires the payment-detection cursor to have advanced recently. The cursor is fresh when a successful poll or stream event landed within `POLL_INTERVAL_SECS × CURSOR_STALENESS_MULTIPLE`; a dead poller with a reachable Horizon is therefore `503`, not green.

```
200 OK          — { "status": "ok" }
503 Unavailable — { "status": "unavailable", "reason": "database unreachable | Horizon unreachable: … | payment detection stalled: …" }
```

### `GET /metrics`

Prometheus exposition format. See [Observability](#observability).

### `GET /dashboard`

The operator dashboard. Also serves `/dashboard/app.css` and
`/dashboard/app.js`. See [Dashboard](#dashboard).

---

## Payment Resolution Policy

Every on-chain payment matched by memo, destination, and asset resolves as follows:

| Scenario | `status` | Event | `delta` |
|---|---|---|---|
| Paid exactly | `completed` | `payment.completed` | — |
| Paid **more** than requested | `completed` | `payment.overpaid` | excess to refund |
| Paid **less** than requested | `underpaid` | `payment.underpaid` | shortfall owed |
| Top-up reaching exactly the total | `completed` | `payment.completed` | — |
| Top-up exceeding the total | `completed` | `payment.overpaid` | cumulative excess |
| TTL elapsed, unpaid | `expired` | `payment.expired` | — |

**Overpayment** fulfils the intent. The `delta` field carries the excess; refunding it is the merchant's responsibility — the gateway cannot send funds.

**Underpayment** leaves the intent open and watched. When a follow-up payment to the same memo brings the cumulative total to or above the requested amount, the intent completes.

**Limitations to be aware of:**

- Only a **single** top-up is tracked per underpaid intent. If more is needed, the payer should send the full remaining `delta` in one transaction.
- Once an intent is `completed`, further payments to the same address and memo are **not** tracked and fire no webhooks.
- Failed on-chain transactions are ignored entirely.

---

## Webhooks

When a payment reaches a terminal state, StellarGate POSTs a signed JSON event to the intent's `webhook_url`.

### Events

| Event | Fired when |
|---|---|
| `payment.completed` | Cumulative received equals the requested amount |
| `payment.overpaid` | Cumulative received exceeds it (`delta` = excess, `full` detail only) |
| `payment.underpaid` | Payment received but short (`delta` = shortfall, `full` detail only) |
| `payment.expired` | TTL elapsed with no payment |

### Payload detail

`WEBHOOK_PAYLOAD_DETAIL` controls how much the body carries. HMAC signing
(below) proves the body is authentic; it says nothing about who else can read
it in transit, and on any network other than `public`,
`ALLOWED_WEBHOOK_SCHEMES` may permit plain `http` — see [SECURITY.md](SECURITY.md#webhook-payload-exposure)
for the full exposure model.

**`minimal` (the default)** — enough to know something happened and look it
up; no tenant or financial detail:

```json
{
  "event": "payment.overpaid",
  "payment_id": "a1b2c3d4-...",
  "status": "completed",
  "updated_at": "2026-01-01T00:00:01Z"
}
```

**`full`** — the previous rich payload, opt in with `WEBHOOK_PAYLOAD_DETAIL=full`:

```json
{
  "event": "payment.overpaid",
  "payment_id": "a1b2c3d4-...",
  "status": "completed",
  "updated_at": "2026-01-01T00:00:01Z",
  "merchant_id": "your-merchant-id",
  "tx_hash": "abc123...",
  "amount": "10",
  "paid_amount": "12.5",
  "asset": "XLM",
  "asset_issuer": null,
  "delta": "2.5"
}
```

`delta` is present only on `payment.overpaid` and `payment.underpaid`, and
only under `full` detail. A receiver that needs the fields `minimal` omits
already holds an API key and can call `GET /v1/payments/:id` for the full
record instead.

> **Migrating from the previous default.** Every field prior versions sent is
> still available — set `WEBHOOK_PAYLOAD_DETAIL=full` to keep receiving
> exactly the payload shown above. There is no forced cutover: `full` is
> supported indefinitely, not a deprecated compatibility mode.

### Verifying Signatures

| Header | Value |
|---|---|
| `X-StellarGate-Timestamp` | Unix seconds at signing time |
| `X-StellarGate-Signature` | Hex HMAC-SHA256 of `"{timestamp}.{raw_body}"`, keyed with `WEBHOOK_SECRET` |
| `X-StellarGate-Event` | Convenience copy of the event type — **not signed** |

Binding the signature to the timestamp (Stripe-style) is what prevents indefinite replay of a captured request.

1. Read the timestamp (`t`) and signature (`sig`).
2. Reject if `abs(now − t) > tolerance`. **5 minutes** is recommended.
3. Concatenate `"{t}.{raw_body}"` using the **exact received bytes** — verify before any JSON re-encoding, which would change them.
4. Compute `HMAC_SHA256(WEBHOOK_SECRET, "{t}.{raw_body}")`, hex-encoded.
5. Compare against `sig` in **constant time**.
6. Only after the signature passes, read `event` from the **body**.

> ⚠️ Never route security-sensitive logic on `X-StellarGate-Event`. It is outside the signed material and can be altered in transit without invalidating the signature. The `event` field inside the verified body is authoritative.

**Node.js**

```js
const crypto = require("crypto");

function verify(rawBody, headers, secret, toleranceSec = 300) {
  const t = Number(headers["x-stellargate-timestamp"]);
  const sig = headers["x-stellargate-signature"];
  if (!Number.isFinite(t) || Math.abs(Date.now() / 1000 - t) > toleranceSec) {
    return false; // stale or missing timestamp
  }
  const expected = crypto
    .createHmac("sha256", secret)
    .update(`${t}.${rawBody}`)
    .digest("hex");
  const a = Buffer.from(sig, "hex");
  const b = Buffer.from(expected, "hex");
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

function handleWebhook(rawBody, headers, secret) {
  if (!verify(rawBody, headers, secret)) throw new Error("invalid signature");
  const { event } = JSON.parse(rawBody); // ← authenticated; safe to route on
  switch (event) {
    case "payment.completed": /* fulfil the order */ break;
    case "payment.overpaid":  /* fulfil, then refund `delta` */ break;
    case "payment.underpaid": /* await top-up of `delta` */ break;
    case "payment.expired":   /* release the cart */ break;
  }
}
```

**Python**

```python
import hmac, hashlib, time

def verify(raw_body: bytes, headers, secret: str, tolerance: int = 300) -> bool:
    try:
        t = int(headers["X-StellarGate-Timestamp"])
    except (KeyError, ValueError):
        return False
    if abs(time.time() - t) > tolerance:
        return False
    expected = hmac.new(
        secret.encode(), f"{t}.".encode() + raw_body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, headers.get("X-StellarGate-Signature", ""))
```

### Delivery Guarantees

Delivery is **at-least-once**. A receiver may see the same event more than once — after an inline retry, a redrive, or a manual redelivery — so handlers must be idempotent. Key on `payment_id` plus `event`.

Every attempt is recorded in `webhook_deliveries` and inspectable via `GET /payments/:id/webhooks`. A delivery that exhausts `WEBHOOK_REDRIVE_MAX_ATTEMPTS` is left `failed` and can still be redelivered manually.

For the full canonical reference, see **[WEBHOOK_REFERENCE.md](WEBHOOK_REFERENCE.md)**.

---

## Security Model

**No custody.** The gateway never holds a secret key, never signs, and never submits a transaction. Compromising it does not move funds — it only watches an address.

**Reads are scoped to the owning merchant.** `GET /payments/:id` returns full detail only to the merchant that owns it; unauthenticated callers get a status-only projection with no merchant id or amounts, and another merchant's key gets a 404 rather than a 403 so the response cannot confirm a payment exists.

**API keys are hashed at rest.** They are 256-bit tokens from the OS CSPRNG, shown once at issue and never stored in plaintext. Each merchant can hold several, so a key can be rotated without downtime, and any key can be revoked instantly — revocation takes effect on the next request. Revoking a merchant's last active key is refused, since this API has no self-service recovery.

**SSRF protection on webhook targets.** A `webhook_url` has its host resolved and rejected if it lands on loopback, link-local (including the cloud metadata address `169.254.169.254`), private, or otherwise reserved ranges. The check runs again on every dispatch and redelivery **against the exact resolved address** rather than a fresh DNS lookup, closing the DNS-rebinding window.

**HTTPS enforced on mainnet.** On `STELLAR_NETWORK=public`, a `webhook_url` must be HTTPS regardless of `ALLOWED_WEBHOOK_SCHEMES` — a permissive scheme list cannot downgrade mainnet delivery to plaintext.

**Rate limiting.** Every route falls into a per-IP bucket. Write and sensitive routes get the base quota; read-only routes get 5×. The limiter cache is capacity-bounded with idle eviction, so key cardinality cannot exhaust memory.

**Client IP attribution is fail-closed.** `X-Forwarded-For`/`X-Real-IP` are client-supplied, so they are honored only when the socket peer is a configured trusted proxy (`TRUSTED_PROXY_CIDRS`) — an unset allow-list means the headers are always ignored and the peer address is used, so a caller can't rotate a header to evade the limiter or poison the auth logs. When no peer address is available at all, every request shares a single key rather than trusting a header.

**Bounded requests.** Bodies are capped at `MAX_BODY_BYTES` (256 KiB by default) and every request is subject to `REQUEST_TIMEOUT_SECS`.

**Fail-fast configuration.** Invalid strkeys, unknown listener modes, and short webhook secrets abort startup instead of degrading silently.

To report a vulnerability, see [SECURITY.md](SECURITY.md).

---

## Observability

`GET /metrics` exposes Prometheus metrics:

| Metric | Type | Description |
|---|---|---|
| `stellargate_auth_attempts_total` | counter | Labelled by `outcome`, and `reason` on failure (`missing_key`, `invalid_key`) |
| `stellargate_webhook_deliveries_total` | counter | Delivery outcomes |
| `stellargate_webhook_retries_total` | counter | Retry attempts |
| `stellargate_webhook_delivery_latency_ms` | histogram | End-to-end delivery latency |
| `stellargate_tasks_started_total` | counter | Background task starts (including restarts) |
| `stellargate_tasks_stopped_total` | counter | Clean background task stops |
| `stellargate_tasks_failed_total` | counter | Background task panics |
| `stellargate_task_restarts_total` | counter | Supervisor restarts, labelled by `task` |
| `stellargate_task_running` | gauge | `1` if the named task is running |
| `stellargate_task_consecutive_failures` | gauge | Consecutive panics since the last stable run |
| `stellargate_tasks_expected` | gauge | Workers this deployment expects to be running, excluding any disabled by configuration |
| `stellargate_tasks_live` | gauge | Expected workers currently running |
| `stellargate_task_disabled` | gauge | `1` if the named task exited because configuration gave it nothing to do |
| `stellargate_horizon_poll_cycles_total` | counter | Horizon poll cycles, labelled by `outcome` (`success`, `rate_limited`, `error`) |
| `stellargate_horizon_last_successful_poll_timestamp_seconds` | gauge | Unix timestamp of the last successful Horizon poll or stream event |
| `stellargate_horizon_cursor_age_seconds` | gauge | Age of the most recently processed Horizon payment record — the sharpest signal of payment-detection lag |
| `stellargate_http_requests_total` | counter | HTTP requests, labelled by `method`, matched `route` (never the raw path — bounded cardinality), and `status` |
| `stellargate_http_request_duration_seconds` | histogram | HTTP request latency, labelled by `method` and matched `route` |
| `stellargate_payments_total` | counter | Payments labelled by lifecycle `status` (`created`, `completed`, `overpaid`, `underpaid`, `expired`) |
| `stellargate_payment_settlement_latency_seconds` | histogram | Time from payment creation to a settlement outcome |
| `stellargate_db_pool_connections` | gauge | SQLite connection pool size, labelled by `state` (`idle`, `in_use`) |
| `stellargate_db_pool_max_connections` | gauge | Configured maximum pool size |
| `stellargate_db_file_size_bytes` | gauge | On-disk size of the SQLite database files, labelled by `file` (`main`, `wal`, `shm`); absent for an in-memory database |

**Alert on `stellargate_tasks_live < stellargate_tasks_expected`.** That
comparison was not previously possible: `stellargate_tasks_stopped_total` was
overloaded across clean shutdown, configuration-disabled exit and fault, so
`started − stopped − failed` was not a live count and there was nothing to
compare it against. `stellargate_task_disabled` is what separates "switched off
on purpose" from "not running", which `stellargate_task_running` alone reports
identically.

Structured logs (via `tracing`) carry an `x-request-id` on every request, propagated to responses. Settlement logs include `settlement_latency_secs`, and both listeners log `cursor_age_secs` so poller lag is visible before a merchant notices.

Control verbosity with `RUST_LOG`, e.g. `RUST_LOG=stellargate=debug,tower_http=debug`.

### Audit events

Every state-changing operation emits a structured `tracing` event at `info`
(or `warn`, for revocation) carrying `audit = true`, so audit records can be
filtered or routed to a separate sink from ordinary operational logs — e.g.
`RUST_LOG` doesn't distinguish them, but a JSON-formatted log pipeline can
select on the `audit` field.

| Field | Meaning |
|---|---|
| `audit` | Always `true`. The marker field to select on. |
| `action` | What happened, as `resource.verb` — see the table below. |
| `actor` | `"merchant"` (acting via their API key) or `"admin"` (acting via `X-Admin-Secret`). |
| `merchant_id` | The merchant that owns the credential used, or (for `merchant.provision`) the merchant just created. |
| `source_ip` | The same attributed client IP used for rate limiting and auth logs — see "Client IP attribution" above. |
| `request_id` | The request's `X-Request-Id`, so an audit event can be correlated with the access log line and response header for the same request. |
| `outcome` | What happened to the resource: `created`, `delivered`/`failed` (redelivery), `requeued`, `issued`, `revoked`. |

Plus an id for whatever the event is about (`payment_id`, `delivery_id`, `key_id`).

| `action` | Emitted from | Notes |
|---|---|---|
| `payment.create` | `POST /payments` | Also carries `amount` and `asset`. |
| `webhook.redeliver` | `POST /payments/:id/webhooks/:delivery_id/redeliver` | `outcome` is the delivery result (`delivered`/`failed`), not whether the redelivery request itself was accepted. |
| `webhook.redeliver_bulk` | `POST /payments/webhooks/redeliver` | Carries `requeued`, the count of deliveries reset to `pending`. |
| `merchant.provision` | `POST /merchants` | Previously logged only on failure — the single most privileged operation (it mints a credential) now leaves a trace on success too. |
| `api_key.issue` | `POST /merchants/:id/keys` | |
| `api_key.revoke` | `DELETE /merchants/:id/keys/:key_id` | Logged at `warn`, not `info` — a revocation is rarer and more consequential than routine issuance. |

---

## Deployment

**[DEPLOYMENT.md](DEPLOYMENT.md) is the production runbook** — pre-flight
checklist, first deploy, secrets, backups, upgrades and rollback, alerting
signals, and scaling limits.

The target is an **Oracle Cloud "Always Free" VM** — free with no expiry, and
one of the few free tiers offering the persistent disk SQLite requires. The
stack is plain Docker Compose (app + Caddy for automatic TLS), so it runs
unchanged on any VPS, home server, or Raspberry Pi.

```bash
# On the VM — installs Docker, opens the host firewall, adds a systemd unit
curl -fsSL https://raw.githubusercontent.com/StellarGateLabs/StellarGate/main/deploy/setup-oracle.sh | bash

cd ~/StellarGate
cp deploy/stellargate.env.example deploy/stellargate.env
chmod 600 deploy/stellargate.env
nano deploy/stellargate.env          # domain, Stellar account, secrets

sudo systemctl start stellargate
curl https://your-domain.com/health
```

Only Caddy binds to the host; the gateway is reachable solely over the internal
Compose network, so the API cannot be hit over plaintext via the VM's IP.

> Most free tiers elsewhere (Render, Cloud Run, Railway) provide **no
> persistent disk** and idle the container out. For a payment gateway that
> means losing the ledger — hence a VM.

> ⚠️ **Run exactly one instance.** SQLite permits a single writer, and the
> background listeners assume they are the only ones running — two instances
> would each keep their own database and could settle a payment twice. This is
> the sharpest operational constraint in the system; see
> [Scaling limits](DEPLOYMENT.md#scaling-limits).

---

## Database Migrations

Schema is applied at startup by `db::migrate` in [`src/db.rs`](src/db.rs), called once from `main` before the HTTP listener binds. It is hand-written Rust, not a migration runner:

- Tables and indexes are created with `CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS`.
- New columns on existing tables are added by probing `pragma_table_info(...)` first, then `ALTER TABLE ... ADD COLUMN`.
- A few one-time data backfills (populating `processed_transactions` from legacy rows, filling `asset_issuer` from `ACCEPTED_ASSETS`, normalising pre-RFC 3339 timestamps) run alongside them.

Every statement is written to be safe to re-run, because **all of them run on every boot**. There is no version table, nothing is recorded as applied, and the whole sequence is not wrapped in a transaction.

> [!IMPORTANT]
> The `migrations/` directory is **not read at runtime.** Nothing in the codebase calls `sqlx::migrate!`, and the SQL in that directory has drifted from the live schema — it is missing `merchants`, `api_keys`, `processed_transactions`, and the `webhook_deliveries.event_type` column. Treat `db::migrate` as the only source of schema truth until this is resolved.
>
> Tracked in #92 (this documentation), #93 (the two diverging sources), and #268 (adopting a recorded schema version).

**Changing the schema**

1. Add the statement to `db::migrate` in `src/db.rs`, keeping it idempotent — it will run on every startup of every existing deployment.
2. For a new column on an existing table, follow the `pragma_table_info` probe pattern already used for `expires_at` and `event_type`. SQLite rejects a non-constant `DEFAULT` on `ALTER TABLE ... ADD COLUMN`, so add the column nullable and backfill it in a second statement.
3. Run `cargo test` — the suite calls `db::migrate` against an in-memory database, so syntax errors surface immediately.

Because there is no version tracking, a change that is *not* safe to re-run cannot currently be expressed. If you need one, resolve #268 first rather than working around it.

---

## Development

```bash
cargo build                 # compile
cargo test                  # full suite (unit + integration)
cargo fmt                   # format
cargo clippy --all-targets -- -D warnings
```

CI enforces all four on every pull request, plus a [`cargo audit`](https://github.com/rustsec/rustsec) RustSec advisory scan (also run weekly on a schedule). The test suite runs on both the minimum supported Rust version (1.88) and stable; `cargo fmt` and `cargo clippy` currently run on stable only, which can differ from the pinned toolchain you get locally (#294).

`deny.toml` is present but no workflow runs `cargo deny` yet, so its license, ban, and duplicate-version policy is not currently enforced (#293).

**Test layout**

| File | Covers |
|---|---|
| `tests/api_tests.rs` | Endpoints, validation, auth, pagination, idempotency |
| `tests/concurrency_tests.rs` | Double-settlement safety under concurrent reconciliation |
| `tests/rate_limit_tests.rs` | Per-bucket limiting |
| `tests/webhook_dispatch_tests.rs` | Signing, retries, redrive |
| `tests/trustline_tests.rs` | Asset trustline checks |
| `tests/db_shared_memory_tests.rs` | Proves the shared-cache in-memory SQLite fixture (below) is actually shared across pooled connections |

Integration tests run against an in-memory SQLite database and a [wiremock](https://github.com/LukeMathWalker/wiremock-rs) HTTP server — no network access or external services required.

> [!IMPORTANT]
> Every test pool connects with `sqlite:file:<random-name>?mode=memory&cache=shared` plus `min_connections(1)`, never bare `sqlite::memory:`. A bare `sqlite::memory:` DSN gives **each pooled connection its own private database** — with the multi-connection pools these tests build (the default is more than one connection), a query can silently land on a connection that never saw an earlier query's writes in the same test. `tests/concurrency_tests.rs` is the sharpest case: it exists to prove single-settlement under *concurrent* reconciliation (issue #78), which only means something if concurrent tasks can land on genuinely different pooled connections talking to the *same* database. `cache=shared` fixes this; a random name per pool keeps parallel test binaries from colliding, and `min_connections(1)` keeps the shared database alive for the pool's lifetime (SQLite drops it once every connection closes). `tests/db_shared_memory_tests.rs` proves both halves of this directly — the fixture is shared, and a bare `sqlite::memory:` DSN is not — so don't reintroduce the bare form (issue #309).

---

## Contributing

Contributions are welcome. Read [CONTRIBUTING.md](CONTRIBUTING.md) for setup, coding standards, and the PR process; participation is governed by our [Code of Conduct](CODE_OF_CONDUCT.md). Scoped, ready-to-pick-up issues are tracked in the [issue list](https://github.com/StellarGateLabs/StellarGate/issues).

1. Fork the repository
2. Branch: `git checkout -b feat/your-feature`
3. Make your changes **with tests**
4. Ensure `cargo test`, `cargo fmt --check`, and `cargo clippy --all-targets -- -D warnings` all pass
5. Open a pull request describing the change and its rationale

Found a security vulnerability? Please report it privately — see [SECURITY.md](SECURITY.md).

Release history is kept in [CHANGELOG.md](CHANGELOG.md).

---

## License

Released under the [MIT License](LICENSE).
