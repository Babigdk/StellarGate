# Deployment

Production runbook for StellarGate on an **Oracle Cloud "Always Free" VM** —
genuinely free with no expiry, and the only common free tier that gives you a
real persistent disk, which SQLite requires.

The stack is a plain Docker Compose deployment, so the same files run on any
VPS, home server, or Raspberry Pi.

- [Before you deploy](#before-you-deploy)
- [Provision the VM](#provision-the-vm)
- [Deploy](#deploy)
- [Provisioning a merchant](#provisioning-a-merchant)
- [Operating](#operating)
- [Upgrades and rollback](#upgrades-and-rollback)
- [Backups](#backups)
- [Scaling limits](#scaling-limits)
- [Other platforms](#other-platforms)

---

## Before you deploy

### 1. A Stellar account to watch

The gateway watches one account for incoming payments. It **never holds the
secret key** — you configure only the public key, and it never signs or submits
a transaction.

- **Testnet:** create and fund one free at [Stellar Laboratory](https://laboratory.stellar.org).
- **Mainnet:** an account you control, funded with the 1 XLM base reserve plus
  0.5 XLM per trustline.

Add a trustline for **every non-native asset** you intend to accept. A payment
in an asset with no trustline **fails on-chain** — the gateway cannot rescue it.
It logs a warning at startup naming any accepted asset that is missing one, so
read the first few lines of the log after your first deploy.

See [Trustlines](README.md#trustlines) for how to check what an account trusts
and how to add one.

### 2. A domain

Caddy issues a Let's Encrypt certificate automatically, which requires a domain
resolving to the VM. A free subdomain from [DuckDNS](https://duckdns.org) or
[Afraid](https://freedns.afraid.org) works.

TLS is not optional here: merchants send API keys as bearer tokens on every
request, and operators paste one into the dashboard.

### 3. Real secrets

```bash
openssl rand -hex 32   # WEBHOOK_SECRET
openssl rand -hex 32   # ADMIN_PROVISIONING_SECRET
```

Never reuse the placeholders — startup rejects known placeholder values, and
`WEBHOOK_SECRET` must be at least 32 characters.

### 4. Pre-flight checklist

| Check | Why |
|---|---|
| `STELLAR_NETWORK` and `STELLAR_HORIZON_URL` agree | Mismatched, they silently watch the wrong chain |
| `WEBHOOK_SECRET` ≥ 32 random chars | Signs every webhook; merchants verify against it |
| `ADMIN_PROVISIONING_SECRET` set | Unset disables merchant provisioning entirely |
| `CORS_ALLOWED_ORIGINS` set | **Required** on `public` — boot fails without it |
| Trustlines added for every accepted asset | Payments in an untrusted asset bounce |
| `WEBHOOK_ALLOW_PRIVATE_TARGETS` false | Enabling it in production reopens the SSRF hole |
| `TRUSTED_PROXY_CIDRS` set correctly | Forwarding headers are honored **only** from these proxies; unset = headers ignored (safe default) — see [Trusted proxies and client IP](#trusted-proxies-and-client-ip) |
| `deploy/stellargate.env` is `chmod 600` | It holds every secret the service has |

---

## Provision the VM

In the [Oracle Cloud console](https://cloud.oracle.com): **Compute → Instances
→ Create instance**.

| Setting | Value |
|---|---|
| Shape | `VM.Standard.A1.Flex` — Ampere ARM, **Always Free** |
| OCPUs / memory | 1 OCPU / 6 GB is ample (up to 4/24 is free) |
| Image | Canonical Ubuntu 22.04 or 24.04 |
| Boot volume | 50 GB (200 GB total is free) |
| SSH key | Upload your public key |

> **On capacity errors.** `Out of host capacity` for the ARM shape is common
> and not a mistake on your part — free ARM capacity is heavily contested. Try
> a different availability domain or region, or retry later. The x86
> `VM.Standard.E2.1.Micro` shape is also Always Free and works fine for this
> service if ARM stays unavailable.

### Open the ports — both firewalls

This is the step that traps nearly everyone on Oracle Cloud. There are **two
independent firewalls**, and traffic must pass both.

**1. The cloud security list** — Networking → Virtual Cloud Networks → your VCN
→ Security Lists → Default → **Add Ingress Rules**:

| Source | Protocol | Destination port |
|---|---|---|
| `0.0.0.0/0` | TCP | 80 |
| `0.0.0.0/0` | TCP | 443 |

**2. The host firewall** — Oracle's stock images also ship a restrictive local
`iptables`/`firewalld` ruleset. `setup-oracle.sh` handles this for you.

> Symptom of missing either: connections **hang** rather than being refused,
> and Caddy never issues a certificate because the ACME challenge cannot reach
> the host.

---

## Deploy

SSH in, then:

```bash
# 1. Bootstrap: installs Docker, opens the host firewall, clones the repo,
#    and installs a systemd unit so the stack survives reboots.
curl -fsSL https://raw.githubusercontent.com/StellarGateLabs/StellarGate/main/deploy/setup-oracle.sh | bash

# Docker group membership needs a new session
newgrp docker   # or log out and back in

# 2. Configure
cd ~/StellarGate
cp deploy/stellargate.env.example deploy/stellargate.env
chmod 600 deploy/stellargate.env
nano deploy/stellargate.env        # domain, email, Stellar account, secrets

# 3. Point your domain's A record at the VM's public IP, and confirm:
dig +short your-domain.com         # must return the VM IP before starting

# 4. Start
sudo systemctl start stellargate

# 5. Verify
curl https://your-domain.com/health   # {"status":"ok"}
curl https://your-domain.com/ready    # {"status":"ok"} once Horizon is reachable
```

`/ready` returning `503` with a `"reason"` field tells you immediately whether
the database, Horizon, or the payment-detection cursor is the problem. Once a
gateway is configured, `/ready` also requires a successful Horizon poll (or
stream event) within `POLL_INTERVAL_SECS × CURSOR_STALENESS_MULTIPLE` — so a
poller that died at startup surfaces as `payment detection stalled` instead of
leaving the probe green (issue #315). `/health` fails when an expected
background task is no longer running, naming the dead task in its `reason`.

**`/health`, `/ready` and `/metrics` are never rate-limited.** They used to
share the same per-IP "default" bucket as ordinary GET traffic, which meant a
traffic spike that tripped the limiter also handed the orchestrator's own
probe a `429`. `curl -f` treats a `429` as a failed check, and after
`retries: 3` the container gets marked unhealthy and restarted — right when it
is least able to absorb that, since restarting this service means the poller
re-baselines and the redrive worker runs a full pass on start. Probes are
cheap, come from a trusted orchestrator, and exist specifically to stay
answerable under stress, so they're exempt from the limiter entirely rather
than sharing a bucket with merchant API traffic.

The first build compiles the whole dependency tree and takes several minutes on
a 1-OCPU shape. Subsequent deploys reuse the Docker layer cache.

### What is exposed

Only Caddy binds to the host, on 80/443. The gateway itself is reachable solely
over the internal Compose network, so there is no way to reach the API over
plaintext by hitting the VM's IP directly.

---

## Trusted proxies and client IP

Rate limiting and the auth logs attribute every request to a client IP.
`X-Forwarded-For` and `X-Real-IP` are **client-supplied** — an attacker can put
anything in them — so StellarGate ignores them unless the request's socket
peer is one of the proxies you name in `TRUSTED_PROXY_CIDRS`
(comma-separated CIDR blocks, IPv4 or IPv6):

```bash
# Behind a reverse proxy on the same host / private network
TRUSTED_PROXY_CIDRS=10.0.0.0/8,192.168.0.0/16
```

**The default (unset) is the safe one:** no proxy is trusted, so the headers
are always ignored and the peer's own address is used. A directly-exposed
gateway must never trust them, and the default doesn't.

When the peer **is** a trusted proxy, the rightmost `X-Forwarded-For` value
that is not itself a trusted proxy is taken as the client (falling back to
`X-Real-IP`, then the peer). This is what keeps every client behind your proxy
on its own rate-limit bucket and with its own attribution in the auth logs,
while still ignoring anything a non-proxy caller tries to inject.

Two log lines let you confirm the setup at boot:

```
INFO client IP strategy: no trusted proxies configured — X-Forwarded-For/X-Real-IP are ignored; the socket peer address is used for rate limiting and auth attribution
INFO client IP strategy: forwarding headers are honored only from trusted proxies; all other peers are attributed by socket address  trusted_proxies=[10.0.0.0/8, 192.168.0.0/16]
```

If the peer address is ever unavailable (the router is served without connect
info), StellarGate fails closed: every such request shares a single key and
the headers are still ignored, with a one-time warning.

---

## Provisioning a merchant

There is no self-service signup — you mint keys:

```bash
curl -X POST https://your-domain.com/merchants \
  -H "X-Admin-Secret: $ADMIN_PROVISIONING_SECRET"
# {"merchant_id":"...","api_key":"..."}
```

The API key is shown **once**; only a hash is stored, so it cannot be
recovered. Deliver it over a secure channel. Merchants use it as
`Authorization: Bearer <key>`, and to sign in to the dashboard at `/dashboard`.

---

## Operating

```bash
cd ~/StellarGate
docker compose -f deploy/docker-compose.prod.yml logs -f app   # stream logs
docker compose -f deploy/docker-compose.prod.yml ps            # container state
systemctl status stellargate                                    # unit state
```

**Metrics.** `GET /metrics` exposes Prometheus counters for webhook delivery
outcomes, retries, delivery latency, and auth success/failure.

**Alerts worth wiring first:**

| Signal | Why it matters |
|---|---|
| `/ready` failing | Horizon or the database is unreachable, **or** the payment-detection cursor is stale — payments will not be detected |
| `/health` failing | An expected background task (poller, stream, sweeper, retention, redrive) died — a restart is the fix |
| `stellargate_webhook_deliveries_total{outcome="failed"}` rising | Merchants are not learning about completed payments |
| `cursor_age_secs` climbing in logs | The listener is falling behind the chain |
| `stellargate_auth_attempts_total{outcome="failure"}` spiking | Credential stuffing, or a broken integration |

**Exposure.** `/dashboard` leaks nothing without a valid API key, but there is
no reason to serve the sign-in page to the whole internet. Restrict it in the
`Caddyfile` by source IP, or put it behind basic auth, if only your team uses it.

**Log growth.** Both containers cap their JSON logs (10 MB × 5 for the app,
10 MB × 3 for Caddy). Uncapped container logs filling the boot volume is a
slow and surprising way to take a service down — the same disk holds
`stellargate_data`, so a full disk stops SQLite writes and, with them,
payment processing.

**Resource limits.** Both containers in `deploy/docker-compose.prod.yml` set
`deploy.resources.limits` and `reservations` (Compose's non-Swarm CLI honors
these directly; no `docker stack deploy` needed):

| Service | CPU limit | Memory limit | Memory reservation |
|---|---|---|---|
| `app` | 1.5 | 1 GB | 256 MB |
| `caddy` | 0.5 | 256 MB | 64 MB |

Sizing assumptions:

- The baseline shape from [Provision the VM](#provision-the-vm) is 1 OCPU /
  6 GB (Ampere Altra — a full core, not a fractional vCPU). `app`'s 1.5 CPU
  limit and 1 GB memory limit leave room for a Horizon catch-up poll cycle to
  burst without starving Caddy or the host OS, while still being well under
  the 6 GB total on the recommended shape.
- `caddy` only terminates TLS and proxies to one backend; 0.5 CPU / 256 MB is
  generous headroom for that job, not a measured ceiling.
- The 256 MB / 64 MB memory **reservations** are soft guarantees, not caps —
  they keep the host scheduler from starving either container under memory
  pressure, without preventing either from using more, up to its limit, when
  the host has room.
- Without a memory limit, a leak or an unusually large Horizon backlog in
  either container competes for the whole host's memory, and the Linux OOM
  killer's choice of victim is not guaranteed to be the process that caused
  the pressure — it can just as easily kill `caddy`, taking down the only
  thing the internet can reach while the real problem continues in `app`.
- If you deploy on the smaller `VM.Standard.E2.1.Micro` shape (1 vCPU / 1 GB)
  mentioned above, lower these limits accordingly — the defaults here assume
  the recommended Ampere shape and will not both fit comfortably on 1 GB of
  total host memory alongside the OS.

The root `docker-compose.yml` (the local quickstart) sets the same
`stop_grace_period` and log rotation, plus a generous `2.0` CPU / `1G`
memory limit — a laptop guardrail against a runaway container, not a tuned
production ceiling. Rely on `deploy/docker-compose.prod.yml`'s numbers above
for actual capacity planning.

**Shutdown grace.** On `SIGTERM`, StellarGate stops accepting new requests and
waits up to `SHUTDOWN_GRACE_SECS` (default 30) for the poller, sweeper,
redrive worker, retention worker, trustline checker, and stream listener to
drain before forcing exit. That budget is only meaningful if the orchestrator
sending the signal waits at least as long before escalating to `SIGKILL`:

- **Kubernetes** — `terminationGracePeriodSeconds` also defaults to 30. Left
  at the default on both sides, the pod can be force-killed at the same
  instant `SHUTDOWN_GRACE_SECS` expires, cutting a still-draining task off
  mid-work. Set `terminationGracePeriodSeconds` a few seconds *above*
  `SHUTDOWN_GRACE_SECS`, not equal to it.
- **Docker / Docker Compose** (used by `deploy/docker-compose.prod.yml`) —
  `stop_grace_period` defaults to 10s, well under `SHUTDOWN_GRACE_SECS`'s
  default of 30. Without raising it, `docker compose down` or a `restart:
  unless-stopped` cycle sends `SIGKILL` long before the app's own drain
  window closes. Set `stop_grace_period` on the `app` service to match (add a
  few seconds of margin, e.g. `35s` for the default `SHUTDOWN_GRACE_SECS=30`).

---

## Upgrades and rollback

Migrations in `migrations/` run automatically at startup and are recorded in
`_sqlx_migrations`, so each runs exactly once.

```bash
cd ~/StellarGate
git pull
sudo systemctl restart stellargate     # rebuilds and restarts
```

To roll back to a previous commit:

```bash
git checkout <previous-sha>
sudo systemctl restart stellargate
```

> **Rolling back across a migration does not undo it.** Migrations here are
> forward-only. If a release added one, take a backup first and treat rollback
> as restore-from-backup rather than a redeploy.

---

## Backups

The entire dataset is one SQLite file in a Docker volume.

```bash
# Consistent snapshot of a live database — do NOT just copy the file.
docker compose -f deploy/docker-compose.prod.yml exec app \
  sqlite3 /data/stellargate.db ".backup '/data/backup.db'"

# Copy it off the VM
docker compose -f deploy/docker-compose.prod.yml cp \
  app:/data/backup.db ./stellargate-$(date +%F).db
scp ubuntu@<vm-ip>:~/StellarGate/stellargate-*.db ./
```

> Copying `stellargate.db` directly while the app runs can capture a torn
> write, because WAL mode keeps recent commits in a side file. `.backup` takes
> a consistent snapshot.

Restore by stopping the stack, replacing the file in the volume, and starting
again. **Test a restore before you need one** — an untested backup is a guess.

Worth automating as a cron job, with the copy going somewhere other than this
VM.

---

## Scaling limits

Read this before scaling out — it is the sharpest constraint in the system.

**Run exactly one instance.** SQLite allows a single writer, and the volume is
local to this host. Two instances would each keep their own database file and
each run their own Horizon listener and expiry sweeper — a payment could settle
twice and fire duplicate webhooks.

This handles a large volume of payments comfortably; the workload is a handful
of small writes per payment. What it does not survive is host failure — you get
the restart window as downtime.

Going multi-node means moving off SQLite to a networked database and electing a
single leader for the background listeners. That is a real project, not a
config change: the sqlx queries and migrations are SQLite-specific today.

**Vertical scaling** is the supported lever — the free ARM shape goes to 4
OCPUs and 24 GB, editable on a running instance.

### Measured write-path ceiling

The single-writer lock is the thing to reason about when asking "how much
traffic can this take?" — every write this service makes (payment creation,
settlement, webhook delivery bookkeeping, the throttled `last_used_at`
refresh) serializes through it.

`tests/write_throughput_bench.rs` (`cargo test --release --test
write_throughput_bench -- --ignored --nocapture`) drives 16 concurrent tasks
inserting payments back-to-back for 10 seconds against a file-backed SQLite
pool opened with the exact PRAGMAs production uses (WAL, `synchronous =
NORMAL`, the tuned `wal_autocheckpoint`/`journal_size_limit` above). On the
2-vCPU CI/dev container this was measured on:

```
payments/sec = 4464.0  (completed=44721, errors=0, concurrency=16, elapsed=10.02s)
```

Read this as an **order-of-magnitude floor on the write path itself**, not an
end-to-end request-handling SLA:

- It measures `db::create_payment` directly — no HTTP, no JSON, no auth, no
  rate limiting, and critically none of settlement's extra writes
  (`processed_transactions` insert, the payment status `UPDATE`) or webhook
  dispatch. A settled payment costs more write-lock time than a bare create.
- It ran on 2 vCPUs in a shared CI environment. The target deployment (the
  free-tier Oracle ARM shape referenced above) has more cores and dedicated
  I/O; real numbers there will differ in either direction depending on disk
  latency.
- Zero errors at this concurrency and duration — `busy_timeout` absorbed
  every lock wait without a caller-visible failure. That is the number that
  would start degrading first under sustained overload: watch `SQLITE_BUSY`
  errors surfacing as `500`s before watching raw throughput.

Even generously discounting this for settlement/webhook overhead and slower
disks, it is comfortably above the request volume a single small-VM
deployment is expected to see. If your merchant volume approaches four
figures of payment creations *per second*, sustained, this is the number to
re-benchmark against your actual hardware before assuming headroom.

### Why there is no Postgres backend yet

Issue #321 asks for two things beyond the write-pressure reductions above:
a database access layer that isn't SQLite-specific, and a Postgres backend
(or a documented decision explaining why not). This is that decision.

`src/db.rs` is SQLite-specific throughout — `strftime`, `pragma_table_info`,
`INSERT OR IGNORE`, `sqlx::sqlite::SqliteRow` in `row_to_payment`, `PRAGMA`
statements — not merely because nobody has abstracted it, but because
`db::migrate` **is** the schema, applied as idempotent `CREATE TABLE IF NOT
EXISTS` / `ALTER TABLE ... ADD COLUMN` statements re-run on every boot, with
no schema-version table (issue #268). A repository trait behind which a
Postgres implementation could live is a mechanical exercise; a *second*
migration path that reaches the same schema on a database with different
`ALTER TABLE` semantics, different upsert syntax, and no `pragma_table_info`
introspection, re-derived by hand from the same ad hoc sequence, is not — it
would need to be re-verified column-by-column against the SQLite path on
every future schema change, indefinitely, which is exactly the kind of
drift-prone duplication a real migration tool exists to prevent.

Building that on top of the current schema mechanism would mean committing
to maintain two hand-written schema definitions in lockstep, which is worse
than the single SQLite-specific one that exists today. Issue #268 (a real,
versioned migration tool) is the prerequisite this needs, and is planned as
separate, prior work. Once it lands, a repository-trait abstraction and a
Postgres implementation behind it become a scoped, independently reviewable
change rather than one that has to solve schema versioning as a side effect.

---

## Other platforms

The stack is plain Docker Compose with no Oracle coupling. On any VPS or home
server, skip `setup-oracle.sh`, install Docker yourself, and run the same
compose file.

For managed container platforms (Render, Koyeb, Railway, Kubernetes), three
things must hold: **one instance**, a **persistent volume at `/data`**, and
secrets supplied as environment variables rather than baked into the image.

> Most free tiers on those platforms give you **no persistent disk** and stop
> the container when idle. For a payment gateway that means losing the ledger,
> which is why this runbook targets a VM instead.
