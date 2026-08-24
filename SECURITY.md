# Security Policy

StellarGate handles Stellar-network payments — destination addresses, memos,
webhook secrets, and (in self-hosted deployments) gateway wallet keys.
Vulnerabilities here can have direct financial impact, so we ask that you
report them privately rather than through a public GitHub issue.

## Supported Versions

StellarGate is pre-1.0 and does not yet maintain parallel release branches.
Security fixes are made against the `main` branch only. Deployments should
track `main` (or the latest tagged release, once releases exist) to receive
fixes.

| Version | Supported |
|---|---|
| `main` | :white_check_mark: |
| older commits / forks | :x: |

## Reporting a Vulnerability

**Do not open a public issue or pull request for a security vulnerability.**
Public disclosure before a fix is available puts every deployment at risk.

Instead, report privately using one of these channels:

1. **Preferred: GitHub Private Vulnerability Reporting.**
   Go to the [Security tab](https://github.com/StellarGateLabs/StellarGate/security/advisories/new)
   of this repository and open a new draft security advisory. This notifies
   maintainers directly without making the report public, and lets us
   collaborate with you on a fix (including credit in the advisory, if
   desired) before disclosure.
2. **Alternative: email.** If you're unable to use GitHub's advisory flow,
   email the maintainers at **security@stellargate.dev** with a description
   of the issue, steps to reproduce, and any proof-of-concept code. If you
   don't receive a response within 5 business days, please follow up — email
   can be missed.

Please include as much of the following as you can:

- A clear description of the vulnerability and its impact (e.g. fund loss,
  webhook signature bypass, SSRF, auth bypass on `/merchants` or `/payments`).
- Steps to reproduce, or a minimal proof of concept.
- The affected component (e.g. webhook signing in `src/webhook.rs`, payment
  verification in `src/horizon.rs`, the SSRF guard on `webhook_url`).
- Any suggested remediation, if you have one.

## What to Expect

- **Acknowledgement:** within 3 business days of your report.
- **Triage:** we'll confirm the issue, assess severity/impact, and let you
  know if we need more information.
- **Fix & disclosure:** we aim to ship a fix as quickly as the severity
  warrants. Once a fix is released, we'll coordinate public disclosure timing
  with you and credit reporters (unless you prefer to stay anonymous).

## Scope

In scope:

- The StellarGate API server (`src/`), including payment creation/lookup,
  webhook signing and delivery, the Horizon listener/poller, merchant
  provisioning, and configuration/validation logic.
- The database schema and migrations (`db::migrate` in `src/db.rs` — there is no `migrations/` directory).
- Supply-chain issues in this repository (e.g. `Cargo.lock`, CI workflows).

Out of scope:

- Vulnerabilities in the Stellar network, Horizon, or third-party wallets
  themselves — report those upstream to the [Stellar Development
  Foundation](https://stellar.org/security-bug-bounty).
- Issues that require an attacker to already control a merchant's
  `ADMIN_PROVISIONING_SECRET`, `WEBHOOK_SECRET`, or Stellar secret key.
- Denial-of-service reports that rely purely on brute-force volume rather
  than a logic flaw.

## Known Security Design Notes

For context when triaging reports, StellarGate already implements:

- HMAC-SHA256 request signing on outbound webhooks with a signed timestamp
  (replay-resistant); see the "Verifying webhooks" section of the
  [README](README.md).
- An SSRF guard on `webhook_url` that rejects loopback/link-local/private/
  reserved destinations, re-checked on redelivery against the resolved
  address (not a fresh DNS lookup) to mitigate DNS rebinding.
- Admin-gated merchant provisioning (`X-Admin-Secret`), disabled entirely
  when `ADMIN_PROVISIONING_SECRET` is unset.

If you find a way around any of the above, that's exactly what this policy
is for — please report it privately.

## Webhook Payload Exposure

HMAC signing (above) proves a webhook body is authentic and unmodified. It
says nothing about confidentiality — anyone who can observe the connection
can read the body, and on any network other than `STELLAR_NETWORK=public`,
`ALLOWED_WEBHOOK_SCHEMES` may be configured to allow plain `http`, so that
connection is not necessarily encrypted. (`public` enforces HTTPS
unconditionally, independent of `ALLOWED_WEBHOOK_SCHEMES`.)

**What the default payload exposes.** `WEBHOOK_PAYLOAD_DETAIL` defaults to
`minimal`: `event`, `payment_id`, `status`, and `updated_at`. None of that is
sensitive on its own — a payment id without its amount or owning merchant
tells an observer only that *some* event happened to *some* payment.

**What `WEBHOOK_PAYLOAD_DETAIL=full` additionally exposes.** `merchant_id`,
`amount`, `paid_amount`, `asset`, `asset_issuer`, `tx_hash`, and (on
over/underpaid events) `delta`. `merchant_id` identifies which tenant the
event belongs to; the amounts reveal transaction size; `tx_hash` links the
event to a specific Stellar ledger entry. An operator who sets
`WEBHOOK_PAYLOAD_DETAIL=full` — or who allows `http` on a non-`public`
network — is choosing to let this detail be readable by anyone who can
observe the connection (or, if `http`, by any network intermediary at all).
Boot logs a `warn` when `ALLOWED_WEBHOOK_SCHEMES` includes `http`, on every
network, so this is never a silent choice.

**Getting the detail back safely.** A receiver that needs the fields
`minimal` omits already holds an API key (it's how the merchant integrated in
the first place) and can call `GET /v1/payments/:id` over that authenticated,
presumably-HTTPS channel instead of receiving it unauthenticated in the
webhook body.
