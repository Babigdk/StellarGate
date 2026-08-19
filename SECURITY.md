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
