# Webhook Reference Guide

This is the **canonical webhook documentation** for StellarGate. For webhook delivery management endpoints (list/redeliver), see [Webhook Delivery Management](#webhook-delivery-management). For integration examples, see [Integration Examples](#integration-examples).

## Overview

When a payment reaches a terminal state, StellarGate POSTs a signed JSON event to your webhook endpoint. Every request carries cryptographic headers that let you verify both authenticity and freshness — preventing replay attacks and tampering.

```
Payment created (pending)
    ↓
On-chain transaction detected
    ↓
Amount reconciled (completed/overpaid/underpaid)
    ↓
Webhook dispatched with signed payload
```

## Payload detail

`WEBHOOK_PAYLOAD_DETAIL` controls how much of the payload below you actually
receive, and **defaults to `minimal`, not `full`**. Every example in this
document shows the `full` payload (`WEBHOOK_PAYLOAD_DETAIL=full`) — the
previously-default, richer shape — for completeness; under the default
`minimal` setting, only `event`, `payment_id`, `status`, and `updated_at` are
sent. `merchant_id`, `tx_hash`, `amount`, `paid_amount`, `asset`,
`asset_issuer`, and `delta` are `full`-only. See the README's "Webhooks" →
"Payload detail" section and [SECURITY.md](SECURITY.md#webhook-payload-exposure)
for why: HMAC signing proves authenticity, not confidentiality, and this
service cannot guarantee your endpoint is reached over an encrypted
connection on every network. If your integration needs the omitted fields,
either set `WEBHOOK_PAYLOAD_DETAIL=full` or call `GET /v1/payments/:id` with
your API key.

## Event Types

StellarGate fires exactly one event when a payment settles, determined by comparing received amount to requested amount:

### `payment.completed`

Fired when cumulative payment equals the requested amount exactly.

```json
{
  "event": "payment.completed",
  "payment_id": "a1b2c3d4-...",
  "merchant_id": "your-merchant-id",
  "tx_hash": "abc123def456...",
  "amount": "10.00",
  "paid_amount": "10.00",
  "asset": "XLM",
  "asset_issuer": null,
  "status": "completed"
}
```

### `payment.overpaid`

Fired when cumulative payment **exceeds** the requested amount. The `delta` field shows the excess amount to consider refunding.

```json
{
  "event": "payment.overpaid",
  "payment_id": "a1b2c3d4-...",
  "merchant_id": "your-merchant-id",
  "tx_hash": "abc123def456...",
  "amount": "10.00",
  "paid_amount": "12.50",
  "asset": "XLM",
  "status": "completed",
  "delta": "2.50"
}
```

### `payment.underpaid`

Fired when a payment arrives but falls **short** of the requested amount. The `delta` field shows the remaining shortfall. The intent remains open for a top-up payment.

```json
{
  "event": "payment.underpaid",
  "payment_id": "a1b2c3d4-...",
  "merchant_id": "your-merchant-id",
  "tx_hash": "abc123def456...",
  "amount": "10.00",
  "paid_amount": "7.00",
  "asset": "XLM",
  "status": "underpaid",
  "delta": "3.00"
}
```

### `payment.expired`

Fired when a payment intent's TTL elapses before payment arrives. No further transactions are watched for this intent.

```json
{
  "event": "payment.expired",
  "payment_id": "a1b2c3d4-...",
  "merchant_id": "your-merchant-id",
  "tx_hash": null,
  "amount": "10.00",
  "paid_amount": null,
  "asset": "XLM",
  "status": "expired"
}
```

## Webhook Headers

Every webhook request includes three headers:

| Header | Description | Signed? |
|---|---|---|
| `X-StellarGate-Timestamp` | Unix time (seconds) when event was signed | ✅ Yes |
| `X-StellarGate-Signature` | Hex HMAC-SHA256 of `"{timestamp}.{raw_body}"` | ✅ Yes |
| `X-StellarGate-Event` | Copy of the `event` field from body (routing convenience) | ❌ No |

**Important:** `X-StellarGate-Event` is not covered by the HMAC signature. It mirrors the body's `event` field but can be altered in transit. Always read the event type from the signed JSON body after verifying the signature.

## Verifying Webhooks

Use this recipe to verify each incoming webhook:

1. **Extract headers:**
   - Read `X-StellarGate-Timestamp` as `t` (Unix seconds)
   - Read `X-StellarGate-Signature` as `sig` (hex string)

2. **Check timestamp freshness:**
   - Reject if `abs(now - t) > tolerance`
   - Recommended tolerance: **5 minutes** (300 seconds)
   - This bounds the replay window: a stolen request becomes useless after 5 minutes

3. **Recompute signature:**
   - Get the **exact raw bytes** received (before JSON re-encoding)
   - Compute `HMAC_SHA256(WEBHOOK_SECRET, "{t}.{raw_body}")`
   - Hex-encode the result
   - Example: if body is `{"event":"payment.completed"...}` and `t` is `1719072645`, compute HMAC over the string `"1719072645.{\"event\":\"payment.completed\"...}"`

4. **Constant-time comparison:**
   - Compare computed signature to `sig` using a **timing-safe** equality check
   - Reject on mismatch

5. **Parse and route:**
   - After signature verification passes, parse the JSON
   - Read the `event` field from the body to determine the event type
   - Route based on `event` (not on the `X-StellarGate-Event` header)

### Verification Examples

**Node.js:**

```javascript
const crypto = require("crypto");

function verify(rawBody, headers, secret, toleranceSec = 300) {
  const t = Number(headers["x-stellargate-timestamp"]);
  const sig = headers["x-stellargate-signature"];
  
  // 1. Check timestamp freshness
  if (!Number.isFinite(t) || Math.abs(Date.now() / 1000 - t) > toleranceSec) {
    return false; // stale or missing timestamp
  }
  
  // 2. Recompute signature
  const expected = crypto
    .createHmac("sha256", secret)
    .update(`${t}.${rawBody}`)
    .digest("hex");
  
  // 3. Constant-time comparison
  return crypto.timingSafeEqual(Buffer.from(sig), Buffer.from(expected));
}

// Usage: always read the event type from the verified body
function handleWebhook(rawBody, headers, secret) {
  if (!verify(rawBody, headers, secret)) {
    throw new Error("invalid signature");
  }
  
  const payload = JSON.parse(rawBody);
  const event = payload.event; // authenticated; safe to route on
  
  switch (event) {
    case "payment.completed":
      console.log("Payment completed:", payload.payment_id);
      break;
    case "payment.overpaid":
      console.log("Overpaid by:", payload.delta);
      break;
    case "payment.underpaid":
      console.log("Underpaid by:", payload.delta);
      break;
    case "payment.expired":
      console.log("Payment expired");
      break;
  }
}
```

**Python:**

```python
import hmac
import hashlib
import json
import time

def verify(raw_body, headers, secret, tolerance_sec=300):
    """Verify webhook signature and timestamp."""
    try:
        t = int(headers.get("x-stellargate-timestamp", 0))
        sig = headers.get("x-stellargate-signature", "")
    except (ValueError, TypeError):
        return False
    
    # 1. Check timestamp freshness
    if not t or abs(time.time() - t) > tolerance_sec:
        return False
    
    # 2. Recompute signature (ensure raw_body is bytes, not string)
    if isinstance(raw_body, str):
        raw_body = raw_body.encode("utf-8")
    
    payload = f"{t}.".encode("utf-8") + raw_body
    computed = hmac.new(
        secret.encode("utf-8"),
        payload,
        hashlib.sha256
    ).hexdigest()
    
    # 3. Constant-time comparison
    return hmac.compare_digest(computed, sig)

def handle_webhook(raw_body, headers, secret):
    """Handle incoming webhook with verification."""
    if not verify(raw_body, headers, secret):
        raise ValueError("invalid signature")
    
    payload = json.loads(raw_body)
    event = payload["event"]  # authenticated; safe to route on
    
    if event == "payment.completed":
        print(f"Payment completed: {payload['payment_id']}")
    elif event == "payment.overpaid":
        print(f"Overpaid by: {payload['delta']}")
    elif event == "payment.underpaid":
        print(f"Underpaid by: {payload['delta']}")
    elif event == "payment.expired":
        print("Payment expired")
```

## Webhook Delivery Management

StellarGate tracks all webhook delivery attempts in the `webhook_deliveries` table. Two endpoints expose this history:

### GET /payments/:id/webhooks

List all delivery attempts for a payment.

**Response (200 OK):**
```json
{
  "payment_id": "550e8400-e29b-41d4-a716-446655440000",
  "deliveries": [
    {
      "id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
      "url": "https://merchant.example.com/webhook",
      "event": "payment.completed",
      "status": "delivered",
      "attempts": 2,
      "last_attempt": "2026-06-22T15:30:45",
      "created_at": "2026-06-22T15:20:00"
    }
  ]
}
```

**Error (404 Not Found):**
```json
{ "error": "payment not found" }
```

### POST /payments/:id/webhooks/:delivery_id/redeliver

Manually re-attempt a webhook delivery.

**Response (200 OK):**
- Delivery succeeded (empty body)

**Error (502 Bad Gateway):**
```json
{ "error": "webhook delivery failed" }
```

**Error (404 Not Found):**
```json
{ "error": "delivery not found" }
```

**Behavior:**
- Re-sends the exact same signed payload (preserves authenticity)
- Records a new attempt, incrementing the attempt counter
- Respects merchant's webhook authentication (signature recomputed from original payload)
- Sets status to `delivered` only if recipient returns 2xx
- Scoped to payment owner (merchant authentication required)

## Configuration

Configure webhook behavior via environment variables:

| Variable | Description | Default |
|---|---|---|
| `WEBHOOK_SECRET` | HMAC signing secret (shared with you at gateway provisioning) | — |
| `WEBHOOK_RETRY_ATTEMPTS` | Inline delivery retry count | `3` |
| `WEBHOOK_RETRY_DELAY_MS` | Delay between retries | `5000` |
| `WEBHOOK_TIMEOUT_SECS` | Per-attempt timeout for outbound POST requests | `10` |
| `WEBHOOK_REDRIVE_INTERVAL_SECS` | How often background redrive worker scans for stuck deliveries | `30` |
| `WEBHOOK_REDRIVE_CONCURRENCY` | Maximum concurrent redrive attempts in flight | `4` |
| `WEBHOOK_REDRIVE_MAX_ATTEMPTS` | Total attempts (inline + redrive) before permanent failure | `8` |
| `WEBHOOK_REDRIVE_GRACE_SECS` | Grace period before a stuck delivery is touched by redrive worker | `60` |
| `WEBHOOK_ALLOW_PRIVATE_TARGETS` | Bypass SSRF guard for private IPs (dev/test only, never production) | `false` |

## Delivery Guarantee

StellarGate guarantees **at-least-once** delivery with automatic retries:

- **Initial dispatch:** When a payment settles, the webhook is dispatched synchronously with inline retries (configurable, default 3 attempts)
- **Background redrive:** If dispatch encounters an error or the process crashes mid-delivery, a background worker periodically scans for stuck deliveries and redrives them
- **Idempotency:** Use the `payment_id` as a deduplication key on your end. If you receive the same `payment_id` twice, it's a retry — process it idempotently
- **Timestamps for ordering:** Use `created_at` (initial creation time) or `updated_at` (last change time) from payment status to order events, not webhook delivery times

## SSRF Protection

All webhook URLs are validated for SSRF attacks:

- Hostname is resolved and checked against loopback (`127.0.0.0/8`), link-local (`169.254.0.0/16`), private (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`), and reserved ranges
- The same check runs on every redelivery against the exact resolved address (not a fresh DNS lookup), preventing DNS-rebinding attacks
- Production deployments enforce HTTPS; testnet/development allows HTTP
- Set `WEBHOOK_ALLOW_PRIVATE_TARGETS=true` only for local development

## Integration Checklist

- [ ] Store `WEBHOOK_SECRET` securely in environment (never commit)
- [ ] Implement signature verification with timestamp freshness check
- [ ] Use constant-time comparison for signature checking
- [ ] Read event type from JSON body, not from `X-StellarGate-Event` header
- [ ] Make webhook handler idempotent (deduplicate by `payment_id`)
- [ ] Return HTTP 2xx for success; any other status triggers retries
- [ ] Keep handler fast; long-running tasks should be queued asynchronously
- [ ] Log all webhook events for audit trail
- [ ] Monitor redelivery dashboard for failed deliveries
- [ ] Set up alerts for delivery failures
- [ ] Test signature verification with provided examples before going live

## Links

- **API Reference:** See `POST /payments` in [README.md](README.md#post-payments) for payment creation with `webhook_url`
- **Event Flow:** See "Payment Flow" section in [README.md](README.md#payment-flow) for end-to-end payment lifecycle
- **Integration Examples:** See [Integration Examples](#integration-examples) below

---

## Integration Examples

### Complete Workflow

**Step 1: Create a payment with webhook**

```bash
curl -X POST http://localhost:3000/payments \
  -H "Content-Type: application/json" \
  -d '{
    "amount": "100.0",
    "asset": "XLM",
    "merchant_id": "my-shop",
    "webhook_url": "https://yourapp.com/webhooks/stellar"
  }'
```

**Step 2: User sends payment via Stellar wallet**

Sends exactly 100 XLM to the destination address with memo included.

**Step 3: StellarGate detects and verifies transaction**

Detects on-chain transaction within ~1-10 seconds (depends on network).

**Step 4: Webhook delivered to your endpoint**

```
POST https://yourapp.com/webhooks/stellar
Content-Type: application/json
X-StellarGate-Timestamp: 1719072645
X-StellarGate-Signature: 3f5e...
X-StellarGate-Event: payment.completed

{
  "event": "payment.completed",
  "payment_id": "550e8400-...",
  "merchant_id": "my-shop",
  "tx_hash": "abc123def456...",
  "amount": "100.00",
  "paid_amount": "100.00",
  "asset": "XLM",
  "status": "completed"
}
```

**Step 5: Check delivery status (optional)**

```bash
curl http://localhost:3000/payments/550e8400-.../webhooks
```

Response shows all attempts and their status (delivered/failed/pending).

**Step 6: If delivery failed, manually redeliver**

```bash
curl -X POST http://localhost:3000/payments/550e8400-.../webhooks/[delivery-id]/redeliver
```

### Handling Overpayment

User sends 120 XLM instead of 100 XLM:

```json
{
  "event": "payment.overpaid",
  "payment_id": "550e8400-...",
  "merchant_id": "my-shop",
  "tx_hash": "abc123def456...",
  "amount": "100.00",
  "paid_amount": "120.00",
  "asset": "XLM",
  "status": "completed",
  "delta": "20.00"
}
```

Your app should track the `delta` and issue a refund to the sender for the excess.

### Handling Underpayment (Top-up)

User sends 70 XLM (shortfall of 30 XLM):

```json
{
  "event": "payment.underpaid",
  "payment_id": "550e8400-...",
  "merchant_id": "my-shop",
  "tx_hash": "abc123def456...",
  "amount": "100.00",
  "paid_amount": "70.00",
  "asset": "XLM",
  "status": "underpaid",
  "delta": "30.00"
}
```

The payment intent stays open and watchable. If user sends the remaining 30 XLM (or more) to the same address and memo, you'll receive:

```json
{
  "event": "payment.completed",
  "payment_id": "550e8400-...",
  "merchant_id": "my-shop",
  "tx_hash": "def456abc123...",  // Different on-chain transaction
  "amount": "100.00",
  "paid_amount": "100.00",  // Cumulative total
  "asset": "XLM",
  "status": "completed"
}
```

### Handling Expiry

No payment arrives before TTL (default 1 hour):

```json
{
  "event": "payment.expired",
  "payment_id": "550e8400-...",
  "merchant_id": "my-shop",
  "tx_hash": null,
  "amount": "100.00",
  "paid_amount": null,
  "asset": "XLM",
  "status": "expired"
}
```

Payment intent is no longer watched. The user must create a new payment intent if they want to retry.
