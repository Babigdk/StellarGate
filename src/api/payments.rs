use crate::{api::AuthenticatedMerchant, db, money, AppState};
use axum::{
    async_trait,
    extract::{Extension, FromRequest, Path, Query, Request, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

/// An error with an HTTP status, a stable machine-readable code, and a human message.
pub struct AppError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl AppError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    pub fn bad_request(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, code, message)
    }

    pub fn unsupported_media_type(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNSUPPORTED_MEDIA_TYPE, code, message)
    }

    pub fn not_found(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, code, message)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({ "error": self.message, "code": self.code })),
        )
            .into_response()
    }
}

impl From<anyhow::Error> for AppError {
    fn from(err: anyhow::Error) -> Self {
        tracing::error!(error = %err, "internal error");
        AppError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "internal server error",
        )
    }
}

/// A drop-in replacement for `Json<T>` that maps any deserialization or
/// content-type failure into our standard `{"error": "..."}` 400 response
/// instead of axum's default 422 plaintext rejection.
pub struct JsonBody<T>(pub T);

#[async_trait]
impl<T, S> FromRequest<S> for JsonBody<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = AppError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        match Json::<T>::from_request(req, state).await {
            Ok(Json(value)) => Ok(JsonBody(value)),
            Err(rejection) => {
                use axum::extract::rejection::JsonRejection;
                match &rejection {
                    JsonRejection::JsonDataError(_) => {
                        let detail = rejection.body_text();
                        /* An unrecognised field is its own failure mode, not a
                        generic deserialization error: it almost always means a
                        typo or a client written against an older spec, and the
                        fix is different from "the value had the wrong type".
                        Give it a dedicated code so a client can branch on it,
                        and keep serde's message — it names the offending field
                        and lists the accepted ones. */
                        let code = if detail.contains("unknown field") {
                            "unknown_field"
                        } else {
                            "invalid_request"
                        };
                        Err(AppError::bad_request(
                            code,
                            format!("invalid request body: {detail}"),
                        ))
                    }
                    JsonRejection::JsonSyntaxError(_) => Err(AppError::bad_request(
                        "invalid_request",
                        "request body contains malformed JSON",
                    )),
                    JsonRejection::MissingJsonContentType(_) => {
                        Err(AppError::unsupported_media_type(
                            "unsupported_media_type",
                            "Content-Type must be application/json",
                        ))
                    }
                    _ => Err(AppError::bad_request(
                        "invalid_request",
                        "invalid request body",
                    )),
                }
            }
        }
    }
}

/// The `POST /payments` body.
///
/// `deny_unknown_fields` because silently discarding what serde does not
/// recognise is the wrong default for a payments API (issue #329). Without it a
/// client could send `merchant_id` — which older revisions of `openapi.yaml`
/// still advertised — and get a `201` describing an intent on whichever
/// merchant owns the API key, believing it had chosen the tenant itself.
///
/// The interaction with `asset` is what makes silence expensive rather than
/// merely untidy: `asset` defaults to `XLM` when absent, so `{"amount":"100",
/// "assset":"USDC"}` — one transposed character — used to mint a 100 XLM intent
/// and return `201`. Rejecting the field name is the only point at which that
/// typo is still cheap to fix.
///
/// This matches how the rest of the API already treats input it does not
/// understand: `status` is checked against an allow-list, an undecodable
/// `cursor` is `400 invalid_cursor`, an unaccepted asset is `400
/// unsupported_asset`. Strictness previously stopped at field names.
/// A JSON body that may be omitted entirely, but must be valid when present.
///
/// `Option<JsonBody<T>>` cannot express this. Axum's `Option` extractor maps
/// *every* rejection to `None`, so a body carrying a mistyped field would be
/// discarded in exactly the same way as no body at all — reintroducing, on the
/// one endpoint with an optional body, the silence issue #329 is about.
///
/// An absent or empty body is `None`; anything else is deserialized strictly
/// and its failures surface as the [`JsonBody`] rejection they are.
pub struct OptionalJsonBody<T>(pub Option<T>);

#[async_trait]
impl<T, S> FromRequest<S> for OptionalJsonBody<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = AppError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let (parts, body) = req.into_parts();

        /* `RequestBodyLimitLayer` already caps the body well below this, and
        exceeding it fails there rather than here; `usize::MAX` just means "no
        second, lower limit of our own". */
        let bytes = axum::body::to_bytes(body, usize::MAX).await.map_err(|_| {
            AppError::bad_request("invalid_request", "could not read the request body")
        })?;

        if bytes.is_empty() {
            return Ok(OptionalJsonBody(None));
        }

        let req = Request::from_parts(parts, axum::body::Body::from(bytes));
        JsonBody::<T>::from_request(req, state)
            .await
            .map(|JsonBody(value)| OptionalJsonBody(Some(value)))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreatePaymentRequest {
    pub amount: String,
    #[serde(default = "default_asset")]
    pub asset: String,
    pub webhook_url: Option<String>,
}

fn default_asset() -> String {
    "XLM".into()
}

pub async fn create(
    State(state): State<Arc<AppState>>,
    Extension(AuthenticatedMerchant(merchant_id)): Extension<AuthenticatedMerchant>,
    headers: HeaderMap,
    JsonBody(body): JsonBody<CreatePaymentRequest>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let asset = body.asset.to_uppercase();
    let accepted = &state.config.accepted_assets;
    let matched: Vec<_> = accepted.iter().filter(|a| a.code == asset).collect();
    let accepted_asset = match matched.as_slice() {
        [] => {
            let codes = accepted
                .iter()
                .map(|a| a.code.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(AppError::bad_request(
                "unsupported_asset",
                format!("unsupported asset '{}'; supported: {}", body.asset, codes),
            ));
        }
        [one] => *one,
        _ => {
            return Err(AppError::bad_request(
                "ambiguous_asset",
                format!(
                    "asset '{asset}' maps to more than one issuer; pin ACCEPTED_ASSETS to a single issuer per code"
                ),
            ));
        }
    };
    let asset_issuer = accepted_asset.issuer.as_deref();
    if !money::is_valid_amount(&body.amount) {
        return Err(AppError::bad_request(
            "invalid_amount",
            "amount must be a positive number with at most 7 decimal places",
        ));
    }
    if let Some(url) = &body.webhook_url {
        if url.len() > 2048 {
            return Err(AppError::bad_request(
                "invalid_webhook_url",
                "webhook_url exceeds max length of 2048 characters",
            ));
        };
        let parsed_url = reqwest::Url::parse(url).map_err(|_| {
            AppError::bad_request("invalid_webhook_url", "webhook_url is not a valid URL")
        })?;

        if !state
            .config
            .allowed_webhook_schemes
            .contains(&parsed_url.scheme().to_string())
        {
            return Err(AppError::bad_request(
                "invalid_webhook_url",
                format!(
                    "webhook_url scheme '{}' not allowed. Allowed schemes: {:?}",
                    parsed_url.scheme(),
                    state.config.allowed_webhook_schemes
                ),
            ));
        }

        /* Independent of the configurable allow-list: on the public network a
        webhook must be HTTPS. Delivery carries payment data and the HMAC
        signature, so a permissive ALLOWED_WEBHOOK_SCHEMES must not be able to
        downgrade mainnet traffic to plaintext. */
        if state.config.network == "public" && parsed_url.scheme() != "https" {
            return Err(AppError::bad_request(
                "invalid_webhook_url",
                "webhook_url must be an HTTPS URL on public network",
            ));
        }

        /* Resolve the host and reject loopback/link-local/private/reserved
        addresses so a webhook_url can't be used to probe the internal network
        (the same guard runs again, against the pinned address, on every
        dispatch and redelivery). */
        if crate::ssrf::validate(url, state.config.webhook_allow_private_targets)
            .await
            .is_err()
        {
            return Err(AppError::bad_request(
                "invalid_webhook_url",
                "webhook_url must be a reachable http(s) URL that does not resolve to a \
                 loopback, link-local, private, or other reserved address",
            ));
        }
    }

    /* An optional Idempotency-Key lets a client safely retry a create after a
    network blip without minting a duplicate intent. Keys are scoped per
    merchant; an empty header value is treated as absent. */
    let idempotency_key = headers
        .get("Idempotency-Key")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|k| !k.is_empty());

    /* If we've already seen this key for this merchant, return the original
    payment with 200 instead of creating a new one. If the mapped payment row
    is missing (e.g. a previous winner crashed after inserting the key but
    before creating the payment), delete the stale mapping and proceed. */
    if let Some(key) = idempotency_key {
        if let Some(existing_id) =
            db::find_payment_id_by_idempotency_key(&state.pool, &merchant_id, key).await?
        {
            if let Some(payment) = db::get_payment(&state.pool, &existing_id).await? {
                return Ok((StatusCode::OK, Json(to_json(&payment))));
            }
            sqlx::query(
                "DELETE FROM idempotency_keys WHERE merchant_id = ? AND idempotency_key = ?",
            )
            .bind(&merchant_id)
            .bind(key)
            .execute(&state.pool)
            .await
            .map_err(anyhow::Error::from)?;
        }
    }

    let id = Uuid::new_v4().to_string();

    /* Reserve the idempotency key before minting the payment so concurrent
    same-key requests can't both create payments. The DB primary key
    serialises the race: only one request's INSERT succeeds. */
    if let Some(key) = idempotency_key {
        let canonical_id = db::save_idempotency_key(&state.pool, &merchant_id, key, &id).await?;
        if canonical_id != id {
            /* Lost the race — the winner is about to create its payment. Wait
            for it with a short retry loop and then return that payment. */
            for _ in 0..50 {
                if let Some(payment) = db::get_payment(&state.pool, &canonical_id).await? {
                    return Ok((StatusCode::OK, Json(to_json(&payment))));
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            return Err(AppError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "idempotency_conflict",
                "concurrent request conflict, please retry",
            ));
        }
    }

    let memo = generate_unique_memo(&state.pool).await?;

    let payment = db::create_payment(
        &state.pool,
        db::NewPayment {
            id: &id,
            merchant_id: &merchant_id,
            destination_address: &state.config.gateway_public,
            memo: &memo,
            amount: &body.amount,
            asset: &asset,
            asset_issuer,
            webhook_url: body.webhook_url.as_deref(),
            ttl_secs: state.config.payment_ttl_secs as i64,
        },
    )
    .await?;

    Ok((StatusCode::CREATED, Json(to_json(&payment))))
}

/// `GET /payments/:id` — status for one payment.
///
/// This route stays reachable without a merchant key so a checkout page can
/// poll it directly, but what it returns depends on who is asking
/// (issues #67, #85):
///
/// - **No credential** → a minimal projection: the id, its status, and when it
///   expires. Enough to answer "has this been paid yet", and nothing else.
/// - **The owning merchant's key** → the full record.
/// - **Another merchant's key** → `404`, the same answer an unknown id gets.
///   A `403` would confirm the payment exists and belongs to someone else,
///   which is exactly the cross-tenant signal these issues are about.
///
/// The public projection deliberately omits `merchant_id`, every amount, and
/// `tx_hash`. Payment ids travel through logs, referrers and browser history,
/// so anything on this response should be treated as effectively public: it
/// previously leaked which merchant an id belonged to and how much it was for.
pub async fn get_by_id(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<Value>, AppError> {
    let payment = db::get_payment(&state.pool, &id)
        .await?
        .ok_or_else(|| AppError::not_found("payment_not_found", "payment not found"))?;

    let Some(key) = bearer_token(&headers) else {
        return Ok(Json(public_view(&payment)));
    };

    /* A credential was offered, so honour it rather than silently falling back
    to the public view — a typo'd or revoked key should say so, not quietly
    return less data and leave the caller wondering why fields are missing. */
    match db::find_merchant_by_key(&state.pool, &key).await? {
        Some(merchant_id) if merchant_id == payment.merchant_id => Ok(Json(to_json(&payment))),
        Some(_) => Err(AppError::not_found(
            "payment_not_found",
            "payment not found",
        )),
        None => Err(AppError::new(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "missing or invalid Authorization header",
        )),
    }
}

/// Extract a bearer token, if one was supplied at all.
fn bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|k| !k.is_empty())
        .map(str::to_string)
}

/// What an unauthenticated caller sees: enough to poll for completion, with
/// nothing that identifies the merchant or the sum involved.
fn public_view(p: &db::Payment) -> Value {
    json!({
        "id": p.id,
        "status": p.status,
        "expires_at": p.expires_at,
    })
}

#[derive(Deserialize)]
pub struct ListQuery {
    pub status: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub cursor: Option<String>,
}

const DEFAULT_LIMIT: i64 = 20;
const MAX_LIMIT: i64 = 100;
/// Statuses a payment can actually hold, and therefore the only ones worth
/// filtering on: `pending` at creation, `completed`/`underpaid` from
/// settlement (`horizon::settle`), and `expired` from the TTL sweeper
/// (`db::expire_overdue`). Nothing writes any other value, so anything else is
/// a guaranteed-empty filter and is rejected as invalid.
const VALID_STATUSES: [&str; 4] = ["pending", "completed", "underpaid", "expired"];

pub async fn list(
    State(state): State<Arc<AppState>>,
    Extension(AuthenticatedMerchant(merchant_id)): Extension<AuthenticatedMerchant>,
    Query(q): Query<ListQuery>,
) -> Result<Json<Value>, AppError> {
    if let Some(s) = &q.status {
        if !VALID_STATUSES.contains(&s.as_str()) {
            return Err(AppError::bad_request(
                "invalid_status",
                format!(
                    "invalid status '{}'; valid: {}",
                    s,
                    VALID_STATUSES.join(", ")
                ),
            ));
        }
    }

    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);

    if let Some(raw_cursor) = &q.cursor {
        // Keyset (cursor) pagination — stable, O(log n) regardless of page depth.
        let (cursor_ts, cursor_id) = decode_cursor(raw_cursor)
            .ok_or_else(|| AppError::bad_request("invalid_cursor", "invalid cursor"))?;

        let payments = db::list_payments_keyset(
            &state.pool,
            &merchant_id,
            q.status.as_deref(),
            limit,
            Some((&cursor_ts, &cursor_id)),
        )
        .await?;

        let next_cursor = if payments.len() == limit as usize {
            payments.last().map(|p| encode_cursor(&p.created_at, &p.id))
        } else {
            None
        };

        Ok(Json(json!({
            "payments": payments.iter().map(to_json).collect::<Vec<_>>(),
            "limit": limit,
            "next_cursor": next_cursor,
        })))
    } else {
        // Legacy offset pagination — kept for backward compatibility.
        let offset = q.offset.unwrap_or(0).max(0);
        let (payments, total) = db::list_payments(
            &state.pool,
            &merchant_id,
            q.status.as_deref(),
            limit,
            offset,
        )
        .await?;

        // A migration affordance, not a second pagination model: the caller
        // may take this cursor as the *first* cursor and then stay in pure
        // cursor mode from the next request on. `list_payments` orders by
        // (created_at DESC, id DESC), identical to the keyset query, so the
        // cursor resumes at the row after this page and never re-reads the
        // whole-second tie group that ends it. A short page returns null,
        // mirroring cursor mode.
        let next_cursor = if payments.len() == limit as usize {
            payments.last().map(|p| encode_cursor(&p.created_at, &p.id))
        } else {
            None
        };

        Ok(Json(json!({
            "payments": payments.iter().map(to_json).collect::<Vec<_>>(),
            "total": total,
            "limit": limit,
            "offset": offset,
            "next_cursor": next_cursor,
        })))
    }
}

fn encode_cursor(ts: &str, id: &str) -> String {
    hex::encode(format!("{ts}\t{id}"))
}

fn decode_cursor(raw: &str) -> Option<(String, String)> {
    let bytes = hex::decode(raw).ok()?;
    let s = String::from_utf8(bytes).ok()?;
    let (ts, id) = s.split_once('\t')?;
    Some((ts.to_string(), id.to_string()))
}

/// Generates an 8-character uppercase-hex `text` memo (32 bits of entropy,
/// well within Stellar's 28-byte text memo limit) and confirms it hasn't been
/// used by *any* payment intent before — `memo_exists` checks the entire
/// `payments` table, not just pending ones, so a memo is never reused for the
/// lifetime of the database. That makes the collision probability for a
/// single call simply `rows-in-table / 2^32`, and the loop retries up to 10
/// times before giving up; exhausting that before billions of payments exist
/// is effectively impossible. If traffic ever approaches that scale, widen
/// the memo (more hex chars, still under the 28-byte limit) rather than
/// switching scheme.
///
/// We chose a `text` memo over `memo_id` (a u64) or `memo_hash`/`memo_return`
/// (32-byte) because it's the simplest scheme that round-trips a
/// human-legible reference through Horizon. The tradeoff: Horizon's JSON
/// `memo` field also holds a string for those other memo types (a decimal
/// string for `memo_id`, base64 for `memo_hash`/`memo_return`), and a
/// `memo_id` consisting only of digits could coincidentally render as the
/// same text as one of our hex memos. `horizon::HorizonPayment::memo()`
/// guards against this by only matching when Horizon reports `memo_type:
/// "text"` (see issue #17).
async fn generate_unique_memo(pool: &db::Db) -> Result<String, AppError> {
    for _ in 0..10 {
        let memo = Uuid::new_v4().to_string().replace('-', "")[..8].to_uppercase();
        if !db::memo_exists(pool, &memo).await? {
            return Ok(memo);
        }
    }
    Err(AppError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal_error",
        "memo generation failed",
    ))
}

fn to_json(p: &db::Payment) -> Value {
    // Canonicalize amount: parse to stroops and format back to canonical form.
    // This ensures "10.00", "10.0", and "10" all serialize identically,
    // eliminating spurious string-based comparisons across responses.
    let canonical_amount = crate::money::parse_stroops(&p.amount)
        .map(crate::money::stroops_to_string)
        .unwrap_or_else(|| p.amount.clone());

    // Canonicalize paid_amount the same way (defensive; it should already be
    // canonical from horizon.rs, but this ensures consistency across all
    // serialization paths).
    let canonical_paid_amount = p
        .paid_amount
        .as_ref()
        .and_then(|pa| crate::money::parse_stroops(pa).map(crate::money::stroops_to_string));

    json!({
        "id": p.id,
        "merchant_id": p.merchant_id,
        "destination_address": p.destination_address,
        "memo": p.memo,
        "amount": canonical_amount,
        "asset": p.asset,
        "asset_issuer": p.asset_issuer,
        "status": p.status,
        "tx_hash": p.tx_hash,
        "paid_amount": canonical_paid_amount,
        "created_at": p.created_at,
        "updated_at": p.updated_at,
        "expires_at": p.expires_at,
    })
}

pub async fn list_webhooks(
    State(state): State<Arc<AppState>>,
    Extension(AuthenticatedMerchant(merchant_id)): Extension<AuthenticatedMerchant>,
    Path(payment_id): Path<String>,
) -> Result<Json<Value>, AppError> {
    // Verify payment exists and belongs to the caller. A payment owned by
    // another merchant reports the same 404 as a missing one, so this can't
    // be used to enumerate which payment ids exist for other tenants.
    let payment = db::get_payment(&state.pool, &payment_id)
        .await?
        .filter(|p| p.merchant_id == merchant_id)
        .ok_or_else(|| {
            AppError::new(
                StatusCode::NOT_FOUND,
                "payment_not_found",
                "payment not found",
            )
        })?;

    let deliveries = db::list_webhook_deliveries(&state.pool, &payment_id).await?;

    Ok(Json(json!({
        "payment_id": payment.id,
        "deliveries": deliveries.iter().map(|d| json!({
            "id": d.id,
            "url": d.url,
            "event": d.event(),
            "status": d.status,
            "attempts": d.attempts,
            "last_attempt": d.last_attempt,
            "created_at": d.created_at,
        })).collect::<Vec<_>>(),
    })))
}

pub async fn redeliver_webhook(
    State(state): State<Arc<AppState>>,
    Extension(AuthenticatedMerchant(merchant_id)): Extension<AuthenticatedMerchant>,
    Path((payment_id, delivery_id)): Path<(String, String)>,
) -> Result<StatusCode, AppError> {
    // Verify payment exists and belongs to the caller. A payment owned by
    // another merchant reports the same 404 as a missing one.
    db::get_payment(&state.pool, &payment_id)
        .await?
        .filter(|p| p.merchant_id == merchant_id)
        .ok_or_else(|| {
            AppError::new(
                StatusCode::NOT_FOUND,
                "payment_not_found",
                "payment not found",
            )
        })?;

    // Get the delivery
    let delivery = db::get_webhook_delivery(&state.pool, &delivery_id)
        .await?
        .ok_or_else(|| {
            AppError::new(
                StatusCode::NOT_FOUND,
                "delivery_not_found",
                "delivery not found",
            )
        })?;

    // Verify the delivery belongs to this payment
    if delivery.payment_id != payment_id {
        return Err(AppError::new(
            StatusCode::NOT_FOUND,
            "delivery_not_found",
            "delivery not found",
        ));
    }

    // Re-validate the target on every redelivery — the delivery row may be old,
    // so a stale-but-once-valid URL must not become a standing SSRF pivot.
    let client = crate::webhook::safe_client(&state, &delivery.url)
        .await
        .map_err(|_| {
            AppError::new(
                StatusCode::BAD_REQUEST,
                "webhook_target_blocked",
                "webhook target is not allowed",
            )
        })?;

    /* Re-send the original payload, re-signed with a fresh timestamp so the
    receiver's replay-tolerance window is measured from this redelivery. The
    event header comes from the stored delivery rather than being hard-coded:
    a receiver that routes on `X-StellarGate-Event` must see the same event the
    body carries, whether that was completed, overpaid, underpaid, or expired. */
    let payload_bytes = delivery.payload.as_bytes();
    let timestamp = crate::webhook::current_timestamp();
    let signature = crate::webhook::sign(&state.config.webhook_secret, timestamp, payload_bytes);
    let event = delivery.event();

    let result = client
        .post(&delivery.url)
        .header("Content-Type", "application/json")
        .header("X-StellarGate-Signature", &signature)
        .header("X-StellarGate-Timestamp", timestamp.to_string())
        // Convenience header — mirrors the `event` field already present in
        // the signed body. NOT covered by the HMAC; receivers must route on
        // the authenticated body field, not this header.
        .header("X-StellarGate-Event", &event)
        .body(delivery.payload.clone())
        .send()
        .await;

    let new_status = match result {
        Ok(resp) if resp.status().is_success() => "delivered",
        _ => "failed",
    };

    db::update_webhook_delivery(&state.pool, &delivery_id, new_status, delivery.attempts + 1)
        .await?;

    if new_status == "delivered" {
        Ok(StatusCode::OK)
    } else {
        Err(AppError::new(
            StatusCode::BAD_GATEWAY,
            "webhook_delivery_failed",
            "webhook delivery failed",
        ))
    }
}
