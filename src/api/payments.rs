use crate::{api::AuthenticatedMerchant, db, money, AppState};
use axum::{
    async_trait,
    extract::{ConnectInfo, Extension, FromRequest, FromRequestParts, Path, Query, Request, State},
    http::{request::Parts, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::sync::Arc;
use uuid::Uuid;

/// An error with an HTTP status, a stable machine-readable code, and a human message.
pub struct AppError {
    status: StatusCode,
    code: &'static str,
    message: String,
    retry_after_secs: Option<u64>,
}

impl AppError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            retry_after_secs: None,
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

    pub fn service_unavailable(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::SERVICE_UNAVAILABLE, code, message)
    }

    pub fn conflict(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, code, message)
    }

    /// Attaches a `Retry-After` header, in delta-seconds, to the response.
    pub fn with_retry_after(mut self, secs: u64) -> Self {
        self.retry_after_secs = Some(secs);
        self
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let mut response = (
            self.status,
            Json(json!({ "error": self.message, "code": self.code })),
        )
            .into_response();
        if let Some(secs) = self.retry_after_secs {
            if let Ok(value) = axum::http::HeaderValue::from_str(&secs.to_string()) {
                response
                    .headers_mut()
                    .insert(axum::http::header::RETRY_AFTER, value);
            }
        }
        response
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

/// Stable machine-readable code for a `JsonRejection` variant not given its
/// own dedicated code, keyed on the HTTP status axum already chose for it —
/// `413` for an oversized body (issue #257), `400` for everything else this
/// catch-all can still see.
fn rejection_code(status: StatusCode) -> &'static str {
    match status {
        StatusCode::PAYLOAD_TOO_LARGE => "payload_too_large",
        _ => "invalid_request",
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
                    // `JsonRejection` is `#[non_exhaustive]`, so a catch-all is
                    // required — but it must preserve the rejection's own status
                    // and reason rather than flattening everything into a
                    // generic 400. This is where `BytesRejection` lands,
                    // covering an oversized body (`RequestBodyLimitLayer`) and a
                    // truncated/aborted one: telling a client its JSON was
                    // malformed when the real problem was the body's size or a
                    // dropped connection sends it chasing the wrong fix (issue
                    // #257).
                    other => {
                        tracing::debug!(rejection = %other, "unhandled JSON rejection");
                        Err(AppError::new(
                            other.status(),
                            rejection_code(other.status()),
                            other.body_text(),
                        ))
                    }
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

/// A drop-in replacement for `Query<T>` that maps a query-string failure into
/// our standard `{"code": "...", "error": "..."}` 400 instead of axum's
/// plaintext rejection.
///
/// Paired with `#[serde(deny_unknown_fields)]` on the target struct, this is
/// what makes an unrecognised *parameter* a first-class error: serde raises
/// "unknown field `stauts`, expected one of ..." and this turns it into
/// `400 unknown_parameter` with that message intact, so the response names the
/// offending key and lists the accepted ones.
pub struct QueryParams<T>(pub T);

#[async_trait]
impl<T, S> FromRequestParts<S> for QueryParams<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = AppError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        match Query::<T>::from_request_parts(parts, state).await {
            Ok(Query(value)) => Ok(QueryParams(value)),
            Err(rejection) => {
                let detail = rejection.body_text();
                /* An unrecognised parameter is its own failure mode, not a
                generic deserialization error: it almost always means a typo or
                a client written against an older spec, and the fix is
                different from "the value had the wrong type". This mirrors the
                `unknown_field` split `JsonBody` already makes for bodies. */
                if detail.contains("unknown field") {
                    Err(AppError::bad_request(
                        "unknown_parameter",
                        format!("invalid query string: {detail}"),
                    ))
                } else {
                    Err(AppError::bad_request(
                        "invalid_query",
                        format!("invalid query string: {detail}"),
                    ))
                }
            }
        }
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
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
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

    /* A trustline confirmed missing means any intent minted here will bounce
    on-chain — reject at creation rather than let the customer pay into a
    black hole (issue: report_trustlines only ran once, at boot). Native XLM
    never needs a trustline, so it's exempt. An asset never yet checked, or
    whose last check errored contacting Horizon, is NOT treated as missing
    here — `is_missing` returns `None` for either, and a Horizon outage must
    not fail every payment creation on top of already failing the check. */
    if asset_issuer.is_some()
        && state.trustline_metrics.is_missing(&accepted_asset.code) == Some(true)
    {
        return Err(AppError::service_unavailable(
            "trustline_missing",
            format!(
                "the gateway account currently has no trustline for {}; payments in this \
                 asset cannot be received until one is established",
                accepted_asset.code
            ),
        ));
    }

    let stroops = money::parse_stroops(&body.amount).ok_or_else(|| {
        AppError::bad_request(
            "invalid_amount",
            "amount must be a positive number with at most 7 decimal places",
        )
    })?;
    /* The overflow bound `parse_stroops` already enforces is an implementation
    artifact (roughly 922 billion units of any asset), not a business rule —
    an intent above it is unpayable and misleadingly reported as malformed.
    These configured bounds are optional and asset-specific (issue #310). */
    if let Some(max) = state
        .config
        .max_payment_amount
        .for_asset(&accepted_asset.code)
    {
        if stroops > max {
            return Err(AppError::bad_request(
                "amount_out_of_range",
                format!(
                    "amount exceeds the configured maximum of {} {} for this asset",
                    money::stroops_to_string(max),
                    accepted_asset.code
                ),
            ));
        }
    }
    if let Some(min) = state
        .config
        .min_payment_amount
        .for_asset(&accepted_asset.code)
    {
        if stroops < min {
            return Err(AppError::bad_request(
                "amount_out_of_range",
                format!(
                    "amount is below the configured minimum of {} {} for this asset",
                    money::stroops_to_string(min),
                    accepted_asset.code
                ),
            ));
        }
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
            /* Lost the race — the winner is about to create its payment,
            typically within a few ms (a single local INSERT). Give it a
            short, bounded chance to land before telling the client to come
            back: a busy-wait that holds a connection and a pool slot is not
            an acceptable price for closing a race window this narrow. */
            const IDEMPOTENCY_POLL_ATTEMPTS: u32 = 5;
            const IDEMPOTENCY_POLL_INTERVAL: std::time::Duration =
                std::time::Duration::from_millis(10);

            for _ in 0..IDEMPOTENCY_POLL_ATTEMPTS {
                if let Some(payment) = db::get_payment(&state.pool, &canonical_id).await? {
                    return Ok((StatusCode::OK, Json(to_json(&payment))));
                }
                tokio::time::sleep(IDEMPOTENCY_POLL_INTERVAL).await;
            }
            /* Not a server error — the winner just hasn't committed yet.
            409 tells the client this is a transient, retryable conflict
            rather than an outage; Retry-After gives it a cheap, concrete
            backoff instead of a busy-wait on our side. */
            return Err(AppError::conflict(
                "idempotency_conflict",
                "concurrent request conflict, please retry",
            )
            .with_retry_after(1));
        }
    }

    // Retry loop to handle UNIQUE constraint violations on memo generation.
    // Each iteration generates a fresh memo and attempts to create the payment.
    // If the memo collides (concurrent request claimed the same memo), we retry
    // with a new memo. Any other error is returned immediately.
    let payment = 'retry: {
        for _ in 0..10 {
            let memo = generate_unique_memo();
            match db::create_payment(
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
            .await
            {
                Ok(p) => break 'retry p,
                Err(err) if is_unique_violation(&err) => continue,
                Err(err) => return Err(err.into()),
            }
        }
        // Exhausted retries after UNIQUE constraint violations
        return Err(AppError::new(
            StatusCode::CONFLICT,
            "memo_collision_exhausted",
            "unable to generate a unique memo after multiple retries",
        ));
    };
    state.payment_metrics.record_created();

    /* A merchant disputing a charge, or investigating a burst of unexpected
    intents, starts from "which merchant created this and from where" — which
    previously had no answer in the logs at all (issue #305). `audit = true`
    lets this be routed to a separate sink from ordinary operational logs;
    see the "Audit events" section of the README for the full field schema. */
    let source_ip = crate::api::client_ip_key_from_parts(
        Some(peer),
        &headers,
        &state.config.trusted_proxy_cidrs,
    );
    tracing::info!(
        audit = true,
        action = "payment.create",
        actor = "merchant",
        outcome = "created",
        %merchant_id,
        payment_id = %payment.id,
        amount = %payment.amount,
        asset = %payment.asset,
        source_ip = %source_ip,
        request_id = %crate::api::request_id(&headers),
        "payment created"
    );

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

/// Query parameters for the payments listing.
///
/// `deny_unknown_fields` because a discarded parameter is a silently
/// unfiltered page (issue #352). `?stauts=completed` — one transposed
/// character — used to return `200 OK` listing *every* payment including
/// pending ones, so a merchant reconciliation script that filters server-side
/// and trusts the result would read unpaid intents as paid.
///
/// It also keeps the parameter set evolvable: while unknown parameters were
/// ignored, adding a real `page` later would change the behaviour of requests
/// that appeared to work before.
///
/// This matches how the rest of the API already treats input it does not
/// understand: `status` is checked against an allow-list, an undecodable
/// `cursor` is `400 invalid_cursor`, an unrecognised body field is `400
/// unknown_field`. Strictness previously stopped at parameter names.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListQuery {
    pub status: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub cursor: Option<String>,
    /// Opt in to a `total` field on the offset-paginated response. Defaults to
    /// omitted: SQLite has no cached row count, so computing it is a full
    /// `COUNT(*)` scan over every matching row, on every request — including
    /// the first page — for a field most callers never read (issue #320).
    /// Has no effect in cursor (keyset) mode, which has never returned `total`.
    pub include_total: Option<bool>,
}

/// Offset pagination is `O(offset)` in SQLite — it produces and discards
/// every skipped row. This ceiling (generous for any real UI) keeps a deep,
/// expensive scan-and-skip from being answered at all; the keyset (`cursor`)
/// path stays `O(log n)` regardless of depth (issue #303).
const MAX_OFFSET: i64 = 10_000;
/// Statuses a payment can actually hold, and therefore the only ones worth
/// filtering on: `pending` at creation, `completed`/`underpaid` from
/// settlement (`horizon::settle`), and `expired` from the TTL sweeper
/// (`db::expire_overdue`). Nothing writes any other value, so anything else is
/// a guaranteed-empty filter and is rejected as invalid.
const VALID_STATUSES: [&str; 4] = ["pending", "completed", "underpaid", "expired"];

/// Validates a caller-supplied `limit` against `(1..=max)`, rather than
/// silently clamping it. Clamping absorbs three distinct bad inputs — too
/// large, zero, negative — into a `200` that gives the caller no signal
/// anything was wrong; a client paginating past `max` would read the
/// silently-shortened page as "end of results" and stop early (issue #258).
/// Matches the existing `invalid_status`/`invalid_cursor` convention: reject
/// rather than coerce.
pub fn validate_limit(limit: Option<i64>, default: i64, max: i64) -> Result<i64, AppError> {
    match limit {
        None => Ok(default),
        Some(n) if (1..=max).contains(&n) => Ok(n),
        Some(n) => Err(AppError::bad_request(
            "invalid_limit",
            format!("limit must be between 1 and {max} (got {n})"),
        )),
    }
}

/// Statuses a webhook delivery can hold: `pending` while attempts are still
/// possible, `delivered` on success, `failed` when the attempt budget is
/// exhausted. Nothing writes any other value, so a filter on anything else is
/// a guaranteed-empty query and is rejected as invalid.
const VALID_DELIVERY_STATUSES: [&str; 3] = ["pending", "delivered", "failed"];

pub async fn list(
    State(state): State<Arc<AppState>>,
    Extension(AuthenticatedMerchant(merchant_id)): Extension<AuthenticatedMerchant>,
    QueryParams(q): QueryParams<ListQuery>,
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

    /* A client migrating from offset to keyset pagination naturally sends both
    for a request or two — the offset branch hands out a `next_cursor` to invite
    exactly that. Answering from the cursor branch would discard `offset` with
    no signal, and return a differently shaped body while it's at it, so refuse
    rather than guess which mode was meant (issue #259). Presence, not value: an
    explicit `offset=0` alongside a cursor is still a caller asserting a
    pagination mode. */
    if q.cursor.is_some() && q.offset.is_some() {
        return Err(AppError::bad_request(
            "conflicting_pagination",
            "cursor and offset are mutually exclusive; use cursor for keyset pagination",
        ));
    }

    let limit = validate_limit(
        q.limit,
        state.config.pagination_default_limit,
        state.config.pagination_max_limit,
    )?;

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
        if offset > MAX_OFFSET {
            return Err(AppError::bad_request(
                "invalid_offset",
                format!(
                    "offset exceeds maximum of {MAX_OFFSET}; use cursor pagination instead \
                     (see the `cursor` parameter and `next_cursor` in the response)"
                ),
            ));
        }
        let payments = db::list_payments(
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

        let mut body = json!({
            "payments": payments.iter().map(to_json).collect::<Vec<_>>(),
            "limit": limit,
            "offset": offset,
            "next_cursor": next_cursor,
        });

        // `total` costs a full COUNT(*) scan (issue #320) — computed only when
        // asked for, and entirely absent from the response otherwise rather
        // than sent as null, so a caller can tell "not computed" from "zero".
        if q.include_total == Some(true) {
            let total = db::count_payments(&state.pool, &merchant_id, q.status.as_deref()).await?;
            body["total"] = json!(total);
        }

        Ok(Json(body))
    }
}

pub fn encode_cursor(ts: &str, id: &str) -> String {
    hex::encode(format!("{ts}\t{id}"))
}

/// A legitimate cursor is the hex encoding of `"{rfc3339}\t{uuid}"` — 57
/// bytes, so 114 hex characters. This ceiling is deliberately generous, not
/// tight, so it rejects only what could not possibly be a real cursor.
const MAX_CURSOR_HEX_LEN: usize = 256;

pub fn decode_cursor(raw: &str) -> Option<(String, String)> {
    // Cheap rejections first: an oversized or non-hex string is rejected
    // before it is ever allocated or decoded (issue #304).
    if raw.len() > MAX_CURSOR_HEX_LEN || !raw.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let s = String::from_utf8(hex::decode(raw).ok()?).ok()?;
    let (ts, id) = s.split_once('\t')?;
    /* Both halves have known shapes — validate rather than trusting them.
    `ts` is always `strftime('%Y-%m-%dT%H:%M:%SZ', ...)`, exactly 20 bytes.
    `id` is a payment or webhook-delivery primary key: always non-empty and,
    in practice, a UUID — but this helper backs cursors over both tables, and
    nothing schema-enforces UUID shape on either, so a bounded length check is
    the strongest assumption that holds for every caller. */
    if ts.len() != 20 || id.is_empty() || id.len() > 64 {
        return None;
    }
    Some((ts.to_string(), id.to_string()))
}

/// Generates an 8-character uppercase-hex `text` memo (32 bits of entropy,
/// well within Stellar's 28-byte text memo limit). Unlike a pre-check approach,
/// this function does not verify uniqueness — the database UNIQUE constraint
/// on the memo column is relied upon to enforce it.
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
fn generate_unique_memo() -> String {
    Uuid::new_v4().to_string().replace('-', "")[..8].to_uppercase()
}

/// Checks if an error is a UNIQUE constraint violation.
fn is_unique_violation(err: &anyhow::Error) -> bool {
    if let Some(sqlx::Error::Database(db_error)) = err.downcast_ref::<sqlx::Error>() {
        // Check for UNIQUE constraint violation
        // SQLite error message format: "UNIQUE constraint failed: table.column"
        let msg = db_error.message();
        return msg.contains("UNIQUE constraint failed");
    }
    false
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

/// Query parameters for the webhook-delivery listing. Matches the payments
/// listing conventions: a `status` filter, a `limit` (default 20, max 100),
/// and an opaque keyset `cursor` whose value comes from a previous response's
/// `next_cursor`. `deny_unknown_fields` for the same reason as [`ListQuery`]:
/// a mistyped parameter must not read as an applied filter (issue #352).
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListDeliveryQuery {
    pub status: Option<String>,
    pub limit: Option<i64>,
    pub cursor: Option<String>,
}

pub async fn list_webhooks(
    State(state): State<Arc<AppState>>,
    Extension(AuthenticatedMerchant(merchant_id)): Extension<AuthenticatedMerchant>,
    Path(payment_id): Path<String>,
    QueryParams(q): QueryParams<ListDeliveryQuery>,
) -> Result<Json<Value>, AppError> {
    if let Some(s) = &q.status {
        if !VALID_DELIVERY_STATUSES.contains(&s.as_str()) {
            return Err(AppError::bad_request(
                "invalid_status",
                format!(
                    "invalid status '{}'; valid: {}",
                    s,
                    VALID_DELIVERY_STATUSES.join(", ")
                ),
            ));
        }
    }

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

    let limit = validate_limit(
        q.limit,
        state.config.pagination_default_limit,
        state.config.pagination_max_limit,
    )?;

    let cursor = match q.cursor.as_deref() {
        Some(raw_cursor) => {
            let (ts, id) = decode_cursor(raw_cursor)
                .ok_or_else(|| AppError::bad_request("invalid_cursor", "invalid cursor"))?;
            Some((ts, id))
        }
        None => None,
    };

    let deliveries = db::list_webhook_deliveries_keyset(
        &state.pool,
        &payment_id,
        q.status.as_deref(),
        limit,
        cursor.as_ref().map(|(ts, id)| (ts.as_str(), id.as_str())),
    )
    .await?;

    // A full page mints a cursor from its last row (created_at, id); a short
    // page means we've reached the end and null signals "no more".
    let next_cursor = if deliveries.len() == limit as usize {
        deliveries
            .last()
            .map(|d| encode_cursor(&d.created_at, &d.id))
    } else {
        None
    };

    Ok(Json(json!({
        "payment_id": payment.id,
        "deliveries": deliveries.iter().map(|d| json!({
            "id": d.id,
            "url": d.url,
            "event": d.event(),
            "status": d.status,
            "attempts": d.attempts,
            "manual_attempts": d.manual_attempts,
            "last_attempt": d.last_attempt,
            "created_at": d.created_at,
        })).collect::<Vec<_>>(),
        "limit": limit,
        "next_cursor": next_cursor,
    })))
}

// ── Dead-letter view (issue #319) ────────────────────────────────────────────

/// `deny_unknown_fields` for the same reason as [`ListQuery`]: a mistyped
/// parameter must not read as an applied filter (issue #352). It matters more
/// here than elsewhere, because `status` defaults to `failed` when absent — so
/// a typo'd `?staus=pending` would answer the dead-letter question with the
/// dead-letter list and look entirely plausible.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListDeliveriesQuery {
    /// Defaults to `failed` — the dead-letter case this endpoint exists for.
    pub status: Option<String>,
    pub limit: Option<i64>,
    pub cursor: Option<String>,
}

/// `GET /payments/webhooks` — a merchant's deliveries across *all* their
/// payments, defaulting to the failed ones.
///
/// Before this, a permanently-failed delivery was reachable only by knowing the
/// payment id and calling `GET /payments/:id/webhooks`. That is backwards: the
/// reason to go looking is almost always "a merchant says they are missing
/// events", and a payment id is precisely what the person asking does not have.
/// Answering it meant querying SQLite directly on the production volume, and a
/// merchant could not self-serve at all.
pub async fn list_merchant_webhooks(
    State(state): State<Arc<AppState>>,
    Extension(AuthenticatedMerchant(merchant_id)): Extension<AuthenticatedMerchant>,
    QueryParams(q): QueryParams<ListDeliveriesQuery>,
) -> Result<Json<Value>, AppError> {
    let status = q.status.as_deref().unwrap_or("failed");
    if !VALID_DELIVERY_STATUSES.contains(&status) {
        return Err(AppError::bad_request(
            "invalid_status",
            format!(
                "invalid delivery status '{}'; valid: {}",
                status,
                VALID_DELIVERY_STATUSES.join(", ")
            ),
        ));
    }

    let limit = validate_limit(
        q.limit,
        state.config.pagination_default_limit,
        state.config.pagination_max_limit,
    )?;

    let cursor = match &q.cursor {
        Some(raw) => Some(
            decode_cursor(raw)
                .ok_or_else(|| AppError::bad_request("invalid_cursor", "invalid cursor"))?,
        ),
        None => None,
    };

    let deliveries = db::list_deliveries_for_merchant(
        &state.pool,
        &merchant_id,
        status,
        limit,
        cursor.as_ref().map(|(ts, id)| (ts.as_str(), id.as_str())),
    )
    .await?;

    let next_cursor = if deliveries.len() == limit as usize {
        deliveries
            .last()
            .map(|d| encode_cursor(&d.created_at, &d.id))
    } else {
        None
    };

    Ok(Json(json!({
        "deliveries": deliveries.iter().map(delivery_to_json).collect::<Vec<_>>(),
        "status": status,
        "limit": limit,
        "next_cursor": next_cursor,
    })))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedeliverBulkRequest {
    /// Specific deliveries to requeue. Omit (or send an empty list) to requeue
    /// every failed delivery this merchant has.
    pub delivery_ids: Option<Vec<String>>,
}

/// Cap on explicitly-listed ids per request, so the generated `IN (...)` stays
/// a sane size. Requeueing *everything* is unaffected — it needs no id list.
const MAX_BULK_DELIVERY_IDS: usize = 100;

/// `POST /payments/webhooks/redeliver` — bulk recovery after a merchant has
/// fixed their endpoint.
///
/// This endpoint sends nothing itself. It resets matching failed rows to
/// `pending` with `attempts = 0` and hands them to the redrive worker, whose
/// `WEBHOOK_REDRIVE_CONCURRENCY` and exponential backoff already bound the
/// outbound rate. So requeueing ten thousand deliveries costs one `UPDATE`,
/// cannot exhaust the redrive budget, and cannot stampede a receiver that has
/// only just come back up (coordinating with issue #235).
///
/// Requeueing also acknowledges: somebody has now acted on these failures, so
/// they stop being exempt from retention.
pub async fn redeliver_webhooks_bulk(
    State(state): State<Arc<AppState>>,
    Extension(AuthenticatedMerchant(merchant_id)): Extension<AuthenticatedMerchant>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OptionalJsonBody(body): OptionalJsonBody<RedeliverBulkRequest>,
) -> Result<Json<Value>, AppError> {
    let ids = body.and_then(|b| b.delivery_ids).unwrap_or_default();
    if ids.len() > MAX_BULK_DELIVERY_IDS {
        return Err(AppError::bad_request(
            "too_many_delivery_ids",
            format!(
                "at most {MAX_BULK_DELIVERY_IDS} delivery_ids per request; \
                 omit the field to requeue every failed delivery"
            ),
        ));
    }

    let requeued = db::requeue_failed_deliveries(&state.pool, &merchant_id, &ids).await?;
    let source_ip = crate::api::client_ip_key_from_parts(
        Some(peer),
        &headers,
        &state.config.trusted_proxy_cidrs,
    );
    tracing::info!(
        audit = true,
        action = "webhook.redeliver_bulk",
        actor = "merchant",
        outcome = "requeued",
        %merchant_id,
        requeued,
        source_ip = %source_ip,
        request_id = %crate::api::request_id(&headers),
        "failed webhook deliveries requeued for redrive"
    );

    Ok(Json(json!({
        "requeued": requeued,
        "detail": "requeued deliveries are retried by the background redrive \
                   worker, subject to its concurrency limit and backoff",
    })))
}

/// One delivery as the API exposes it. The stored `payload` is deliberately
/// omitted — it is the signed event body, it can be large, and a listing is for
/// triage rather than replay.
fn delivery_to_json(d: &db::WebhookDelivery) -> Value {
    json!({
        "id": d.id,
        "payment_id": d.payment_id,
        "url": d.url,
        "event": d.event(),
        "status": d.status,
        "attempts": d.attempts,
        "manual_attempts": d.manual_attempts,
        "last_attempt": d.last_attempt,
        "acknowledged_at": d.acknowledged_at,
        "created_at": d.created_at,
    })
}

pub async fn redeliver_webhook(
    State(state): State<Arc<AppState>>,
    Extension(AuthenticatedMerchant(merchant_id)): Extension<AuthenticatedMerchant>,
    Path((payment_id, delivery_id)): Path<(String, String)>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
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

    /* Manual redelivery must not share the automatic redrive budget or refresh
    `last_attempt` — otherwise a merchant clicking "resend" can exhaust
    `WEBHOOK_REDRIVE_MAX_ATTEMPTS` and permanently disable background recovery
    (issue #235). */
    db::record_manual_redelivery(&state.pool, &delivery_id, new_status).await?;

    /* A burst of redeliveries previously had no attributable origin in the
    logs at all (issue #305). Logged regardless of outcome — `outcome` here
    is the delivery result, not whether the redelivery *request* succeeded;
    the request itself always reached this point. */
    let source_ip = crate::api::client_ip_key_from_parts(
        Some(peer),
        &headers,
        &state.config.trusted_proxy_cidrs,
    );
    tracing::info!(
        audit = true,
        action = "webhook.redeliver",
        actor = "merchant",
        outcome = %new_status,
        %merchant_id,
        payment_id = %payment_id,
        delivery_id = %delivery_id,
        source_ip = %source_ip,
        request_id = %crate::api::request_id(&headers),
        "webhook redelivery triggered"
    );

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

#[cfg(test)]
mod cursor_tests {
    use super::*;

    /// A cursor minted by `encode_cursor` must always decode back to the same
    /// `(timestamp, id)` pair (issue #304).
    #[test]
    fn decode_cursor_round_trips_encode_cursor() {
        let ts = "2026-01-01T00:00:00Z";
        let id = Uuid::new_v4().to_string();
        let cursor = encode_cursor(ts, &id);
        assert_eq!(decode_cursor(&cursor), Some((ts.to_string(), id)));
    }

    #[test]
    fn decode_cursor_rejects_oversized_input() {
        let huge = "a".repeat(MAX_CURSOR_HEX_LEN + 1);
        assert_eq!(decode_cursor(&huge), None);
    }

    #[test]
    fn decode_cursor_rejects_non_hex_input() {
        assert_eq!(decode_cursor("not-valid-hex!!"), None);
    }

    #[test]
    fn decode_cursor_rejects_malformed_timestamp() {
        // Valid hex, valid UTF-8, valid UUID — but the timestamp half is the
        // wrong length.
        let id = Uuid::new_v4().to_string();
        let cursor = encode_cursor("2026-01-01", &id);
        assert_eq!(decode_cursor(&cursor), None);
    }

    #[test]
    fn decode_cursor_accepts_non_uuid_ids() {
        // Webhook-delivery cursors share this helper, and delivery ids are
        // not schema-enforced to be UUIDs (test fixtures use short ids like
        // "delivery-1") — only a bounded length is required.
        let cursor = encode_cursor("2026-01-01T00:00:00Z", "delivery-1");
        assert_eq!(
            decode_cursor(&cursor),
            Some(("2026-01-01T00:00:00Z".to_string(), "delivery-1".to_string()))
        );
    }

    #[test]
    fn decode_cursor_rejects_empty_id() {
        let cursor = encode_cursor("2026-01-01T00:00:00Z", "");
        assert_eq!(decode_cursor(&cursor), None);
    }

    #[test]
    fn decode_cursor_rejects_oversized_id() {
        let cursor = encode_cursor("2026-01-01T00:00:00Z", &"x".repeat(65));
        assert_eq!(decode_cursor(&cursor), None);
    }

    #[test]
    fn decode_cursor_rejects_missing_separator() {
        let cursor = hex::encode("no-tab-in-here");
        assert_eq!(decode_cursor(&cursor), None);
    }
}
