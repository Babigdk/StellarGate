use crate::api::payments::{AppError, JsonBody, OptionalJsonBody};
use crate::{db, AppState};
use axum::{
    extract::{ConnectInfo, Path, Request, State},
    http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::IntoResponse,
    routing::{get, post},
    Json,
};
use governor::clock::{Clock, DefaultClock};
use governor::middleware::StateInformationMiddleware;
use ipnet::IpNet;
use moka::sync::Cache;
use serde_json::{json, Value};
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use tower_http::{
    cors::CorsLayer,
    limit::RequestBodyLimitLayer,
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, RequestId, SetRequestIdLayer},
    timeout::TimeoutLayer,
    trace::TraceLayer,
};

mod payments;

/// Reject request bodies larger than this (256 KiB) before they hit a handler.
const MAX_BODY_BYTES: usize = 256 * 1024;

/// The authenticated merchant ID injected by the auth middleware.
#[derive(Clone)]
pub struct AuthenticatedMerchant(pub String);

/// Maximum number of distinct IP+bucket keys tracked at once.
/// Once this is reached, moka evicts the least-recently-used entry,
/// bounding resident memory regardless of key cardinality.
const RATE_LIMITER_MAX_KEYS: u64 = 10_000;

/// How long a per-key limiter is retained after its last access.
/// Keys for IPs that go quiet are automatically reclaimed.
const RATE_LIMITER_IDLE_TTL: Duration = Duration::from_secs(60);

#[derive(Clone)]
struct RateLimitState {
    requests_per_sec: u32,
    /// Trusted proxies from `TRUSTED_PROXY_CIDRS`. Forwarding headers
    /// (`X-Forwarded-For` / `X-Real-IP`) are honoured only when the socket
    /// peer is one of these; every other peer is bucketed by its own address
    /// and its headers are ignored (issue #330). Empty means no proxy is
    /// trusted and the headers are always ignored.
    trusted_proxies: Vec<IpNet>,
    /// Bounded, TTL-evicting cache of per-(bucket, IP) rate limiters.
    ///
    /// Replaces the previous `Mutex<HashMap<...>>`:
    /// - Capacity is capped at `RATE_LIMITER_MAX_KEYS` entries (moka evicts
    ///   via a W-TinyLFU policy when the cap is hit).
    /// - Each entry expires `RATE_LIMITER_IDLE_TTL` after its last access,
    ///   so limiter state for quiet IPs is automatically reclaimed.
    /// - moka uses internal sharding, eliminating the single global lock that
    ///   the old `Mutex` imposed.
    ///
    /// Built `with_middleware::<StateInformationMiddleware>` so a successful
    /// `check()` returns a `StateSnapshot` rather than `()`. That snapshot is
    /// where `X-RateLimit-Remaining` comes from; without it the limiter can
    /// only answer "allowed or not", and a client has no way to pace itself
    /// before being throttled (issue #327).
    limiters: Cache<String, Arc<governor::DefaultDirectRateLimiter<StateInformationMiddleware>>>,
}

/// Rate-limit headers. Named here so the CORS `expose_headers` list and the
/// middleware cannot drift apart — a header a browser client can't read is the
/// same as one that was never sent.
const X_RATELIMIT_LIMIT: HeaderName = HeaderName::from_static("x-ratelimit-limit");
const X_RATELIMIT_REMAINING: HeaderName = HeaderName::from_static("x-ratelimit-remaining");
const X_RATELIMIT_RESET: HeaderName = HeaderName::from_static("x-ratelimit-reset");

impl RateLimitState {
    fn new(requests_per_sec: u32, trusted_proxies: Vec<IpNet>) -> Self {
        let limiters = Cache::builder()
            .max_capacity(RATE_LIMITER_MAX_KEYS)
            .time_to_idle(RATE_LIMITER_IDLE_TTL)
            .build();
        Self {
            requests_per_sec: requests_per_sec.max(1),
            trusted_proxies,
            limiters,
        }
    }
}

/// The default per-merchant quota is this multiple of the configured base
/// `rate_limit_requests_per_sec`, matching the outer IP limiter's own
/// "default" (read) bucket multiplier. A merchant's authenticated traffic is
/// a mix of writes and polling reads, so it needs at least that much room to
/// avoid throttling well-behaved integrators (issue: rate limiter keyed on
/// IP, not identity).
const MERCHANT_DEFAULT_QUOTA_MULTIPLIER: u32 = 5;

/// Per-merchant quota state for the authenticated rate limiter. Unlike
/// [`RateLimitState`], this is keyed on [`AuthenticatedMerchant`] alone —
/// independent of source IP — so a merchant's quota is a property of who
/// they are, not where they connect from.
#[derive(Clone)]
struct MerchantRateLimitState {
    pool: db::Db,
    /// Fallback quota for a merchant with no `rate_limit_per_sec` override.
    default_rps: u32,
    limiters: Cache<String, Arc<governor::DefaultDirectRateLimiter<StateInformationMiddleware>>>,
}

impl MerchantRateLimitState {
    fn new(pool: db::Db, default_rps: u32) -> Self {
        let limiters = Cache::builder()
            .max_capacity(RATE_LIMITER_MAX_KEYS)
            // A time-to-live (rather than only idle eviction) bounds how long
            // a busy merchant can keep serving a stale, cached quota after an
            // operator changes `rate_limit_per_sec` — a merchant that never
            // goes idle would otherwise never pick up the new value.
            .time_to_live(RATE_LIMITER_IDLE_TTL)
            .time_to_idle(RATE_LIMITER_IDLE_TTL)
            .build();
        Self {
            pool,
            default_rps: default_rps.max(1),
            limiters,
        }
    }
}

pub fn router(state: Arc<AppState>) -> axum::Router {
    let cors = build_cors(&state.config);
    let rate_limit = RateLimitState::new(
        state.config.rate_limit_requests_per_sec,
        state.config.trusted_proxy_cidrs.clone(),
    );
    /* Built once and shared (moka `Cache` clones point at the same underlying
    store) across both mounts of `api_v1` below. Built per-call instead, a
    merchant could double its effective quota by splitting traffic between
    `/payments` and `/v1/payments` — each mount would track it separately. */
    let merchant_rate_limit = MerchantRateLimitState::new(
        state.pool.clone(),
        state
            .config
            .rate_limit_requests_per_sec
            .saturating_mul(MERCHANT_DEFAULT_QUOTA_MULTIPLIER),
    );
    let request_timeout = Duration::from_secs(state.config.request_timeout_secs);

    axum::Router::new()
        .route("/", get(root))
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics_handler))
        /* Operator dashboard. The assets are static and carry no data, so they
        are served unauthenticated — every figure they display is fetched by
        the browser from the same authenticated endpoints a merchant would
        call directly, using an API key the operator supplies. */
        .route("/dashboard", get(dashboard_html))
        .route("/dashboard/app.css", get(dashboard_css))
        .route("/dashboard/app.js", get(dashboard_js))
        /* The versioned API surface, mounted twice.
        `/v1` is canonical. The same routes stay mounted unprefixed so every
        existing integrator keeps working — shipping versioning by breaking all
        current callers at once would be precisely the failure versioning
        exists to prevent (issue #121). Legacy responses carry `Deprecation`
        and `Link` headers pointing at their `/v1` equivalent, so a client can
        discover the move from a response it already parses. */
        .nest("/v1", api_v1(&state, merchant_rate_limit.clone()))
        .merge(api_v1(&state, merchant_rate_limit).layer(middleware::from_fn(mark_deprecated)))
        .fallback(not_found)
        .layer(PropagateRequestIdLayer::x_request_id())
        .layer(TraceLayer::new_for_http().make_span_with(|req: &Request<_>| {
            let request_id = req
                .extensions()
                .get::<RequestId>()
                .and_then(|id| id.header_value().to_str().ok())
                .unwrap_or("-");
            tracing::info_span!(
                "http",
                %request_id,
                method = %req.method(),
                uri = %req.uri(),
                version = ?req.version()
            )
        }))
        .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
        .layer(RequestBodyLimitLayer::new(MAX_BODY_BYTES))
        .layer(middleware::from_fn_with_state(
            rate_limit,
            rate_limit_middleware,
        ))
        .layer(cors)
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            request_timeout,
        ))
        .with_state(state)
}

/// The versioned API surface: everything that forms the public contract.
///
/// Operational endpoints (`/health`, `/ready`, `/metrics`, `/dashboard`, `/`)
/// are deliberately excluded. They are infrastructure rather than contract —
/// a probe URL that moved with every API revision would break liveness checks
/// and scrape configs for no benefit.
fn api_v1(
    state: &Arc<AppState>,
    merchant_rate_limit: MerchantRateLimitState,
) -> axum::Router<Arc<AppState>> {
    /* Merchant provisioning and API key lifecycle. All admin-gated behind
    ADMIN_PROVISIONING_SECRET: this service has no self-service signup, and
    minting or revoking a credential is an operator action. */
    let merchants = axum::Router::new()
        .route("/", post(provision_merchant))
        .route("/:id/keys", post(issue_api_key).get(list_api_keys))
        .route("/:id/keys/:key_id", axum::routing::delete(revoke_api_key))
        .route(
            "/:id/rate-limit",
            axum::routing::put(set_merchant_rate_limit),
        )
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_admin_secret,
        ));

    /* Auth middleware on the write + list routes, the webhook listing, and
    redelivery (it triggers a merchant-scoped outbound request). The
    per-payment status endpoint handles credentials itself, because it serves
    both authenticated and anonymous callers — see `payments::get_by_id`. */
    let payments_authed = axum::Router::new()
        .route("/", post(payments::create).get(payments::list))
        /* The dead-letter view: a merchant's deliveries across *all* their
        payments (issue #319). Declared before `/:id` and matched ahead of it —
        `webhooks` is a static segment, and matchit gives static segments
        priority over parameters, so this can never be shadowed by a payment
        whose id happens to be the literal string "webhooks". */
        .route("/webhooks", get(payments::list_merchant_webhooks))
        .route(
            "/webhooks/redeliver",
            post(payments::redeliver_webhooks_bulk),
        )
        .route("/:id/webhooks", get(payments::list_webhooks))
        .route(
            "/:id/webhooks/:delivery_id/redeliver",
            post(payments::redeliver_webhook),
        )
        /* Per-merchant quota, added as a `route_layer` *inside* `auth_middleware`
        (see ordering note there) so it keys on `AuthenticatedMerchant` rather
        than source IP: a merchant's capacity is theirs regardless of where
        they connect from, and cannot be multiplied by spreading requests
        across addresses. */
        .route_layer(middleware::from_fn_with_state(
            merchant_rate_limit,
            merchant_rate_limit_middleware,
        ))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));

    axum::Router::new().nest("/merchants", merchants).nest(
        "/payments",
        axum::Router::new()
            .merge(payments_authed)
            .route("/:id", get(payments::get_by_id)),
    )
}

/// Tag a response served from an unversioned path as deprecated.
///
/// `Deprecation` and `Link: rel="successor-version"` are the RFC 8594 /
/// RFC 8288 way of saying this, so a client can find the replacement in a
/// response it is already parsing rather than by reading release notes.
///
/// No `Sunset` header is emitted. That header is a commitment to a removal
/// date, and inventing one here would announce a promise nobody has made —
/// see the deprecation policy in the README.
async fn mark_deprecated(req: Request, next: Next) -> axum::response::Response {
    let successor = format!("/v1{}", req.uri().path());
    let mut res = next.run(req).await;

    let headers = res.headers_mut();
    headers.insert(
        axum::http::HeaderName::from_static("deprecation"),
        HeaderValue::from_static("true"),
    );
    if let Ok(link) = HeaderValue::from_str(&format!("<{successor}>; rel=\"successor-version\"")) {
        headers.insert(header::LINK, link);
    }
    res
}

/// Authenticates via the `Authorization: Bearer <key>` header, injecting
/// [`AuthenticatedMerchant`] into request extensions on success.
///
/// Every outcome is both logged (`source_ip` + `reason`, at a level matched
/// to severity — failures visible by default, success at `debug` to avoid
/// flooding logs) and counted in `AuthMetrics` (issue #139), so
/// credential-stuffing or a misconfigured client shows up in logs/metrics
/// instead of silently returning 401s.
async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    mut req: Request,
    next: Next,
) -> axum::response::Response {
    let source_ip = client_ip_key(&req, &state.config.trusted_proxy_cidrs);

    let raw_key = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|k| !k.is_empty())
        .map(str::to_string);

    let Some(key) = raw_key else {
        tracing::warn!(
            %source_ip,
            reason = "missing_key",
            "auth denied: missing or malformed Authorization header"
        );
        state.auth_metrics.record_failure_missing_key();
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "missing or invalid Authorization header", "code": "unauthorized" })),
        )
            .into_response();
    };

    match db::find_merchant_by_key(&state.pool, &key).await {
        Ok(Some(merchant_id)) => {
            tracing::debug!(%source_ip, %merchant_id, "auth succeeded");
            state.auth_metrics.record_success();
            req.extensions_mut()
                .insert(AuthenticatedMerchant(merchant_id));
            next.run(req).await
        }
        Ok(None) => {
            tracing::warn!(
                %source_ip,
                reason = "invalid_key",
                "auth denied: API key did not match any merchant"
            );
            state.auth_metrics.record_failure_invalid_key();
            (
                StatusCode::UNAUTHORIZED,
                Json(json!({ "error": "invalid API key", "code": "unauthorized" })),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!(%source_ip, error = %e, "auth errored: merchant key lookup failed");
            state.auth_metrics.record_failure_internal_error();
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "internal server error", "code": "internal_error" })),
            )
                .into_response()
        }
    }
}

/// Guards `POST /merchants` with a shared admin secret sent via the
/// `X-Admin-Secret` header. An unset `ADMIN_PROVISIONING_SECRET` disables
/// provisioning entirely rather than leaving the endpoint open.
async fn require_admin_secret(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> axum::response::Response {
    let configured = &state.config.admin_provisioning_secret;
    let provided = req
        .headers()
        .get("x-admin-secret")
        .and_then(|v| v.to_str().ok());

    if configured.is_empty() || provided != Some(configured.as_str()) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "missing or invalid admin secret", "code": "unauthorized" })),
        )
            .into_response();
    }

    next.run(req).await
}

/// `POST /merchants` — provision a new merchant and return its API key once.
/// Requires the `X-Admin-Secret` header (see `require_admin_secret`).
///
/// Accepts an optional `{ "rate_limit_per_sec": <n> }` body to give this
/// merchant a per-merchant quota override from the start; omitted or `null`
/// leaves it on the configured default (issue: rate limiter keyed on IP, not
/// identity).
async fn provision_merchant(
    State(state): State<Arc<AppState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OptionalJsonBody(body): OptionalJsonBody<ProvisionMerchantRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    let source_ip =
        client_ip_key_from_parts(Some(peer), &headers, &state.config.trusted_proxy_cidrs);
    let req_id = request_id(&headers);

    let rate_limit_per_sec = body.and_then(|b| b.rate_limit_per_sec);
    if let Some(n) = rate_limit_per_sec {
        if n <= 0 {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(
                    json!({ "error": "rate_limit_per_sec must be positive", "code": "invalid_rate_limit" }),
                ),
            ));
        }
    }

    let merchant_id = uuid::Uuid::new_v4().to_string();
    let (raw_key, prefix) = db::generate_api_key();

    let key_id = db::create_merchant(
        &state.pool,
        &merchant_id,
        &raw_key,
        &prefix,
        rate_limit_per_sec,
    )
    .await
    .map_err(|e| {
        // Issue #125: swallowing this left an operator with a 500 and no
        // way to tell a disk error from a UNIQUE collision.
        tracing::error!(error = %e, "failed to provision merchant");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "internal server error", "code": "internal_error" })),
        )
    })?;

    /* Successful provisioning is the single most privileged operation in the
    system — it mints a credential — and previously left no trace at all
    while a *failed* one did (issue #305). `audit = true` lets this be routed
    to a separate sink from ordinary operational logs; see the "Audit events"
    section of the README for the full field schema. */
    tracing::info!(
        audit = true,
        action = "merchant.provision",
        actor = "admin",
        outcome = "created",
        %merchant_id,
        %key_id,
        source_ip = %source_ip,
        request_id = %req_id,
        "merchant provisioned"
    );

    Ok((
        StatusCode::CREATED,
        Json(json!({
            "merchant_id": merchant_id,
            "api_key": raw_key,
            "key_id": key_id,
            "rate_limit_per_sec": rate_limit_per_sec,
        })),
    ))
}

/// The optional `POST /merchants` body.
///
/// `deny_unknown_fields` for the same reason as `IssueKeyRequest` — a typo in
/// the field name should surface as a `400`, not silently fall back to the
/// default quota.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ProvisionMerchantRequest {
    rate_limit_per_sec: Option<i64>,
}

/// `PUT /merchants/:id/rate-limit` — set or clear a merchant's per-second
/// quota override for the authenticated rate limiter. `null` (or an absent
/// field) clears the override and returns the merchant to the configured
/// default. Admin-gated, like the rest of `/merchants`.
async fn set_merchant_rate_limit(
    State(state): State<Arc<AppState>>,
    Path(merchant_id): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    JsonBody(body): JsonBody<SetRateLimitRequest>,
) -> Result<impl IntoResponse, AppError> {
    if let Some(n) = body.rate_limit_per_sec {
        if n <= 0 {
            return Err(AppError::bad_request(
                "invalid_rate_limit",
                "rate_limit_per_sec must be positive",
            ));
        }
    }

    if !db::set_merchant_rate_limit(&state.pool, &merchant_id, body.rate_limit_per_sec).await? {
        return Err(AppError::not_found(
            "merchant_not_found",
            "merchant not found",
        ));
    }

    let source_ip =
        client_ip_key_from_parts(Some(peer), &headers, &state.config.trusted_proxy_cidrs);
    tracing::info!(
        audit = true,
        action = "merchant.rate_limit.set",
        actor = "admin",
        outcome = "updated",
        %merchant_id,
        rate_limit_per_sec = ?body.rate_limit_per_sec,
        source_ip = %source_ip,
        request_id = %request_id(&headers),
        "merchant rate limit updated"
    );

    Ok((
        StatusCode::OK,
        Json(json!({
            "merchant_id": merchant_id,
            "rate_limit_per_sec": body.rate_limit_per_sec,
        })),
    ))
}

/// The `PUT /merchants/:id/rate-limit` body. `deny_unknown_fields` so a typo
/// surfaces as a `400` rather than silently doing nothing.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SetRateLimitRequest {
    rate_limit_per_sec: Option<i64>,
}

/// `POST /merchants/:id/keys` — issue an additional API key.
///
/// Rotation is issue-then-revoke rather than replace-in-place: the new key is
/// live immediately while the old one keeps working, so a merchant can deploy
/// the new credential before retiring the old one and never has a window with
/// no valid key. Admin-gated, like provisioning.
async fn issue_api_key(
    State(state): State<Arc<AppState>>,
    Path(merchant_id): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OptionalJsonBody(body): OptionalJsonBody<IssueKeyRequest>,
) -> Result<impl IntoResponse, AppError> {
    if !db::merchant_exists(&state.pool, &merchant_id).await? {
        return Err(AppError::not_found(
            "merchant_not_found",
            "merchant not found",
        ));
    }

    let label = body.and_then(|b| b.label);
    if let Some(l) = &label {
        if l.len() > 100 {
            return Err(AppError::bad_request(
                "invalid_label",
                "label exceeds max length of 100 characters",
            ));
        }
    }

    let (raw_key, prefix) = db::generate_api_key();
    let key_id = db::create_api_key(
        &state.pool,
        &merchant_id,
        &raw_key,
        &prefix,
        label.as_deref(),
    )
    .await?;

    let source_ip =
        client_ip_key_from_parts(Some(peer), &headers, &state.config.trusted_proxy_cidrs);
    tracing::info!(
        audit = true,
        action = "api_key.issue",
        actor = "admin",
        outcome = "issued",
        %merchant_id,
        %key_id,
        source_ip = %source_ip,
        request_id = %request_id(&headers),
        "api key issued"
    );

    Ok((
        StatusCode::CREATED,
        Json(json!({
            "key_id": key_id,
            "api_key": raw_key,
            "prefix": prefix,
            "label": label,
        })),
    ))
}

/// `GET /merchants/:id/keys` — list a merchant's keys.
///
/// Returns metadata only. The secret is unrecoverable by design, so this can
/// never leak a usable credential; `prefix` exists so an operator can identify
/// which key to revoke.
async fn list_api_keys(
    State(state): State<Arc<AppState>>,
    Path(merchant_id): Path<String>,
) -> Result<Json<Value>, AppError> {
    if !db::merchant_exists(&state.pool, &merchant_id).await? {
        return Err(AppError::not_found(
            "merchant_not_found",
            "merchant not found",
        ));
    }

    let keys = db::list_api_keys(&state.pool, &merchant_id).await?;
    Ok(Json(json!({
        "merchant_id": merchant_id,
        "keys": keys.iter().map(|k| json!({
            "key_id": k.id,
            "prefix": k.prefix,
            "label": k.label,
            "created_at": k.created_at,
            "last_used_at": k.last_used_at,
            "revoked_at": k.revoked_at,
            "active": k.revoked_at.is_none(),
        })).collect::<Vec<_>>(),
    })))
}

/// `DELETE /merchants/:id/keys/:key_id` — revoke a key immediately.
///
/// Refuses to revoke a merchant's last active key. Doing so would lock them
/// out of an API that has no self-service recovery, turning a routine
/// revocation into an incident; issue a replacement first.
async fn revoke_api_key(
    State(state): State<Arc<AppState>>,
    Path((merchant_id, key_id)): Path<(String, String)>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, AppError> {
    if !db::merchant_exists(&state.pool, &merchant_id).await? {
        return Err(AppError::not_found(
            "merchant_not_found",
            "merchant not found",
        ));
    }

    if db::count_active_api_keys(&state.pool, &merchant_id).await? <= 1 {
        return Err(AppError::bad_request(
            "last_active_key",
            "cannot revoke a merchant's only active key — issue a replacement first",
        ));
    }

    if !db::revoke_api_key(&state.pool, &merchant_id, &key_id).await? {
        return Err(AppError::not_found(
            "key_not_found",
            "no active key with that id for this merchant",
        ));
    }

    let source_ip =
        client_ip_key_from_parts(Some(peer), &headers, &state.config.trusted_proxy_cidrs);
    tracing::warn!(
        audit = true,
        action = "api_key.revoke",
        actor = "admin",
        outcome = "revoked",
        %merchant_id,
        %key_id,
        source_ip = %source_ip,
        request_id = %request_id(&headers),
        "api key revoked"
    );
    Ok((
        StatusCode::OK,
        Json(json!({ "key_id": key_id, "revoked": true })),
    ))
}

/// The optional `POST /merchants/:id/keys` body.
///
/// `deny_unknown_fields` for the same reason as `CreatePaymentRequest`
/// (issue #329): a mistyped `lable` should say so rather than quietly issuing
/// an unlabelled key.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct IssueKeyRequest {
    label: Option<String>,
}

/// Enforce the per-(bucket, client) quota and describe it to the caller.
///
/// Every response — throttled or not — carries `X-RateLimit-Limit`,
/// `X-RateLimit-Remaining` and `X-RateLimit-Reset`, so a client can pace itself
/// *before* being throttled rather than discovering the limit by hitting it
/// (issue #327).
///
/// On a rejection, `Retry-After` is derived from the limiter's own state.
/// `governor` computes exactly this value — `check()` returns
/// `Err(NotUntil<_>)`, whose `wait_time_from` is the duration until the request
/// would be permitted — and the previous code discarded it in favour of a
/// hard-coded `1`. That constant was wrong in both directions: too short and a
/// well-behaved client that honours it retries straight into another `429`,
/// generating exactly the load the limiter is shedding; too long and throughput
/// is left on the table. It only looked right because the default quota is
/// per-second, and grew steadily more wrong as `RATE_LIMIT_REQUESTS_PER_SEC`
/// was tuned.
async fn rate_limit_middleware(
    State(rate_limit): State<RateLimitState>,
    req: Request,
    next: Next,
) -> axum::response::Response {
    let Some(bucket) = rate_limited_bucket(&req) else {
        return next.run(req).await;
    };

    let key = rate_limit_key(bucket, &req, &rate_limit.trusted_proxies);
    let base_rps = rate_limit.requests_per_sec;
    let effective_rps = base_rps
        .saturating_mul(bucket_rate_multiplier(bucket))
        .max(1);
    /* `get_with` clones the `Arc` out of the cache rather than handing back a
    guard, so nothing borrowed from the cache is held across the `.await`
    below. */
    let limiter = rate_limit.limiters.get_with(key, || {
        Arc::new(
            governor::RateLimiter::direct(governor::Quota::per_second(
                NonZeroU32::new(effective_rps).unwrap(),
            ))
            .with_middleware::<StateInformationMiddleware>(),
        )
    });

    match limiter.check() {
        Ok(snapshot) => {
            let remaining = snapshot.remaining_burst_capacity();
            let reset = reset_secs(snapshot.quota(), remaining, Duration::ZERO);
            let mut res = next.run(req).await;
            set_rate_limit_headers(res.headers_mut(), effective_rps, remaining, reset);
            res
        }
        Err(not_until) => {
            let wait = not_until.wait_time_from(DefaultClock::default().now());
            let retry_after = retry_after_secs(wait);
            let reset = reset_secs(not_until.quota(), 0, wait);

            let mut res = (
                StatusCode::TOO_MANY_REQUESTS,
                Json(json!({
                    "error": "rate limit exceeded",
                    "code": "rate_limit_exceeded"
                })),
            )
                .into_response();

            let headers = res.headers_mut();
            if let Ok(value) = HeaderValue::from_str(&retry_after.to_string()) {
                headers.insert(header::RETRY_AFTER, value);
            }
            set_rate_limit_headers(headers, effective_rps, 0, reset);
            res
        }
    }
}

/// Enforces a per-merchant quota, independent of client IP, so one merchant
/// can never exhaust another's capacity and a merchant cannot multiply its
/// own quota by spreading requests across source addresses.
///
/// A `route_layer` mounted *inside* `auth_middleware` (see `api_v1`), so it
/// runs only once a request is attributed to an [`AuthenticatedMerchant`] and
/// keys purely on that identity. The coarse IP limiter above it still runs
/// first and is left untouched — cheap flood rejection before touching the
/// database is the right instinct, this just adds the identity-aware layer
/// the issue calls for on top of it.
///
/// Requests that clear the quota pass through unmodified; the success-path
/// `X-RateLimit-*` headers stay owned by the outer IP limiter, which already
/// sets them on every response. On a rejection, this builds its own `429`
/// carrying the merchant-scoped `Retry-After` and `X-RateLimit-*` values —
/// `set_rate_limit_headers` only fills headers that aren't already present,
/// so the outer layer cannot overwrite them on the way back out.
async fn merchant_rate_limit_middleware(
    State(rate_limit): State<MerchantRateLimitState>,
    req: Request,
    next: Next,
) -> axum::response::Response {
    let Some(AuthenticatedMerchant(merchant_id)) =
        req.extensions().get::<AuthenticatedMerchant>().cloned()
    else {
        // No authenticated merchant in scope for this route — nothing to key
        // on, so defer entirely to the outer IP limiter.
        return next.run(req).await;
    };

    let limiter = match rate_limit.limiters.get(&merchant_id) {
        Some(limiter) => limiter,
        None => {
            let rps = merchant_quota_rps(&rate_limit, &merchant_id).await;
            let fresh = Arc::new(
                governor::RateLimiter::direct(governor::Quota::per_second(
                    NonZeroU32::new(rps).unwrap(),
                ))
                .with_middleware::<StateInformationMiddleware>(),
            );
            rate_limit
                .limiters
                .insert(merchant_id.clone(), fresh.clone());
            fresh
        }
    };

    match limiter.check() {
        Ok(_) => next.run(req).await,
        Err(not_until) => {
            let wait = not_until.wait_time_from(DefaultClock::default().now());
            let retry_after = retry_after_secs(wait);
            let reset = reset_secs(not_until.quota(), 0, wait);
            let limit = not_until.quota().burst_size().get();

            let mut res = (
                StatusCode::TOO_MANY_REQUESTS,
                Json(json!({
                    "error": "merchant rate limit exceeded",
                    "code": "merchant_rate_limit_exceeded"
                })),
            )
                .into_response();

            let headers = res.headers_mut();
            if let Ok(value) = HeaderValue::from_str(&retry_after.to_string()) {
                headers.insert(header::RETRY_AFTER, value);
            }
            set_rate_limit_headers(headers, limit, 0, reset);
            res
        }
    }
}

/// Resolve the quota a merchant's limiter should be built with: its own
/// `rate_limit_per_sec` override if an operator has set one, otherwise the
/// configured default. A lookup failure falls back to the default rather than
/// failing the request — an operator-set override is an optimization, not a
/// safety property this path needs to be strict about.
async fn merchant_quota_rps(rate_limit: &MerchantRateLimitState, merchant_id: &str) -> u32 {
    match db::get_merchant_rate_limit(&rate_limit.pool, merchant_id).await {
        Ok(Some(custom)) if custom > 0 => custom.min(u32::MAX as i64) as u32,
        Ok(_) => rate_limit.default_rps,
        Err(e) => {
            tracing::error!(%merchant_id, error = %e, "merchant rate limit lookup failed; using default quota");
            rate_limit.default_rps
        }
    }
}

/// `Retry-After`, in delta-seconds per RFC 9110.
///
/// Rounded *up*, with a floor of 1. Truncating would hand back a value the
/// limiter has not actually reached — a 1.4s wait reported as `1` is a
/// guaranteed second rejection — and `0` would tell a client honouring the
/// header to retry immediately, i.e. to hot-loop.
fn retry_after_secs(wait: Duration) -> u64 {
    wait.as_secs_f64().ceil().max(1.0) as u64
}

/// Seconds until the bucket is back to full capacity.
///
/// `wait_for_next` is the delay before the *first* cell becomes available (zero
/// when the request was allowed); the remaining cells refill one per
/// `replenish_interval` after that. Reported as a delta rather than an epoch
/// timestamp so a client with a skewed clock still gets a usable answer.
fn reset_secs(quota: governor::Quota, remaining: u32, wait_for_next: Duration) -> u64 {
    let missing = quota.burst_size().get().saturating_sub(remaining);
    let refill = quota.replenish_interval().saturating_mul(missing);
    // `wait_for_next` already covers the first missing cell on the throttled
    // path, so it replaces one interval rather than adding to the total.
    let total = refill.max(wait_for_next);
    total.as_secs_f64().ceil() as u64
}

/// Sets a header only if it isn't already present. The per-merchant limiter
/// (issue: rate limiter keyed on IP, not identity) runs *inside* this one and
/// sets these same header names on its own rejections first; using `entry`
/// here means the outer IP-bucket layer never clobbers the more specific,
/// identity-bound values with its own.
fn set_rate_limit_headers(headers: &mut HeaderMap, limit: u32, remaining: u32, reset: u64) {
    for (name, value) in [
        (X_RATELIMIT_LIMIT, limit as u64),
        (X_RATELIMIT_REMAINING, remaining as u64),
        (X_RATELIMIT_RESET, reset),
    ] {
        if let Ok(value) = HeaderValue::from_str(&value.to_string()) {
            headers.entry(name).or_insert(value);
        }
    }
}

/// Identifies which rate-limit bucket a request falls into, or `None` to
/// exempt it from the limiter entirely.
///
/// Every request is assigned a bucket so all routes are protected by default.
/// Write and sensitive routes use named buckets that receive the base quota
/// (`requests_per_sec × 1`). Read-only routes fall into the `"default"` bucket
/// which receives a more generous quota (`requests_per_sec × 5`) to avoid
/// throttling normal polling.
///
/// `/health`, `/ready` and `/metrics` are exempt rather than sharing the
/// `"default"` bucket with merchant API traffic. They used to share it, which
/// created a feedback loop under load: a traffic spike exhausts the per-IP
/// quota, the orchestrator's liveness probe — same source IP, same bucket —
/// gets a `429`, `curl -f` treats that as a failed check, and the instance is
/// restarted or pulled from rotation right when it's under the most load.
/// Restarting this service is not free (the poller re-baselines and the
/// redrive worker runs a full pass on start), so that restart concentrates
/// load on the remaining instances instead of relieving it. Probes are also
/// the wrong thing to protect: they're cheap, they come from a trusted
/// orchestrator rather than the public internet, and their entire purpose is
/// to stay answerable when the system is stressed. If unauthenticated abuse
/// of these paths ever becomes a real concern, give them their own generous
/// bucket rather than folding them back into `"default"`.
///
/// Redelivery is bucketed by shape rather than by path: the URL carries a
/// payment and delivery id, and keying on those would let every id mint its
/// own limiter entry — both an unbounded map and a trivially bypassed limit.
fn rate_limited_bucket(req: &Request) -> Option<&'static str> {
    let path = req.uri().path();
    if path == "/health" || path == "/ready" || path == "/metrics" {
        return None;
    }
    if req.method() == axum::http::Method::POST {
        return match path {
            "/payments" => Some("payments"),
            "/merchants" => Some("merchants"),
            _ if path.starts_with("/payments/") && path.ends_with("/redeliver") => {
                Some("redeliver")
            }
            _ => Some("default"),
        };
    }
    // All other non-POST requests (GET, etc.) fall into the default bucket so
    // that payment enumeration and webhook listing are covered by a baseline
    // limit.
    Some("default")
}

/// Returns the rate multiplier for a bucket.
///
/// Write/sensitive buckets get the base rate (× 1). Read-only traffic gets a
/// higher allowance (× 5) so normal API consumers aren't throttled by polling.
fn bucket_rate_multiplier(bucket: &str) -> u32 {
    match bucket {
        "payments" | "merchants" | "redeliver" => 1,
        _ => 5,
    }
}

/// Keyed by bucket + client so each bucket is rate-limited independently —
/// provisioning a merchant should never eat into a client's payment quota (or
/// vice versa).
fn rate_limit_key(bucket: &str, req: &Request, trusted_proxies: &[IpNet]) -> String {
    format!("{bucket}:{}", client_ip_key(req, trusted_proxies))
}

/// Client key used when a request carries no peer address at all (the router
/// is served without `into_make_service_with_connect_info`). Fail-closed:
/// every such request shares this one key rather than trusting client-supplied
/// forwarding headers (issue #330).
const CLIENT_IP_UNKNOWN: &str = "unknown";

/// Resolve the client IP used for rate-limit bucketing and auth-log source
/// attribution.
///
/// `X-Forwarded-For` and `X-Real-IP` are client-supplied, so they are honoured
/// ONLY when the socket peer is a configured trusted proxy
/// (`TRUSTED_PROXY_CIDRS`). In every other case the peer's own address is used
/// and the headers are ignored entirely:
///
/// - Untrusted peer (or no trusted proxies configured): the peer address wins,
///   so a caller cannot rotate a header per request to evade rate limiting or
///   poison the auth logs.
/// - Trusted peer: the rightmost `X-Forwarded-For` hop that is not itself a
///   trusted proxy is taken as the client, falling back to `X-Real-IP` and
///   then the peer address. This preserves per-client attribution behind a
///   reverse proxy without letting the proxy chain be forged.
/// - No peer address available: fail closed to a single shared key
///   ([`CLIENT_IP_UNKNOWN`]) with a one-time warning, regardless of headers.
fn client_ip_key(req: &Request, trusted_proxies: &[IpNet]) -> String {
    let peer = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(addr)| *addr);
    client_ip_key_from_parts(peer, req.headers(), trusted_proxies)
}

/// The part of [`client_ip_key`] that doesn't need a whole [`Request`] —
/// split out so handlers that only have a [`ConnectInfo`] extractor and a
/// [`HeaderMap`] (rather than the raw request) can compute the same
/// attributed source IP for audit logging (issue #305).
pub(crate) fn client_ip_key_from_parts(
    peer: Option<SocketAddr>,
    headers: &HeaderMap,
    trusted_proxies: &[IpNet],
) -> String {
    let Some(addr) = peer else {
        warn_missing_connect_info_once();
        return CLIENT_IP_UNKNOWN.to_string();
    };

    let peer_ip = addr.ip();

    // Untrusted peer: the forwarding headers are ignored entirely.
    if !trusted_proxies.iter().any(|net| net.contains(&peer_ip)) {
        return peer_ip.to_string();
    }

    // Trusted peer: walk X-Forwarded-For from the rightmost hop, skipping hops
    // that are themselves trusted proxies, and take the first remaining value.
    if let Some(value) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
        for hop in value.split(',').map(str::trim).rev() {
            if hop.is_empty() {
                continue;
            }
            match hop.parse::<IpAddr>() {
                // A hop that is itself a trusted proxy was added by a proxy we
                // trust — keep walking left toward the real client.
                Ok(ip) if trusted_proxies.iter().any(|net| net.contains(&ip)) => continue,
                // First hop that is not a trusted proxy: this is the client.
                Ok(ip) => return ip.to_string(),
                // Unparseable value: cannot be a trusted proxy, so treat it as
                // the client rather than guessing.
                Err(_) => return hop.to_string(),
            }
        }
    }

    // No X-Forwarded-For, or every hop was a trusted proxy: fall back to the
    // single-value X-Real-IP header, also gated on the trusted peer.
    if let Some(value) = headers.get("x-real-ip").and_then(|v| v.to_str().ok()) {
        if let Ok(ip) = value.trim().parse::<IpAddr>() {
            if !trusted_proxies.iter().any(|net| net.contains(&ip)) {
                return ip.to_string();
            }
        }
    }

    peer_ip.to_string()
}

/// Extracts `X-Request-Id`, set on every request by `SetRequestIdLayer`
/// before it reaches a handler. Falls back to `"-"` for the (untestable in
/// production, but real in unit tests that call a handler directly) case
/// where the layer didn't run.
pub(crate) fn request_id(headers: &HeaderMap) -> String {
    headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-")
        .to_string()
}

/// Warn once (per process) when a request has no peer address, i.e. the router
/// is being served without `into_make_service_with_connect_info`. From then on
/// every such request shares a single fail-closed client key, so an operator
/// embedding the router sees exactly one warning instead of one per request.
fn warn_missing_connect_info_once() {
    static WARNED: std::sync::Once = std::sync::Once::new();
    WARNED.call_once(|| {
        tracing::warn!(
            "no ConnectInfo peer address on request: forwarding headers are ignored and all \
             such requests share one client key. Serve the router with \
             into_make_service_with_connect_info to restore per-client attribution."
        );
    });
}

fn build_cors(cfg: &crate::config::Config) -> CorsLayer {
    use axum::http::HeaderName;
    use tower_http::cors::AllowOrigin;

    let origins = &cfg.cors_allowed_origins;

    if origins.is_empty() {
        if cfg.network == "public" {
            tracing::error!(
                "CORS_ALLOWED_ORIGINS is not set on a public-network deployment. \
                 Denying all origins by default."
            );
            return CorsLayer::new();
        }
        tracing::warn!(
            "CORS_ALLOWED_ORIGINS is not set on a testnet deployment. \
             Falling back to permissive CORS for development and test environments."
        );
        return CorsLayer::permissive();
    }

    let allow_origins: Vec<axum::http::HeaderValue> = origins
        .iter()
        .map(|o| {
            o.parse().unwrap_or_else(|e| {
                // Origins are validated in Config::from_env, so this branch is
                // unreachable in production. Treat it as a programming error.
                panic!("BUG: unparseable CORS origin {o:?} reached build_cors: {e}")
            })
        })
        .collect();

    CorsLayer::new()
        .allow_origin(AllowOrigin::list(allow_origins))
        .allow_methods([
            axum::http::Method::GET,
            axum::http::Method::POST,
            axum::http::Method::OPTIONS,
        ])
        .allow_headers([
            HeaderName::from_static("content-type"),
            HeaderName::from_static("authorization"),
        ])
        /* Without this a browser client can see the 429 but not the headers
        explaining it: the CORS spec hides every response header outside the
        safelist unless it is named here, and `Retry-After` is not on that
        safelist. Emitting a self-pacing contract the browser cannot read would
        be the same as not emitting it (issue #327). */
        .expose_headers([
            header::RETRY_AFTER,
            X_RATELIMIT_LIMIT,
            X_RATELIMIT_REMAINING,
            X_RATELIMIT_RESET,
        ])
}

/// Service root.
///
/// A browser gets sent to the dashboard; anything else gets the version string.
/// Hosting platforms hand you the root URL ("open in browser"), so a bare line
/// of text there reads as "nothing is running" and gives no hint that a UI
/// exists one path away.
///
/// The split is on `Accept`, not User-Agent: `fetch()` and `curl` send `*/*`
/// and keep the plaintext response, so the dashboard's own version lookup and
/// any uptime check are unaffected.
async fn root(headers: axum::http::HeaderMap) -> impl IntoResponse {
    let wants_html = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|accept| accept.contains("text/html"));

    if wants_html {
        axum::response::Redirect::temporary("/dashboard").into_response()
    } else {
        (
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            )],
            concat!("StellarGate API v", env!("CARGO_PKG_VERSION")),
        )
            .into_response()
    }
}

/// Liveness probe — cheap by design, and fails only on conditions a restart
/// would fix: an expected background task is no longer running (issue #315),
/// or a required task is crash-looping under the supervisor (issue #316).
/// A poller or listener that died at startup must not leave the process
/// looking healthy forever, so this checks [`TaskHealth`](crate::TaskHealth)
/// rather than just returning a hard-coded ok.
async fn health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let dead = state.task_health.dead_required_tasks();
    let looping = state.task_health.crash_looping_required_tasks();

    /* Expected-versus-live, so the probe answers "how many workers should be
    running, and how many are?" rather than only "is anything wrong?"
    (issue #317). `expected` already excludes workers configuration has
    deliberately switched off, so a poll-only deployment does not read as
    permanently degraded. */
    let expected = state.task_health.expected_tasks();
    let live = state.task_health.live_tasks();
    let disabled: Vec<_> = state
        .task_health
        .snapshot()
        .into_iter()
        .filter_map(|s| {
            s.disabled_reason
                .map(|r| json!({ "task": s.name, "reason": r }))
        })
        .collect();

    if dead.is_empty() && looping.is_empty() {
        return Json(json!({
            "status": "ok",
            "tasks": { "expected": expected, "live": live, "disabled": disabled },
        }))
        .into_response();
    }

    let mut reasons = Vec::new();
    if !dead.is_empty() {
        reasons.push(format!(
            "background task(s) not running: {}",
            dead.join(", ")
        ));
    }
    if !looping.is_empty() {
        reasons.push(format!(
            "background task(s) crash-looping: {}",
            looping.join(", ")
        ));
    }

    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({
            "status": "unavailable",
            "reason": reasons.join("; "),
            "tasks": { "expected": expected, "live": live, "disabled": disabled },
        })),
    )
        .into_response()
}

/// Readiness probe — returns 200 only when the database AND Horizon are
/// reachable AND the payment-detection cursor is advancing. A pod that cannot
/// reach Horizon cannot detect on-chain payments; routing traffic to it is
/// worse than routing it elsewhere (issue #172). And a reachable Horizon plus
/// a dead poller is indistinguishable from a healthy system, so the probe also
/// requires a successful poll (or stream event) within a configurable window
/// (issue #315).
///
/// Uses a 3-second timeout on the Horizon check so a slow node never hangs
/// the probe. Both the Horizon probe and the cursor-freshness check are
/// skipped when no gateway is configured (STELLAR_GATEWAY_PUBLIC=UNCONFIGURED)
/// since without a gateway there is no on-chain work to do.
async fn ready(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    // 1. Database must respond.
    if db::ping(&state.pool).await.is_err() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "unavailable", "reason": "database unreachable" })),
        )
            .into_response();
    }

    // 2. Horizon must respond and the detection cursor must be fresh (only
    //    when a gateway wallet is configured).
    if state.config.gateway_configured() {
        if let Err(reason) = check_horizon_ready(&state).await {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "status": "unavailable", "reason": reason })),
            )
                .into_response();
        }

        /* The poller/stream must have made progress recently. Horizon being
        reachable says nothing about whether a worker is actually watching it
        (issue #315): a poller that exited at startup leaves /ready green
        forever without this check. The window is POLL_INTERVAL_SECS ×
        CURSOR_STALENESS_MULTIPLE, so a healthy poller — which cycles on the
        poll interval — always clears it. */
        let staleness_limit =
            state.config.poll_interval_secs as i64 * state.config.cursor_staleness_multiple as i64;
        let cursor_age = state.task_health.last_success_age_secs();
        if cursor_age > staleness_limit {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "status": "unavailable",
                    "reason": format!(
                        "payment detection stalled: no successful poll or stream event in \
                         {cursor_age}s (limit {staleness_limit}s)"
                    ),
                })),
            )
                .into_response();
        }
    }

    (StatusCode::OK, Json(json!({ "status": "ok" }))).into_response()
}

/// Probe Horizon with a hard 3-second timeout.
/// Returns Ok(()) when reachable (any non-5xx response), or an error string.
async fn check_horizon_ready(state: &Arc<AppState>) -> Result<(), String> {
    let url = state.config.horizon_url.trim_end_matches('/').to_string();
    let result = tokio::time::timeout(
        Duration::from_millis(3_000),
        state
            .http
            .get(&url)
            .header("Accept", "application/json")
            .send(),
    )
    .await;
    match result {
        Ok(Ok(resp)) if resp.status().as_u16() < 500 => Ok(()),
        Ok(Ok(resp)) => Err(format!("Horizon returned {}", resp.status())),
        Ok(Err(e)) => Err(format!("Horizon unreachable: {e}")),
        Err(_) => Err("Horizon health check timed out".to_string()),
    }
}

/// `GET /metrics` — Prometheus-compatible plain-text metrics snapshot.
async fn metrics_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let body = crate::metrics::render(
        &state.webhook_metrics,
        &state.auth_metrics,
        &state.task_health,
        &state.horizon_metrics,
        &state.trustline_metrics,
    );
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
        )],
        body,
    )
}

/* The dashboard is compiled into the binary rather than read from disk, so a
deployment stays a single artifact with no asset path to configure and no way
for the two to drift apart. */
const DASHBOARD_HTML: &str = include_str!("../../static/dashboard.html");
const DASHBOARD_CSS: &str = include_str!("../../static/dashboard.css");
const DASHBOARD_JS: &str = include_str!("../../static/dashboard.js");

/// Locks the dashboard to its own origin: no third-party script, style, frame
/// or connection. The page ships no inline script or style, so this needs no
/// `unsafe-inline` escape hatch.
const DASHBOARD_CSP: &str = "default-src 'none'; \
     script-src 'self'; \
     style-src 'self'; \
     img-src 'self' data:; \
     connect-src 'self'; \
     form-action 'none'; \
     frame-ancestors 'none'; \
     base-uri 'none'";

fn dashboard_asset(body: &'static str, content_type: &'static str) -> impl IntoResponse {
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, HeaderValue::from_static(content_type)),
            (
                header::CONTENT_SECURITY_POLICY,
                HeaderValue::from_static(DASHBOARD_CSP),
            ),
            (
                header::X_CONTENT_TYPE_OPTIONS,
                HeaderValue::from_static("nosniff"),
            ),
        ],
        body,
    )
}

async fn dashboard_html() -> impl IntoResponse {
    dashboard_asset(DASHBOARD_HTML, "text/html; charset=utf-8")
}

async fn dashboard_css() -> impl IntoResponse {
    dashboard_asset(DASHBOARD_CSS, "text/css; charset=utf-8")
}

async fn dashboard_js() -> impl IntoResponse {
    dashboard_asset(DASHBOARD_JS, "text/javascript; charset=utf-8")
}

async fn not_found() -> impl IntoResponse {
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "error": "not found", "code": "not_found" })),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum_test::TestServer;
    use tower_http::timeout::TimeoutLayer;

    /// Exercises the exact `TimeoutLayer` construction used in `router()`,
    /// against a router small enough to run with millisecond durations —
    /// `request_timeout_secs` itself is whole seconds, too coarse for a fast test.
    fn timeout_test_router(timeout: Duration) -> axum::Router {
        axum::Router::new()
            .route(
                "/slow",
                get(|| async {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                }),
            )
            .route("/fast", get(|| async { "ok" }))
            .layer(TimeoutLayer::with_status_code(
                StatusCode::REQUEST_TIMEOUT,
                timeout,
            ))
    }

    #[tokio::test]
    async fn slow_handler_is_aborted_with_408() {
        let server = TestServer::new(timeout_test_router(Duration::from_millis(20))).unwrap();
        let response = server.get("/slow").await;
        response.assert_status(StatusCode::REQUEST_TIMEOUT);
    }

    #[tokio::test]
    async fn fast_handler_is_unaffected() {
        let server = TestServer::new(timeout_test_router(Duration::from_millis(200))).unwrap();
        let response = server.get("/fast").await;
        response.assert_status_ok();
    }

    // ── client_ip_key (issue #330) ───────────────────────────────────────────

    /// Build a bare request with an optional peer IP and headers, the inputs
    /// `client_ip_key` actually reads (extensions + headers).
    fn req_with(peer: Option<&str>, headers: &[(&str, &str)]) -> Request<axum::body::Body> {
        let mut req = Request::builder().body(axum::body::Body::empty()).unwrap();
        if let Some(ip) = peer {
            let addr = SocketAddr::new(ip.parse().unwrap(), 0);
            req.extensions_mut().insert(ConnectInfo(addr));
        }
        for (name, value) in headers {
            req.headers_mut().insert(
                axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                value.parse().unwrap(),
            );
        }
        req
    }

    fn cidrs(list: &[&str]) -> Vec<IpNet> {
        list.iter().map(|c| c.parse().unwrap()).collect()
    }

    /// TEST-NET-3 (RFC 5737) — documentation IPs, safe to use as fake clients.
    const PEER: &str = "203.0.113.9";
    const FAKE_CLIENT: &str = "198.51.100.7";

    #[test]
    fn untrusted_peer_ignores_spoofed_forwarding_headers() {
        let req = req_with(
            Some(PEER),
            &[
                ("x-forwarded-for", FAKE_CLIENT),
                ("x-real-ip", "198.51.100.8"),
            ],
        );
        assert_eq!(
            client_ip_key(&req, &cidrs(&["10.0.0.0/8"])),
            PEER,
            "an untrusted peer must be attributed by its own address"
        );
    }

    #[test]
    fn headers_ignored_when_no_trusted_proxies_configured() {
        // The acceptance criterion: with TRUSTED_PROXY_CIDRS unset, forwarding
        // headers are ignored regardless of ConnectInfo.
        let req = req_with(Some(PEER), &[("x-forwarded-for", FAKE_CLIENT)]);
        assert_eq!(client_ip_key(&req, &[]), PEER);
    }

    #[test]
    fn trusted_proxy_chain_takes_rightmost_non_trusted_hop() {
        // Peer is a trusted proxy; the chain was appended by trusted proxies
        // (rightmost first), so the leftmost client survives.
        let req = req_with(
            Some("10.0.0.1"),
            &[("x-forwarded-for", "198.51.100.7, 10.0.0.5, 10.0.0.1")],
        );
        assert_eq!(client_ip_key(&req, &cidrs(&["10.0.0.0/8"])), FAKE_CLIENT);
    }

    #[test]
    fn trusted_proxy_chain_of_only_trusted_hops_falls_back_to_peer() {
        let req = req_with(
            Some("10.0.0.1"),
            &[("x-forwarded-for", "10.0.0.9, 10.0.0.1")],
        );
        assert_eq!(client_ip_key(&req, &cidrs(&["10.0.0.0/8"])), "10.0.0.1");
    }

    #[test]
    fn trusted_proxy_honors_x_real_ip_when_no_forwarded_for() {
        let req = req_with(Some("10.0.0.1"), &[("x-real-ip", FAKE_CLIENT)]);
        assert_eq!(client_ip_key(&req, &cidrs(&["10.0.0.0/8"])), FAKE_CLIENT);
    }

    #[test]
    fn missing_connect_info_fails_closed_to_shared_key() {
        // Router served without connect info: headers must never be trusted,
        // and every request shares one key rather than minting per-header ones.
        let req = req_with(None, &[("x-forwarded-for", FAKE_CLIENT)]);
        assert_eq!(
            client_ip_key(&req, &cidrs(&["10.0.0.0/8"])),
            CLIENT_IP_UNKNOWN
        );
    }

    #[test]
    fn plain_peer_with_no_headers_uses_peer_address() {
        let req = req_with(Some(PEER), &[]);
        assert_eq!(client_ip_key(&req, &cidrs(&["10.0.0.0/8"])), PEER);
    }

    // ── reset_and_retry_after (issue #327) ───────────────────────────────────
    //
    // These are where the "derived, not fabricated" claim is actually pinned.
    // The integration tests cannot do it: every quota the service builds is a
    // `Quota::per_second(n)`, under which one cell replenishes in `1/n` seconds,
    // so the wait is always sub-second and `Retry-After` — an integer per
    // RFC 9110 — rounds to `1` at every configured rate. The hard-coded `1` was
    // numerically indistinguishable from the truth for every reachable
    // configuration, which is precisely why it survived review.
    //
    // A slow quota is where the two diverge, so that is what these use. They
    // also mean the derivation stays correct if the quota shape ever changes
    // (a per-minute window, a burst allowance) without anyone remembering that
    // a constant elsewhere encodes an assumption about it.

    /// A ten-minute replenish interval: the derived answer is 600, a fabricated
    /// one is 1.
    #[test]
    fn retry_after_follows_a_slow_quota_instead_of_a_constant() {
        assert_eq!(retry_after_secs(Duration::from_secs(600)), 600);
    }

    #[test]
    fn retry_after_rounds_up_rather_than_truncating() {
        // 1.4s truncated to 1 would retry into another rejection.
        assert_eq!(retry_after_secs(Duration::from_millis(1_400)), 2);
    }

    #[test]
    fn retry_after_never_tells_a_client_to_retry_immediately() {
        assert_eq!(retry_after_secs(Duration::ZERO), 1);
        assert_eq!(retry_after_secs(Duration::from_millis(1)), 1);
    }

    fn slow_quota(period: Duration, burst: u32) -> governor::Quota {
        governor::Quota::with_period(period)
            .unwrap()
            .allow_burst(NonZeroU32::new(burst).unwrap())
    }

    #[test]
    fn reset_is_zero_when_the_bucket_is_untouched() {
        let quota = slow_quota(Duration::from_secs(60), 5);
        assert_eq!(reset_secs(quota, 5, Duration::ZERO), 0);
    }

    #[test]
    fn reset_counts_every_missing_cell_not_just_the_next_one() {
        // 3 of 5 cells spent, one minute each to replenish → 180s to full.
        let quota = slow_quota(Duration::from_secs(60), 5);
        assert_eq!(reset_secs(quota, 2, Duration::ZERO), 180);
    }

    #[test]
    fn reset_on_a_drained_bucket_covers_the_whole_refill() {
        // Empty, with the next cell 60s out: still 5 × 60s to full capacity.
        let quota = slow_quota(Duration::from_secs(60), 5);
        assert_eq!(reset_secs(quota, 0, Duration::from_secs(60)), 300);
    }

    /// `Reset` is time-to-full and `Retry-After` is time-to-one-cell, so the
    /// former can never be the smaller of the two. A client that waited
    /// `Reset` and still got throttled would have no way to make progress.
    #[test]
    fn reset_is_never_shorter_than_retry_after() {
        let quota = slow_quota(Duration::from_secs(600), 1);
        let wait = Duration::from_secs(600);
        assert!(reset_secs(quota, 0, wait) >= retry_after_secs(wait));
    }

    // ── rate_limited_bucket — probe exemption ────────────────────────────────

    fn method_req(method: axum::http::Method, path: &str) -> Request<axum::body::Body> {
        Request::builder()
            .method(method)
            .uri(path)
            .body(axum::body::Body::empty())
            .unwrap()
    }

    #[test]
    fn probe_endpoints_bypass_the_rate_limiter() {
        for path in ["/health", "/ready", "/metrics"] {
            assert_eq!(
                rate_limited_bucket(&method_req(axum::http::Method::GET, path)),
                None,
                "{path} must return None so rate_limit_middleware skips it entirely"
            );
        }
    }

    /// The exemption is scoped to exactly the three probe paths — it must not
    /// widen into "every GET is exempt", which would silently undo the
    /// per-IP protection on payment enumeration and webhook listing.
    #[test]
    fn other_get_routes_still_fall_into_the_default_bucket() {
        assert_eq!(
            rate_limited_bucket(&method_req(axum::http::Method::GET, "/payments/abc")),
            Some("default")
        );
    }
}
