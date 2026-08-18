use crate::api::payments::{AppError, JsonBody};
use crate::{db, AppState};
use axum::{
    extract::{ConnectInfo, Path, Request, State},
    http::{header, HeaderValue, Method, StatusCode},
    middleware::{self, Next},
    response::IntoResponse,
    routing::{get, post},
    Json,
};
use moka::sync::Cache;
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use tower_http::{
    cors::CorsLayer,
    limit::RequestBodyLimitLayer,
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
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
    /// Bounded, TTL-evicting cache of per-(bucket, IP) rate limiters.
    ///
    /// Replaces the previous `Mutex<HashMap<...>>`:
    /// - Capacity is capped at `RATE_LIMITER_MAX_KEYS` entries (moka evicts
    ///   via a W-TinyLFU policy when the cap is hit).
    /// - Each entry expires `RATE_LIMITER_IDLE_TTL` after its last access,
    ///   so limiter state for quiet IPs is automatically reclaimed.
    /// - moka uses internal sharding, eliminating the single global lock that
    ///   the old `Mutex` imposed.
    limiters: Cache<String, Arc<governor::DefaultDirectRateLimiter>>,
}

impl RateLimitState {
    fn new(requests_per_sec: u32) -> Self {
        let limiters = Cache::builder()
            .max_capacity(RATE_LIMITER_MAX_KEYS)
            .time_to_idle(RATE_LIMITER_IDLE_TTL)
            .build();
        Self {
            requests_per_sec: requests_per_sec.max(1),
            limiters,
        }
    }
}

pub fn router(state: Arc<AppState>) -> axum::Router {
    let cors = build_cors(&state.config);
    let rate_limit = RateLimitState::new(state.config.rate_limit_requests_per_sec);
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
        .nest("/v1", api_v1(&state))
        .merge(api_v1(&state).layer(middleware::from_fn(mark_deprecated)))
        .fallback(not_found)
        .layer(PropagateRequestIdLayer::x_request_id())
        .layer(TraceLayer::new_for_http())
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
fn api_v1(state: &Arc<AppState>) -> axum::Router<Arc<AppState>> {
    /* Merchant provisioning and API key lifecycle. All admin-gated behind
    ADMIN_PROVISIONING_SECRET: this service has no self-service signup, and
    minting or revoking a credential is an operator action. */
    let merchants = axum::Router::new()
        .route("/", post(provision_merchant))
        .route("/:id/keys", post(issue_api_key).get(list_api_keys))
        .route("/:id/keys/:key_id", axum::routing::delete(revoke_api_key))
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
        .route("/:id/webhooks", get(payments::list_webhooks))
        .route(
            "/:id/webhooks/:delivery_id/redeliver",
            post(payments::redeliver_webhook),
        )
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
    let source_ip = client_ip_key(&req);

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
async fn provision_merchant(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    let merchant_id = uuid::Uuid::new_v4().to_string();
    let (raw_key, prefix) = db::generate_api_key();

    let key_id = db::create_merchant(&state.pool, &merchant_id, &raw_key, &prefix)
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

    Ok((
        StatusCode::CREATED,
        Json(json!({
            "merchant_id": merchant_id,
            "api_key": raw_key,
            "key_id": key_id,
        })),
    ))
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
    body: Option<JsonBody<IssueKeyRequest>>,
) -> Result<impl IntoResponse, AppError> {
    if !db::merchant_exists(&state.pool, &merchant_id).await? {
        return Err(AppError::not_found(
            "merchant_not_found",
            "merchant not found",
        ));
    }

    let label = body.and_then(|JsonBody(b)| b.label);
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

    tracing::info!(%merchant_id, %key_id, "api key issued");

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

    tracing::warn!(%merchant_id, %key_id, "api key revoked");
    Ok((
        StatusCode::OK,
        Json(json!({ "key_id": key_id, "revoked": true })),
    ))
}

#[derive(serde::Deserialize)]
struct IssueKeyRequest {
    label: Option<String>,
}

async fn rate_limit_middleware(
    State(rate_limit): State<RateLimitState>,
    req: Request,
    next: Next,
) -> axum::response::Response {
    if let Some(bucket) = rate_limited_bucket(&req) {
        let key = rate_limit_key(bucket, &req);
        let base_rps = rate_limit.requests_per_sec;
        let effective_rps = base_rps
            .saturating_mul(bucket_rate_multiplier(bucket))
            .max(1);
        /* `get_with` clones the `Arc` out of the cache rather than handing back a
        guard, so nothing borrowed from the cache is held across the `.await`
        below. */
        let limiter = rate_limit.limiters.get_with(key, || {
            Arc::new(governor::RateLimiter::direct(governor::Quota::per_second(
                NonZeroU32::new(effective_rps).unwrap(),
            )))
        });

        if limiter.check().is_err() {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                [(header::RETRY_AFTER, HeaderValue::from_static("1"))],
                Json(json!({
                    "error": "rate limit exceeded",
                    "code": "rate_limit_exceeded"
                })),
            )
                .into_response();
        }
    }

    next.run(req).await
}

/// Identifies which rate-limit bucket a request falls into.
///
/// Every request is assigned a bucket so all routes are protected by default.
/// Write and sensitive routes use named buckets that receive the base quota
/// (`requests_per_sec × 1`). Read-only routes fall into the `"default"` bucket
/// which receives a more generous quota (`requests_per_sec × 5`) to avoid
/// throttling normal polling.
///
/// Redelivery is bucketed by shape rather than by path: the URL carries a
/// payment and delivery id, and keying on those would let every id mint its
/// own limiter entry — both an unbounded map and a trivially bypassed limit.
fn rate_limited_bucket(req: &Request) -> Option<&'static str> {
    let path = req.uri().path();
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
    // All non-POST requests (GET, etc.) fall into the default bucket so that
    // payment enumeration, webhook listing, and health/ready probes are all
    // covered by a baseline limit.
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
fn rate_limit_key(bucket: &str, req: &Request) -> String {
    format!("{bucket}:{}", client_ip_key(req))
}

fn client_ip_key(req: &Request) -> String {
    if let Some(ConnectInfo(addr)) = req.extensions().get::<ConnectInfo<SocketAddr>>() {
        return addr.ip().to_string();
    }

    for name in ["x-forwarded-for", "x-real-ip"] {
        if let Some(value) = req.headers().get(name).and_then(|v| v.to_str().ok()) {
            if let Some(first) = value.split(',').map(str::trim).find(|s| !s.is_empty()) {
                return first.to_string();
            }
        }
    }

    "local".to_string()
}

/// The HTTP methods the strict CORS layer permits from a browser.
///
/// This is the complete set any mounted route responds to — `GET`, `POST`, and
/// `DELETE` (`DELETE /merchants/:id/keys/:key_id`, key revocation) — plus
/// `OPTIONS` for the preflight itself. `DELETE` was previously absent, so key
/// revocation failed preflight in every browser on a strict deployment (#281).
///
/// `tests/cors_tests.rs` checks this against the live router: the methods the
/// router's routes use must be exactly the non-`OPTIONS` methods here, so
/// dropping one — or adding a route whose method is missing here — fails CI.
const CORS_ALLOWED_METHODS: [Method; 4] =
    [Method::GET, Method::POST, Method::DELETE, Method::OPTIONS];

/// Request headers a browser may send. `content-type`/`authorization` cover a
/// JSON body and bearer auth; `idempotency-key` (safe retry of `POST /payments`)
/// and `x-admin-secret` (admin routes) are read by specific handlers and must
/// clear preflight too — without them, idempotent creation and all merchant/key
/// provisioning were unreachable from a browser on a strict deployment (#281).
///
/// Header names must be lowercase: `HeaderName::from_static` panics otherwise.
const CORS_ALLOWED_REQUEST_HEADERS: [&str; 4] = [
    "content-type",
    "authorization",
    "idempotency-key",
    "x-admin-secret",
];

/// Response headers a browser client is allowed to *read*. Without these on
/// `Access-Control-Expose-Headers` a browser can see the status but not the
/// header value. `x-request-id` is for support correlation; `deprecation`/`link`
/// are how a legacy (unprefixed) response advertises its `/v1` successor, which
/// exists specifically so a client can discover the move (#281).
const CORS_EXPOSED_HEADERS: [&str; 3] = ["x-request-id", "deprecation", "link"];

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
        .allow_methods(CORS_ALLOWED_METHODS)
        .allow_headers(CORS_ALLOWED_REQUEST_HEADERS.map(HeaderName::from_static))
        .expose_headers(CORS_EXPOSED_HEADERS.map(HeaderName::from_static))
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

async fn health() -> impl IntoResponse {
    Json(json!({ "status": "ok" }))
}

/// Readiness probe — returns 200 only when both the database AND Horizon are
/// reachable. A pod that cannot reach Horizon cannot detect on-chain payments;
/// routing traffic to it is worse than routing it elsewhere (issue #172).
///
/// Uses a 3-second timeout on the Horizon check so a slow node never hangs
/// the probe. The check is skipped when no gateway is configured
/// (STELLAR_GATEWAY_PUBLIC=UNCONFIGURED) since without a gateway there is no
/// on-chain work to do.
async fn ready(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    // 1. Database must respond.
    if db::ping(&state.pool).await.is_err() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "unavailable", "reason": "database unreachable" })),
        )
            .into_response();
    }

    // 2. Horizon must respond (only when a gateway wallet is configured).
    if state.config.gateway_configured() {
        if let Err(reason) = check_horizon_ready(&state).await {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "status": "unavailable", "reason": reason })),
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
    let body = crate::metrics::render(&state.webhook_metrics, &state.auth_metrics);
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
}
