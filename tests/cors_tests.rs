//! CORS contract guard (issue #281).
//!
//! When `CORS_ALLOWED_ORIGINS` is set, `build_cors` produces a *strict* layer:
//! only the listed origins, methods, and request headers clear a browser
//! preflight, and only the listed response headers are readable. Three features
//! live behind a method or header the original strict layer omitted:
//!
//!   * key revocation — `DELETE /merchants/:id/keys/:key_id`
//!   * idempotent create — the `Idempotency-Key` request header on `POST /payments`
//!   * merchant/key admin — the `X-Admin-Secret` request header
//!
//! so on a properly configured production deployment they failed preflight in
//! every browser, while working on testnet's permissive fallback — a gap that is
//! invisible until you ship.
//!
//! These tests build the real `api::router(...)` with a concrete origin (the
//! strict branch) and check the layer's *observable* behaviour against the
//! *live router*: every route the API serves must resolve, and the methods those
//! routes use must be exactly the non-`OPTIONS` methods the layer advertises.
//! Drop `DELETE` from the layer and `strict_cors_allow_methods_match_the_routers_methods`
//! fails; add a route with a method the layer omits and it fails too.

use axum::http::{Method, StatusCode};
use axum_test::{TestResponse, TestServer};
use serde_json::Value;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::collections::BTreeSet;
use std::str::FromStr;
use std::sync::Arc;
use stellargate::{
    api,
    config::{Config, ListenerMode},
    db, AppState,
};

/// An origin in the strict allow-list. A preflight from this origin is answered;
/// one from any other origin is not (see `strict_cors_denies_unlisted_origin`).
const ALLOWED_ORIGIN: &str = "https://app.example.com";
const TEST_ADMIN_SECRET: &str = "test-admin-secret";

/// Mirrors the `make_config` in the other integration suites, but with
/// `cors_allowed_origins` populated so `build_cors` takes its strict, non-
/// permissive branch — the one that ships on a `public` deployment, where this
/// bug bites.
fn make_config() -> Config {
    Config {
        port: 0,
        database_url: "sqlite::memory:".into(),
        network: "testnet".into(),
        horizon_url: String::new(),
        gateway_public: "UNCONFIGURED".into(),
        accepted_assets: stellargate::config::AcceptedAsset::default_list(),
        webhook_secret: String::new(),
        webhook_retry_attempts: 1,
        webhook_retry_delay_ms: 0,
        allowed_webhook_schemes: vec!["https".into(), "http".into()],
        webhook_timeout_secs: 10,
        webhook_redrive_interval_secs: 30,
        webhook_redrive_concurrency: 4,
        webhook_redrive_max_attempts: 8,
        webhook_redrive_grace_secs: 60,
        webhook_redrive_backoff_initial_secs: 0,
        webhook_redrive_backoff_max_secs: 0,
        retention_interval_secs: 3600,
        webhook_delivery_retention_days: 30,
        idempotency_retention_days: 7,
        poll_interval_secs: 10,
        payment_ttl_secs: 3600,
        rate_limit_requests_per_sec: 1000,
        db_pool_max_connections: 10,
        db_busy_timeout_ms: 5000,
        cors_allowed_origins: vec![ALLOWED_ORIGIN.into()],
        listener_mode: ListenerMode::Poll,
        webhook_allow_private_targets: false,
        admin_provisioning_secret: TEST_ADMIN_SECRET.into(),
        request_timeout_secs: 30,
    }
}

async fn strict_cors_server() -> TestServer {
    let cfg = make_config();
    let pool = SqlitePoolOptions::new()
        .connect_with(
            SqliteConnectOptions::from_str(&cfg.database_url)
                .unwrap()
                .create_if_missing(true),
        )
        .await
        .unwrap();
    db::migrate(&pool).await.unwrap();
    let router = api::router(Arc::new(AppState {
        pool,
        config: cfg,
        http: reqwest::Client::new(),
        webhook_http: reqwest::Client::new(),
        webhook_metrics: stellargate::metrics::WebhookMetrics::new(),
        auth_metrics: stellargate::metrics::AuthMetrics::new(),
        task_health: stellargate::TaskHealth::new(),
    }))
    .into_make_service_with_connect_info::<std::net::SocketAddr>();
    TestServer::new(router).unwrap()
}

/// Issue a CORS preflight (`OPTIONS` + the two `Access-Control-Request-*`
/// headers) for `method` on `path`, from the allowed origin.
async fn preflight(
    server: &TestServer,
    method: &str,
    path: &str,
    request_headers: &str,
) -> TestResponse {
    server
        .method(Method::OPTIONS, path)
        .add_header("Origin", ALLOWED_ORIGIN)
        .add_header("Access-Control-Request-Method", method)
        .add_header("Access-Control-Request-Headers", request_headers)
        .await
}

/// Split a comma-separated response header into a lowercased set. Header names
/// are case-insensitive, so lowercasing lets `contains`/equality ignore the
/// casing tower-http happens to emit.
fn header_set(res: &TestResponse, name: &str) -> BTreeSet<String> {
    res.header(name)
        .to_str()
        .unwrap()
        .split(',')
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Criterion: "a test asserts the strict (non-permissive) CORS layer permits a
/// DELETE preflight with X-Admin-Secret." This is the exact preflight a browser
/// sends before an admin UI calls `DELETE /merchants/:id/keys/:key_id`.
#[tokio::test]
async fn strict_cors_permits_delete_preflight_with_admin_secret() {
    let server = strict_cors_server().await;

    let res = preflight(
        &server,
        "DELETE",
        "/v1/merchants/m/keys/k",
        "x-admin-secret",
    )
    .await;

    // The preflight is answered (not the JSON 404 fallback, not a hard rejection).
    assert!(
        res.status_code().is_success(),
        "strict CORS preflight for DELETE was not answered: {}",
        res.status_code()
    );
    // The requesting origin is echoed — the layer is strict, not deny-all.
    assert_eq!(
        res.header("access-control-allow-origin").to_str().unwrap(),
        ALLOWED_ORIGIN
    );

    let methods = header_set(&res, "access-control-allow-methods");
    assert!(
        methods.contains("delete"),
        "DELETE not permitted by strict CORS; allow-methods = {methods:?}"
    );

    let headers = header_set(&res, "access-control-allow-headers");
    assert!(
        headers.contains("x-admin-secret"),
        "X-Admin-Secret not permitted by strict CORS; allow-headers = {headers:?}"
    );
}

/// Criterion: "every request header the API accepts is in the allow-list."
/// Covers the two the original layer had plus the two it dropped.
#[tokio::test]
async fn strict_cors_permits_every_request_header_the_api_reads() {
    let server = strict_cors_server().await;

    // A checkout page's preflight before `POST /payments`: a JSON body
    // (Content-Type) with a safe-retry key (Idempotency-Key).
    let res = preflight(
        &server,
        "POST",
        "/v1/payments",
        "content-type, authorization, idempotency-key",
    )
    .await;

    let headers = header_set(&res, "access-control-allow-headers");
    for needed in [
        "content-type",
        "authorization",
        "idempotency-key",
        "x-admin-secret",
    ] {
        assert!(
            headers.contains(needed),
            "request header `{needed}` is not in the strict CORS allow-list: {headers:?}"
        );
    }
}

/// Criterion: "X-Request-Id, Deprecation and Link are exposed to browser
/// clients." `Access-Control-Expose-Headers` is returned on the *actual*
/// response, not the preflight, so this probes a real request.
#[tokio::test]
async fn strict_cors_exposes_request_id_and_deprecation_headers() {
    let server = strict_cors_server().await;

    let res = server
        .get("/health")
        .add_header("Origin", ALLOWED_ORIGIN)
        .await;
    res.assert_status_ok();

    let exposed = header_set(&res, "access-control-expose-headers");
    for needed in ["x-request-id", "deprecation", "link"] {
        assert!(
            exposed.contains(needed),
            "response header `{needed}` is not exposed to browser clients: {exposed:?}"
        );
    }
}

/// One route the router serves. `method`/`path` double as a resolution probe:
/// hitting `path` with `method` must not fall through to the catch-all 404.
struct Route {
    method: &'static str,
    path: &'static str,
}

/// Every route the API serves, on the canonical `/v1` mount (the CORS layer
/// wraps the whole router, so the legacy twin's methods are identical), plus the
/// one operational route in the documented contract.
///
/// Kept explicit because axum exposes no route enumeration — but guarded below:
/// each entry is proven to resolve, so a renamed or removed route breaks the
/// test and forces this list back into sync with the router.
fn served_routes() -> Vec<Route> {
    vec![
        Route {
            method: "GET",
            path: "/health",
        },
        Route {
            method: "POST",
            path: "/v1/merchants",
        },
        Route {
            method: "GET",
            path: "/v1/merchants/m/keys",
        },
        Route {
            method: "POST",
            path: "/v1/merchants/m/keys",
        },
        Route {
            method: "DELETE",
            path: "/v1/merchants/m/keys/k",
        },
        Route {
            method: "POST",
            path: "/v1/payments",
        },
        Route {
            method: "GET",
            path: "/v1/payments",
        },
        Route {
            method: "GET",
            path: "/v1/payments/p",
        },
        Route {
            method: "GET",
            path: "/v1/payments/p/webhooks",
        },
        Route {
            method: "POST",
            path: "/v1/payments/p/webhooks/d/redeliver",
        },
    ]
}

async fn probe(server: &TestServer, method: &str, path: &str) -> TestResponse {
    // `http::Method` is a struct, so its constants can't be match patterns —
    // dispatch on the string instead.
    match method {
        "GET" => server.get(path).await,
        "POST" => server.post(path).await,
        "DELETE" => server.delete(path).await,
        other => panic!("served_routes uses an unhandled method: {other}"),
    }
}

/// True when a response is the router's catch-all fallback (`not_found`) rather
/// than a real route's answer — i.e. the path is not mounted. Mirrors
/// `tests/openapi_contract.rs`.
fn is_unrouted(res: &TestResponse) -> bool {
    res.status_code() == StatusCode::NOT_FOUND && res.json::<Value>()["code"] == "not_found"
}

/// Criterion: "the lists are derived from, or checked against, the router so a
/// new route cannot be forgotten." Ties the CORS method list to what the router
/// actually serves, both directions:
///
///   * Drop `DELETE` from the layer (the #281 bug) → advertised ≠ served → fail.
///   * Add a route with a new method → served has it, advertised doesn't → fail.
///   * Add a method to the layer no route uses → advertised has it, served
///     doesn't → fail.
#[tokio::test]
async fn strict_cors_allow_methods_match_the_routers_methods() {
    let server = strict_cors_server().await;

    // 1. Every inventoried route resolves, so the methods we collect genuinely
    //    reflect the live router rather than a stale fixture.
    let mut router_methods = BTreeSet::new();
    for route in served_routes() {
        let res = probe(&server, route.method, route.path).await;
        assert!(
            !is_unrouted(&res),
            "{} {} is in the CORS route inventory but does not resolve on the router",
            route.method,
            route.path
        );
        router_methods.insert(route.method.to_ascii_lowercase());
    }

    // 2. The strict layer's advertised methods, minus the preflight's own
    //    OPTIONS, must be exactly the methods those routes use.
    let res = preflight(&server, "GET", "/v1/payments", "authorization").await;
    let mut advertised = header_set(&res, "access-control-allow-methods");
    advertised.remove("options");

    assert_eq!(
        advertised,
        router_methods,
        "strict CORS allow-methods (minus OPTIONS) must equal the methods the router serves\n  \
         advertised but unserved: {:?}\n  \
         served but not advertised: {:?}",
        advertised.difference(&router_methods).collect::<Vec<_>>(),
        router_methods.difference(&advertised).collect::<Vec<_>>(),
    );
}

/// Negative control: proves the layer really is non-permissive. An origin that
/// is not on the list gets no `Access-Control-Allow-Origin`, so the browser
/// blocks the response — this is what makes the criteria above meaningful rather
/// than an artefact of `CorsLayer::permissive()`.
#[tokio::test]
async fn strict_cors_denies_unlisted_origin() {
    let server = strict_cors_server().await;

    let res = server
        .method(Method::OPTIONS, "/v1/payments")
        .add_header("Origin", "https://evil.example.com")
        .add_header("Access-Control-Request-Method", "POST")
        .await;

    assert!(
        !res.headers().contains_key("access-control-allow-origin"),
        "strict CORS must not echo an unlisted origin"
    );
}
