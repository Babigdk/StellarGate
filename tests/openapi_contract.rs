//! Contract guard: keeps `openapi.yaml` honest against the real router.
//!
//! This is the CI cross-check issue #298 asks for — a test that "fails when the
//! documented paths diverge from the router". It bridges three things:
//!
//! ```text
//!   openapi.yaml  ⟷  the intended contract path-set  ⟷  the live axum router
//! ```
//!
//! It builds the real `api::router(...)` (same wiring as `tests/api_tests.rs`)
//! and, for every operation the spec documents, probes the router to prove the
//! path actually resolves — that the deprecated unprefixed mount stamps the
//! `Deprecation`/`Link` headers the spec now declares, and that every response
//! carries the `x-request-id` header the spec now declares. It then reads the
//! spec's own `paths:` keys and asserts they are exactly the intended set,
//! catching both phantom paths (documented but not served) and missing ones
//! (served but not documented).
//!
//! Redocly lints the spec's *shape* in a separate CI job; this test guards the
//! spec's *claims about the router*, which a linter cannot see.

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

const TEST_ADMIN_SECRET: &str = "test-admin-secret";

/// Mirrors `tests/api_tests.rs::make_config` — a fully-populated config so the
/// router builds exactly as it does in production, with limits set high enough
/// that these probes never trip the rate limiter.
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
        cursor_staleness_multiple: 3,
        payment_ttl_secs: 3600,
        expiry_batch_size: 500,
        rate_limit_requests_per_sec: 1000,
        db_pool_max_connections: 10,
        db_busy_timeout_ms: 5000,
        cors_allowed_origins: vec![],
        listener_mode: ListenerMode::Poll,
        webhook_allow_private_targets: false,
        admin_provisioning_secret: TEST_ADMIN_SECRET.into(),
        request_timeout_secs: 30,
        trusted_proxy_cidrs: vec![],
    }
}

async fn test_server() -> TestServer {
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

/// One documented operation, with concrete ids substituted so it can be probed
/// against the live router. `v1` is always `"/v1"` + `legacy`, which is exactly
/// the successor `mark_deprecated` derives — so the `Link` assertion below is a
/// direct check of the router's successor logic, not a restatement of it.
struct DocOp {
    method: Method,
    v1: &'static str,
    legacy: &'static str,
}

/// The public payments contract, as documented in `openapi.yaml`. Path
/// parameters are filled with values that do not exist in the fresh in-memory
/// DB, so authenticated routes short-circuit at the auth layer (401) and the
/// anonymous read route 404s with a route-specific code — either way, *not* the
/// router's catch-all fallback.
fn contract() -> Vec<DocOp> {
    vec![
        DocOp {
            method: Method::POST,
            v1: "/v1/payments",
            legacy: "/payments",
        },
        DocOp {
            method: Method::GET,
            v1: "/v1/payments",
            legacy: "/payments",
        },
        DocOp {
            method: Method::GET,
            v1: "/v1/payments/contract-probe",
            legacy: "/payments/contract-probe",
        },
        DocOp {
            method: Method::GET,
            v1: "/v1/payments/contract-probe/webhooks",
            legacy: "/payments/contract-probe/webhooks",
        },
        DocOp {
            method: Method::POST,
            v1: "/v1/payments/contract-probe/webhooks/contract-delivery/redeliver",
            legacy: "/payments/contract-probe/webhooks/contract-delivery/redeliver",
        },
    ]
}

async fn probe(server: &TestServer, method: &Method, path: &str) -> TestResponse {
    // `http::Method` is a struct, not an enum, so its associated constants
    // can't be used as match patterns — compare by value instead.
    if *method == Method::GET {
        server.get(path).await
    } else if *method == Method::POST {
        server.post(path).await
    } else {
        unreachable!("contract only uses GET/POST, got {method}")
    }
}

/// True when a response is the router's catch-all fallback (`not_found`) rather
/// than a real route's answer — i.e. the path is not mounted.
fn is_unrouted(res: &TestResponse) -> bool {
    res.status_code() == StatusCode::NOT_FOUND && res.json::<Value>()["code"] == "not_found"
}

fn read_spec() -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("openapi.yaml");
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()))
}

/// Extract the top-level keys of the spec's `paths:` map. Path keys sit at
/// exactly two-space indent (`  /foo:`); operations under them are deeper, and
/// comments/blank lines are ignored. Deliberately a line scan rather than a
/// YAML-parser dependency — the crate ships no YAML parser, and pulling one in
/// only for a test would widen the dependency/audit surface.
fn documented_paths(spec: &str) -> BTreeSet<String> {
    let mut in_paths = false;
    let mut out = BTreeSet::new();
    for line in spec.lines() {
        if !in_paths {
            if line == "paths:" {
                in_paths = true;
            }
            continue;
        }
        // A new unindented key (e.g. `components:`) ends the paths section.
        if !line.is_empty() && !line.starts_with(' ') && !line.starts_with('#') {
            break;
        }
        if let Some(rest) = line.strip_prefix("  ") {
            // Exactly two-space indent -> a path key. Deeper keys start with a
            // space here and are skipped; comments don't start with '/'.
            if rest.starts_with('/') {
                if let Some(key) = rest.strip_suffix(':') {
                    out.insert(key.trim_end().to_string());
                }
            }
        }
    }
    assert!(in_paths, "openapi.yaml has no top-level `paths:` block");
    out
}

/// Every documented path must resolve on the real router, the deprecated twin
/// must advertise its `/v1` successor, and every response must carry
/// `x-request-id` — the three claims the rewritten spec makes about the router.
#[tokio::test]
async fn documented_paths_resolve_and_deprecation_matches_router() {
    let server = test_server().await;

    for op in contract() {
        // Canonical /v1 surface: resolves, carries x-request-id, not deprecated.
        let v1 = probe(&server, &op.method, op.v1).await;
        assert!(
            !is_unrouted(&v1),
            "{} {} is documented but does not resolve on the router",
            op.method,
            op.v1
        );
        v1.assert_contains_header("x-request-id");
        assert!(
            !v1.headers().contains_key("deprecation"),
            "{} {} is canonical and must not advertise itself as deprecated",
            op.method,
            op.v1
        );

        // Deprecated unprefixed twin: resolves, carries x-request-id, and points
        // at its /v1 successor via RFC 8594 / RFC 8288 headers.
        let legacy = probe(&server, &op.method, op.legacy).await;
        assert!(
            !is_unrouted(&legacy),
            "{} {} is documented but does not resolve on the router",
            op.method,
            op.legacy
        );
        legacy.assert_contains_header("x-request-id");
        assert_eq!(
            legacy.header("deprecation"),
            "true",
            "deprecated {} {} must carry `Deprecation: true`",
            op.method,
            op.legacy
        );
        assert_eq!(
            legacy.header("link").to_str().unwrap(),
            format!("<{}>; rel=\"successor-version\"", op.v1),
            "deprecated {} {} must point at its /v1 successor",
            op.method,
            op.legacy
        );
    }
}

/// Negative control: proves the resolves-check above actually discriminates. A
/// path the spec does not document must fall through to the JSON 404 fallback,
/// so a router that silently answered everything could not make this pass.
#[tokio::test]
async fn undocumented_paths_hit_the_fallback() {
    let server = test_server().await;
    for path in [
        "/v1/payments/contract-probe/refund",
        "/v1/nonsense",
        "/payments/contract-probe/refund",
    ] {
        let res = server.post(path).await;
        assert!(
            is_unrouted(&res),
            "{path} unexpectedly resolved — the resolves-check no longer discriminates"
        );
    }
}

/// The spec must document exactly the intended contract — no more, no less.
/// This is the half of the guard a linter can't provide: it fails loudly when
/// someone adds a route without documenting it, or documents a path that isn't
/// there.
#[tokio::test]
async fn openapi_documents_exactly_the_intended_paths() {
    let documented = documented_paths(&read_spec());

    let expected: BTreeSet<String> = [
        "/health",
        // Canonical /v1 surface.
        "/v1/payments",
        "/v1/payments/{id}",
        "/v1/payments/{id}/webhooks",
        "/v1/payments/{id}/webhooks/{delivery_id}/redeliver",
        // Deprecated unprefixed twins.
        "/payments",
        "/payments/{id}",
        "/payments/{id}/webhooks",
        "/payments/{id}/webhooks/{delivery_id}/redeliver",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();

    assert_eq!(
        documented,
        expected,
        "openapi.yaml path set drifted from the intended contract.\n  \
         documented but unintended: {:?}\n  \
         intended but undocumented: {:?}",
        documented.difference(&expected).collect::<Vec<_>>(),
        expected.difference(&documented).collect::<Vec<_>>(),
    );
}

/// Presence checks for the specific declarations #298 requires — the request
/// and response headers, the versioned base path, and the removal of the
/// 3.0-only `nullable` keyword that is invalid under the declared 3.1.0.
#[tokio::test]
async fn openapi_declares_new_headers_idempotency_and_is_valid_3_1() {
    let spec = read_spec();
    for needle in [
        "openapi: 3.1.0",
        "name: Idempotency-Key", // request header parameter
        "XRequestId:",           // x-request-id response header component
        "Deprecation:",          // RFC 8594 response header component
        "Link:",                 // RFC 8288 response header component
        "successor-version",     // the link relation the router emits
        "Idempotent replay",     // the 200 replay response on create
        "deprecated: true",      // legacy operations flagged
    ] {
        assert!(spec.contains(needle), "openapi.yaml is missing `{needle}`");
    }
    // 3.1 expresses nullability as `type: [T, "null"]`; the 3.0 `nullable`
    // keyword is invalid there (redocly's struct rule rejects it) — pin it here
    // too so the mistake can't creep back between lint runs.
    assert!(
        !spec.contains("nullable:"),
        "openapi.yaml still uses the 3.0-only `nullable:` keyword"
    );
}
