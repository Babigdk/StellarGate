const DASHBOARD_JS: &str = include_str!("../static/dashboard.js");

#[test]
fn dashboard_api_requests_use_canonical_v1_base() {
    assert!(
        DASHBOARD_JS.contains(r#"var API_BASE = "/v1";"#),
        "the dashboard must define the canonical API version once"
    );
    assert!(
        DASHBOARD_JS.contains("return fetch(API_BASE + path,"),
        "the authenticated API helper must prefix every request with API_BASE"
    );

    let direct_fetches: Vec<_> = DASHBOARD_JS
        .lines()
        .filter(|line| line.contains("fetch("))
        .map(str::trim)
        .collect();
    assert_eq!(
        direct_fetches,
        [
            "return fetch(API_BASE + path, { method: opts.method || \"GET\", headers: headers }).then(",
            "fetch(\"/\")",
            "fetch(\"/ready\", { headers: { Accept: \"application/json\" } })",
        ],
        "new dashboard fetches must use the versioned API helper unless they target an explicitly unversioned operational endpoint"
    );

    assert_eq!(
        DASHBOARD_JS.matches("/v1").count(),
        1,
        "API_BASE must be the only /v1 literal so requests cannot become /v1/v1/..."
    );
}
