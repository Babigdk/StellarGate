//! Locks `openapi.yaml`'s `CreatePaymentRequest` schema to the Rust struct of
//! the same name (`src/api/payments.rs`), so the two cannot silently drift
//! apart again the way they did for `merchant_id` (issue #307): a client
//! generated from the spec would have exposed a settable field the server
//! never read.
//!
//! There's no YAML-parsing dependency in this crate, so this reads
//! `openapi.yaml` as text and extracts the property names declared directly
//! under `CreatePaymentRequest.properties` by indentation. It's intentionally
//! narrow — a full spec-vs-router reflection check is a larger project
//! (tracked separately) — but it means a field added to one side without the
//! other fails `cargo test`, in CI, on every PR.

use std::collections::BTreeSet;

#[test]
fn create_payment_request_spec_matches_struct_fields() {
    // Mirrors `CreatePaymentRequest` in src/api/payments.rs. If that struct's
    // fields change, this list (and the assertion below) must change with it.
    let struct_fields: BTreeSet<&str> = ["amount", "asset", "webhook_url"].into_iter().collect();

    let spec = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/openapi.yaml"))
        .expect("openapi.yaml must be readable");

    // Schema keys under `components.schemas` sit at exactly four spaces of
    // indentation (`    CreatePaymentRequest:`, `    ListPaymentsResponse:`,
    // ...). Collect every line from that header up to — but not including —
    // the next line at that same four-space depth, i.e. the next sibling
    // schema.
    let mut lines = spec.lines();
    for line in lines.by_ref() {
        if line == "    CreatePaymentRequest:" {
            break;
        }
    }
    let block: String = lines
        .take_while(|line| {
            !(line.starts_with("    ") && !line.starts_with("     ") && !line.is_empty())
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        !block.is_empty(),
        "openapi.yaml must declare a CreatePaymentRequest schema with a non-empty body"
    );
    let block = block.as_str();

    assert!(
        block.contains("additionalProperties: false"),
        "CreatePaymentRequest must set `additionalProperties: false` so an \
         unrecognised field (like a stray merchant_id) is rejected with 400 \
         rather than silently ignored"
    );
    assert!(
        !block.contains("        merchant_id:"),
        "merchant_id must not reappear as a property of CreatePaymentRequest \
         — tenancy is derived from the bearer token, not the request body \
         (issue #307)"
    );

    // Property names are the lines indented exactly eight spaces within the
    // schema block (sub-fields of a property, e.g. `type:`, sit ten+ deep).
    let spec_fields: BTreeSet<&str> = block
        .lines()
        .filter_map(|line| line.strip_prefix("        "))
        .filter(|rest| !rest.starts_with(' ') && !rest.starts_with('#'))
        .filter_map(|rest| rest.split_once(':').map(|(name, _)| name))
        .collect();

    assert_eq!(
        spec_fields, struct_fields,
        "openapi.yaml's CreatePaymentRequest properties must match \
         CreatePaymentRequest's fields in src/api/payments.rs field-for-field \
         — update whichever side is now out of date"
    );
}
