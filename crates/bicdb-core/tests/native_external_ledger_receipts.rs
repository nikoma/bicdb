//! The normal Core `cargo test -p bicdb-core` executes this integration target.
//! It imports the SAME parser used by the example; no TLS/SDK/DB is simulated.
//! Actual execution remains unclaimed until its real test-result log exists.
#[path = "../examples/native_external_ledger/receipt.rs"]
mod receipt_schema;
