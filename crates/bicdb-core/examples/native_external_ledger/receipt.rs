//! Shared example/integration-test receipt parser. These are DATA, never DB authority.
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeSet;
use std::io;
use uuid::Uuid;

fn invalid_schema(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Receipt {
    pub(super) format: u32,
    pub(super) action: String,
    pub(super) run_id: Uuid,
    pub(super) producer_id: Uuid,
    pub(super) session_id: Uuid,
    pub(super) payload_sha256: String,
    pub(super) payload_bytes: usize,
    pub(super) collector_certificate_sha256: String,
    pub(super) client_certificate_sha256: String,
    pub(super) receipt_sequence: u64,
    #[serde(default)]
    pub(super) name: Option<String>,
    #[serde(default)]
    pub(super) sequence: Option<u64>,
    #[serde(default)]
    pub(super) record_sha256: Option<String>,
    #[serde(default)]
    pub(super) previous_record_sha256: Option<String>,
    #[serde(default)]
    pub(super) durable_end_offset: Option<u64>,
}

pub(super) fn decode_receipt(raw: &[u8], action: &str) -> io::Result<Receipt> {
    // Keep derived strict type/duplicate-field validation FIRST. A Value-only
    // parser would silently collapse duplicate keys before checking the set.
    let receipt: Receipt = serde_json::from_slice(raw)?;
    let fields: serde_json::Map<String, Value> = serde_json::from_slice(raw)?;
    let mut expected: BTreeSet<&str> = [
        "format",
        "action",
        "run_id",
        "producer_id",
        "session_id",
        "payload_sha256",
        "payload_bytes",
        "collector_certificate_sha256",
        "client_certificate_sha256",
        "receipt_sequence",
    ]
    .into_iter()
    .collect();
    match action {
        "start" => {}
        "input" => {
            expected.insert("name");
        }
        "row" => expected.extend([
            "sequence",
            "record_sha256",
            "previous_record_sha256",
            "durable_end_offset",
        ]),
        _ => return Err(invalid_schema("unsupported producer receipt action")),
    }
    let actual: BTreeSet<&str> = fields.keys().map(String::as_str).collect();
    // `previous_record_sha256:null` is mandatory on the FIRST row. Missing
    // cannot default to None, and forbidden extras cannot hide as JSON null.
    if actual != expected || receipt.action != action {
        return Err(invalid_schema(
            "collector receipt exact action-specific field set mismatch",
        ));
    }
    if (action == "input" && receipt.name.is_none())
        || (action == "row"
            && (receipt.sequence.is_none()
                || receipt.record_sha256.is_none()
                || receipt.durable_end_offset.is_none()))
    {
        return Err(invalid_schema("collector receipt required value is null"));
    }
    Ok(receipt)
}

#[cfg(test)]
#[path = "receipt_tests.rs"]
mod tests;
