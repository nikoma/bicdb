//! Example parser controls only, not a TLS/SDK/database or durability proof.
//! Exercise the SAME decode_receipt used before the sink returns a receipt.
use super::*;
use serde_json::json;

fn raw(action: &str) -> Value {
    let mut value = json!({
        "format":1,"action":action,
        "run_id":"019a80b8-6594-7000-8000-000000000001",
        "producer_id":"019a80b8-6594-7000-8000-000000000010",
        "session_id":"019a80b8-6594-7000-8000-000000000011",
        "payload_sha256":"1".repeat(64),"payload_bytes":200,
        "collector_certificate_sha256":"2".repeat(64),
        "client_certificate_sha256":"3".repeat(64),"receipt_sequence":6,
    });
    if action == "input" {
        value["name"] = json!("plan.json");
    } else if action == "row" {
        value["sequence"] = json!(1);
        value["record_sha256"] = json!("4".repeat(64));
        value["previous_record_sha256"] = Value::Null;
        value["durable_end_offset"] = json!(200);
    }
    value
}

#[test]
fn all_action_receipt_typed_values_are_preserved() {
    for action in ["start", "input", "row"] {
        let receipt = decode_receipt(&serde_json::to_vec(&raw(action)).unwrap(), action).unwrap();
        assert_eq!(receipt.format, 1);
        assert_eq!(receipt.action, action);
        assert!(!receipt.run_id.is_nil());
        assert!(!receipt.producer_id.is_nil());
        assert!(!receipt.session_id.is_nil());
        assert_eq!(receipt.payload_sha256, "1".repeat(64));
        assert_eq!(receipt.payload_bytes, 200);
        assert_eq!(receipt.collector_certificate_sha256, "2".repeat(64));
        assert_eq!(receipt.client_certificate_sha256, "3".repeat(64));
        assert_eq!(receipt.receipt_sequence, 6);
        assert_eq!(
            receipt.name.as_deref(),
            (action == "input").then_some("plan.json")
        );
        assert_eq!(receipt.sequence, (action == "row").then_some(1));
        assert_eq!(receipt.durable_end_offset, (action == "row").then_some(200));
        assert_eq!(
            receipt.record_sha256,
            (action == "row").then(|| "4".repeat(64))
        );
        assert!(receipt.previous_record_sha256.is_none());
    }
}

#[test]
fn first_row_explicit_null_is_valid_but_missing_previous_hash_is_refused() {
    let mut value = raw("row");
    let decoded = decode_receipt(&serde_json::to_vec(&value).unwrap(), "row").unwrap();
    assert!(decoded.previous_record_sha256.is_none());
    value
        .as_object_mut()
        .unwrap()
        .remove("previous_record_sha256");
    let error = decode_receipt(&serde_json::to_vec(&value).unwrap(), "row")
        .err()
        .unwrap();
    assert!(error
        .to_string()
        .contains("exact action-specific field set"));
}

#[test]
fn start_and_input_refuse_each_forbidden_row_field_even_when_null() {
    for action in ["start", "input"] {
        let baseline = raw(action);
        assert!(decode_receipt(&serde_json::to_vec(&baseline).unwrap(), action).is_ok());
        for field in [
            "sequence",
            "record_sha256",
            "previous_record_sha256",
            "durable_end_offset",
        ] {
            let mut damaged = baseline.clone();
            damaged[field] = Value::Null;
            let error = decode_receipt(&serde_json::to_vec(&damaged).unwrap(), action)
                .err()
                .unwrap();
            assert!(
                error
                    .to_string()
                    .contains("exact action-specific field set"),
                "{action}/{field}: {error}"
            );
        }
    }
}

#[test]
fn each_action_requires_all_own_fields_and_refuses_unknown_fields() {
    for action in ["start", "input", "row"] {
        let baseline = raw(action);
        for field in baseline.as_object().unwrap().keys() {
            let mut damaged = baseline.clone();
            damaged.as_object_mut().unwrap().remove(field);
            assert!(
                decode_receipt(&serde_json::to_vec(&damaged).unwrap(), action).is_err(),
                "{action}/{field}"
            );
        }
        let mut unknown = baseline.clone();
        unknown["unknown_even_if_null"] = Value::Null;
        assert!(decode_receipt(&serde_json::to_vec(&unknown).unwrap(), action).is_err());
    }
}

#[test]
fn derived_duplicate_field_refusal_survives_raw_field_set_validation() {
    for action in ["start", "input", "row"] {
        let original = serde_json::to_string(&raw(action)).unwrap();
        let duplicate = format!("{{\"format\":1,{}", &original[1..]);
        let error = decode_receipt(duplicate.as_bytes(), action).err().unwrap();
        assert!(error.to_string().contains("duplicate field"), "{error}");
    }
    let original = serde_json::to_string(&raw("row")).unwrap();
    let duplicate = format!("{{\"previous_record_sha256\":null,{}", &original[1..]);
    assert!(decode_receipt(duplicate.as_bytes(), "row").is_err());
}

#[test]
fn typed_integer_fields_refuse_boolean_fractional_or_null_values() {
    for action in ["start", "input", "row"] {
        for field in ["format", "payload_bytes", "receipt_sequence"] {
            for invalid_value in [json!(true), json!(1.5), Value::Null] {
                let mut value = raw(action);
                value[field] = invalid_value;
                assert!(decode_receipt(&serde_json::to_vec(&value).unwrap(), action).is_err());
            }
        }
    }
    for field in ["sequence", "record_sha256", "durable_end_offset"] {
        let mut value = raw("row");
        value[field] = Value::Null;
        assert!(decode_receipt(&serde_json::to_vec(&value).unwrap(), "row").is_err());
    }
    let original = serde_json::to_string(&raw("start")).unwrap();
    let overflow = original.replace(
        "\"receipt_sequence\":6",
        "\"receipt_sequence\":18446744073709551616",
    );
    assert_ne!(
        original, overflow,
        "oversized counter must actually be injected"
    );
    assert!(decode_receipt(overflow.as_bytes(), "start").is_err());
}

#[test]
fn parser_refuses_mismatched_action_or_non_object_receipt() {
    assert!(decode_receipt(&serde_json::to_vec(&raw("start")).unwrap(), "row").is_err());
    assert!(decode_receipt(&serde_json::to_vec(&raw("row")).unwrap(), "start").is_err());
    assert!(decode_receipt(b"[]", "start").is_err());
    assert!(decode_receipt(b"null", "start").is_err());
    assert!(decode_receipt(&serde_json::to_vec(&raw("start")).unwrap(), "witness").is_err());
}
