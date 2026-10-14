use super::format::{Envelope, Payload, SERVICE_ID, SIGNING_PREFIX};

const ENVELOPE: &str = r#"{"key_id":"test-rfc8032-01","payload":"e30","signature":"AA"}"#;
const PAYLOAD: &str = r#"{"version":1,"key_id":"test-rfc8032-01","authorization_id":"test-001","authorized_to":"Example test customer","services":["channels-manager-v1"]}"#;

#[test]
fn decodes_envelope_fields_without_claiming_verification() {
    let value: Envelope = serde_json::from_str(ENVELOPE).unwrap();
    assert_eq!(value.key_id, "test-rfc8032-01");
    assert_eq!(value.payload, "e30");
    assert_eq!(value.signature, "AA");
}

#[test]
fn decodes_payload_and_protocol_constants() {
    let value: Payload = serde_json::from_str(PAYLOAD).unwrap();
    assert_eq!(value.version, 1);
    assert_eq!(value.key_id, "test-rfc8032-01");
    assert_eq!(value.authorization_id, "test-001");
    assert_eq!(value.authorized_to, "Example test customer");
    assert_eq!(value.services, [SERVICE_ID]);
    assert_eq!(SIGNING_PREFIX, b"application-authorization/v1\n");
}

fn assert_strict_fields<T: serde::de::DeserializeOwned>(valid: &str) {
    let object: serde_json::Map<String, serde_json::Value> = serde_json::from_str(valid).unwrap();
    for (field, original) in &object {
        let mut missing = object.clone();
        missing.remove(field);
        assert!(
            serde_json::from_value::<T>(missing.into()).is_err(),
            "missing {field}"
        );
        for replacement in [
            serde_json::Value::Null,
            serde_json::json!({}),
            serde_json::json!(true),
        ] {
            let mut malformed = object.clone();
            malformed.insert(field.clone(), replacement);
            assert!(
                serde_json::from_value::<T>(malformed.into()).is_err(),
                "wrong type {field}"
            );
        }
        let duplicate = format!("{},\"{field}\":{original}}}", &valid[..valid.len() - 1]);
        assert!(
            serde_json::from_str::<T>(&duplicate).is_err(),
            "duplicate {field}"
        );
    }
    let unknown = format!("{},\"unknown\":true}}", &valid[..valid.len() - 1]);
    for malformed in [
        unknown,
        format!("{valid} garbage"),
        format!("{valid} {{}}"),
        "null".into(),
        "[]".into(),
    ] {
        assert!(
            serde_json::from_str::<T>(&malformed).is_err(),
            "accepted {malformed}"
        );
    }
}

#[test]
fn rejects_malformed_envelopes() {
    assert_strict_fields::<Envelope>(ENVELOPE);
    assert!(serde_json::from_str::<Envelope>(r#"["test-rfc8032-01","e30","AA"]"#).is_err());
    for field in ["key_id", "payload", "signature"] {
        let mut value: serde_json::Value = serde_json::from_str(ENVELOPE).unwrap();
        for replacement in [serde_json::json!(1), serde_json::json!([])] {
            value[field] = replacement;
            assert!(serde_json::from_value::<Envelope>(value.clone()).is_err());
        }
    }
}

#[test]
fn rejects_malformed_payloads() {
    assert_strict_fields::<Payload>(PAYLOAD);
    for malformed in [
        r#"[1,"test-rfc8032-01","test-001","Example test customer",["channels-manager-v1"]]"#
            .into(),
        PAYLOAD.replace("\"version\":1", "\"version\":1.0"),
        PAYLOAD.replace("\"version\":1", "\"version\":\"1\""),
        PAYLOAD.replace("\"version\":1", "\"version\":-1"),
        PAYLOAD.replace("\"version\":1", "\"version\":256"),
    ] {
        assert!(
            serde_json::from_str::<Payload>(&malformed).is_err(),
            "accepted {malformed}"
        );
    }
    let mut value: serde_json::Value = serde_json::from_str(PAYLOAD).unwrap();
    for services in [
        serde_json::json!("channels-manager-v1"),
        serde_json::json!([1]),
        serde_json::json!([null]),
        serde_json::json!([[]]),
    ] {
        value["services"] = services;
        assert!(serde_json::from_value::<Payload>(value.clone()).is_err());
    }
}

#[test]
fn structural_decode_leaves_semantic_checks_to_verifier() {
    let payload: Payload = serde_json::from_str(
        r#"{"version":2,"key_id":"","authorization_id":"","authorized_to":"","services":[]}"#,
    )
    .unwrap();
    assert_eq!(payload.version, 2);
    assert!(payload.services.is_empty());
}

#[test]
fn stored_cross_language_vectors_have_expected_structure() {
    let fixtures: serde_json::Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/authorization/vectors.json"
    ))
    .unwrap();
    let rfc = &fixtures["rfc8032_test1"];
    assert_eq!(rfc["message_hex"], "");
    assert_eq!(rfc["public_key_hex"].as_str().unwrap().len(), 64);
    assert_eq!(rfc["signature_hex"].as_str().unwrap().len(), 128);
    assert_eq!(rfc["expected_signature_valid"], true);
    let vectors = fixtures["authorization_vectors"].as_array().unwrap();
    assert_eq!(vectors.len(), 3);
    for (vector, (name, signature_valid, service_allowed, authorized)) in vectors.iter().zip([
        ("valid", true, true, true),
        ("tampered-payload", false, true, false),
        ("wrong-service", true, false, false),
    ]) {
        assert_eq!(vector["name"], name);
        assert_eq!(vector["expected_signature_valid"], signature_valid);
        assert_eq!(vector["expected_service_allowed"], service_allowed);
        assert_eq!(vector["expected_authorized"], authorized);
        let envelope: Envelope = serde_json::from_value(vector["envelope"].clone()).unwrap();
        let bytes = vector["payload_utf8"].as_str().unwrap();
        let payload: Payload = serde_json::from_str(bytes).unwrap();
        assert_eq!(payload.key_id, envelope.key_id);
        assert_eq!(payload.key_id, "test-rfc8032-01");
        assert_eq!(payload.version, 1);
        assert_eq!(
            payload.services.iter().any(|service| service == SERVICE_ID),
            service_allowed
        );
        let hex: String = bytes
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        assert_eq!(vector["payload_hex"], hex);
        assert_eq!(vector["public_key_hex"], rfc["public_key_hex"]);
        assert_eq!(vector["signature_hex"].as_str().unwrap().len(), 128);
        assert!(
            envelope
                .payload
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
        );
        assert!(
            envelope
                .signature
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
        );
    }
}
