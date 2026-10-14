use super::*;
use ed25519_dalek::{Signer, SigningKey};
use serde_json::{Value, json};

const TEST_ID: &str = "test-rfc8032-01";
fn hex<const N: usize>(s: &str) -> [u8; N] {
    std::array::from_fn(|i| u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).unwrap())
}
fn test_key() -> [u8; 32] {
    hex("d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a")
}
fn claims() -> Value {
    json!({"version":1,"key_id":TEST_ID,"authorization_id":"test-001","authorized_to":"Example test customer","services":[format::SERVICE_ID]})
}
fn signed(raw: &[u8]) -> Value {
    // Published RFC 8032 test vector 1 seed, never an owner private key.
    let key = SigningKey::from_bytes(&hex(
        "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60",
    ));
    let message = [format::SIGNING_PREFIX, raw].concat();
    json!({"key_id":TEST_ID,"payload":URL_SAFE_NO_PAD.encode(raw),"signature":URL_SAFE_NO_PAD.encode(key.sign(&message).to_bytes())})
}
fn check(envelope: &Value) -> Result<(), AuthorizationError> {
    verify(
        &serde_json::to_vec(envelope).unwrap(),
        format::SERVICE_ID,
        &[(TEST_ID, test_key())],
    )
}
fn assert_code(result: Result<(), AuthorizationError>, code: &str) {
    let error = result.unwrap_err();
    assert_eq!(error.code(), code);
    assert_eq!(error.to_string(), code);
    assert_eq!(format!("{error:?}"), code);
    assert!(std::error::Error::source(&error).is_none());
}

#[test]
fn verifies_stored_cross_language_vectors() {
    let fixtures: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/authorization/vectors.json"
    ))
    .unwrap();
    for (vector, expected) in fixtures["authorization_vectors"]
        .as_array()
        .unwrap()
        .iter()
        .zip([
            None,
            Some("AUTH_INVALID_SIGNATURE"),
            Some("AUTH_SERVICE_DENIED"),
        ])
    {
        let result = check(&vector["envelope"]);
        if let Some(code) = expected {
            assert_code(result, code);
        } else {
            result.unwrap();
        }
    }
}

#[test]
fn verifies_exact_payload_bytes_and_domain_prefix() {
    let raw = serde_json::to_string_pretty(&claims()).unwrap();
    check(&signed(raw.as_bytes())).unwrap();
    let mut envelope = signed(raw.as_bytes());
    envelope["payload"] = json!(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims()).unwrap()));
    assert_code(check(&envelope), "AUTH_INVALID_SIGNATURE");
    let key = SigningKey::from_bytes(&hex(
        "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60",
    ));
    envelope["signature"] =
        json!(URL_SAFE_NO_PAD.encode(key.sign(&serde_json::to_vec(&claims()).unwrap()).to_bytes()));
    assert_code(check(&envelope), "AUTH_INVALID_SIGNATURE");
}

#[test]
fn rejects_untrusted_keys_before_decoding_and_wrong_keys_or_signatures() {
    let mut envelope = signed(&serde_json::to_vec(&claims()).unwrap());
    let raw = serde_json::to_vec(&envelope).unwrap();
    assert_code(
        verify(&raw, format::SERVICE_ID, &[(TEST_ID, [0; 32])]),
        "AUTH_INVALID_SIGNATURE",
    );
    assert_code(verify(&raw, format::SERVICE_ID, &[]), "AUTH_UNKNOWN_KEY");
    envelope["signature"] = json!(URL_SAFE_NO_PAD.encode([0; 64]));
    assert_code(check(&envelope), "AUTH_INVALID_SIGNATURE");
    envelope["key_id"] = json!("unknown-secret-key");
    envelope["payload"] = json!("invalid==");
    assert_code(check(&envelope), "AUTH_UNKNOWN_KEY");
}

#[test]
fn rejects_noncanonical_base64_and_wrong_signature_lengths() {
    for field in ["payload", "signature"] {
        for invalid in [
            "=", "AA=", "AA==", "AB", "A", "+w", "/w", "AA\n", " AA", "AA ",
        ] {
            let mut envelope = signed(&serde_json::to_vec(&claims()).unwrap());
            envelope[field] = json!(invalid);
            assert_code(check(&envelope), "AUTH_MALFORMED");
        }
    }
    for length in [0, 1, 63, 65] {
        let mut envelope = signed(&serde_json::to_vec(&claims()).unwrap());
        envelope["signature"] = json!(URL_SAFE_NO_PAD.encode(vec![0; length]));
        assert_code(check(&envelope), "AUTH_INVALID_SIGNATURE");
    }
}

#[test]
fn authenticates_before_parsing_or_validating_claims() {
    for raw in [
        b"not json secret".as_slice(),
        br#"{"version":2}"#,
        b"\xff",
        br#"[]"#,
    ] {
        let mut envelope = signed(raw);
        envelope["signature"] = json!(URL_SAFE_NO_PAD.encode([0; 64]));
        assert_code(check(&envelope), "AUTH_INVALID_SIGNATURE");
        assert_code(check(&signed(raw)), "AUTH_MALFORMED");
    }
    for raw in [
        serde_json::to_string(&claims())
            .unwrap()
            .replace("\"version\":1", "\"version\":1.0"),
        serde_json::to_string(&claims())
            .unwrap()
            .replace("\"version\":1", "\"version\":1,\"expires_at\":0"),
    ] {
        assert_code(check(&signed(raw.as_bytes())), "AUTH_MALFORMED");
    }
}

#[test]
fn validates_claim_limits_using_utf8_bytes() {
    for field in ["authorization_id", "authorized_to"] {
        for valid in ["a".repeat(128), "é".repeat(64)] {
            let mut value = claims();
            value[field] = json!(valid);
            check(&signed(&serde_json::to_vec(&value).unwrap())).unwrap();
        }
        for invalid in [String::new(), "a".repeat(129), "é".repeat(65)] {
            let mut value = claims();
            value[field] = json!(invalid);
            assert_code(
                check(&signed(&serde_json::to_vec(&value).unwrap())),
                "AUTH_INVALID_CLAIMS",
            );
        }
    }
    for (field, value) in [
        ("version", json!(2)),
        ("key_id", json!("different")),
        ("services", json!([])),
        ("services", json!([format::SERVICE_ID, format::SERVICE_ID])),
    ] {
        let mut payload = claims();
        payload[field] = value;
        assert_code(
            check(&signed(&serde_json::to_vec(&payload).unwrap())),
            "AUTH_INVALID_CLAIMS",
        );
    }
    for invalid in ["", "*", "UPPER", "with_space ", "a/b", "é"] {
        let mut value = claims();
        value["services"] = json!([format::SERVICE_ID, invalid]);
        assert_code(
            check(&signed(&serde_json::to_vec(&value).unwrap())),
            "AUTH_INVALID_CLAIMS",
        );
    }
    for (count, length, good) in [(32, 64, true), (33, 64, false), (2, 65, false)] {
        let mut services = vec![format::SERVICE_ID.to_string()];
        services.extend((1..count).map(|i| format!("{i:0>length$}")));
        let mut value = claims();
        value["services"] = json!(services);
        let result = check(&signed(&serde_json::to_vec(&value).unwrap()));
        if good {
            result.unwrap();
        } else {
            assert_code(result, "AUTH_INVALID_CLAIMS");
        }
    }
}

#[test]
fn rejects_oversize_before_parsing_and_accepts_exact_limit() {
    let mut raw = serde_json::to_vec(&signed(&serde_json::to_vec(&claims()).unwrap())).unwrap();
    raw.resize(16 * 1024, b' ');
    verify(&raw, format::SERVICE_ID, &[(TEST_ID, test_key())]).unwrap();
    raw.push(b' ');
    assert_code(verify(&raw, format::SERVICE_ID, &[]), "AUTH_TOO_LARGE");
    assert_code(verify(&[], format::SERVICE_ID, &[]), "AUTH_MALFORMED");
}

#[test]
fn production_trust_is_exact_and_excludes_fixture_key() {
    assert_eq!(
        trusted_keys::TRUSTED_KEYS,
        &[(
            "owner-2026-01",
            hex::<32>("a18dd86a3d503562dd8d5c0f13ac6258acb684ad2e426727fe825d5ae0ef46c7")
        )]
    );
    assert_code(
        verify(
            &serde_json::to_vec(&signed(&serde_json::to_vec(&claims()).unwrap())).unwrap(),
            format::SERVICE_ID,
            trusted_keys::TRUSTED_KEYS,
        ),
        "AUTH_UNKNOWN_KEY",
    );
}

#[test]
fn key_id_limits_apply_after_matching_trust_and_signature() {
    for (id, valid) in [
        ("a".repeat(128), true),
        ("é".repeat(64), true),
        (String::new(), false),
        ("a".repeat(129), false),
        ("é".repeat(65), false),
    ] {
        let mut payload = claims();
        payload["key_id"] = json!(id);
        let mut envelope = signed(&serde_json::to_vec(&payload).unwrap());
        envelope["key_id"] = json!(id);
        let result = verify(
            &serde_json::to_vec(&envelope).unwrap(),
            format::SERVICE_ID,
            &[(&id, test_key())],
        );
        if valid {
            result.unwrap();
        } else {
            assert_code(result, "AUTH_INVALID_CLAIMS");
        }
    }
}

#[test]
fn verifier_rejects_structurally_invalid_envelopes_and_signed_payloads() {
    let envelope = serde_json::to_string(&signed(&serde_json::to_vec(&claims()).unwrap())).unwrap();
    let payload = serde_json::to_string(&claims()).unwrap();
    for raw in [
        envelope.replace("\"key_id\":", "\"key_id\":null,\"key_id\":"),
        format!("{envelope} trailing"),
        "[]".into(),
        "null".into(),
        "{}".into(),
    ] {
        assert_code(
            verify(raw.as_bytes(), format::SERVICE_ID, &[(TEST_ID, test_key())]),
            "AUTH_MALFORMED",
        );
    }
    for raw in [
        payload.replace("\"key_id\":", "\"key_id\":null,\"key_id\":"),
        format!("{payload} trailing"),
        "[]".into(),
        "null".into(),
        "{}".into(),
    ] {
        assert_code(check(&signed(raw.as_bytes())), "AUTH_MALFORMED");
    }
    let raw = serde_json::to_vec(&signed(payload.as_bytes())).unwrap();
    assert_code(
        verify(&raw, "other-service", &[(TEST_ID, test_key())]),
        "AUTH_SERVICE_DENIED",
    );
}

#[test]
fn bounded_reader_preserves_error_codes_without_sources() {
    struct Fails;
    impl std::io::Read for Fails {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("SECRET path /private/key"))
        }
    }
    assert_code(read_bounded(Fails).map(|_| ()), "AUTH_UNREADABLE");
    assert_eq!(
        read_bounded(&vec![0; 16 * 1024][..]).unwrap().len(),
        16 * 1024
    );
    assert_code(
        read_bounded(&vec![0; 16 * 1024 + 1][..]).map(|_| ()),
        "AUTH_TOO_LARGE",
    );
    let mut endless = std::io::repeat(0);
    assert_code(read_bounded(&mut endless).map(|_| ()), "AUTH_TOO_LARGE");
}

#[test]
fn loader_rejects_missing_directory_and_oversize_and_follows_symlinks() {
    let dir = std::env::temp_dir().join(format!(
        "authorization-test-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir(&dir).unwrap();
    assert_code(
        load_file(&dir.join("SECRET-missing")).map(|_| ()),
        "AUTH_MISSING",
    );
    assert_code(load_file(&dir).map(|_| ()), "AUTH_UNREADABLE");
    let file = dir.join("authorization.json");
    std::fs::write(&file, vec![0; 16 * 1024 + 1]).unwrap();
    assert_code(load_file(&file).map(|_| ()), "AUTH_TOO_LARGE");
    std::fs::write(&file, b"document").unwrap();
    assert_eq!(load_file(&file).unwrap(), b"document");
    #[cfg(unix)]
    {
        let link = dir.join("mounted.json");
        std::os::unix::fs::symlink(&file, &link).unwrap();
        assert_eq!(load_file(&link).unwrap(), b"document");
        let fifo = dir.join("pipe");
        assert!(
            std::process::Command::new("mkfifo")
                .arg(&fifo)
                .status()
                .unwrap()
                .success()
        );
        assert_code(load_file(&fifo).map(|_| ()), "AUTH_UNREADABLE");
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn startup_child() {
    if let Ok(expected) = std::env::var("AUTHORIZATION_TEST_EXPECTED") {
        assert_code(check_startup(), &expected);
    }
}

#[test]
fn public_startup_uses_path_but_never_environment_trust_or_bypass() {
    let dir = std::env::temp_dir().join(format!(
        "authorization-startup-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir(&dir).unwrap();
    let file = dir.join("authorization.json");
    for expected in ["AUTH_MISSING", "AUTH_UNKNOWN_KEY"] {
        if expected == "AUTH_UNKNOWN_KEY" {
            std::fs::write(
                &file,
                serde_json::to_vec(&signed(&serde_json::to_vec(&claims()).unwrap())).unwrap(),
            )
            .unwrap();
        }
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "authorization::verifier_tests::startup_child",
                "--nocapture",
            ])
            .env("APP_AUTHORIZATION_FILE", &file)
            .env("AUTHORIZATION_TEST_EXPECTED", expected)
            .env("APP_AUTHORIZATION_DISABLED", "true")
            .env(
                "APP_AUTHORIZATION_PUBLIC_KEY",
                "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a",
            )
            .env("APP_AUTHORIZATION_KEY_ID", TEST_ID)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
    }
    std::fs::remove_dir_all(dir).unwrap();
}
