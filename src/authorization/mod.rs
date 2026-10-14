//! Offline startup authorization. No authorization data or underlying errors are
//! retained in diagnostics; the embedded public trust set is the only authority.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ed25519_dalek::{Signature, VerifyingKey};
use std::{
    collections::HashSet,
    fs::OpenOptions,
    io::{self, Read},
    path::Path,
};

mod format;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod verifier_tests;

mod trusted_keys;

const MAX_FILE_BYTES: usize = 16 * 1024;
const DEFAULT_FILE: &str = "/run/authorization/authorization.json";
const MISSING: AuthorizationError = AuthorizationError("AUTH_MISSING");
const UNREADABLE: AuthorizationError = AuthorizationError("AUTH_UNREADABLE");
const TOO_LARGE: AuthorizationError = AuthorizationError("AUTH_TOO_LARGE");
const MALFORMED: AuthorizationError = AuthorizationError("AUTH_MALFORMED");
const UNKNOWN_KEY: AuthorizationError = AuthorizationError("AUTH_UNKNOWN_KEY");
const INVALID_SIGNATURE: AuthorizationError = AuthorizationError("AUTH_INVALID_SIGNATURE");
const INVALID_CLAIMS: AuthorizationError = AuthorizationError("AUTH_INVALID_CLAIMS");
const SERVICE_DENIED: AuthorizationError = AuthorizationError("AUTH_SERVICE_DENIED");

/// An intentionally opaque failure containing only a fixed, safe diagnostic code.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct AuthorizationError(&'static str);
impl AuthorizationError {
    pub fn code(&self) -> &'static str {
        self.0
    }
}
impl std::fmt::Display for AuthorizationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}
impl std::fmt::Debug for AuthorizationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}
impl std::error::Error for AuthorizationError {}
/// Check the configured file against the service identity and embedded owner keys.
/// Only the file location is configurable; trust and enforcement are not.
pub fn check_startup() -> Result<(), AuthorizationError> {
    let path = std::env::var_os("APP_AUTHORIZATION_FILE").unwrap_or_else(|| DEFAULT_FILE.into());
    let bytes = load_file(Path::new(&path))?;
    verify(&bytes, format::SERVICE_ID, trusted_keys::TRUSTED_KEYS)
}

/// Integration executables may verify public fixtures with a separate trust set.
/// This entry point is absent from the production binary.
#[cfg(test)]
#[allow(
    dead_code,
    reason = "used by integration test executables, not binary unit tests"
)]
pub(crate) fn check_startup_with_test_trust(
    trust: &[(&str, [u8; 32])],
) -> Result<(), AuthorizationError> {
    let path = std::env::var_os("APP_AUTHORIZATION_FILE").unwrap_or_else(|| DEFAULT_FILE.into());
    let bytes = load_file(Path::new(&path))?;
    verify(&bytes, format::SERVICE_ID, trust)
}

fn verify(
    bytes: &[u8],
    expected_service: &str,
    trust: &[(&str, [u8; 32])],
) -> Result<(), AuthorizationError> {
    if bytes.len() > MAX_FILE_BYTES {
        return Err(TOO_LARGE);
    }
    let envelope: format::Envelope = serde_json::from_slice(bytes).map_err(|_| MALFORMED)?;
    let (_, public_key) = trust
        .iter()
        .find(|(id, _)| *id == envelope.key_id)
        .ok_or(UNKNOWN_KEY)?;
    // This engine requires no padding and rejects nonzero unused trailing bits.
    let payload_bytes = URL_SAFE_NO_PAD
        .decode(&envelope.payload)
        .map_err(|_| MALFORMED)?;
    let signature_bytes = URL_SAFE_NO_PAD
        .decode(&envelope.signature)
        .map_err(|_| MALFORMED)?;
    let signature = Signature::from_slice(&signature_bytes).map_err(|_| INVALID_SIGNATURE)?;
    let key = VerifyingKey::from_bytes(public_key).map_err(|_| INVALID_SIGNATURE)?;
    let message = [format::SIGNING_PREFIX, payload_bytes.as_slice()].concat();
    key.verify_strict(&message, &signature)
        .map_err(|_| INVALID_SIGNATURE)?;

    // Parse and validate claims only after authenticating their exact wire bytes.
    let payload: format::Payload = serde_json::from_slice(&payload_bytes).map_err(|_| MALFORMED)?;
    let bounded_text = |text: &str| !text.is_empty() && text.len() <= 128;
    if payload.version != 1
        || payload.key_id != envelope.key_id
        || !bounded_text(&payload.key_id)
        || !bounded_text(&payload.authorization_id)
        || !bounded_text(&payload.authorized_to)
        || payload.services.is_empty()
        || payload.services.len() > 32
    {
        return Err(INVALID_CLAIMS);
    }
    let mut unique = HashSet::new();
    for service in &payload.services {
        if service.is_empty()
            || service.len() > 64
            || !service
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            || !unique.insert(service.as_str())
        {
            return Err(INVALID_CLAIMS);
        }
    }
    if !unique.contains(expected_service) {
        return Err(SERVICE_DENIED);
    }
    Ok(())
}

fn read_bounded(reader: impl Read) -> Result<Vec<u8>, AuthorizationError> {
    let mut bytes = Vec::new();
    reader
        .take((MAX_FILE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| UNREADABLE)?;
    if bytes.len() > MAX_FILE_BYTES {
        return Err(TOO_LARGE);
    }
    Ok(bytes)
}

fn load_file(path: &Path) -> Result<Vec<u8>, AuthorizationError> {
    let mut options = OpenOptions::new();
    options.read(true);
    // Following symlinks supports Kubernetes projected volumes. Nonblocking open
    // prevents FIFO hangs; inspecting the opened descriptor avoids a stat/open race.
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK | libc::O_NOCTTY);
    }
    let file = options.open(path).map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            MISSING
        } else {
            UNREADABLE
        }
    })?;
    if !file.metadata().map_err(|_| UNREADABLE)?.is_file() {
        return Err(UNREADABLE);
    }
    read_bounded(file)
}
