# Authorization format fixtures

These are deliberately public, nonproduction test data. **Never trust this key
in production.** `test-rfc8032-01` identifies the published Ed25519 Test 1 key
from [RFC 8032 section 7.1](https://www.rfc-editor.org/rfc/rfc8032#section-7.1).
The seed in `verify.cjs` is public RFC test material, not an owner key. No owner
key files or production configuration are needed or read.

`vectors.json` preserves the Trading Station fixture schema: exact UTF-8 payload
strings, equivalent hex bytes, public keys, signatures, ready-to-parse envelopes,
and expected results. The standalone RFC empty-message vector is unchanged and
has no application prefix. Application signatures use pure Ed25519 over ASCII
`application-authorization/v1`, exactly one LF byte (`0a`), then the exact payload
bytes. There is no trailing payload newline. Envelope payload and signature use
canonical unpadded Base64URL. Verify the original bytes before parsing; do not
parse and reserialize the payload for signature verification.

| Vector | Signature valid | `channels-manager-v1` listed | Expected authorization |
| --- | --- | --- | --- |
| valid | yes | yes | accept |
| tampered-payload | no | yes | reject |
| wrong-service | yes | no | reject |

`wrong-service` retains the exact payload, signature, public key, and envelope of
Trading Station's original `valid` vector for `trading-station-v1`. Only its name
and service/authorization expectations change. It demonstrates that a valid
signature for Trading Station does not authorize Channels Manager.
`expected_service_allowed` describes membership only, not trust in an
unauthenticated payload. `expected_authorized` describes the future verifier's
intended result under this test key. These fixtures do not implement runtime
authorization, expiry, an issuer, or startup enforcement.

From the `channels-manager-v1` crate root, run:

```sh
node tests/fixtures/authorization/verify.cjs
```

The script needs only Node's native crypto implementation, with Base64URL Buffer
support (Node 18 or newer). It independently verifies all signatures and reproduces
the RFC and both valid application signatures from the public seed. It pins the
three cases and their expected results, exact fixture JSON bytes and field order,
hex and Base64URL encodings, key identity, and envelope key ID. Negative checks
cover the tampered customer, a flipped signature byte, omitted prefix, missing LF,
CRLF, an extra separator LF, a trailing LF, and pretty-printed JSON bytes.

## Deterministic reproduction

`verify.cjs` contains executable reproduction assertions. Its only private-key
input is the public RFC seed:

```text
9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60
```

Wrap those 32 seed bytes with PKCS#8 DER hex prefix
`302e020100300506032b657004220420` and pass the result to
`crypto.createPrivateKey({ key, format: 'der', type: 'pkcs8' })`. Sign with
`crypto.sign(null, message, fixtureKey)`; no digest or Ed25519 prehash is used.
For the RFC vector, `message` is `Buffer.alloc(0)`. For each valid application
signature, it is exactly:

```js
Buffer.concat([
  Buffer.from('application-authorization/v1\n', 'ascii'),
  Buffer.from(vector.payload_utf8, 'utf8'),
])
```

To reconstruct the application vectors from the source snapshot:

1. Copy its `valid` vector and replace only `trading-station-v1` with
   `channels-manager-v1` inside `payload_utf8`, preserving all other bytes.
   Recompute the signature using the procedure above. Set `payload_hex` to
   `Buffer.from(payload_utf8, 'utf8').toString('hex')`, `signature_hex` to the
   signature's lowercase hex, and both envelope encodings with `.toString('base64url')`.
   Keep its name and all three `true` expectations.
2. Copy the new valid vector as `tampered-payload`. Replace only
   `Example test customer` with `Tampered test customer` and recompute its payload
   hex and Base64URL. Retain the valid signature bytes and signature encoding;
   set signature-valid and authorized expectations to `false` and service-allowed
   to `true`. Do not sign the changed payload.
3. Copy the original Trading Station `valid` vector unchanged except name
   `wrong-service`, `expected_service_allowed: false`, and
   `expected_authorized: false`. Retain `expected_signature_valid: true`.
4. Preserve the RFC object unchanged. Store the vectors in the order above using
   `JSON.stringify(fixtures, null, 2) + '\n'` as UTF-8. Run `verify.cjs`.

## Source snapshot and integrity

Source: workspace `trading-station-rust/tests/fixtures/authorization/`, captured
2026-10-13 with repository HEAD `ef03a783a742413bff57257f150405ffa1ee11e0`.
These source fixture files were **untracked working-copy files**, so that commit
alone does not reproduce them. The hashes below identify the exact source bytes;
the local snapshot, rather than committed files at that revision, was reused.

| Source file | SHA-256 |
| --- | --- |
| `README.md` | `a451132d1ca9de53be2a5a28cdf480f8063679772eb3e6ab6c06b71e4df0fdfb` |
| `vectors.json` | `68b42fc69fdbb498f0bb06ee349a522a2fbe66b7dfaefa4d73d98c998faddd48` |
| `verify.cjs` | `47bb756dc439451fc172a73e83cc887d9d5669b650df6e101a20a312335d620c` |

| New Channels Manager file | SHA-256 |
| --- | --- |
| `vectors.json` | `4cb58ff9aa6129dc52c4f772df0c4e035945dcb32108b3417f5c595c406e7a82` |
| `verify.cjs` | `8d3bc45a4cf05e5d284d831d47535e698614092a596595e250be9a280a83a5d4` |

Check the new hashes from the crate root with:

```sh
shasum -a 256 tests/fixtures/authorization/vectors.json tests/fixtures/authorization/verify.cjs
```

This README intentionally has no self-hash. Update the new-file hashes whenever
maintaining the fixture or verifier, while preserving source snapshot provenance.
