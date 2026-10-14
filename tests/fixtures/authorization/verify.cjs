// Fixture maintenance only. This is not an application verifier or issuer.
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const fixtures = JSON.parse(fs.readFileSync(path.join(__dirname, 'vectors.json'), 'utf8'));
const prefix = Buffer.from('application-authorization/v1\n', 'ascii');
const requiredService = 'channels-manager-v1';
const publicKeyHex = 'd75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a';
const key = crypto.createPublicKey({
  key: Buffer.from('302a300506032b6570032100' + publicKeyHex, 'hex'),
  format: 'der', type: 'spki',
});

// Published RFC 8032 Test 1 seed: deliberately public, never a production key.
const seed = '9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60';
const fixtureKey = crypto.createPrivateKey({
  key: Buffer.from('302e020100300506032b657004220420' + seed, 'hex'),
  format: 'der', type: 'pkcs8',
});
const rfc = fixtures.rfc8032_test1;
assert.equal(rfc.public_key_hex, publicKeyHex);
assert.equal(rfc.message_hex, '');
assert.equal(rfc.expected_signature_valid, true);
assert.equal(rfc.signature_hex, 'e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b');
assert.equal(crypto.verify(null, Buffer.alloc(0), key, Buffer.from(rfc.signature_hex, 'hex')), true);
assert.equal(crypto.sign(null, Buffer.alloc(0), fixtureKey).toString('hex'), rfc.signature_hex);

const expected = [
  ['valid', true, true, true, 'Example test customer', requiredService],
  ['tampered-payload', false, true, false, 'Tampered test customer', requiredService],
  ['wrong-service', true, false, false, 'Example test customer', 'trading-station-v1'],
];
assert.equal(fixtures.authorization_vectors.length, expected.length);
for (const [index, vector] of fixtures.authorization_vectors.entries()) {
  const [name, valid, allowed, authorized, customer, service] = expected[index];
  assert.equal(vector.name, name);
  assert.equal(vector.expected_signature_valid, valid, name);
  assert.equal(vector.expected_service_allowed, allowed, name);
  assert.equal(vector.expected_authorized, authorized, name);
  // Pin this fixture's exact compact JSON bytes and field ordering. Runtime
  // verification must use the original bytes, never parse and reserialize them.
  assert.equal(vector.payload_utf8, JSON.stringify({
    version: 1, key_id: 'test-rfc8032-01', authorization_id: 'test-001',
    authorized_to: customer, services: [service],
  }), name);
  const payload = Buffer.from(vector.payload_utf8, 'utf8');
  const signature = Buffer.from(vector.signature_hex, 'hex');
  assert.equal(payload.toString('hex'), vector.payload_hex, name);
  assert.equal(signature.toString('hex'), vector.signature_hex, name);
  assert.equal(payload.toString('base64url'), vector.envelope.payload, name);
  assert.equal(signature.toString('base64url'), vector.envelope.signature, name);
  assert.equal(signature.length, 64, name);
  assert.equal(vector.public_key_hex, publicKeyHex, name);
  const message = Buffer.concat([prefix, payload]);
  const signatureValid = crypto.verify(null, message, key, signature);
  const decoded = JSON.parse(vector.payload_utf8);
  assert.equal(decoded.key_id, vector.envelope.key_id, name);
  const serviceAllowed = decoded.services.includes(requiredService);
  assert.equal(signatureValid, valid, name);
  assert.equal(serviceAllowed, allowed, name);
  assert.equal(signatureValid && serviceAllowed, authorized, name);
  if (signatureValid) {
    assert.equal(crypto.sign(null, message, fixtureKey).toString('hex'), vector.signature_hex, name);
    const alteredMessages = [
      payload, // Omitted domain prefix.
      Buffer.concat([prefix.subarray(0, -1), payload]), // Omitted LF separator.
      Buffer.concat([Buffer.from('application-authorization/v1\r\n'), payload]),
      Buffer.concat([prefix, Buffer.from('\n'), payload]), // Extra separator LF.
      Buffer.concat([message, Buffer.from('\n')]), // Trailing payload LF.
      Buffer.concat([prefix, Buffer.from(JSON.stringify(decoded, null, 2))]),
    ];
    for (const altered of alteredMessages) {
      assert.equal(crypto.verify(null, altered, key, signature), false, name);
    }
    const changedSignature = Buffer.from(signature);
    changedSignature[0] ^= 1;
    assert.equal(crypto.verify(null, message, key, changedSignature), false, name);
  } else {
    assert.equal(vector.signature_hex, fixtures.authorization_vectors[0].signature_hex, name);
    assert.equal(vector.envelope.signature, fixtures.authorization_vectors[0].envelope.signature, name);
  }
}
console.log('RFC 8032 Test 1 and 3 authorization vectors verified for channels-manager-v1; signatures reproduced from public test seed.');
