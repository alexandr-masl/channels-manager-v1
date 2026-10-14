# Build and deploy

Run from `channels-manager-v1`. Obtain owner approval and a signed authorization
for service `channels-manager-v1`; a Trading Station-only authorization will fail.
See [issuance and permissions](docs/authorization.md).

## Local application

Set this once per shell to the existing owner-issued file outside the repository;
replace the example path. Do not put it only in `.env.local`.

```sh
export APP_AUTHORIZATION_FILE=/absolute/operator/path/channels-manager-authorization.json
test -f "${APP_AUTHORIZATION_FILE:?}" && test -r "$APP_AUTHORIZATION_FILE"
cargo run -- --check-config
cargo run
```

`--check-config` parses configuration offline without requiring authorization or
connecting to dependencies. It does not validate the signed file or its permissions.
Normal startup requires authorization; reuse this file across runs and releases.

## Local containers

Dependencies alone need no authorization:

```sh
docker compose -f compose.yaml up -d rabbitmq redis mongo-tg-client mongo mongo-account-validator
```

For the application, export the path above and ensure UID/GID 65532 can read the
signed file ([permissions](docs/authorization.md#file-permissions)).

```sh
docker compose -f compose.yaml -f compose.authorization.yaml build
docker compose -f compose.yaml -f compose.authorization.yaml run --rm --no-deps satoshi-channel-updates-manager --check-config
docker compose -f compose.yaml -f compose.authorization.yaml up -d
docker compose -f compose.yaml -f compose.authorization.yaml logs -f satoshi-channel-updates-manager
docker compose -f compose.yaml down
```

## Release

Follow the complete [deployment procedure](docs/deployment.md#build-and-replace):
explicit context/namespace, unique image tag, pushed immutable digest, approved
Secret update and a reviewed single pod-template patch against live state.
Do not apply the checked-in Deployment or KEDA configuration over live policy.
For first TypeScript → Rust replacement, also follow [cutover](docs/verification-and-rollout.md).
