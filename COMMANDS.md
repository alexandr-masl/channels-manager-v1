# Build and deploy

Run from `channels-manager-v1`. Obtain owner approval and a signed authorization
for service `channels-manager-v1`; a Trading Station-only authorization will fail.
See [issuance and permissions](docs/authorization.md).

## Local application

Set this once per shell to your existing Channels Manager authorization file.
Do not put it only in `.env.local`.

```sh
export APP_AUTHORIZATION_FILE="$HOME/.config/application-authorization/owner-2026-01/channels-manager-local-2026-001.json"
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

## Production release

Verified on 2026-10-18: context `do-fra1-mar-bot`, namespace `default`, existing
Rust Deployment `satoshi-channel-updates-manager`. Requires Docker, kubectl and jq.

### 0. Start Docker Desktop, then open Bash

Open Docker Desktop:

```sh
open -a Docker
```

Wait until its engine is running, then check:

```sh
docker info >/dev/null && echo "Docker engine ready"
```

Continue only after that prints `Docker engine ready`.

Run this command **alone** in a fresh terminal:

```sh
bash --noprofile --norc
```

Wait for the Bash prompt (usually `bash-3.2$`). Then paste steps 1–5, in order,
in **this same terminal**. A second terminal does not inherit these variables.
Do not run `set -u` in your Zsh prompt; prompt plugins can fail on unset variables.
If any step fails and the prompt returns to Zsh, stop pasting commands. Fix the
failure, open Bash again and restart at step 1; its variables were lost.

### 1. Set release variables

```bash
if [ -z "${BASH_VERSION:-}" ]; then
  printf '%s\n' 'Run bash --noprofile --norc alone first, then paste step 1 again.'
else
set -euo pipefail
set +x
cd /Users/maslovalexandr/code/satoshi-bot/channels-manager-v1

export KUBE_CONTEXT="do-fra1-mar-bot"
export KUBE_NAMESPACE="default"
export APP_AUTHORIZATION_FILE="$HOME/.config/application-authorization/owner-2026-01/channels-manager-local-2026-001.json"

DEPLOYMENT=satoshi-channel-updates-manager
AUTHORIZATION_SECRET=channels-manager-authorization
IMAGE_REPOSITORY=kolobobolobo/satoshi-channels-updates
IMAGE_TAG="$(date -u +%Y%m%d-%H%M%S)"

[[ -f "$APP_AUTHORIZATION_FILE" && -r "$APP_AUTHORIZATION_FILE" ]]
for binary in docker kubectl jq; do command -v "$binary" >/dev/null; done
docker info >/dev/null
kubectl --context "$KUBE_CONTEXT" -n "$KUBE_NAMESPACE" get deployment "$DEPLOYMENT" -o name
echo "Deploying to: $KUBE_CONTEXT / $KUBE_NAMESPACE"

umask 077
RELEASE_DIR="$(mktemp -d)"
fi
```

### 2. Build and push to Docker Hub

```bash
: "${IMAGE_REPOSITORY:?Run step 1 in this same Bash terminal}"
: "${IMAGE_TAG:?Run step 1 in this same Bash terminal}"
: "${RELEASE_DIR:?Run step 1 in this same Bash terminal}"
docker info >/dev/null
docker login

docker buildx build --platform linux/amd64 \
  -t "$IMAGE_REPOSITORY:$IMAGE_TAG" \
  --metadata-file "$RELEASE_DIR/build.json" \
  --push .

IMAGE_DIGEST="$(jq -er '."containerimage.digest"' "$RELEASE_DIR/build.json")"
[[ "$IMAGE_DIGEST" =~ ^sha256:[0-9a-f]{64}$ ]]
IMAGE_REF="$IMAGE_REPOSITORY@$IMAGE_DIGEST"
```

The authorization file stays outside the image.

### 3. Ensure the authorization Secret exists

This reuses the existing Secret. If absent, it creates it from your signed file.
Authorization rotation is separate; use the [deployment procedure](docs/deployment.md#build-and-replace).

```bash
: "${KUBE_CONTEXT:?Run step 1 in this same Bash terminal}"
: "${KUBE_NAMESPACE:?Run step 1 in this same Bash terminal}"
: "${AUTHORIZATION_SECRET:?Run step 1 in this same Bash terminal}"
: "${APP_AUTHORIZATION_FILE:?Run step 1 in this same Bash terminal}"
EXISTING_SECRET="$(kubectl --context "$KUBE_CONTEXT" -n "$KUBE_NAMESPACE" \
  get secret "$AUTHORIZATION_SECRET" --ignore-not-found -o name)"

if [[ -n "$EXISTING_SECRET" ]]; then
  echo "Reusing $EXISTING_SECRET"
else
  kubectl --context "$KUBE_CONTEXT" -n "$KUBE_NAMESPACE" \
    create secret generic "$AUTHORIZATION_SECRET" \
    --from-file="authorization.json=$APP_AUTHORIZATION_FILE"
fi
```

### 4. Prepare the image and authorization mount together

This preserves existing environment variables, replicas and resources.

```bash
: "${IMAGE_REF:?Complete step 2 in this same Bash terminal}"
: "${RELEASE_DIR:?Run step 1 in this same Bash terminal}"
kubectl --context "$KUBE_CONTEXT" -n "$KUBE_NAMESPACE" \
  get deployment "$DEPLOYMENT" -o json > "$RELEASE_DIR/live.json"

jq --arg image "$IMAGE_REF" --arg container "$DEPLOYMENT" --arg secret "$AUTHORIZATION_SECRET" '
  if ([.spec.template.spec.containers[] | select(.name == $container)] | length) != 1
  then error("Expected exactly one application container") else . end
  | .metadata.resourceVersion as $version
  | .spec.template
  | .spec.securityContext.fsGroup = 65532
  | .spec.volumes = (
      (.spec.volumes // [] | map(select(.name != "application-authorization")))
      + [{
          name: "application-authorization",
          secret: {
            secretName: $secret,
            defaultMode: 288,
            items: [{key: "authorization.json", path: "authorization.json"}]
          }
        }]
    )
  | (.spec.containers[] | select(.name == $container)) |= (
      .image = $image
      | .volumeMounts = (
          (.volumeMounts // [] | map(select(.name != "application-authorization")))
          + [{
              name: "application-authorization",
              mountPath: "/run/authorization",
              readOnly: true
            }]
        )
    )
  | [
      {op: "test", path: "/metadata/resourceVersion", value: $version},
      {op: "replace", path: "/spec/template", value: .}
    ]
' "$RELEASE_DIR/live.json" > "$RELEASE_DIR/patch.json"

kubectl patch --local -f "$RELEASE_DIR/live.json" \
  --type=json --patch-file "$RELEASE_DIR/patch.json" \
  -o yaml > "$RELEASE_DIR/rendered.yaml"
cat "$RELEASE_DIR/patch.json"
```

Review the printed patch before step 5. The live deployment currently uses the
default authorization path and already has the required mount. If you add an
`APP_AUTHORIZATION_FILE` container setting, use `/run/authorization/authorization.json`.
A stale resource-version error means repeat steps 4–5 using fresh live state.

### 5. Deploy and inspect logs

```bash
test -s "${RELEASE_DIR:?Complete step 4 in this same Bash terminal}/patch.json"
kubectl --context "$KUBE_CONTEXT" -n "$KUBE_NAMESPACE" \
  patch deployment "$DEPLOYMENT" \
  --type=json --patch-file "$RELEASE_DIR/patch.json"

kubectl --context "$KUBE_CONTEXT" -n "$KUBE_NAMESPACE" \
  rollout status deployment/"$DEPLOYMENT" --timeout=180s

kubectl --context "$KUBE_CONTEXT" -n "$KUBE_NAMESPACE" \
  logs -f deployment/"$DEPLOYMENT" --all-pods=true --all-containers=true --prefix --tail=100
```

These steps assume the cluster already runs the Rust deployment. The new image triggers rollout automatically; no separate `rollout restart` is needed.
