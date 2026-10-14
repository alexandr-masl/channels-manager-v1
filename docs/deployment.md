# Container deployment

Matches the original TypeScript service:

| Resource | Name |
| --- | --- |
| Docker Hub repository | `kolobobolobo/satoshi-channels-updates` (deploy by digest) |
| Deployment / container / Compose service | `satoshi-channel-updates-manager` |
| Pod selector | `io.kompose.service: satoshi-channel-updates-manager` |
| Optional KEDA ScaledObject | `satoshi-channel-updates-manager-rabbitmq` |

**Current scope:** the full BingX Futures pipeline: Telegram intake → client jobs →
validated trade construction → confirmed Trading Station publication. The deployment
and Compose files enable Telegram intake; client-job publication and the worker
are always active. Commands, other exchanges
and Spot remain outside this implementation; retain their existing owners.

See [verification and cutover](verification-and-rollout.md) for worker-only migration,
queue ownership, acceptance checks and rollback. These files do not apply themselves.

## Local Compose

The final image uses the same non-root distroless runtime as `trading-station-rust`.
It contains `/app/channels-manager-v1`, read-only `/app/USAGE-NOTICE.md` (0444),
and runtime libraries/certificates, with no
source, Cargo, shell, or package manager. Compose and Kubernetes use a read-only
root filesystem. Use container logs for diagnostics. `kubectl exec ... -- sh`
and `bash` cannot work; restricting all exec/debug access requires Kubernetes RBAC
for `pods/exec` and `pods/ephemeralcontainers`. The image cannot prevent an
authorized cluster administrator from inspecting it.

Release builds strip the ordinary symbol table, disable debug information, and
use ThinLTO with one codegen unit. Docker builds also remap application and Cargo
build paths and verify that ELF symbol/debug sections are absent. Required dynamic
symbols, protocol field names, logs, and other strings can remain. These settings
remove analysis clues, not the ability to disassemble or decompile the binary.
Symbol stripping also reduces production backtrace detail; optimization increases
build time. Panic unwinding is preserved.

Keep the Docker Hub repository private and restrict pull credentials and cluster
exec/debug access if binary confidentiality matters. No registry visibility or
cluster permissions are changed by these files. Never embed secrets in the binary.
Cargo profile reference: https://doc.rust-lang.org/cargo/reference/profiles.html

Use [local commands](../COMMANDS.md#local-containers). Dependency-only Compose
commands use `compose.yaml` without authorization. Full application runs add
`-f compose.authorization.yaml` and require an exported `APP_AUTHORIZATION_FILE`
pointing to an existing owner-issued JSON outside the repository. The override
mounts it read-only at `/run/authorization/authorization.json`, with
`bind.create_host_path: false`. Export the host path in the shell; putting it only
in `.env.local` does not configure the authorization gate. Validate configuration
offline with `--check-config` before starting the stack; this mode requires no
authorization and does not prove the signed file is valid or readable.
See [authorization](authorization.md) for issuance and UID/GID 65532 permissions.

Compose provides RabbitMQ, Redis and three MongoDB instances. It supplies its own
connection settings, without reading `.env.local`. RabbitMQ binds to localhost
ports 5672 and 15672; stop any conflicting local broker first. Mongo volumes
survive `down`. Startup retries handle dependency startup order. The existing
Telegram example can publish through localhost:5672. Populate channel/account/user
fixtures before testing intake. Compose does not start Trading Station: inspect the
queued final envelope locally. The optional Telegram sender stays disabled unless
you explicitly supply `SATOSHI_TG_TOKEN`.

## Build and replace

The Deployment reads `SATOSHI_TG_TOKEN` from the existing Secret
`mcr-tg-client-secrets` in the same namespace, sharing the Telegram bot's token.
The Secret and its `SATOSHI_TG_TOKEN` key must exist before applying the Deployment.
After changing the Secret, restart this Deployment to reload the token.

Run from this repository, using an explicitly approved Kubernetes context and namespace.
The manifests preserve dependency DNS names, database names, resource limits and
`regcred`. They contain the original guest RabbitMQ connection; adapt credentials
to the actual environment if it differs. No HTTP probes are configured because
health endpoints belong to a separate issue. Rollout success alone does not prove
dependency readiness; check enabled queue listener logs and `Client trade published`
from controlled work. An absent publish log alone is not proof of failure.

Fill `KUBE_CONTEXT`, `KUBE_NAMESPACE`, and a new, never-reused `IMAGE_TAG` in your
shell. Export `APP_AUTHORIZATION_FILE` to the approved signed JSON as in
[local commands](../COMMANDS.md). The commands require Bash, Docker, kubectl and jq.
Use a dedicated shell with `set -euo pipefail`; all placeholders are operator-filled.
Keep the temporary review directory restricted and outside Git.

```bash
set -euo pipefail
: "${KUBE_CONTEXT:?Set the approved context}" "${KUBE_NAMESPACE:?Set the approved namespace}"
: "${IMAGE_TAG:?Set a unique release tag}" "${APP_AUTHORIZATION_FILE:?Export the signed JSON path}"
test -f "$APP_AUTHORIZATION_FILE" && test -r "$APP_AUTHORIZATION_FILE"
IMAGE_REPOSITORY=kolobobolobo/satoshi-channels-updates
DEPLOYMENT=satoshi-channel-updates-manager
umask 077
REVIEW_DIR=$(mktemp -d "${TMPDIR:-/tmp}/channels-manager-release.XXXXXX")
kubectl --context "$KUBE_CONTEXT" --namespace "$KUBE_NAMESPACE" get deployment "$DEPLOYMENT" -o json > "$REVIEW_DIR/live.json"
kubectl --context "$KUBE_CONTEXT" --namespace "$KUBE_NAMESPACE" get pods -l io.kompose.service=satoshi-channel-updates-manager \
  -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.containerStatuses[*].imageID}{"\n"}{end}'
```

Record the running immutable image digest, source commit, authorization ID,
operator, maintenance window, live replica count and current KEDA/HPA policy.
Save the live KEDA policy separately if present; do not apply the checked-in
ScaledObject during this authorization release. Confirm the selected context and
namespace and obtain operator approval before registry pushes or cluster writes.
After push approval, build once under the unique tag and capture its digest:

```bash
docker buildx build --platform linux/amd64 \
  -t "$IMAGE_REPOSITORY:${IMAGE_TAG:?}" --metadata-file "$REVIEW_DIR/build.json" --push .
IMAGE_DIGEST=$(jq -er '."containerimage.digest"' "$REVIEW_DIR/build.json")
[[ "$IMAGE_DIGEST" =~ ^sha256:[0-9a-f]{64}$ ]]
IMAGE_REF="$IMAGE_REPOSITORY@$IMAGE_DIGEST"
```

Render a local patch and manifest for review. The patch changes image, pod
`fsGroup`, Secret volume and read-only directory mount in **one** pod-template
replacement. Other live fields, including replicas, env, resources and strategy,
are preserved. It uses a resource-version test to reject a stale snapshot,
including changes caused by scaling. No authorization JSON is copied into these files.

```bash
jq --arg image "${IMAGE_REF:?}" --arg container "$DEPLOYMENT" '
  if ([.spec.template.spec.containers[] | select(.name == $container)] | length) != 1
  then error("Expected exactly one application container") else . end
  | .metadata.resourceVersion as $version
  | .spec.template
  | .spec.securityContext.fsGroup = 65532
  | .spec.volumes = ((.spec.volumes // [] | map(select(.name != "application-authorization"))) + [{
      name: "application-authorization",
      secret: {secretName: "channels-manager-authorization", optional: true,
        defaultMode: 288, items: [{key: "authorization.json", path: "authorization.json"}]}}])
  | (.spec.containers[] | select(.name == $container)) |= (
      .image = $image
      | .volumeMounts = ((.volumeMounts // [] | map(select(.name != "application-authorization"))) + [{
          name: "application-authorization", mountPath: "/run/authorization", readOnly: true}]))
  | [{op: "test", path: "/metadata/resourceVersion", value: $version},
     {op: "replace", path: "/spec/template", value: .}]
' "$REVIEW_DIR/live.json" > "$REVIEW_DIR/authorization.patch.json"
kubectl patch --local -f "$REVIEW_DIR/live.json" --type=json \
  --patch-file "$REVIEW_DIR/authorization.patch.json" -o yaml > "$REVIEW_DIR/rendered.yaml"
```

Review `rendered.yaml` and the patch with the operator, checking the pinned image,
mount permissions, preserved security hardening, env and queue ownership. Resolve
any existing mount at `/run/authorization` or custom `APP_AUTHORIZATION_FILE`
setting before approval. Decimal `288` is octal `0440`. For a first TypeScript
replacement, also incorporate the reviewed Rust runtime/env/probe changes from
`kubernetes/deployment.yaml` into the same prepared pod template. Deployment
strategy is outside the template: include an additional JSON Patch operation in
the same resource-version-guarded update that replaces `/spec/strategy` with
`{"type":"Recreate"}`, removing any existing `rollingUpdate` configuration. Review
that strategy change explicitly. The patch above assumes the existing runtime
settings and strategy already fit this Rust service.
Do not blindly apply the checked-in manifest or a stale full Deployment snapshot.

After operator approval of this exact result and the ownership/drain checklist,
create/update the Secret from the external file, without printing JSON. Disable
shell tracing and do not use `tee` or log the pipeline contents. Reuse an existing
approved Secret unchanged when no rotation is required:

```bash
set +x
kubectl --context "${KUBE_CONTEXT:?}" --namespace "${KUBE_NAMESPACE:?}" create secret generic channels-manager-authorization \
  --from-file="authorization.json=${APP_AUTHORIZATION_FILE:?}" --dry-run=client -o yaml \
  | kubectl --context "$KUBE_CONTEXT" --namespace "$KUBE_NAMESPACE" apply --server-side --field-manager=channels-manager-authorization -f -
kubectl --context "$KUBE_CONTEXT" --namespace "$KUBE_NAMESPACE" patch deployment "$DEPLOYMENT" \
  --type=json --patch-file "${REVIEW_DIR:?}/authorization.patch.json"
kubectl --context "$KUBE_CONTEXT" --namespace "$KUBE_NAMESPACE" rollout status deployment "$DEPLOYMENT"
kubectl --context "$KUBE_CONTEXT" --namespace "$KUBE_NAMESPACE" logs -f deployment "$DEPLOYMENT"
```

Stop on Secret ownership conflicts; review them rather than forcing ownership.
If the resource-version test fails, fetch fresh live state, re-render and obtain
approval for the refreshed result. Do not remove the test. For a new Deployment,
prepare and review an environment-specific manifest with this digest and mount
before an approved creation; the existing-Deployment patch deliberately fails if
the resource does not exist.

`Recreate` stops old pods of this Deployment before starting Rust. Other Deployments
and external consumers must be checked separately. The checked-in Deployment has
two replicas. The optional example KEDA policy independently sets minimum and
maximum to one and watches both enabled queues; it is not a declaration of live
policy. Preserve live replicas and autoscaler settings during this release.
For worker-only mode, separately review removal of the `tg_bot_channel_update`
trigger and `TELEGRAM_INTAKE_ENABLED=false` before cutover.

If TypeScript currently uses this same Deployment name, replacing it also stops its
intake and unrelated workers. Retain those roles in a separate workload before a
worker-only replacement. Never leave both implementations consuming the same queue.
No Service is needed because this app exposes no inbound port.

For later releases, use another unique tag and the same digest-based procedure.
Authorization and Telegram token changes are startup-only. A Secret-only rotation
requires separate operator approval and an explicit restart after the file-based
Secret update above (omit the image patch):

```sh
kubectl --context "${KUBE_CONTEXT:?}" --namespace "${KUBE_NAMESPACE:?}" rollout restart deployment/satoshi-channel-updates-manager
kubectl --context "$KUBE_CONTEXT" --namespace "$KUBE_NAMESPACE" rollout status deployment/satoshi-channel-updates-manager
```

## Rollback

Restore the original TypeScript manifest (including probes, environment and KEDA
triggers) and pin its image to the recorded `repository@sha256:...` digest before
applying an operator-reviewed update with explicit context/namespace. Preserve live
replica/autoscaler policy unless a separate change is approved. A rollback to an
unprotected binary requires an explicit owner-approved security exception; prefer
restoring approved authorization/mounts first. `rollout undo` alone is unsafe when
old revisions use mutable tags. Already acknowledged signals cannot be recovered
by rollback.

Files are deployment preparation; creating them does not push or apply anything.
