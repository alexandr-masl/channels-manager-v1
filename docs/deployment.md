# Container deployment

Matches the original TypeScript service:

| Resource | Name |
| --- | --- |
| Docker Hub image | `kolobobolobo/satoshi-channels-updates:latest` |
| Deployment / container / Compose service | `satoshi-channel-updates-manager` |
| Pod selector | `io.kompose.service: satoshi-channel-updates-manager` |
| Optional KEDA ScaledObject | `satoshi-channel-updates-manager-rabbitmq` |

**Current scope:** Rust consumes, logs and acknowledges `tg_bot_channel_update`.
It does not publish trades or process the original command/client-job queues yet.
Replacing TypeScript now stops those workflows and consumes incoming signals
without executing trades. Use an isolated environment until migration is ready.

## Local Compose

The final image uses the same non-root distroless runtime as `trading-station-rust`.
It contains `/app/channels-manager-v1` plus runtime libraries/certificates, with no
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

```sh
docker compose up --build -d
docker compose logs -f satoshi-channel-updates-manager
docker compose run --rm --no-deps satoshi-channel-updates-manager --check-config
docker compose down
```

Compose provides RabbitMQ, Redis and three MongoDB instances. It supplies its own
connection settings, without reading `.env.local`. RabbitMQ binds to localhost
ports 5672 and 15672; stop any conflicting local broker first. Mongo volumes
survive `down`. Startup retries handle dependency startup order. The existing
Telegram example can publish through localhost:5672.

## Build and replace

Run from this repository, using the original Kubernetes context and namespace.
The manifests preserve dependency DNS names, database names, resource limits and
`regcred`. They contain the original guest RabbitMQ connection; adapt credentials
to the actual environment if it differs. No HTTP probes are configured because
health endpoints belong to a separate issue. Rollout success alone does not prove
dependency readiness; check logs for `Listening for messages on tg_bot_channel_update.`

Before overwriting `latest`, record the running TypeScript image digest for rollback:

```sh
kubectl get pods -l io.kompose.service=satoshi-channel-updates-manager \
  -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.containerStatuses[*].imageID}{"\n"}{end}'
```

After confirming that replacing the service is appropriate:

```sh
docker buildx build --platform linux/amd64 \
  -t kolobobolobo/satoshi-channels-updates:latest --push .
kubectl apply -f kubernetes/deployment.yaml
# Only if the original service uses KEDA:
kubectl apply -f kubernetes/rabbitmq-scaledobject.yaml
kubectl rollout status deployment/satoshi-channel-updates-manager
kubectl logs -f deployment/satoshi-channel-updates-manager
```

`Recreate` stops the old Deployment pods before starting Rust, avoiding a mixed
TypeScript/Rust rollout. Check for consumers from other Deployments separately.
The KEDA file retains only the queue this binary consumes; leaving the original
six triggers would scale Rust for queues it cannot process. The worker-queue env
setting remains a configuration contract; this binary explicitly selects raw
Telegram intake. No Service is needed because the app exposes no inbound port.

For later pushes to the same tag, apply alone may not change the pod template:

```sh
kubectl rollout restart deployment/satoshi-channel-updates-manager
kubectl rollout status deployment/satoshi-channel-updates-manager
```

## Rollback

Restore the original TypeScript manifest (including probes, environment and KEDA
triggers) and pin its image to the recorded `repository@sha256:...` digest before
applying it. `rollout undo` alone may pull the new Rust image because both revisions
use mutable `latest`. Already acknowledged signals cannot be recovered by rollback.

Files are deployment preparation; creating them does not push or apply anything.
