# Owner authorization

Operation requires the repository owner's permission and an owner-signed JSON
authorization including service `channels-manager-v1`. A file scoped only to
`trading-station-v1` is insufficient. Reuse an approved file across routine starts;
do not issue a new authorization for every build.

On normal startup the gate runs before `.env.local`, configuration validation and
all network clients. Export `APP_AUTHORIZATION_FILE` as an absolute path
in the process environment, or mount the file at the default
`/run/authorization/authorization.json`. See [local commands](../COMMANDS.md).
Missing, unreadable, malformed, tampered or wrong-service authorization exits 78
with a fixed reason code and owner notice; paths and contents are not logged.
There is no runtime key override or bypass for service operation. Checks occur
once at startup, with no remote licensing service or periodic shutdown.
`--check-config` is an offline configuration-only mode that intentionally requires
no authorization. It proves configuration parsing only, not authorization validity,
file readability or dependency readiness.

## Issue once, outside the repository

The owner uses the existing standalone
[Trading Station issuer](../../trading-station-rust/docs/authorization.md).
From `channels-manager-v1`, build it without adding it to this app or image:

```sh
cargo build --release --locked --manifest-path ../trading-station-rust/tools/authorization-issuer/Cargo.toml
```

The owner fills the following shell variables privately: `OWNER_PRIVATE_KEY`
(existing encrypted key path), `AUTHORIZATION_ID` (new unique ID), `AUTHORIZED_TO`
(recipient), and `APP_AUTHORIZATION_FILE` (new output path in an existing,
owner-controlled directory outside the repository). Then run manually in a terminal:

```sh
../trading-station-rust/tools/authorization-issuer/target/release/authorization-issuer issue \
  --private-key "${OWNER_PRIVATE_KEY:?Set the existing encrypted owner key path}" \
  --key-id owner-2026-01 \
  --authorization-id "${AUTHORIZATION_ID:?Set a new authorization ID}" \
  --authorized-to "${AUTHORIZED_TO:?Set the approved recipient}" \
  --service channels-manager-v1 \
  --out "${APP_AUTHORIZATION_FILE:?Set a new external output file path}"
```

The signing key and ID must match the application's embedded owner trust. The
issuer prompts for the passphrase on the controlling terminal with echo disabled;
there is no password argument/environment/stdin option. Existing output is not
overwritten. Keep private keys and passphrases out of chat, logs, Git, CI, images
and clusters. Distribute only the signed JSON to its approved recipient; keep it
and its restricted backup outside Git. Do not paste its contents into tickets.
No owner file is needed for automated tests, which use public fixtures rejected
by the production trust set.

## File permissions

The issuer creates JSON with Unix mode `0600`. Local Cargo must be able to read
it. Containers run as UID/GID `65532:65532`; Linux bind mounts retain host ownership
and permissions. Prepare a separate restricted deployment copy of the signed
JSON, readable by GID 65532 (for example owner-controlled, group 65532, mode 0440),
with sufficient directory traversal permissions. Change permissions only on this
deployment copy, never on the owner's private key. Docker Desktop on macOS may
translate bind permissions differently; verify access during approved normal
startup with the actual container. `--check-config` does not read authorization.
A read-only bind does not itself grant read access. The Compose override requires an existing source file and
does not create a directory for a missing path.

Kubernetes projects Secret `channels-manager-authorization`, key
`authorization.json`, as mode `0440`, with pod `fsGroup: 65532` and a read-only
directory mount. It uses no `subPath`. `optional: true` lets a pod without the
Secret reach the application gate and reject startup; it does not make application
authorization optional. Image notice `/app/USAGE-NOTICE.md` is mode `0444`.

## Rotation and recovery

Follow the approved [file-based Secret procedure](deployment.md#build-and-replace).
Secret projection may update a running pod's file, but authorization is checked
only at startup. After an approved rotation, restart the Deployment using the
explicit context/namespace commands in that guide. Confirm authorized startup and
dependency/consumer readiness; Running/Ready alone is insufficient.

On exit 78, inspect the fixed reason code and mount/permission configuration.
Restore the approved known-good signed file or correct its mount, then perform
an approved restart. Do not remove enforcement or silently roll back to an old
unprotected binary. Such a rollback requires an explicit owner-approved security
exception. Recreate can cause downtime when replacement authorization fails.
