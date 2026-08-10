# Release checklist

CameoDB has no CI. Verification is manual and this file is the record of it — copy the
template to the bottom of this document, fill it in per release, and commit it. An empty
box is a question that has not been answered, not a formality.

## Procedure

1. **Set the version.** Every crate under `crates/` carries its own `version`, and the
   internal path dependencies pin it too — a bump touches both in each manifest, then
   `cargo check --workspace` to refresh `Cargo.lock`. Confirm with
   `cargo run --bin cameodb -- --version`, which reads `CARGO_PKG_VERSION` and is the only
   place the number is observable at runtime.
   ```bash
   grep -rn '^version = ' crates/*/Cargo.toml     # all six must agree
   ```
2. **Move `[Unreleased]` to the new version in `CHANGELOG.md`**, dated, with a fresh empty
   `[Unreleased]` above it.
3. **Build and describe every target you intend to ship.**
   ```bash
   scripts/release/release.sh --stage build,sbom
   ```
   Stages artifacts under `dist/<version>/` and reads the version from the manifests, so the
   filenames cannot disagree with what the binary reports. Docker must be running: the
   zigbuild path cannot produce a static-pie binary and `build.sh` refuses to use it for a
   release. See [scripts/release/README.md](scripts/release/README.md).
4. **Run the validation suite** on the host build.
   ```bash
   scripts/validate/all.sh
   ```
5. **Run `remote-sources` on every other target.** It is the only suite whose result does
   not transfer between platforms — the trust store differs (macOS Keychain, Linux
   `/etc/ssl/certs`, musl containers need `ca-certificates`). See
   [scripts/validate/README.md](scripts/validate/README.md).
6. **Review the advisory exceptions.** `deps.sh` fails once a `review-by` date in
   `deny.toml` has passed. Renewing one means checking for an upstream fix first, not
   moving the date.
7. **Confirm the posture of the configs you ship or document.** Two files matter: the example
   in the repo root, and `crates/server/cameodb.toml`, which is what both the DEB and the RPM
   install to `/etc/cameodb/cameodb.toml`.
   ```bash
   cameodb check-config -c cameodb.example.toml
   cameodb check-config -c crates/server/cameodb.toml
   ```
8. **Build Windows on the Windows machine** and copy the result into the staging tree, which
   is where the sign stage will look for it.
   ```
   dist/<version>/windows/cameodb.exe
   ```
9. **Sign, checksum and publish.** Signing verifies each signature against the published
   public key as it goes; publishing is a dry run until `--commit`.
   ```bash
   COSIGN_PASSWORD=… scripts/release/release.sh --stage sign
   scripts/release/publish.sh              # review the add/replace report
   scripts/release/publish.sh --commit
   ```
10. **Commit the web project.** `publish.sh` only copies files; the `cameodb-web` checkout is
    left dirty for review.
11. **Record the outcome below**, including anything skipped and why.

## Known gaps carried into every release

These are accepted, not fixed. They belong in release notes as much as here.

- **Authentication is off by default.** A node with `[security] enabled = false` serves every
  HTTP and MCP endpoint to whoever can reach the port, `/_admin/*` included. That is a
  supported configuration for `local` and a warning for `internal`; `external` refuses to
  start without keys. Enabling it is one `cameodb keygen` and one stanza.
- **An MCP client authenticates with an HTTP header or not at all.** The key travels as
  `Authorization: Bearer`, so an MCP client that cannot set a header cannot reach an
  authenticated node. There is no OAuth flow and no per-client credential issuance.
- **API keys are read at startup.** Adding or revoking one is add → restart → migrate
  clients → remove → restart. No hot reload, and no lockout or throttle on failed
  authentication (against a 256-bit key it buys nothing and is itself a DoS lever — refusals
  are counted and logged instead).
- **Cluster peers are trusted by the PSK, not by API keys.** `allowed_indexes` is enforced at
  the HTTP/MCP ingress, where identity exists; it is not a defense against a compromised
  cluster member.
- **Cluster PSK has no rotation path.** Changing it requires stopping every node.
- **The audit trail is off by default, and is not tamper-evident.** With
  `[security.audit] enabled = false` a node keeps no record of who read what. Turned on, it
  is a file the node writes — nothing signs or chains the records, so ship it off the node if
  it needs to survive the node.
- **Three transitive advisories are ignored** (`deny.toml`), all via libp2p 0.56.0.

---

## Template

```
## vX.Y.Z — YYYY-MM-DD

Commit: <sha>
Built targets: <list>

| Suite | Host build | musl | windows | notes |
|-------|-----------|------|---------|-------|
| deps           |  |  |  |  |
| unit           |  |  |  |  |
| posture        |  |  |  |  |
| auth           |  |  |  |  |
| tls            |  |  |  |  |
| remote-sources |  |  |  |  |
| artifact       |  |  |  |  |

Advisory exceptions reviewed: <ids and their review-by dates>
Skipped checks and why:
Known gaps acknowledged: yes / no
Signed off by:
```

---

## History

<!-- Newest first. Append a filled-in template per release. -->
