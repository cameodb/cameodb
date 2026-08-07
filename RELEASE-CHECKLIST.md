# Release checklist

CameoDB has no CI. Verification is manual and this file is the record of it — copy the
template to the bottom of this document, fill it in per release, and commit it. An empty
box is a question that has not been answered, not a formality.

## Procedure

1. **Build every target you intend to ship.**
   ```bash
   cargo build --release
   scripts/build/build-musl.sh          # static Linux
   scripts/build/build-dist.sh          # packages
   ```
2. **Run the validation suite** on the host build.
   ```bash
   scripts/validate/all.sh
   ```
3. **Run `remote-sources` on every other target.** It is the only suite whose result does
   not transfer between platforms — the trust store differs (macOS Keychain, Linux
   `/etc/ssl/certs`, musl containers need `ca-certificates`). See
   [scripts/validate/README.md](scripts/validate/README.md).
4. **Review the advisory exceptions.** `deps.sh` fails once a `review-by` date in
   `deny.toml` has passed. Renewing one means checking for an upstream fix first, not
   moving the date.
5. **Confirm the posture of the configs you ship or document.**
   ```bash
   cameodb check-config -c cameodb.example.toml
   ```
6. **Record the outcome below**, including anything skipped and why.

## Known gaps carried into every release

These are accepted, not fixed. They belong in release notes as much as here.

- **No authentication anywhere.** Every HTTP and MCP endpoint is open to whoever can reach
  the port. The `external` profile refuses to start for this reason; `internal` deployments
  must sit behind a network boundary or an authenticating proxy. Tracked as ROADMAP
  Phase 14 Stage B1.
- **`/_admin/*` is unauthenticated** when enabled — memory purge, forced commit, writer
  eviction. Set `admin_enabled = false` where the node is reachable off-box.
- **Cluster PSK has no rotation path.** Changing it requires stopping every node.
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
| tls            |  |  |  |  |
| remote-sources |  |  |  |  |

Advisory exceptions reviewed: <ids and their review-by dates>
Skipped checks and why:
Known gaps acknowledged: yes / no
Signed off by:
```

---

## History

<!-- Newest first. Append a filled-in template per release. -->
