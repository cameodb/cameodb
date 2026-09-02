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
   cameodb check-config -c cameodb.example.toml --allow-unauthenticated
   cameodb check-config -c crates/server/cameodb.toml --allow-unauthenticated
   ```
   Both ship `profile = "internal"` on `0.0.0.0` with `[security] enabled = false`, which the
   check fails without that flag — correctly, since such a node is open to anyone who can reach
   the port. The flag records that the shipped default is deliberate; drop it and read the
   failure if you are considering changing that default. Everything else it reports still has to
   be reviewed.
8. **Build Windows on the Windows machine** and copy the result into the staging tree, which
   is where the sign stage will look for it.
   ```
   dist/<version>/windows/cameodb.exe
   ```
9. **Sign, checksum and publish.** Signing verifies each signature against the published
   public key as it goes; publishing is a dry run until `--commit`.
   ```bash
   scripts/release/release.sh --stage sign
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

## v0.3.3 — 2026-09-03

Commit: 673ca55 — `MANIFEST.txt` records `673ca5513524676011a4fa7ef150e03ac270b2fe`, which is the
tree the validation suite ran against, so for once the recorded commit and the validated one are
the same. Commits after it are this checklist record and nothing else.

Built targets: macOS arm64, x86_64-unknown-linux-musl (binary + `cameodb_0.3.3_amd64.deb` +
`cameodb-0.3.3-1.x86_64.rpm`), Windows x86_64 (`cameodb.exe`, 22M). All seven artifacts signed.

Validation suite, host build (macOS arm64), 2026-09-03 01:02:06 → 01:04:08:

```
binary: target/release/cameodb

  PASS deps
  PASS unit
  PASS posture
  PASS auth
  PASS tls
  PASS remote-sources
  PASS artifact
```

That binary is byte-identical to the staged `dist/0.3.3/mac/cameodb`
(sha256 `b90f335bdf25f925f4a895930a9bafca2c4268f63d2edcde2bc2b7759fdaaa8e`), so the result
describes the artifact that ships rather than a neighbouring build of it. It also still describes
the current tree: the last commit to touch `crates/` or `Cargo.*` was fed149a, before this binary
was built, and everything committed since is docs and scripts.

| Suite | Host build | musl | windows | notes |
|-------|-----------|------|---------|-------|
| deps           | PASS | —    | —    | three transitive advisories ignored, all review-by 2026-11-01 |
| unit           | PASS | —    | —    | |
| posture        | PASS | —    | —    | 44 checks; one had gone stale and is corrected in 673ca55 — see below |
| auth           | PASS | —    | —    | |
| tls            | PASS | —    | —    | |
| remote-sources | PASS | —    | —    | **outstanding on musl and Windows** — trust store differs per platform, so this result does not transfer (procedure step 5) |
| artifact       | PASS | —    | —    | host run |

Published to DockerHub: `goranc/cameodb:0.3.3` (index `sha256:ff88023d`) and `:latest`
(`sha256:660a3d77`), both `linux/amd64` + `linux/arm64` with per-platform SLSA provenance.
Verified by running each tag on each platform — all four report `cameodb 0.3.3`. The two platform
manifests under `latest`, `sha256:5796dc97` (amd64) and `sha256:9d6aad62` (arm64), are the same
digests the `--no-push` rehearsal exported locally, so the rehearsal built the bits that shipped.
Publishing remains manual: no step of this procedure asks for it.

Signed and staged: `dist/0.3.3/` holds all seven artifacts with a `.bundle` and `.sha256` each,
plus `SHA256SUMS`, `MANIFEST.txt` and both SBOMs. Signatures spot-checked here against the key
`dl.cameodb.com` serves rather than taken from the manifest's own "signed" column —
`cosign verify-blob` returns `Verified OK` for `windows/cameodb.exe`, `mac/cameodb` and
`linux/cameodb`. The staged `mac/cameodb` is still `b90f335b`, the binary the suite ran against.

`publish.sh` has copied everything into the `cameodb-web` checkout: 17 files replaced and the six
0.3.3 `.deb`/`.rpm` files added, with `public/downloads/MANIFEST.txt` now reading `CameoDB 0.3.3`
at the same commit.

**Recorded error — a posture check had gone stale and failed against correct behaviour.**
`oversized single record on /document/stream is rejected` asserted `413`; the endpoint answers
`200` and reports the refusal as a reason on the line. The behaviour is right and the check was
wrong. The stream handler commits micro-batches as the body arrives, so refusing the whole
request with a status abandons an import that has already written documents it can no longer
report — a bare `413`, no counts, nowhere to resume — and it was changed to answer the way
`_bulk` answers a bad row. The check dates from `fda841e` and was never updated, so it reported a
defect that did not exist while no longer covering the one it was written for: that an oversized
record is not *written*. It now asserts the response body — `items_written` and the reason text —
because a `200` that quietly wrote the record would satisfy a status check. Confirmed against a
live node before changing it: `items_written` is `0` and the reason reads
`line 1: exceeds the 1 MB single-record limit`. The two sibling checks still answer `413`
legitimately, from the wire-level body limit, and `RSS stayed bounded` holds at 35 MB.

The same drift had reached `docs/API_REFERENCE.md`. The streaming-write response was documented
as `took_ms` and `items_received`, neither of which the server emits, and omitted `status`,
`lines_received` and `batches`, all three of which it does; `_bulk` was documented with `took_ms`
for what is `duration_ms`. A client written against that page would have broken on fields it
never receives. Both corrected against a live 0.3.3 node.

The lesson for the procedure: a check that asserts a status code goes stale silently when the
contract moves to the body, and it fails *loudly in the wrong direction* — the suite accuses the
product. Prefer asserting the answer over the envelope.

Advisory exceptions reviewed: RUSTSEC-2026-0118, RUSTSEC-2026-0119 (hickory-proto 0.25.x,
transitive via libp2p 0.56.0, needs hickory-proto >=0.26.1), RUSTSEC-2024-0436 (`paste`
unmaintained, transitive via libp2p → if-watch). All three review-by 2026-11-01, so none was
renewed for this release; `deps` passing is the gate that says no date has lapsed.

Skipped checks and why:
- `remote-sources` on musl and Windows (procedure step 5) — not run.
- `check-config` on the two shipped configs (procedure step 7) — not recorded.

Outstanding, and neither blocks the artifacts:
- **`cameodb-web` is not committed.** `publish.sh` only copies; the checkout is a separate repo
  and its last commit is still `96c83da` "Publish 0.3.2 binaries". 17 modified and 6 untracked
  files are sitting there (procedure step 10). Nothing is served until that is committed.
- **No `v0.3.3` git tag.** `git tag` is empty — as it was for 0.3.0 through 0.3.2, so this is the
  standing habit rather than a lapse specific to this release.

Known gaps acknowledged: yes
Signed off by: Goran Cvijanovic

## v0.3.2 — 2026-08-20

Commit: 84b5d90 (dirty at build time — the version bump and changelog move were not yet
committed when the artifacts were staged, so `MANIFEST.txt` will record a commit that predates
them; same situation as 0.3.1, and deliberate here to keep the staging tree ready for the
Windows build)
Built targets: macOS arm64, x86_64-unknown-linux-musl (binary + `cameodb_0.3.2_amd64.deb` +
`cameodb-0.3.2-1.x86_64.rpm`). **Windows outstanding.**

Validation suite, host build (macOS arm64), 2026-08-20 18:33:34 → 18:45:00:

```
binary: target/release/cameodb

  PASS deps
  PASS unit
  PASS posture
  PASS auth
  PASS tls
  PASS remote-sources
  PASS artifact
```

That binary is byte-identical to the staged `dist/0.3.2/mac/cameodb`
(sha256 `a810f9d4ef225bea54581d9abde12e77ac175ccae58170cce04d502f9ffe4adc`), so the result
describes the artifact that ships rather than a neighbouring build of it.

| Suite | Host build | musl | windows | notes |
|-------|-----------|------|---------|-------|
| deps           | PASS | n/a  | n/a  | workspace-wide, not per-target |
| unit           | PASS | —    | —    | 553 tests across 31 targets |
| posture        | PASS | —    | —    | 44 checks; gained an HTTP/2 section in 84b5d90 |
| auth           | PASS | —    | —    | |
| tls            | PASS | —    | —    | |
| remote-sources | PASS | —    | —    | **outstanding on musl and Windows** — trust store differs per platform, so this result does not transfer (procedure step 5) |
| artifact       | PASS | PASS | —    | see below; musl re-run at 18:56:31 against the 0.3.2 binary |

The `artifact` PASS inside the 18:33 run inspected the *0.3.1* musl binary — the only one that
existed at the time, built 2026-08-17. It passed while reporting `cameodb 0.3.1`, which is why
the suite is not by itself evidence about this version. The 0.3.2 musl binary was then checked
twice: by `artifact.sh` inside `build-musl.sh` as a build gate, and by a standalone re-run at
18:56:31. Both 8/8, both reporting `cameodb 0.3.2`. Worth teaching the suite to compare the
artifact's version against the manifests, since a stale binary passes every hardening check it
has.

Advisory exceptions reviewed: RUSTSEC-2026-0118, RUSTSEC-2026-0119 (hickory-proto 0.25.x,
transitive via libp2p 0.56.0, needs hickory-proto >=0.26.1), RUSTSEC-2024-0436 (`paste`
unmaintained, transitive via libp2p → if-watch). All three review-by 2026-11-01 — not due,
not renewed. `cargo audit` clean over 578 crates.

SBOM: SPDX 579 packages, CycloneDX 578 components against a 578-package `Cargo.lock`, no local
paths in either header.

`check-config` on both shipped configs (`cameodb.example.toml`, `crates/server/cameodb.toml`):
OK — 5 and 4 accepted warnings. Both gained a `limits` warning since 0.3.1, from the middle
verdict added in 5390e5f. The example config's fifth is `node_key` reporting a 0644
`./data/cameodb/node_identity.json`; that is a local untracked file (gitignored at
`.gitignore:124`) and not a property of the shipped config.

Skipped checks and why:
- `remote-sources` on musl and Windows (procedure step 5) — not run.

Not yet done, release is incomplete:
- **`dist/0.3.2/windows/cameodb.exe` does not exist.** Build it on the Windows machine and copy
  it to exactly that path, then `--stage sign`.
- `--stage sign` and `publish.sh` have not run, so there are no `.bundle`, `.sha256`,
  `SHA256SUMS` or `MANIFEST.txt` files under `dist/0.3.2/` yet.
- `cameodb-web` has not been updated.
- The version bump and changelog move are still uncommitted; commit them before signing so the
  recorded commit describes the artifacts.

Known gaps acknowledged: yes
Signed off by:

## v0.3.1 — 2026-08-16

Commit: cb1b188 (dirty at build time — the version bump and changelog move were committed
after staging; nothing else in the tree changed between build and this commit)
Built targets: macOS arm64, x86_64-unknown-linux-musl (binary + `cameodb_0.3.1_amd64.deb` +
`cameodb-0.3.1-1.x86_64.rpm`), x86_64 Windows

Validation suite, host build (macOS arm64), 2026-08-16 15:00:50 → 15:03:08:

```
binary: target/release/cameodb

  PASS deps
  PASS unit
  PASS posture
  PASS auth
  PASS tls
  PASS remote-sources
  PASS artifact
```

`remote-sources` also run inside the musl builder container against
`target/x86_64-unknown-linux-musl/release/cameodb`, invoked directly rather than through
`scripts/validate/remote-sources.sh`: the builder image (`cameo-builder-base-amd64`) has
neither `curl` nor `wget`, so the script's own reachability pre-check fails closed and skips
the whole suite silently. All four checks the script would have made were run by hand and
passed — fetched `dl.cameodb.com/cameodb.spdx.json` and a GitHub raw file, and a self-signed
cert was rejected by default and accepted only with `--insecure-source`. Worth fixing in the
builder image or the script before the next release, so this doesn't need doing by hand again.

| Suite | Host build | musl | windows | notes |
|-------|-----------|------|---------|-------|
| deps           | PASS | n/a  | n/a  | workspace-wide, not per-target |
| unit           | PASS | —    | —    | |
| posture        | PASS | —    | —    | |
| auth           | PASS | —    | —    | |
| tls            | PASS | —    | —    | |
| remote-sources | PASS | PASS (manual, see above) | PASS | |
| artifact       | PASS | PASS | PASS | musl binary passed `artifact.sh` inside `build-musl.sh` as a build gate |

Advisory exceptions reviewed: RUSTSEC-2026-0118, RUSTSEC-2026-0119 (hickory-proto 0.25.x,
transitive via libp2p 0.56.0, needs hickory-proto >=0.26.1), RUSTSEC-2024-0436 (`paste`
unmaintained, transitive via libp2p → if-watch). All three review-by 2026-11-01 — not due,
not renewed.

`check-config` on both shipped configs (`cameodb.example.toml`,
`crates/server/cameodb.toml`): OK, 3 accepted warnings each (plaintext, unauthenticated admin
API, no auth) — the documented `internal`-profile posture, unchanged from 0.3.0.

Skipped checks and why:
- None. All suites run on all targets.

Known gaps acknowledged: yes
Signed off by:

## v0.3.0 — 2026-08-11

Commit: f1285de
Built targets: macOS arm64, x86_64-unknown-linux-musl (binary + `cameodb_0.3.0_amd64.deb` +
`cameodb-0.3.0-1.x86_64.rpm`), x86_64 Windows

Validation suite, host build (macOS arm64), 2026-08-11 10:51:16 → 10:53:21:

```
binary: target/release/cameodb

  PASS deps
  PASS unit
  PASS posture
  PASS auth
  PASS tls
  PASS remote-sources
  PASS artifact
```

That binary is byte-identical to the staged `dist/0.3.0/mac/cameodb`
(sha256 `26e5c1a2c1e0e441eb83aab5565667ae0d9c90b6b229f57da1782262b870b1a8`), so the result
describes the artifact that ships, not a neighbouring build of it.

| Suite | Host build | musl | windows | notes |
|-------|-----------|------|---------|-------|
| deps           | PASS | n/a  | n/a  | workspace-wide, not per-target |
| unit           | PASS | —    | —    | |
| posture        | PASS | —    | —    | |
| auth           | PASS | —    | —    | includes the credential-leak regression probe added in 6303ec1 |
| tls            | PASS | —    | —    | |
| remote-sources | PASS | —    | —    | **outstanding on musl and Windows** — trust store differs per platform, so this result does not transfer (procedure step 5) |
| artifact       | PASS | —    | —    | host run; the musl binary passed `artifact.sh` inside `build-musl.sh` as a build gate |

Advisory exceptions reviewed: RUSTSEC-2026-0118, RUSTSEC-2026-0119 (hickory-proto 0.25.x,
transitive via libp2p 0.56.0, needs hickory-proto >=0.26.1), RUSTSEC-2024-0436 (`paste`
unmaintained, transitive via libp2p → if-watch). All three review-by 2026-11-01, so none was
renewed for this release.

Skipped checks and why:
- `remote-sources` on musl and Windows — not yet run. Required before publishing.
- `check-config` on the two shipped configs (procedure step 7) — not yet recorded.

Not yet done, release is incomplete:
- **`dist/0.3.0/windows/cameodb.exe` predates the credential fix and must be rebuilt.** It was
  copied here 2026-08-11 00:08, and 6303ec1 — which stops the API key being sent to
  user-supplied source hosts — was committed at 10:09 the same day, so the staged `.exe` cannot
  contain it. The `cameodb` binary carries the client CLI, so this affects `schema detect` and
  `data load` on Windows. The macOS and musl artifacts were rebuilt after the fix (10:45,
  10:50) and are unaffected.
- `--stage sbom` and `--stage sign` have run — `dist/0.3.0/` holds both SBOMs and a verified
  `.bundle` per artifact — but `windows/cameodb.exe.bundle` signs the stale executable above.
  Rebuilding the `.exe` invalidates that bundle and its `.sha256`; re-run `--stage sign`.
- `publish.sh` has not been committed, and `cameodb-web` has not been updated.

Known gaps acknowledged: yes
Signed off by:
