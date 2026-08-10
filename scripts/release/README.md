# Release pipeline

Builds, describes, signs and publishes a CameoDB release. Four stages, driven by
`release.sh`, each also runnable on its own.

```bash
scripts/release/release.sh --stage build         # mac + linux binaries, DEB, RPM
scripts/release/release.sh --stage sbom          # SPDX + CycloneDX

#   ... build cameodb.exe on the Windows machine and copy it into dist/<version>/windows/

scripts/release/release.sh --stage sign           # see COSIGN_PASSWORD below
scripts/release/publish.sh                       # dry run — shows what would change
scripts/release/publish.sh --commit              # copy into cameodb-web
```

Everything is staged under `dist/<version>/` (gitignored) and every stage after `build`
reads from there rather than from `target/`, so signing or re-hashing does not depend on a
build tree that may have been cleaned in between.

## The version is read from the tree

`crates/*/Cargo.toml` is the only source. `lib.sh` refuses to run if the six crates disagree,
and no stage accepts a version argument. `build-packages.sh` previously carried
`VERSION="0.2.3"` as a literal, so it named its packages 0.2.3 regardless of the tree it was
run against: at 0.3.0 it would have produced a `cameodb_0.2.3_amd64.deb` whose binary answers
`--version` with 0.3.0, and nothing downstream compares the two.

Override for a test run with `CAMEODB_VERSION=…`.

## What each stage produces

```
dist/0.3.0/
  mac/cameodb                       + .bundle .sha256    arm64, Apple Silicon only
  linux/cameodb                     + .bundle .sha256    static-pie musl x86_64
  linux/cameodb_0.3.0_amd64.deb     + .bundle .sha256
  linux/cameodb-0.3.0-1.x86_64.rpm  + .bundle .sha256
  windows/cameodb.exe               + .bundle .sha256    built elsewhere, dropped in
  windows/cameodb.exe.version                            optional, not published
  cameodb.spdx.json                 + .bundle .sha256
  cameodb.cyclonedx.json            + .bundle .sha256
  SHA256SUMS                                             whole release in one `shasum -c`
  MANIFEST.txt                                           version, commit, sizes, signed y/n
```

### build

macOS comes from `cargo build --release`; the stage asserts the result is `arm64` rather than
trusting the host, because a wrong-architecture Mach-O fails at exec time with "bad CPU type"
and no checksum or signature catches that.

Linux goes through `scripts/build/build-musl.sh`, which runs
`scripts/validate/artifact.sh` and exits non-zero on a hardening failure. **A release must use
the container path.** The zigbuild path always fails the PIE check — zig's linker refuses
`-static-pie` and rustc silently downgrades to `-static`, costing ASLR and BIND_NOW — so
`build.sh` refuses to run with `BUILD_WITH=zig` unless you pass `--allow-unhardened`, which is
for rehearsing the pipeline, not for shipping.

The DEB and RPM wrap that same binary via `--no-build`. They are not rebuilt.
`scripts/build/build-packages.sh` rebuilds under `release-docker` (thin LTO, 4 codegen units)
instead, which meant the binary inside the packages was a different, less-optimized artifact
than the standalone binary published beside it — indistinguishable afterwards, since both
report the same `--version`.

### sbom

Scope is `Cargo.lock` and nothing else:

```
syft dir:. --override-default-catalogers rust-cargo-lock-cataloger \
           --select-catalogers -file \
           --source-name cameodb --source-version <version>
```

Both flags are load-bearing, and they are not interchangeable — `--select-catalogers` accepts
only tags, never cataloger names, and syft re-adds the `file` cataloger by default unless
told otherwise. The result is ~580 cargo packages in about 1MB.

For comparison, the SBOM shipped before this stage existed came from a plain `syft dir:.`: 30MB,
an SPDX `name` field reading `/Users/gc/code/cameodb`, 147k individual file entries, and among
its 882 "packages" ten copies each of `actions/setup-node` and `actions/checkout` scavenged from
workflow files under the tree. None of that described the shipped binary; a consumer scanning it
for CVEs was matching against a laptop. The stage now asserts the package count against
`Cargo.lock` and greps its own output for `$HOME`, so a cataloger rename cannot quietly restore
the old scan.

> The Rust binaries carry no embedded dependency list, so syft cannot derive components from
> the artifact itself. Building with [`cargo auditable`](https://github.com/rust-secure-code/cargo-auditable)
> would embed one, and `syft dist/0.3.0/linux/cameodb` would then describe the binary rather
> than the lock file it was built from. That is a build-toolchain change, so it is not done
> here.

### sign

`cosign sign-blob --yes --key … --bundle <artifact>.bundle`, matching the sigstore bundle v0.3
format already published, with a Rekor transparency-log entry.

Every signature is verified immediately with `cosign verify-blob` against
`$WEB_ROOT/cosign.pub` — the public key downloaders actually fetch, not a local copy. A wrong
key, stale bundle or truncated upload all produce a plausible-looking `.bundle`; without the
round-trip the first party to find out is a user.

SBOMs are signed too. They are security documents served over the same channel as the
binaries, and leaving them unsigned makes the one file describing the release contents the
one file nobody can authenticate.

#### Key and password

The signing key is read from `COSIGN_KEY`, default `~/.cosign/cosign.key`. Keep it at mode 600
in a directory at mode 700 that holds nothing else — not alongside CA certificates, which is a
directory something eventually treats as certs-to-distribute.

cosign asks for the key password once per invocation, and there is one invocation per artifact,
so `COSIGN_PASSWORD` has to be in the environment for the stage to run unattended. Any value
works, including an empty one where the key has no password — `sign.sh` defaults it to empty
when it is unset, and prints which case applies.

```bash
scripts/release/release.sh --stage sign

COSIGN_PASSWORD="$(security find-generic-password -s cosign-cameodb -w)" \
  scripts/release/release.sh --stage sign
```

#### Rotating the key invalidates published signatures

`downloads/cosign.pub` is a single unversioned file that every published `.bundle` verifies
against. Replacing it — a new key, a move to KMS — silently breaks every signature already
published. If that day comes, publish the public key version-stamped as well
(`cosign-0.3.0.pub`) and keep the old ones. `publish.sh` never touches `cosign.pub`, so nothing
here will overwrite it by accident.

#### cosign v3 always writes to the transparency log

`sign-blob` in cosign v3.1.3 has no `--tlog-upload` flag; it was removed. Every signature
creates a public Rekor entry, so the stage needs network access, and signing cannot be
rehearsed offline against a throwaway key without publishing entries to that log.

### publish

Writes `<artifact>.sha256` as `<hash>  <basename>`, plus an aggregate `SHA256SUMS`, and
verifies the aggregate against the staged files before going further.

Earlier releases published a `.sha256` holding an absolute path:

```
a319fd07…  /Users/gc/code/cameodb-web/public/downloads/linux/cameodb
```

which leaked the signing machine's layout and made `shasum -c cameodb.sha256` fail for every
downloader, since that directory does not exist on their machine. A bare basename verifies from
the download directory, which is where anyone would actually run it.

Then it copies into `cameodb-web/public/downloads/`. **Dry run by default** — it writes into
another repository's live tree, so it reports each file as `add`, `same` or `replace`, showing
the old and new size and hash for a replacement. `--commit` performs the copy.

It refuses to publish a release that is unsigned — any staged artifact without a `.bundle` — or
one staged with `--allow-unhardened`, which the build stage records in `dist/<version>/.unhardened`
because an unhardened binary signs and checksums exactly as cleanly as a hardened one. Nothing is
ever deleted: version-stamped files from earlier releases are listed as stale and left in place.

## Environment

| Variable | Default | Purpose |
|---|---|---|
| `CAMEODB_VERSION` | from `crates/*/Cargo.toml` | override for a test run |
| `CAMEODB_WEB` | `/Users/gc/code/cameodb-web/public/downloads` | publish target |
| `COSIGN_KEY` | `~/.cosign/cosign.key` | signing key |
| `COSIGN_PUB` | `$CAMEODB_WEB/cosign.pub` | key signatures are verified against |
| `COSIGN_PASSWORD` | empty | key password; any value, empty included |
| `LINUX_ARCHS` | `x86_64` | `x86_64`, `aarch64`, or both space-separated |
| `BUILD_WITH` | `auto` | `container` (required for releases) or `zig` |

Required tools: `cargo`, `cargo-deb`, `cargo-generate-rpm`, `docker`, `syft`, `cosign`, `jq`,
`shasum`. Each stage checks for its own before doing any work.

## Platform coverage

macOS is **arm64 only** — a deliberate choice, asserted by the build stage. Intel Macs are not
served. Adding them means building `x86_64-apple-darwin` and either `lipo`-ing a universal
binary or publishing a second file.

Linux is x86_64 only by default, matching what `downloads/linux/` publishes. `LINUX_ARCHS`
adds `aarch64`; the primary architecture keeps the bare `cameodb` name so existing URLs stay
valid, and any other is suffixed (`cameodb-aarch64`).

### Windows

Built on the Windows machine, then copied to exactly this path:

```
dist/<version>/windows/cameodb.exe          # e.g. dist/0.3.0/windows/cameodb.exe
```

Create the directory if the build stage has not run yet (`mkdir -p dist/0.3.0/windows`). From
there it is handled like every other artifact: signed, verified, checksummed, and copied to
`downloads/windows/`. Nothing else needs to be told about it.

Optionally, record the version on the machine that can actually run the binary:

```powershell
cameodb.exe --version > cameodb.exe.version
```

and copy that next to the `.exe`. The build stage then fails if it disagrees with the release
version. Without it the version is simply unverified — this host cannot execute a PE binary, and
`CARGO_PKG_VERSION` does not survive as a greppable string in any of our binaries, so there is
nothing to check locally. The sidecar is not itself published.

What the pipeline does and does not do for Windows:

| | |
|---|---|
| ✅ | rejects a file that is not a `PE32+ x86-64` executable — catches a truncated copy, a 32-bit build, or an ELF copied by mistake |
| ✅ | `cosign sign-blob` → `cameodb.exe.bundle`, verified before publishing |
| ✅ | `cameodb.exe.sha256`, inclusion in `SHA256SUMS`, listing in `MANIFEST.txt` |
| ✅ | fails the release if the optional `.version` sidecar disagrees |
| ❌ | does not build it — no cross-compilation is attempted |
| ❌ | no hardening validation; `validate/artifact.sh` reads ELF headers and does not apply to PE |
| ❌ | no smoke test — the binary cannot run here |
| ❌ | **no Authenticode signing.** The `.bundle` is a detached sigstore signature, good for `cosign verify-blob` but invisible to Windows itself. SmartScreen still treats the download as unsigned; that needs a code-signing certificate and `signtool`, which is out of scope here |

`build.sh` validates and reports it; `sign.sh` warns if it is still missing, so a release cannot
silently ship without it.

## Relationship to the rest of `scripts/`

This pipeline calls `build/build-musl.sh` and reuses `validate/artifact.sh` through it. It does
not call `build/build-packages.sh` — that script's container-rebuild path is the one this pipeline
deliberately replaces, though its hardcoded version has been fixed since it is still reachable
by hand. Run `validate/all.sh` before releasing; see [RELEASE-CHECKLIST.md](../../RELEASE-CHECKLIST.md).
