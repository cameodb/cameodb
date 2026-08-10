# Build Instructions

## Development builds

```bash
cargo build --release              # host binary; `cargo run --release` runs it
cargo build --release --workspace  # includes crates/bench
```

`default-members = ["crates/server"]` in the workspace manifest means a bare `cargo build` /
`cargo run` targets the server. Without it, `cargo run` cannot choose between the `cameodb`
and `cameodb-bench` binaries and errors out.

## Static Linux (musl)

```bash
scripts/build/build-musl.sh release x86_64     # container path: static-pie, RELRO, BIND_NOW
BUILD_WITH=zig scripts/build/build-musl.sh     # cross-compile from macOS; NOT release-grade
```

The zigbuild path cannot produce a position-independent binary — zig's linker refuses
`-static-pie` and rustc silently falls back to `-static`, losing ASLR. Fine for local testing,
never for a published artifact. Either way `scripts/validate/artifact.sh` checks the result
rather than assuming it.

Running `cargo zigbuild` directly requires `AR="zig ar" RANLIB="zig ranlib"` exported first, or
jemalloc's static archive is built with macOS `ranlib` and comes out empty — surfacing much
later as `undefined symbol: mallocx`. See [docs/BUILDING.md](../../docs/BUILDING.md).

## Releases — packages, SBOMs, signing, publishing

Do not assemble a release by hand. Every version-stamped filename, SBOM scope and signature is
produced by the pipeline in [scripts/release/](../../scripts/release/README.md):

```bash
scripts/release/release.sh --stage build,sbom    # binaries, DEB, RPM, SPDX, CycloneDX
#   ... build cameodb.exe on the Windows machine → dist/<version>/windows/
scripts/release/release.sh --stage sign
scripts/release/publish.sh                       # dry run
scripts/release/publish.sh --commit              # copy into cameodb-web
```

The version comes from `crates/*/Cargo.toml` and is never passed as an argument. Hardcoding it
in a build script is what produced `cameodb_0.2.3_amd64.deb` from a 0.3.0 tree, containing a
binary that answers 0.3.0 to `--version`.

Signing is `cosign sign-blob --key … --bundle <artifact>.bundle` per artifact, each one
verified with `cosign verify-blob` against the public key in the published tree before the
release proceeds.
