# Validation suite

Manual verification for CameoDB. There is no CI by design — these scripts are the gate,
and running them is a deliberate act before a release or after a change to configuration,
TLS, limits, or dependencies.

```bash
cargo build --release          # or export CAMEODB_BIN=/path/to/cameodb
scripts/validate/all.sh        # everything
scripts/validate/all.sh posture tls
```

Each suite exits non-zero on failure and prints a `PASS`/`FAIL` line per check, so the
output of a run is the evidence. Paste the summary into `RELEASE-CHECKLIST.md`.

| Suite | What it proves | Why it cannot be a unit test |
|-------|----------------|------------------------------|
| `deps` | fmt, clippy (`-D warnings`), `cargo audit`, `cargo deny`, advisory exceptions still in date | Needs the real dependency graph and the current advisory database |
| `unit` | `cargo test --workspace` | — |
| `posture` | Body limits, request timeout, concurrency shedding, health exemption, CORS headers, admin gating, preset rejections | Only fails in a real HTTP stack: a limit that covers some handlers, a guard that starves liveness, a timeout never wired into the router |
| `tls` | HTTPS actually serves; bad certificates fail before the banner; the TLS listener drains on shutdown | TLS shipped broken because nothing ever bound a socket — rustls panicked at first use |
| `remote-sources` | The client's outbound HTTPS works against real hosts and still verifies certificates | Trust stores differ per target: macOS Keychain, Linux `/etc/ssl/certs`, musl containers need `ca-certificates` |

## Per-target runs

`remote-sources` is the one suite whose result does not transfer between platforms. Run it
on each target you ship — including inside the musl container — because the trust store,
not the TLS code, is what varies:

```bash
docker run --rm -v "$PWD:/src" -w /src <builder-image> \
    env CAMEODB_BIN=/src/target/x86_64-unknown-linux-musl/release/cameodb \
    scripts/validate/remote-sources.sh
```

Behind a TLS-inspecting proxy the corporate CA must be in the OS trust store. That was
also true of the previous native-tls stack; nothing about the requirement changed when the
client moved to rustls.

## Environment

| Variable | Effect |
|----------|--------|
| `CAMEODB_BIN` | Binary under test (default: `target/release/cameodb`, then `target/debug/cameodb`) |
| `POSTURE_PORT` / `TLS_PORT` | Ports for the probe servers (default 19490 / 19491) |
| `REMOTE_SOURCE_1`, `REMOTE_SOURCE_2` | Override the fetched URLs for an offline network |
| `BADSSL_URL` | Host used for the certificate-rejection check |
