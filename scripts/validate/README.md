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

Before any suite runs, `all.sh` refuses a binary whose `--version` reports
`+fault-injection` — a build carrying the panic-test seams, including an unauthenticated
`/__fault/panic` route. Every suite here would pass against one and the run would be
recorded as a validation of the product. The panic smoke test builds into
`target/panic-smoke/` so it cannot land on `target/release/cameodb` by accident; this is
the check for when one arrives some other way.

| Suite | What it proves | Why it cannot be a unit test |
|-------|----------------|------------------------------|
| `deps` | fmt, clippy (`-D warnings`), `cargo audit`, `cargo deny`, advisory exceptions still in date | Needs the real dependency graph and the current advisory database |
| `unit` | `cargo test --workspace` | — |
| `posture` | Body limits, request timeout, concurrency shedding, health exemption, CORS headers, admin gating, preset rejections, `keygen` and the `[security]` section it prints | Only fails in a real HTTP stack: a limit that covers some handlers, a guard that starves liveness, a timeout never wired into the router |
| `auth` | Every route refuses an anonymous caller; each role reaches exactly its capabilities; index scopes hold, both when an index is named and when one is listed; MCP tools, the tool catalogue and sessions are held to the key that called them; keygen writes key and hash files at 0600 and refuses to overwrite either; health tells an anonymous caller less; an unauthenticated flood does not shed authenticated requests; no key reaches a log line; the bundled client authenticates from a flag, a file or the environment and refuses to carry a key over plaintext to another host | The route table is decided in unit tests. This proves the decision is *in the request path*, in the right place in the layer stack — a middleware that is correct but mounted in the wrong order passes every unit test there is |
| `tls` | HTTPS actually serves; bad certificates fail before the banner; the TLS listener drains on shutdown | TLS shipped broken because nothing ever bound a socket — rustls panicked at first use |
| `remote-sources` | The client's outbound HTTPS works against real hosts and still verifies certificates | Trust stores differ per target: macOS Keychain, Linux `/etc/ssl/certs`, musl containers need `ca-certificates` |
| `cluster` (opt-in) | Three nodes in Docker: started together they find each other and agree on one ring, and a node started before its seeds joins once they are up; new-index writes and index deletes on every node at once leave every node answering; two nodes restarted together rejoin, converge and serve, and no committed document is lost; with one node frozen (`docker pause`) the others keep answering, and once it resumes the ring converges, every node serves and nothing is lost; no panic. Prints `METRIC` lines — storm latency and timeouts, latency while a peer is frozen and whether health noticed, time to converge, peer-loss and connection counts — to compare across builds | Formation, gossip and the cross-node actor asks exist only between processes that reach each other over libp2p |
| `artifact` | A Linux release binary has no interpreter and no `NEEDED` entries, is hardened as intended, and starts | Every property here is silently droppable — rustc falls back from `-static-pie` to `-static` with a warning when the linker refuses it, and `cargo zigbuild` triggers exactly that. Passing the flag and having the property are different claims, and only the second one ships |

## Cluster suite

Not in the default run: it builds a Docker image from the repo (or uses `CLUSTER_IMAGE`) and
takes about five minutes. Run it after any change to the swarm, the coordinator, the
orchestrator's cross-node paths, or the kameo / libp2p versions:

```bash
scripts/validate/all.sh cluster
CLUSTER_IMAGE=cameodb:pre scripts/validate/cluster.sh    # compare an older image
```

`PASS`/`FAIL` lines are properties that must hold. `METRIC` lines are measurements with no
verdict; paste them next to the build they came from, because the cluster work is judged by
how they move.

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
| `POSTURE_PORT` / `TLS_PORT` / `AUTH_PORT` | Ports for the probe servers (default 19490 / 19491 / 19492) |
| `CLUSTER_IMAGE` | Image for the cluster suite (default: build `cameodb:validate` from the repo) |
| `CLUSTER_PORT_BASE` | First of the three HTTP ports the cluster publishes on 127.0.0.1 (default 19481) |
| `CLUSTER_STORM_SECS`, `CLUSTER_STORM_WRITERS`, `CLUSTER_STORM_DELETERS` | Storm length and load per node (default 60 s, 2, 1) |
| `CLUSTER_FORM_ROUNDS`, `CLUSTER_FORM_SECS` | Fresh-start formation rounds, and how long each may take to converge (default 3, 60) |
| `CLUSTER_IDLE_SECS`, `CLUSTER_RESTART_ROUNDS`, `CLUSTER_FREEZE_SECS` | Idle wait before the second probe, restart rounds, how long node3 stays frozen (default 60, 3, 60) |
| `CLUSTER_KEEP` | Keep the scratch directory — compose file, data, node logs — even on a clean run |
| `REMOTE_SOURCE_1`, `REMOTE_SOURCE_2` | Override the fetched URLs for an offline network |
| `BADSSL_URL` | Host used for the certificate-rejection check |
