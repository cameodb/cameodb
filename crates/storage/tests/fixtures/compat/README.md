# Compatibility fixtures

Shards written by released builds, opened by `tests/compat_fixture_test.rs` to prove this build
still reads them (ROADMAP E4). Each one holds one index, `compat`, left the way a killed process
leaves it:

- `d1`–`d6` written and committed, with `d3` deleted before the commit;
- a WAL tail durable in redb and never committed to Tantivy: `t1` and `t2` added, `d2`
  replaced, `d4` deleted;
- then `std::process::exit`: no commit, no close, no destructor. The writer's lock files, a
  half-written segment and a Tantivy temp file are part of that state and are kept.

| Fixture | Written by | What it is the evidence for |
|---|---|---|
| `v0.3.0/` | 0.3.0, `3a952ae` | The legacy shapes: the index declares `_seq`, and WAL entries are whole `WalOp` JSON values. Retired 2026-08-19; nothing at HEAD can write either |
| `v0.3.9/` | 0.3.9, `3e36143` | The shapes 0.3.x ends on: id-only WAL entries, the checkpoint in the commit payload, a ghost field |

`generate.rs` in each directory is the program that wrote it. It is not built here: it compiles
against the storage crate as it was at that commit, and the fixture is only evidence if that
build wrote it.

## Regenerating, or adding a release

Never rewrite a fixture to make a test pass: a fixture that changes stops being evidence of what
the old build wrote. Add a new one for a new release instead.

```sh
git worktree add /tmp/compat-<version> <release commit>
mkdir -p /tmp/compat-<version>/crates/storage/examples
cp crates/storage/tests/fixtures/compat/v0.3.9/generate.rs \
   /tmp/compat-<version>/crates/storage/examples/compat_fixture.rs
# adjust the generator to that commit's StorageConfig and schema API, then:
(cd /tmp/compat-<version> && cargo run -p storage --example compat_fixture -- "$PWD/out")
cp -R /tmp/compat-<version>/out crates/storage/tests/fixtures/compat/v<version>
cp /tmp/compat-<version>/crates/storage/examples/compat_fixture.rs \
   crates/storage/tests/fixtures/compat/v<version>/generate.rs
git worktree remove --force /tmp/compat-<version>
```

Then add a test for it in `tests/compat_fixture_test.rs`.
