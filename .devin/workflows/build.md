# Build Instructions

## Standard Release Build

```bash
cargo build --release
```

## Linux MUSL Target Build

```bash
cargo zigbuild --release --target x86_64-unknown-linux-musl \
  --no-default-features
```

## RPM Package Generation

```bash
cargo generate-rpm -p crates/server --target x86_64-unknown-linux-musl --auto-req disabled \
  -o target/x86_64-unknown-linux-musl/release/cameodb-0.2.2-1.x86_64.rpm \
  --set-metadata 'package.name="cameodb"'
```

## DEB Package Generation

```bash
cargo deb --no-build --no-strip --target x86_64-unknown-linux-musl -p server \
  --output target/x86_64-unknown-linux-musl/release/cameodb_0.2.2_amd64.deb
```

## Cosign Signing

### Sign native binary
cosign sign-blob \
  --key /usr/local/share/ca-certificates/cosign.key \
  --bundle target/release/cameodb.bundle \
  target/release/cameodb

### Sign MUSL binary
cosign sign-blob \
  --key /usr/local/share/ca-certificates/cosign.key \
  --bundle target/x86_64-unknown-linux-musl/release/cameodb.bundle \
  target/x86_64-unknown-linux-musl/release/cameodb

### Sign RPM package
cosign sign-blob \
  --key /usr/local/share/ca-certificates/cosign.key \
  --bundle target/x86_64-unknown-linux-musl/release/cameodb-0.2.2-1.x86_64.rpm.bundle \
  target/x86_64-unknown-linux-musl/release/cameodb-0.2.2-1.x86_64.rpm

### Sign DEB package
cosign sign-blob \
  --key /usr/local/share/ca-certificates/cosign.key \
  --bundle target/x86_64-unknown-linux-musl/release/cameodb_0.2.2_amd64.deb.bundle \
  target/x86_64-unknown-linux-musl/release/cameodb_0.2.2_amd64.deb
