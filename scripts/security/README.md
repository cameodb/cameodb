# Security Scripts

Scripts and tooling for security, compliance, and supply chain management.

## Dependency Auditing

### cargo-audit

Checks workspace dependencies against the [RustSec advisory database](https://rustsec.org/advisories/) for known vulnerabilities.

**Install:**
```bash
cargo install cargo-audit
```

**Usage:**
```bash
# Audit all workspace crates
cargo audit

# Audit and exit non-zero on warnings (for CI)
cargo audit --deny warnings
```

**Notes:**
- Vulnerabilities in transitive dependencies without upstream fixes are documented in `deny.toml` under `[advisories] ignore` with rationale.
- Run before releasing or when updating `Cargo.lock`.

### cargo-deny

Checks dependencies for advisories, license compliance, banned crates, and disallowed registry sources. Configuration lives in `deny.toml` at the workspace root.

**Install:**
```bash
cargo install cargo-deny
```

**Usage:**
```bash
# Run all checks (advisories, bans, licenses, sources)
cargo deny check

# Run individual checks
cargo deny check advisories
cargo deny check bans
cargo deny check licenses
cargo deny check sources
```

**What it checks:**
- **Advisories**: Same database as cargo-audit, plus configurable ignore list for unfixed transitive vulnerabilities
- **Bans**: Rejects wildcard dependencies and duplicate crate versions (with allow-listed exceptions)
- **Licenses**: Ensures all dependencies use approved licenses (configured in `deny.toml`)
- **Sources**: Blocks dependencies from non-crates.io registries

## SBOM Generation

### `generate-sbom.sh`

Generates Software Bill of Materials (SBOM) in both SPDX and CycloneDX formats using [syft](https://github.com/anchore/syft).

**Usage:**
```bash
# From Docker image (default) - outputs to scripts/security/
./scripts/security/generate-sbom.sh
./scripts/security/generate-sbom.sh 0.2.2

# From native binary
./scripts/security/generate-sbom.sh --native

# From source code (most complete)
./scripts/security/generate-sbom.sh --source

# Output to different directory
./scripts/security/generate-sbom.sh --output ./sboms
```

**Outputs:**
- `cameodb.spdx.json` - SPDX 2.3 format (written to `scripts/security/`)
- `cameodb.cyclonedx.json` - CycloneDX 1.5 format (written to `scripts/security/`)

**Prerequisites:**
```bash
brew install syft  # macOS
# Or: https://github.com/anchore/syft/releases
```

**Inspect SBOMs:**
```bash
# SPDX uses 'packages' array
jq '.packages | length' scripts/security/cameodb.spdx.json
jq -r '.packages[].name' scripts/security/cameodb.spdx.json

# CycloneDX uses 'components' array
jq '.components | length' scripts/security/cameodb.cyclonedx.json
jq -r '.components[].name' scripts/security/cameodb.cyclonedx.json
```

**Published SBOMs:**
- https://dl.cameodb.com/cameodb.spdx.json
- https://dl.cameodb.com/cameodb.cyclonedx.json
