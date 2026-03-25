# Security Scripts

Scripts for security, compliance, and supply chain management.

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
