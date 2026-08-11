#!/usr/bin/env bash
# Stage 3 — sign every staged artifact with cosign, then verify each signature.
#
#   scripts/release/sign.sh            # COSIGN_PASSWORD from the environment, or empty
#
# Produces <artifact>.bundle next to each artifact, in the same sigstore bundle format the
# currently published .bundle files use (v0.3, with a Rekor transparency-log entry).
#
# Every signature is verified immediately after it is made, with the *public* key from the
# published tree — the copy downloaders actually fetch. An unverified signature is worth
# nothing: a wrong key, a stale bundle or a truncated upload all produce a plausible-looking
# .bundle, and the first party to discover it would otherwise be a user running
# `cosign verify-blob` against a release that cannot be verified.
#
# SBOMs are signed too. They are a security document served over the same channel as the
# binaries; leaving them unsigned means the one file describing what is inside the release is
# the one file nobody can authenticate.

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

case "${1:-}" in
    "")        ;;
    -h|--help) usage "${BASH_SOURCE[0]}" ;;
    *)         reject_unknown_arg "$1" ;;
esac

require_tool cosign

[ -d "$DIST" ] || die "nothing staged at $DIST — run scripts/release/build.sh first"
refuse_if_unhardened
[ -f "$COSIGN_KEY" ] || die "signing key not found at $COSIGN_KEY (override with COSIGN_KEY=...)"
[ -f "$COSIGN_PUB" ] || die "public key not found at $COSIGN_PUB (override with COSIGN_PUB=...)"

section "sign $VERSION"
info "key    $COSIGN_KEY"
info "pubkey $COSIGN_PUB"

# cosign prompts for the key password on every invocation, and there is one invocation per
# artifact — so without COSIGN_PASSWORD in the environment a release is eight or ten identical
# prompts. Default it to empty so the stage runs unattended; override for a key that has one.
#
# `${VAR+set}` rather than `${VAR:-}`: empty is a legitimate value here, and testing for
# non-emptiness cannot tell "deliberately empty" from "not configured".
if [ -z "${COSIGN_PASSWORD+set}" ]; then
    export COSIGN_PASSWORD=""
    info "password  COSIGN_PASSWORD unset, defaulted to empty"
elif [ -z "$COSIGN_PASSWORD" ]; then
    info "password  COSIGN_PASSWORD is empty"
else
    info "password  from COSIGN_PASSWORD"
fi

[ "$(staged_artifacts | wc -l | tr -d ' ')" -gt 0 ] || die "no artifacts staged under $DIST"

# A release that quietly ships without the Windows build is a release someone has to notice
# by looking at the published directory. Say it here instead.
[ -f "$DIST/windows/cameodb.exe" ] || warn "no windows/cameodb.exe staged — Windows will be missing from this release"

# Process substitution, not a pipe: a `while read` on the right-hand side of a pipe runs in a
# subshell, and the counter — plus any `die` — would be lost with it. (macOS ships bash 3.2,
# so `mapfile` is not available.)
signed=0
while IFS= read -r f; do
    rel="$(rel_to_dist "$f")"
    step "$rel"

    cosign sign-blob --yes \
        --key "$COSIGN_KEY" \
        --bundle "$f.bundle" \
        "$f" > /dev/null

    # Verifying with the bundle alone would confirm the bundle is internally consistent, not
    # that it was made with our key — hence --key on the verify side as well.
    if cosign verify-blob \
        --key "$COSIGN_PUB" \
        --bundle "$f.bundle" \
        "$f" > /dev/null 2>&1; then
        ok "signed and verified"
        signed=$((signed + 1))
    else
        die "signature for $rel does not verify against $COSIGN_PUB — do not publish this release"
    fi
done < <(staged_artifacts)

section "signed"
info "$signed artifacts, each verified against the published public key"
