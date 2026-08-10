#!/usr/bin/env bash
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
authority=/usr/local/libexec/gpt-live/candidate-canary-edge
authority_dir=/usr/local/libexec/gpt-live
halt_policy=$root/deploy/release/candidate-canary-halt-policy-v1.json

fail() {
  echo "candidate canary platform edge failed" >&2
  exit 1
}

commit= config_hash= manifest= manifest_sha256= region=
gateway_image= worker_image= migrate_image= public_key= public_key_sha256=
envelope_out= minimum_observation_seconds=
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --commit) commit=${2-}; shift 2 ;;
    --config-hash) config_hash=${2-}; shift 2 ;;
    --manifest) manifest=${2-}; shift 2 ;;
    --manifest-sha256) manifest_sha256=${2-}; shift 2 ;;
    --region) region=${2-}; shift 2 ;;
    --gateway-image) gateway_image=${2-}; shift 2 ;;
    --worker-image) worker_image=${2-}; shift 2 ;;
    --migrate-image) migrate_image=${2-}; shift 2 ;;
    --public-key) public_key=${2-}; shift 2 ;;
    --public-key-sha256) public_key_sha256=${2-}; shift 2 ;;
    --envelope-out) envelope_out=${2-}; shift 2 ;;
    --minimum-observation-seconds) minimum_observation_seconds=${2-}; shift 2 ;;
    *) fail ;;
  esac
done

[[ "$commit" =~ ^[0-9a-f]{40}$ ]] || fail
[[ "$config_hash" =~ ^v1:[0-9a-f]{64}$ ]] || fail
[[ "$manifest_sha256" =~ ^[0-9a-f]{64}$ ]] || fail
[[ "$region" =~ ^[a-z]{2}(-gov)?-[a-z]+-[1-9][0-9]?$ ]] || fail
[[ "$gateway_image" =~ ^ghcr\.io/chuanmingliu/openai-gpt-live/gateway@sha256:[0-9a-f]{64}$ ]] || fail
[[ "$worker_image" =~ ^ghcr\.io/chuanmingliu/openai-gpt-live/worker@sha256:[0-9a-f]{64}$ ]] || fail
[[ "$migrate_image" =~ ^ghcr\.io/chuanmingliu/openai-gpt-live/migrate@sha256:[0-9a-f]{64}$ ]] || fail
[[ "$minimum_observation_seconds" == 1800 ]] || fail
[[ "$public_key_sha256" =~ ^[0-9a-f]{64}$ ]] || fail
for input_path in "$manifest" "$public_key"; do
  [[ "$input_path" == /* && -f "$input_path" && ! -L "$input_path" ]] || fail
done
[[ "$envelope_out" == /* && ! -e "$envelope_out" ]] || fail
[[ -f "$halt_policy" && ! -L "$halt_policy" ]] || fail
[[ "$(shasum -a 256 "$manifest" | awk '{print $1}')" == "$manifest_sha256" ]] || fail
decoded_public_key_sha256=$(base64 -D < "$public_key" | shasum -a 256 | awk '{print $1}') || fail
[[ "$decoded_public_key_sha256" == "$public_key_sha256" ]] || fail

# This executable and its containing directory are provisioned by the platform,
# not by the repository checkout. They must remain root-owned and non-writable
# by group/other so workflow content cannot replace the deployer/monitor.
for platform_path in "$authority_dir" "$authority"; do
  [[ -e "$platform_path" && ! -L "$platform_path" ]] || fail
  owner=$(stat -f '%u' "$platform_path" 2>/dev/null) || fail
  mode=$(stat -f '%Lp' "$platform_path" 2>/dev/null) || fail
  [[ "$owner" == 0 && "$mode" =~ ^[0-7]{3,4}$ ]] || fail
  (( (8#$mode & 0022) == 0 )) || fail
done
[[ -f "$authority" && -x "$authority" ]] || fail

scratch=$(mktemp -d "${TMPDIR:-/tmp}/gpt-live-candidate-canary.XXXXXX")
trap 'rm -rf -- "$scratch"' EXIT HUP INT TERM
receipt=$scratch/receipt.json
signature=$scratch/receipt.sig
halt_policy_sha256=$(shasum -a 256 "$halt_policy" | awk '{print $1}')

# The fixed authority owns deploy, >=30-minute observation, halt evaluation,
# and rollback on both success and failure. It emits a receipt only after the
# candidate has been removed and the prior state restored.
"$authority" run \
  --commit "$commit" --config-hash "$config_hash" \
  --manifest "$manifest" --manifest-sha256 "$manifest_sha256" \
  --halt-policy "$halt_policy" --halt-policy-sha256 "$halt_policy_sha256" \
  --region "$region" --gateway-image "$gateway_image" \
  --worker-image "$worker_image" --migrate-image "$migrate_image" \
  --minimum-observation-seconds "$minimum_observation_seconds" \
  --receipt-out "$receipt" --signature-out "$signature" || fail

cd "$root"
go run ./cmd/live-canary-receipt \
  --receipt "$receipt" --signature "$signature" --public-key "$public_key" \
  --public-key-sha256 "$public_key_sha256" \
  --envelope-out "$envelope_out" --commit "$commit" --config-hash "$config_hash" \
  --manifest-sha256 "$manifest_sha256" --halt-policy-sha256 "$halt_policy_sha256" \
  --region "$region" --gateway-image "$gateway_image" \
  --worker-image "$worker_image" --migrate-image "$migrate_image" || fail
