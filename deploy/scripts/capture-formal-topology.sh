#!/usr/bin/env bash
set -euo pipefail

[ "$#" -eq 5 ] || { echo "usage: capture-formal-topology.sh <output-dir> <formal-config> <runtime-hash> <gateway-id> <worker-id>" >&2; exit 64; }
output_dir=$1
formal_config=$2
runtime_hash=$3
gateway_id=$4
worker_id=$5
case "$output_dir:$formal_config" in /*:/*) ;; *) echo "formal topology paths must be absolute" >&2; exit 64 ;; esac
printf '%s\n' "$runtime_hash" | grep -Eq '^v1:[0-9a-f]{64}$'
printf '%s\n%s\n' "$gateway_id" "$worker_id" | grep -Eq '^[0-9a-f]{64}$'
[ ! -e "$output_dir" ] || { echo "formal topology output directory already exists" >&2; exit 1; }

config_source=$(CDPATH= cd -- "$(dirname -- "$formal_config")" && pwd)/$(basename -- "$formal_config")
formal_config_sha=$(shasum -a 256 "$config_source" | awk '{print $1}')
temporary=$(mktemp -d "$(dirname -- "$output_dir")/.formal-topology.XXXXXX")
trap 'rm -rf "$temporary"' EXIT HUP INT TERM

inspect_runtime() {
  container_id=$1
  docker inspect "$container_id" | jq -ce --arg id "$container_id" --arg hash "$runtime_hash" --arg source "$config_source" '
    if type != "array" or length != 1 then error("inspect must contain exactly one container") else .[0] end
    | . as $container
    | [$container.Config.Env[]? | select(type == "string" and startswith("LIVE_EXPECTED_CONFIG_HASH="))] as $hashes
    | [$container.Mounts[]? | select(type == "object" and .Destination == "/etc/live/config.yaml")] as $mounts
    | if $container.Id != $id or $hashes != [("LIVE_EXPECTED_CONFIG_HASH=" + $hash)] or
         ($mounts | length) != 1 or $mounts[0].Source != $source or $mounts[0].RW != false
      then error("container runtime identity does not match the formal topology")
      else {
        container_id: $container.Id,
        runtime_config_hash: $hash,
        formal_config_source: $mounts[0].Source,
        formal_config_read_only: true
      }
      end
  '
}
gateway_identity=$(inspect_runtime "$gateway_id")
worker_identity=$(inspect_runtime "$worker_id")
compose_images=$(docker compose --env-file /dev/null -p "$CHAOS_COMPOSE_PROJECT" -f "$CHAOS_COMPOSE_BASE" -f "$CHAOS_COMPOSE_OVERRIDE" images --format json)
printf '%s\n' "$compose_images" | jq -e 'type == "array" and all(.[]; type == "object")' >/dev/null
images_sha=$(printf '%s\n' "$compose_images" | shasum -a 256 | awk '{print $1}')
jq -cn --arg runtime "$runtime_hash" --arg config_sha "$formal_config_sha" --arg images_sha "$images_sha" \
  --argjson gateway "$gateway_identity" --argjson worker "$worker_identity" \
  '{schema_version:"formal-topology-identity-v1",execution_environment:"formal-chaos",runtime_config_hash:$runtime,formal_config_sha256:$config_sha,compose_images_sha256:$images_sha,gateway:$gateway,worker:$worker}' \
  >"$temporary/topology-identity.json"
chmod 0600 "$temporary/topology-identity.json"
mv "$temporary" "$output_dir"
trap - EXIT HUP INT TERM
