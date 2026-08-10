#!/bin/sh
set -eu

[ "$#" -eq 1 ] || { echo "usage: run-release-migration-evidence.sh <evidence-out>" >&2; exit 64; }
evidence_out=$1
case "$evidence_out" in /*) ;; *) echo "migration evidence output must be absolute" >&2; exit 64 ;; esac
[ ! -e "$evidence_out" ] || { echo "migration evidence output already exists" >&2; exit 1; }
: "${RELEASE_INPUT_candidate_migrate_image:?candidate migrate image is required}"
: "${RELEASE_POSTGRES_IMAGE:?pinned PostgreSQL image is required}"
: "${CHAOS_COMPOSE_PROJECT:?formal Compose project is required}"
: "${CHAOS_COMPOSE_BASE:?formal Compose base is required}"
: "${CHAOS_COMPOSE_OVERRIDE:?formal Compose override is required}"

pinned_postgres='postgres:16-alpine@sha256:57c72fd2a128e416c7fcc499958864df5301e940bca0a56f58fddf30ffc07777'
[ "$RELEASE_POSTGRES_IMAGE" = "$pinned_postgres" ] || { echo "unexpected PostgreSQL image" >&2; exit 1; }
formal_compose() {
  docker compose --env-file /dev/null -p "$CHAOS_COMPOSE_PROJECT" -f "$CHAOS_COMPOSE_BASE" -f "$CHAOS_COMPOSE_OVERRIDE" "$@"
}
exact_container_id() { printf '%s\n' "$1" | grep -Eq '^[0-9a-f]{64}$'; }

postgres_container=$(formal_compose ps -q postgres)
exact_container_id "$postgres_container"
database_image=$(docker inspect --format '{{.Config.Image}}' "$postgres_container")
database_server_major=$(docker exec "$postgres_container" psql -U live -d live -Atc 'SHOW server_version_num' | cut -c1-2)
[ "$database_image" = "$pinned_postgres" ] && [ "$database_server_major" = 16 ]

migration_started=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
execution_index=0
for direction in up down up; do
  migration_container=$(formal_compose run -d --no-deps migrate -direction "$direction")
  exact_container_id "$migration_container"
  actual_migrate_image=$(docker inspect --format '{{.Config.Image}}' "$migration_container")
  [ "$actual_migrate_image" = "$RELEASE_INPUT_candidate_migrate_image" ]
  exit_code=$(docker wait "$migration_container")
  docker rm "$migration_container" >/dev/null
  case "$exit_code" in 0) ;; *) echo "candidate migrate $direction exited $exit_code" >&2; exit 1 ;; esac
  execution_index=$((execution_index + 1))
  case "$execution_index" in
    1) migration_id_1=$migration_container ;;
    2) migration_id_2=$migration_container ;;
    3) migration_id_3=$migration_container ;;
    *) exit 1 ;;
  esac
done
[ "$execution_index" -eq 3 ]
migration_finished=$(date -u '+%Y-%m-%dT%H:%M:%SZ')

temporary=$(mktemp "$(dirname -- "$evidence_out")/.migration-evidence.XXXXXX")
trap 'rm -f "$temporary"' EXIT HUP INT TERM
jq -cn \
  --arg candidate "$RELEASE_INPUT_candidate_migrate_image" \
  --arg database "$database_image" --arg database_container "$postgres_container" \
  --arg started "$migration_started" --arg finished "$migration_finished" \
  --arg up1 "$migration_id_1" --arg down "$migration_id_2" --arg up2 "$migration_id_3" \
  --argjson major "$database_server_major" \
  '{schema_version:"release-artifact-v2",artifact_type:"migration_rollback",status:"passed",migration:"up-down-up",candidate_image:$candidate,executions:[{direction:"up",container_id:$up1,exit_code:0},{direction:"down",container_id:$down,exit_code:0},{direction:"up",container_id:$up2,exit_code:0}],database_image:$database,database_container_id:$database_container,database_server_major:$major,started_at:$started,finished_at:$finished}' >"$temporary"
chmod 0600 "$temporary"
mv "$temporary" "$evidence_out"
trap - EXIT HUP INT TERM
