#!/bin/sh
set -eu
if [ "$#" -ne 3 ]; then
  echo "usage: validate-render.sh local|production local|fixture|deploy RENDERED_YAML" >&2
  exit 64
fi
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"
cache=${TMPDIR:-/tmp}/gpt-live-task23-go-cache
[ -s "$3" ] || { echo "deployment policy failed: render is empty" >&2; exit 1; }
(cd "$root" && GOCACHE="$cache" go run ./deploy/validate --overlay "$1" --readiness "$2" "$3")
