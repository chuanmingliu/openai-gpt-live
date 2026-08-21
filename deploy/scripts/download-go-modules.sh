#!/bin/sh
set -eu

attempt=1
while :; do
  if go mod download; then
    exit 0
  fi
  if [ "$attempt" -ge 3 ]; then
    echo "Go module download failed after three attempts" >&2
    exit 1
  fi
  echo "Go module download failed; retrying" >&2
  sleep "$attempt"
  attempt=$((attempt + 1))
done
