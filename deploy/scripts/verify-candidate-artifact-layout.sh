#!/bin/sh
set -eu

[ "$#" -eq 2 ] || { echo "usage: verify-candidate-artifact-layout.sh <target> <directory>" >&2; exit 64; }
target=$1
directory=$2
case "$target" in gateway|worker|migrate) ;; *) echo "invalid candidate target" >&2; exit 64 ;; esac
[ -d "$directory" ] && [ ! -L "$directory" ] || { echo "candidate artifact directory is invalid" >&2; exit 1; }

expected=$(printf '%s\n' \
  "$target.digest" \
  "$target.index.json" \
  "$target.oci.tar" \
  "$target.provenance-summary.json" \
  "$target.provenance.json" \
  "$target.sbom-summary.json" \
  "$target.sbom.json" \
  "$target.supply-validation.json" | LC_ALL=C sort)
actual=$(find "$directory" -mindepth 1 -maxdepth 1 -exec basename {} \; | LC_ALL=C sort)
[ "$actual" = "$expected" ] || { echo "candidate artifact layout is not the closed expected file set" >&2; exit 1; }
for name in $expected; do
  [ -f "$directory/$name" ] && [ ! -L "$directory/$name" ] || { echo "candidate artifact $name is not a regular non-symlink file" >&2; exit 1; }
done
