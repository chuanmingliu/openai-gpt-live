#!/bin/sh
set -eu

[ "$#" -eq 1 ] || { echo "usage: inspect-formal-artifact.sh <file>" >&2; exit 64; }
path=$1
[ ! -L "$path" ] && [ -f "$path" ] || { echo "formal artifact must be a regular non-symlink file" >&2; exit 1; }
name=${path##*/}
case "$name" in *[!A-Za-z0-9._-]*|'') echo "formal artifact name is unsafe: $name" >&2; exit 1 ;; esac
size=$(wc -c <"$path")
[ "$size" -le 1048576 ] || { echo "formal artifact exceeds the 1 MiB release artifact cap" >&2; exit 1; }
sha=$(shasum -a 256 "$path" | awk '{print $1}')
case "$sha" in [0-9a-f][0-9a-f][0-9a-f][0-9a-f]*) [ "${#sha}" -eq 64 ] ;; *) false ;; esac || {
  echo "formal artifact digest is malformed" >&2
  exit 1
}
printf '%s %s\n' "$name" "$sha"
