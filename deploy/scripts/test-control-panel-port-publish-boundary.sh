#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
scratch=$(mktemp -d "${TMPDIR:-/tmp}/gpt-live-port-publish-boundary.XXXXXX")
trap 'rm -rf "$scratch"' EXIT HUP INT TERM
mkdir -p "$scratch/bin"

cat >"$scratch/bin/docker" <<'EOF'
#!/bin/sh
set -eu
printf '%s\n' "$*" >>"${PORT_TEST_CALLS:?}"
case "${1:-}" in
  info|build|run|rm) exit 0 ;;
  version) printf '%s\n' 'arm64'; exit 0 ;;
  image) exit 1 ;;
  port) printf '%s\n' '127.0.0.1:49123'; exit 0 ;;
  pull) exit 0 ;;
esac
exit 71
EOF
cat >"$scratch/bin/go" <<'EOF'
#!/bin/sh
set -eu
output=
while [ "$#" -gt 0 ]; do
  if [ "$1" = -o ]; then output=$2; shift 2; continue; fi
  shift
done
[ -n "$output" ] || exit 72
: >"$output"
EOF
cat >"$scratch/bin/curl" <<'EOF'
#!/bin/sh
exit 0
EOF
cat >"$scratch/bin/sleep" <<'EOF'
#!/bin/sh
exit 0
EOF
chmod +x "$scratch/bin"/*

: >"$scratch/calls"
PATH="$scratch/bin:/usr/bin:/bin" PORT_TEST_CALLS="$scratch/calls" \
  "$root/deploy/scripts/test-control-panel-port-publish.sh"

grep -F 'build ' "$scratch/calls" >/dev/null || { echo "port regression did not build its local listener image" >&2; exit 1; }
if grep -F 'pull ' "$scratch/calls" >/dev/null; then
  echo "port regression contacted a mutable registry image" >&2
  exit 1
fi
grep -F 'run -d --rm --name ' "$scratch/calls" >/dev/null || { echo "port regression did not start its disposable local container" >&2; exit 1; }

echo "control-panel port publish boundary test passed"
