#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
scratch=$(mktemp -d "${TMPDIR:-/tmp}/gpt-live-go-mod-download-test.XXXXXX")
trap 'rm -rf "$scratch"' EXIT HUP INT TERM
[ -x "$root/deploy/scripts/download-go-modules.sh" ] || {
  echo "bounded Go module download helper is missing" >&2
  exit 1
}

cat >"$scratch/go" <<'EOF'
#!/bin/sh
set -eu
[ "$*" = "mod download" ] || exit 90
printf '%s\n' attempt >>"$GO_DOWNLOAD_CALLS"
count=$(wc -l <"$GO_DOWNLOAD_CALLS" | tr -d ' ')
if [ -n "${GO_DOWNLOAD_ALWAYS_FAIL:-}" ] || [ "$count" -lt 3 ]; then
  echo "go: module proxy: EOF" >&2
  exit 1
fi
EOF

cat >"$scratch/sleep" <<'EOF'
#!/bin/sh
set -eu
printf '%s\n' "$1" >>"$GO_DOWNLOAD_SLEEPS"
EOF
chmod +x "$scratch/go" "$scratch/sleep"

: >"$scratch/calls"
: >"$scratch/sleeps"
PATH="$scratch:/usr/bin:/bin" GO_DOWNLOAD_CALLS="$scratch/calls" GO_DOWNLOAD_SLEEPS="$scratch/sleeps" \
  "$root/deploy/scripts/download-go-modules.sh" >"$scratch/retry.out" 2>"$scratch/retry.err"
[ "$(wc -l <"$scratch/calls" | tr -d ' ')" -eq 3 ] || { echo "module download was not retried twice" >&2; exit 1; }
[ "$(wc -l <"$scratch/sleeps" | tr -d ' ')" -eq 2 ] || { echo "module retry backoff count is wrong" >&2; exit 1; }

: >"$scratch/calls"
: >"$scratch/sleeps"
if PATH="$scratch:/usr/bin:/bin" GO_DOWNLOAD_ALWAYS_FAIL=1 \
  GO_DOWNLOAD_CALLS="$scratch/calls" GO_DOWNLOAD_SLEEPS="$scratch/sleeps" \
  "$root/deploy/scripts/download-go-modules.sh" >"$scratch/fail.out" 2>"$scratch/fail.err"; then
  echo "module download accepted three consecutive failures" >&2
  exit 1
fi
[ "$(wc -l <"$scratch/calls" | tr -d ' ')" -eq 3 ] || { echo "terminal module failure was not bounded at three attempts" >&2; exit 1; }

grep -F 'COPY deploy/scripts/download-go-modules.sh /usr/local/bin/download-go-modules' "$root/Dockerfile" >/dev/null || {
  echo "Docker build does not copy the bounded module downloader" >&2
  exit 1
}
grep -F 'RUN --mount=type=cache,target=/go/pkg/mod /usr/local/bin/download-go-modules' "$root/Dockerfile" >/dev/null || {
  echo "Docker build bypasses the bounded module downloader" >&2
  exit 1
}

echo "bounded Go module download tests passed"
