#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
runner=$root/deploy/scripts/run-go-test-no-skip.sh
scratch=$(mktemp -d "${TMPDIR:-/tmp}/gpt-live-go-test-runner.XXXXXX")
trap 'rm -rf "$scratch"' EXIT HUP INT TERM

cat >"$scratch/go" <<'EOF'
#!/bin/sh
set -eu
[ "$1" = test ] && [ "$2" = -json ]
case "${FAKE_GO_CASE:?}" in
  pass)
    printf '%s\n' \
      '{"Action":"start","Package":"example/a"}' \
      '{"Action":"run","Package":"example/a","Test":"TestAlpha"}' \
      '{"Action":"pass","Package":"example/a","Test":"TestAlpha"}' \
      '{"Action":"pass","Package":"example/a"}'
    ;;
  two_tests)
    printf '%s\n' \
      '{"Action":"start","Package":"example/a"}' \
      '{"Action":"run","Package":"example/a","Test":"TestAlpha"}' \
      '{"Action":"pass","Package":"example/a","Test":"TestAlpha"}' \
      '{"Action":"run","Package":"example/a","Test":"TestBeta"}' \
      '{"Action":"pass","Package":"example/a","Test":"TestBeta"}' \
      '{"Action":"pass","Package":"example/a"}'
    ;;
  child_failure)
    printf '%s\n' \
      '{"Action":"start","Package":"example/a"}' \
      '{"Action":"run","Package":"example/a","Test":"TestAlpha"}' \
      '{"Action":"fail","Package":"example/a","Test":"TestAlpha"}' \
      '{"Action":"fail","Package":"example/a"}'
    exit 17
    ;;
  package_skip)
    printf '%s\n' \
      '{"Action":"start","Package":"example/a"}' \
      '{"Action":"skip","Package":"example/a"}'
    ;;
  named_skip)
    printf '%s\n' \
      '{"Action":"start","Package":"example/a"}' \
      '{"Action":"run","Package":"example/a","Test":"TestAlpha"}' \
      '{"Action":"skip","Package":"example/a","Test":"TestAlpha"}' \
      '{"Action":"pass","Package":"example/a"}'
    ;;
  malformed)
    printf '%s\n' '{"Action":"start","Package":"example/a"}' 'not-json-GO_SENTINEL'
    ;;
  truncated)
    printf '%s\n' '{"Action":"start","Package":"example/a"}'
    printf '%s' '{"Action":"run","Package":"example/a","Test":"TestAlpha"'
    ;;
  zero_tests)
    printf '%s\n' \
      '{"Action":"start","Package":"example/a"}' \
      '{"Action":"pass","Package":"example/a"}'
    ;;
  missing_terminal)
    printf '%s\n' \
      '{"Action":"start","Package":"example/a"}' \
      '{"Action":"run","Package":"example/a","Test":"TestAlpha"}' \
      '{"Action":"pass","Package":"example/a","Test":"TestAlpha"}'
    ;;
  subtest_only)
    printf '%s\n' \
      '{"Action":"start","Package":"example/a"}' \
      '{"Action":"run","Package":"example/a","Test":"TestAlpha/TestBeta"}' \
      '{"Action":"pass","Package":"example/a","Test":"TestAlpha/TestBeta"}' \
      '{"Action":"pass","Package":"example/a"}'
    ;;
  *) exit 98 ;;
esac
EOF
chmod 0755 "$scratch/go"

run_case() {
  case_name=$1
  shift
  FAKE_GO_CASE=$case_name PATH="$scratch:$PATH" "$runner" "$@" -- go test -json ./... \
    >"$scratch/stdout" 2>"$scratch/stderr"
}

expect_reject() {
  label=$1
  case_name=$2
  shift 2
  if run_case "$case_name" "$@"; then
    echo "$label was accepted" >&2
    exit 1
  fi
  if grep -F 'GO_SENTINEL' "$scratch/stdout" "$scratch/stderr" >/dev/null; then
    echo "$label leaked child output" >&2
    exit 1
  fi
}

run_case pass --expect-minimum 1 --expect-top-level TestAlpha
[ "$(cat "$scratch/stdout")" = 'go_test_summary tests=1 packages=1 skips=0' ]
[ ! -s "$scratch/stderr" ]

expect_reject 'child failure' child_failure --expect-minimum 1
expect_reject 'package skip' package_skip --expect-minimum 1
expect_reject 'named-test skip' named_skip --expect-minimum 1
expect_reject 'malformed JSON' malformed --expect-minimum 1
expect_reject 'truncated JSON' truncated --expect-minimum 1
expect_reject 'zero tests' zero_tests --expect-minimum 1
expect_reject 'missing package terminal' missing_terminal --expect-minimum 1
expect_reject 'unsorted top-level expectation' pass --expect-minimum 1 --expect-top-level TestBeta,TestAlpha
expect_reject 'duplicate top-level expectation' pass --expect-minimum 1 --expect-top-level TestAlpha,TestAlpha
expect_reject 'malformed top-level expectation' pass --expect-minimum 1 --expect-top-level TestAlpha/TestChild
expect_reject 'missing expected top-level test' pass --expect-minimum 1 --expect-top-level TestAlpha,TestBeta
expect_reject 'unexpected top-level test' two_tests --expect-minimum 1 --expect-top-level TestAlpha
expect_reject 'subtest satisfying top-level expectation' subtest_only --expect-minimum 1 --expect-top-level TestBeta

echo 'go no-skip runner boundary tests passed'
