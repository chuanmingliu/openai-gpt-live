#!/usr/bin/env bash
set -euo pipefail

fail() {
  echo "event-aware whitespace gate failed" >&2
  exit 1
}

[[ "$#" -eq 4 ]] || fail
event_name=$1
pull_base=$2
push_before=$3
event_head=$4
commit_pattern='^[0-9a-f]{40}$'
zero_commit=0000000000000000000000000000000000000000
empty_tree=4b825dc642cb6eb9a060e54bf8d69288fbee4904

[[ "$event_head" =~ $commit_pattern ]] || fail
git cat-file -e "$event_head^{commit}" 2>/dev/null || fail

case "$event_name" in
  pull_request)
    [[ "$pull_base" =~ $commit_pattern ]] || fail
    git cat-file -e "$pull_base^{commit}" 2>/dev/null || fail
    git diff --check "$pull_base...$event_head" --
    ;;
  push)
    [[ "$push_before" =~ $commit_pattern ]] || fail
    if [[ "$push_before" == "$zero_commit" ]]; then
      git diff --check "$empty_tree" "$event_head" --
    else
      git cat-file -e "$push_before^{commit}" 2>/dev/null || fail
      git diff --check "$push_before..$event_head" --
    fi
    ;;
  *)
    fail
    ;;
esac
