#!/usr/bin/env node

import assert from 'node:assert/strict';
import { chmod, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const runner = join(root, 'deploy/scripts/run-node-test-no-skip.mjs');
const scratch = await mkdtemp(join(tmpdir(), 'gpt-live-node-test-runner.'));
const fakeNode = join(scratch, 'node');

try {
  await writeFile(fakeNode, `#!/bin/sh
set -eu
[ "$1" = --test ]
case "\${FAKE_NODE_CASE:?}" in
  pass)
    printf '%s\\n' 'TAP version 13' 'ok 1 - alpha' 'ok 2 - beta' '1..2' '# tests 2' '# pass 2' '# fail 0' '# skipped 0'
    ;;
  failure)
    printf '%s\\n' 'TAP version 13' 'not ok 1 - alpha' '1..1' '# tests 1' '# pass 0' '# fail 1'
    exit 1
    ;;
  mismatch)
    printf '%s\\n' 'TAP version 13' 'ok 1 - alpha' '1..1' '# tests 1' '# pass 1' '# fail 0'
    ;;
  plan_skip)
    printf '%s\\n' 'TAP version 13' 'ok 1 - alpha' '1..1 # SKIP all' '# tests 1' '# pass 1' '# fail 0' '# skipped 0' '# todo 0'
    ;;
  plan_suffix)
    printf '%s\\n' 'TAP version 13' 'ok 1 - alpha' '1..1 # arbitrary' '# tests 1' '# pass 1' '# fail 0' '# skipped 0' '# todo 0'
    ;;
  summary_fail)
    printf '%s\\n' 'TAP version 13' 'ok 1 - alpha' '1..1' '# tests 1' '# pass 1' '# fail 1' '# skipped 0' '# todo 0'
    ;;
  summary_skipped)
    printf '%s\\n' 'TAP version 13' 'ok 1 - alpha' '1..1' '# tests 1' '# pass 1' '# fail 0' '# skipped 1' '# todo 0'
    ;;
  summary_todo)
    printf '%s\\n' 'TAP version 13' 'ok 1 - alpha' '1..1' '# tests 1' '# pass 1' '# fail 0' '# skipped 0' '# todo 1'
    ;;
  summary_disabled)
    printf '%s\\n' 'TAP version 13' 'ok 1 - alpha' '1..1' '# tests 1' '# pass 1' '# fail 0' '# skipped 0' '# todo 0' '# disabled 1'
    ;;
  skipped)
    printf '%s\\n' 'TAP version 13' 'ok 1 - alpha # SKIP unavailable' '1..1' '# tests 1' '# pass 0' '# skipped 1'
    ;;
  disabled)
    printf '%s\\n' 'TAP version 13' 'ok 1 - alpha # disabled' '1..1' '# tests 1'
    ;;
  truncated)
    printf '%s\\n' 'TAP version 13' 'ok 1 - alpha'
    ;;
  oversized)
    dd if=/dev/zero bs=1048576 count=9 2>/dev/null
    printf '%s\\n' 'ok 1 - alpha' '1..1'
    ;;
  sentinel)
    printf '%s\\n' 'TAP version 13' 'NODE_SENTINEL' >&2
    printf '%s\\n' 'ok 1 - alpha' '1..1' '# tests 1' '# pass 1' '# fail 0'
    ;;
  *) exit 98 ;;
esac
`, { mode: 0o755 });
  await chmod(fakeNode, 0o755);

  function invoke(caseName, expected = 2) {
    return spawnSync(process.execPath, [runner, '--expect-tests', String(expected), '--', fakeNode, '--test', 'fixture.mjs'], {
      encoding: 'utf8',
      env: { ...process.env, FAKE_NODE_CASE: caseName },
      maxBuffer: 16 * 1024 * 1024,
    });
  }

  const pass = invoke('pass');
  assert.equal(pass.status, 0, pass.stderr);
  assert.equal(pass.stdout, 'node_test_summary tests=2 skips=0\n');
  assert.equal(pass.stderr, '');

  for (const [label, caseName, expected] of [
    ['child failure', 'failure', 1],
    ['root-count mismatch', 'mismatch', 2],
    ['root-plan skip directive', 'plan_skip', 1],
    ['root-plan arbitrary suffix', 'plan_suffix', 1],
    ['nonzero fail summary', 'summary_fail', 1],
    ['nonzero skipped summary', 'summary_skipped', 1],
    ['nonzero todo summary', 'summary_todo', 1],
    ['nonzero disabled summary', 'summary_disabled', 1],
    ['skip annotation', 'skipped', 1],
    ['disabled annotation', 'disabled', 1],
    ['truncated TAP', 'truncated', 1],
    ['oversized output', 'oversized', 1],
  ]) {
    const result = invoke(caseName, expected);
    assert.notEqual(result.status, 0, `${label} was accepted`);
    assert.doesNotMatch(result.stdout + result.stderr, /NODE_SENTINEL/, `${label} leaked child output`);
    assert.ok(Buffer.byteLength(result.stdout + result.stderr) < 4096, `${label} emitted unbounded diagnostics`);
  }

  const sentinel = invoke('sentinel', 1);
  assert.equal(sentinel.status, 0, sentinel.stderr);
  assert.equal(sentinel.stdout, 'node_test_summary tests=1 skips=0\n');
  assert.equal(sentinel.stderr, '');
  assert.doesNotMatch(sentinel.stdout + sentinel.stderr, /NODE_SENTINEL/, 'child output sentinel leaked');

  console.log('node no-skip runner boundary tests passed');
} finally {
  await rm(scratch, { recursive: true, force: true });
}
