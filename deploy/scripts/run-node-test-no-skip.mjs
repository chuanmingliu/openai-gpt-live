#!/usr/bin/env node

import { basename } from 'node:path';
import { spawn } from 'node:child_process';
import { TextDecoder } from 'node:util';

const CAPTURE_LIMIT = 8 * 1024 * 1024;

function reject() {
  process.stderr.write('node_test_no_skip: verification failed\n');
  process.exitCode = 1;
}

const args = process.argv.slice(2);
let expected;
let index = 0;
while (index < args.length && args[index] !== '--') {
  if (args[index] !== '--expect-tests' || expected !== undefined || index + 1 >= args.length) {
    reject();
    process.exit();
  }
  if (!/^[1-9][0-9]*$/.test(args[index + 1])) {
    reject();
    process.exit();
  }
  expected = Number(args[index + 1]);
  if (!Number.isSafeInteger(expected)) {
    reject();
    process.exit();
  }
  index += 2;
}

const command = args.slice(index + 1);
if (expected === undefined || args[index] !== '--' || command.length < 2 || basename(command[0]) !== 'node' || command[1] !== '--test') {
  reject();
  process.exit();
}

let child;
try {
  child = spawn(command[0], command.slice(1), { stdio: ['ignore', 'pipe', 'pipe'] });
} catch {
  reject();
  process.exit();
}

const stdout = [];
let totalBytes = 0;
let oversized = false;
let forcedKill;
const collect = (chunk, retain) => {
  totalBytes += chunk.length;
  if (totalBytes > CAPTURE_LIMIT) {
    oversized = true;
    child.kill('SIGTERM');
    forcedKill ??= setTimeout(() => child.kill('SIGKILL'), 1000);
    return;
  }
  if (retain) stdout.push(chunk);
};
child.stdout.on('data', (chunk) => collect(chunk, true));
child.stderr.on('data', (chunk) => collect(chunk, false));

const exit = await new Promise((resolve) => {
  child.once('error', () => resolve({ code: null, error: true }));
  child.once('close', (code, signal) => resolve({ code, signal, error: false }));
});
if (forcedKill !== undefined) clearTimeout(forcedKill);

if (oversized || exit.error || exit.code !== 0) {
  reject();
  process.exit();
}

let tap;
try {
  tap = new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(stdout));
} catch {
  reject();
  process.exit();
}

const lines = tap.split(/\r?\n/);
const plans = [];
const rootResults = [];
let invalid = false;
for (const line of lines) {
  const plan = /^1\.\.([0-9]+)$/.exec(line);
  if (plan) plans.push(Number(plan[1]));
  const result = /^(not ok|ok)\s+([0-9]+)(?:\s|$)/.exec(line);
  if (result) {
    if (result[1] === 'not ok') invalid = true;
    rootResults.push(Number(result[2]));
  }
  if (/^\s*not ok(?:\s|$)/.test(line)
      || /^\s*(?:not )?ok\b.*#\s*(?:skip|todo|disabled)\b/i.test(line)
      || (/^1\.\./.test(line) && !plan)
      || /^Bail out!/i.test(line)) {
    invalid = true;
  }
  const failureSummary = /^# (?:fail|skipped|todo|disabled) ([0-9]+)$/.exec(line);
  if (failureSummary && Number(failureSummary[1]) !== 0) invalid = true;
}

const requiredIds = Array.from({ length: expected }, (_, position) => position + 1);
if (invalid || plans.length !== 1 || plans[0] !== expected || rootResults.length !== expected
    || rootResults.some((value, position) => value !== requiredIds[position])) {
  reject();
  process.exit();
}

process.stdout.write(`node_test_summary tests=${expected} skips=0\n`);
