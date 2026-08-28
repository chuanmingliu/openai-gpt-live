#!/bin/sh
set -eu

exec python3 - "$@" <<'PY'
import json
import os
import re
import subprocess
import sys

CAPTURE_LIMIT = 8 * 1024 * 1024

def reject():
    print("go_test_no_skip: verification failed", file=sys.stderr)
    raise SystemExit(1)

args = sys.argv[1:]
minimum = None
expected_text = None
index = 0
while index < len(args) and args[index] != "--":
    option = args[index]
    if option not in ("--expect-minimum", "--expect-top-level") or index + 1 >= len(args):
        reject()
    if option == "--expect-minimum":
        if minimum is not None:
            reject()
        try:
            minimum = int(args[index + 1])
        except ValueError:
            reject()
        if minimum <= 0 or str(minimum) != args[index + 1]:
            reject()
    else:
        if expected_text is not None:
            reject()
        expected_text = args[index + 1]
    index += 2

if minimum is None or index >= len(args) or args[index] != "--":
    reject()
command = args[index + 1:]
if len(command) < 3 or os.path.basename(command[0]) != "go" or command[1:3] != ["test", "-json"]:
    reject()

expected = None
if expected_text is not None:
    expected = expected_text.split(",")
    valid_name = re.compile(r"^Test(?:[A-Z0-9_][A-Za-z0-9_]*)?$")
    if (not expected or any(not valid_name.fullmatch(name) for name in expected)
            or expected != sorted(expected) or len(expected) != len(set(expected))):
        reject()

try:
    child = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
except (OSError, ValueError):
    reject()

capture = bytearray()
oversized = False
assert child.stdout is not None
while True:
    chunk = child.stdout.read(65536)
    if not chunk:
        break
    if len(capture) + len(chunk) > CAPTURE_LIMIT:
        oversized = True
        child.terminate()
        break
    capture.extend(chunk)

if oversized:
    try:
        child.wait(timeout=1)
    except subprocess.TimeoutExpired:
        child.kill()
        child.wait()
    reject()

exit_code = child.wait()
if exit_code != 0 or not capture or not capture.endswith(b"\n"):
    reject()

try:
    text = capture.decode("utf-8", "strict")
except UnicodeDecodeError:
    reject()

packages = set()
terminated = set()
test_runs = set()
top_level = set()
actions = {"start", "run", "pause", "cont", "pass", "bench", "fail", "output", "skip"}
def reject_json_constant(_value):
    raise ValueError("non-standard JSON constant")

for line in text.splitlines():
    if not line:
        reject()
    try:
        event = json.loads(line, parse_constant=reject_json_constant)
    except (json.JSONDecodeError, UnicodeError, ValueError):
        reject()
    if not isinstance(event, dict):
        reject()
    action = event.get("Action")
    package = event.get("Package")
    test = event.get("Test")
    if action not in actions or not isinstance(package, str) or not package:
        reject()
    if test is not None and (not isinstance(test, str) or not test):
        reject()
    if package in terminated:
        reject()
    packages.add(package)
    if action in ("skip", "fail"):
        reject()
    if action == "run":
        if test is None:
            reject()
        identity = (package, test)
        if identity in test_runs:
            reject()
        test_runs.add(identity)
        if "/" not in test:
            top_level.add(test)
    if test is None and action == "pass":
        terminated.add(package)

if len(test_runs) < minimum or not packages or terminated != packages:
    reject()
if expected is not None and top_level != set(expected):
    reject()

print(f"go_test_summary tests={len(test_runs)} packages={len(packages)} skips=0")
PY
