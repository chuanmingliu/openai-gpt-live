#!/usr/bin/env python3
"""Fixed local launcher for the reviewed external speech service."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shlex
import signal
import stat
import subprocess
import sys
import tempfile


SPEECH_ROOT = Path("/Users/chuanmingliu/speech-to-speech")
SPEECH_EXECUTABLE_ROOT = SPEECH_ROOT / ".venv"
SPEECH_EXECUTABLE = SPEECH_EXECUTABLE_ROOT / "bin/speech-to-speech"
SPEECH_PYTHON = SPEECH_EXECUTABLE_ROOT / "bin/python"
SPEECH_ENVIRONMENT_ROOT = Path("/Users/chuanmingliu/speech-to-speech")
SPEECH_ENVIRONMENT = SPEECH_ENVIRONMENT_ROOT / ".env.local"
SPEECH_PROFILE = SPEECH_ROOT / "configs/tencent-deepseek-minimax.json"
SPEECH_REVISION = "d0270d79ccb6f5c99f76499bb1dbe8a65608cf51"
ENVIRONMENT_NAME = re.compile(r"^[A-Z][A-Z0-9_]*$")
SAFE_PARENT_ENVIRONMENT = ("PATH", "HOME", "TMPDIR", "SSL_CERT_FILE", "SSL_CERT_DIR")
CHILD_TERMINATE_WAIT_SECONDS = 1


class LauncherError(Exception):
    pass


def validate_trusted_root(root: Path) -> Path:
    if not root.is_absolute():
        raise LauncherError()
    try:
        metadata = root.lstat()
        resolved = root.resolve(strict=True)
    except OSError as error:
        raise LauncherError() from error
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode) or resolved != root:
        raise LauncherError()
    if metadata.st_uid != os.getuid() or metadata.st_mode & 0o022:
        raise LauncherError()
    return resolved


def validate_trusted_file(path: Path, root: Path, *, executable: bool = False, private: bool = False) -> os.stat_result:
    trusted_root = validate_trusted_root(root)
    if not path.is_absolute():
        raise LauncherError()
    try:
        if os.path.commonpath((str(trusted_root), str(path))) != str(trusted_root):
            raise LauncherError()
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except (OSError, ValueError) as error:
        raise LauncherError() from error
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid():
            raise LauncherError()
        if metadata.st_mode & 0o022 or executable and not metadata.st_mode & 0o100:
            raise LauncherError()
        if private and metadata.st_mode & 0o077:
            raise LauncherError()
        opened = Path(os.path.realpath(path))
        if opened != path or os.path.commonpath((str(trusted_root), str(opened))) != str(trusted_root):
            raise LauncherError()
        return metadata
    finally:
        os.close(descriptor)


def read_no_follow(path: Path, expected: os.stat_result | None = None) -> str:
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        with os.fdopen(descriptor, encoding="utf-8") as source:
            current = os.fstat(source.fileno())
            if expected is not None and (current.st_dev, current.st_ino) != (expected.st_dev, expected.st_ino):
                raise LauncherError()
            return source.read()
    except (OSError, UnicodeError) as error:
        raise LauncherError() from error


def load_private_environment(path: Path, expected: os.stat_result | None = None) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in read_no_follow(path, expected).splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            raise LauncherError()
        name, raw_value = line.split("=", 1)
        name = name.strip()
        if not ENVIRONMENT_NAME.fullmatch(name) or name in values:
            raise LauncherError()
        try:
            tokens = shlex.split(raw_value, comments=True, posix=True)
        except ValueError as error:
            raise LauncherError() from error
        if len(tokens) != 1 or not tokens[0]:
            raise LauncherError()
        values[name] = tokens[0]
    if not values:
        raise LauncherError()
    return values


def create_runtime_profile(source: Path, expected: os.stat_result | None = None) -> Path:
    try:
        content = json.loads(read_no_follow(source, expected))
    except (json.JSONDecodeError, LauncherError) as error:
        raise LauncherError() from error
    if not isinstance(content, dict):
        raise LauncherError()
    content["ws_host"] = "127.0.0.1"
    content["ws_port"] = 8766
    descriptor, name = tempfile.mkstemp(prefix="gpt-live-operator-speech-", suffix=".json")
    path = Path(name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as destination:
            json.dump(content, destination, separators=(",", ":"))
            destination.flush()
            os.fsync(destination.fileno())
    except Exception:
        path.unlink(missing_ok=True)
        raise
    return path


def build_child_environment(parent: dict[str, str], speech: dict[str, str]) -> dict[str, str]:
    environment = {name: parent[name] for name in SAFE_PARENT_ENVIRONMENT if parent.get(name)}
    environment.update(speech)
    return environment


def remove_runtime_profile(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def terminate_child_group(child: subprocess.Popen[bytes], signum: int) -> None:
    if child.poll() is not None:
        return
    try:
        os.killpg(child.pid, signum)
        child.wait(timeout=CHILD_TERMINATE_WAIT_SECONDS)
    except (OSError, subprocess.TimeoutExpired):
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except OSError:
            pass


def validate_runtime_source() -> None:
    expected = (SPEECH_ROOT / "src" / "speech_to_speech" / "__init__.py").resolve(strict=True)
    try:
        imported = subprocess.run(
            (str(SPEECH_PYTHON), "-c", "import speech_to_speech; print(speech_to_speech.__file__)"),
            env={"PATH": os.environ.get("PATH", "")},
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=3,
            check=True,
        ).stdout.strip()
        revision = subprocess.run(
            ("git", "-C", str(SPEECH_ROOT), "rev-parse", "HEAD"),
            env={"PATH": os.environ.get("PATH", "")},
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=3,
            check=True,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError) as error:
        raise LauncherError() from error
    try:
        if Path(imported).resolve(strict=True) != expected or revision != SPEECH_REVISION:
            raise LauncherError()
    except OSError as error:
        raise LauncherError() from error


def launch(profile: Path) -> int:
    if profile != SPEECH_PROFILE:
        raise LauncherError()
    validate_trusted_file(SPEECH_EXECUTABLE, SPEECH_EXECUTABLE_ROOT, executable=True)
    validate_runtime_source()
    environment_file = validate_trusted_file(SPEECH_ENVIRONMENT, SPEECH_ENVIRONMENT_ROOT, private=True)
    profile_file = validate_trusted_file(profile, SPEECH_ROOT)
    environment = build_child_environment(dict(os.environ), load_private_environment(SPEECH_ENVIRONMENT, environment_file))
    runtime_profile = create_runtime_profile(profile, profile_file)
    child: subprocess.Popen[bytes] | None = None
    try:
        # The service gets its own group.  The launcher's handlers terminate
        # that complete group and wait before exit; the BFF separately owns the
        # launcher group, so either bounded shutdown path leaves no child.
        child = subprocess.Popen((str(SPEECH_EXECUTABLE), str(runtime_profile)), env=environment, start_new_session=True)

        def forward(signum: int, _frame: object) -> None:
            if child is not None:
                terminate_child_group(child, signum)

        signal.signal(signal.SIGTERM, forward)
        signal.signal(signal.SIGINT, forward)
        return child.wait()
    finally:
        remove_runtime_profile(runtime_profile)


def main(argv: list[str]) -> int:
    try:
        if len(argv) != 1:
            raise LauncherError()
        return launch(Path(argv[0]))
    except (LauncherError, OSError, subprocess.SubprocessError):
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
