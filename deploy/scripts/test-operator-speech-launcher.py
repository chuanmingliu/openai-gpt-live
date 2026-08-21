#!/usr/bin/env python3
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import subprocess
import time
import unittest
from types import SimpleNamespace
from unittest import mock


sys.dont_write_bytecode = True

SCRIPT = Path(__file__).with_name("operator-speech-launcher.py")
SPEC = importlib.util.spec_from_file_location("operator_speech_launcher", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class OperatorSpeechLauncherTest(unittest.TestCase):
    def test_trusted_files_are_canonical_private_and_no_follow(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            executable = root / "speech-to-speech"
            executable.write_text("#!/bin/sh\n")
            executable.chmod(0o700)
            profile = root / "profile.json"
            profile.write_text('{"mode":"realtime","ws_port":8765}')
            profile.chmod(0o600)
            environment = root / ".env.local"
            environment.write_text("S2S_API_KEY=test-only\n")
            environment.chmod(0o600)

            stdout, stderr = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                MODULE.validate_trusted_file(executable, root, executable=True)
                MODULE.validate_trusted_file(profile, root)
                MODULE.validate_trusted_file(environment, root, private=True)

                public_environment = root / "public.env"
                public_environment.write_text("S2S_API_KEY=test-only\n")
                public_environment.chmod(0o644)
                with self.assertRaises(MODULE.LauncherError):
                    MODULE.validate_trusted_file(public_environment, root, private=True)

                symlink = root / "profile-link.json"
                symlink.symlink_to(profile)
                with self.assertRaises(MODULE.LauncherError):
                    MODULE.validate_trusted_file(symlink, root)

            self.assertEqual("", stdout.getvalue())
            self.assertEqual("", stderr.getvalue())

    def test_runtime_profile_is_private_loopback_8766_and_removed(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "profile.json"
            source.write_text('{"mode":"realtime","ws_host":"0.0.0.0","ws_port":8765}')
            source.chmod(0o600)
            runtime = MODULE.create_runtime_profile(source)
            try:
                self.assertEqual(0o600, runtime.stat().st_mode & 0o777)
                content = json.loads(runtime.read_text())
                self.assertEqual("127.0.0.1", content["ws_host"])
                self.assertEqual(8766, content["ws_port"])
                self.assertEqual(8765, json.loads(source.read_text())["ws_port"])
            finally:
                MODULE.remove_runtime_profile(runtime)
            self.assertFalse(runtime.exists())

    def test_child_environment_excludes_unrelated_operator_secrets(self):
        parent = {
            "PATH": "/usr/bin:/bin",
            "HOME": "/private/home",
            "POSTGRES_PASSWORD": "database-secret",
            "LIVE_STATIC_BEARER_TOKEN": "gateway-secret",
            "OPENAI_API_KEY": "unrelated-parent-secret",
        }
        child = MODULE.build_child_environment(parent, {"DEEPSEEK_API_KEY": "speech-secret"})
        self.assertEqual("speech-secret", child["DEEPSEEK_API_KEY"])
        self.assertEqual("speech-secret", child["OPENAI_API_KEY"])
        self.assertEqual("/usr/bin:/bin", child["PATH"])
        self.assertNotIn("POSTGRES_PASSWORD", child)
        self.assertNotIn("LIVE_STATIC_BEARER_TOKEN", child)

    def test_runtime_import_resolves_to_the_reviewed_checkout(self):
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            MODULE.validate_runtime_source()
        self.assertEqual("", stdout.getvalue())
        self.assertEqual("", stderr.getvalue())

    def test_runtime_source_rejects_dirty_reviewed_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            package = root / "src" / "speech_to_speech"
            package.mkdir(parents=True)
            expected = package / "__init__.py"
            expected.write_text("")
            original = (MODULE.SPEECH_ROOT, MODULE.SPEECH_PYTHON, MODULE.SPEECH_REVISION)
            try:
                MODULE.SPEECH_ROOT = root
                MODULE.SPEECH_PYTHON = Path(sys.executable)
                MODULE.SPEECH_REVISION = "reviewed-revision"

                def reviewed_run(command, **_kwargs):
                    if command[0] == str(sys.executable):
                        return SimpleNamespace(stdout=str(expected) + "\n")
                    if "rev-parse" in command:
                        return SimpleNamespace(stdout="reviewed-revision\n")
                    if "diff" in command:
                        return SimpleNamespace(returncode=1)
                    if "status" in command:
                        return SimpleNamespace(stdout="")
                    raise AssertionError(command)

                with mock.patch.object(MODULE.subprocess, "run", side_effect=reviewed_run):
                    with self.assertRaises(MODULE.LauncherError):
                        MODULE.validate_runtime_source()
            finally:
                MODULE.SPEECH_ROOT, MODULE.SPEECH_PYTHON, MODULE.SPEECH_REVISION = original

    def test_validated_private_file_swap_fails_closed_without_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            environment = root / ".env.local"
            environment.write_text("DEEPSEEK_API_KEY=first\n")
            environment.chmod(0o600)
            expected = MODULE.validate_trusted_file(environment, root, private=True)
            replacement = root / "replacement.env"
            replacement.write_text("DEEPSEEK_API_KEY=second\n")
            replacement.chmod(0o600)
            replacement.replace(environment)
            stdout, stderr = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                with self.assertRaises(MODULE.LauncherError):
                    MODULE.load_private_environment(environment, expected)
            self.assertEqual("", stdout.getvalue())
            self.assertEqual("", stderr.getvalue())

    def test_terminating_launcher_kills_term_ignoring_child_and_removes_profile(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            child = root / "speech-to-speech"
            pid_file = root / "pid"
            child.write_text("#!" + sys.executable + "\nimport os, signal, sys, time\nsignal.signal(signal.SIGTERM, signal.SIG_IGN)\nopen(os.environ['SPEECH_TEST_PID'], 'w').write(str(os.getpid()))\nwhile True: time.sleep(1)\n")
            child.chmod(0o700)
            profile = root / "profile.json"
            profile.write_text('{"mode":"realtime"}')
            profile.chmod(0o600)
            environment = root / ".env.local"
            environment.write_text("SPEECH_TEST_PID=" + str(pid_file) + "\n")
            environment.chmod(0o600)
            package = root / "src" / "speech_to_speech"
            package.mkdir(parents=True)
            (package / "__init__.py").write_text("")
            original = (MODULE.SPEECH_PROFILE, MODULE.SPEECH_EXECUTABLE, MODULE.SPEECH_ROOT, MODULE.SPEECH_EXECUTABLE_ROOT, MODULE.SPEECH_ENVIRONMENT, MODULE.SPEECH_ENVIRONMENT_ROOT, MODULE.SPEECH_PYTHON, MODULE.SPEECH_REVISION)
            try:
                MODULE.SPEECH_ROOT = root
                MODULE.SPEECH_EXECUTABLE_ROOT = root
                MODULE.SPEECH_PROFILE = profile
                MODULE.SPEECH_EXECUTABLE = child
                MODULE.SPEECH_ENVIRONMENT = environment
                MODULE.SPEECH_ENVIRONMENT_ROOT = root
                MODULE.SPEECH_PYTHON = Path(sys.executable)
                MODULE.SPEECH_REVISION = "test-revision"
                program = "import importlib.util,sys; s=importlib.util.spec_from_file_location('x',sys.argv[1]); m=importlib.util.module_from_spec(s); s.loader.exec_module(m); P=m.Path; m.SPEECH_ROOT=P(sys.argv[2]); m.SPEECH_EXECUTABLE_ROOT=P(sys.argv[2]); m.SPEECH_PROFILE=P(sys.argv[3]); m.SPEECH_EXECUTABLE=P(sys.argv[4]); m.SPEECH_ENVIRONMENT=P(sys.argv[5]); m.SPEECH_ENVIRONMENT_ROOT=P(sys.argv[2]); m.SPEECH_PYTHON=P(sys.executable); m.SPEECH_REVISION='test-revision'; m.validate_runtime_source=lambda:None; raise SystemExit(m.launch(P(sys.argv[3])))"
                process = subprocess.Popen([sys.executable, "-c", program, str(SCRIPT), str(root), str(profile), str(child), str(environment)], env={"PATH": os.environ.get("PATH", ""), "SPEECH_TEST_PID": str(pid_file)})
                deadline = time.time() + 2
                while not pid_file.exists() and time.time() < deadline:
                    time.sleep(0.01)
                self.assertTrue(pid_file.exists())
                process.terminate()
                process.wait(timeout=3)
                child_pid = int(pid_file.read_text())
                with self.assertRaises(ProcessLookupError):
                    os.kill(child_pid, 0)
            finally:
                MODULE.SPEECH_PROFILE, MODULE.SPEECH_EXECUTABLE, MODULE.SPEECH_ROOT, MODULE.SPEECH_EXECUTABLE_ROOT, MODULE.SPEECH_ENVIRONMENT, MODULE.SPEECH_ENVIRONMENT_ROOT, MODULE.SPEECH_PYTHON, MODULE.SPEECH_REVISION = original

    def test_child_immediately_signalling_launcher_cleans_its_group_and_profile(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            child = root / "speech-to-speech"
            pid_file = root / "pid"
            child.write_text("#!" + sys.executable + "\nimport os, signal, time\nopen(os.environ['SPEECH_TEST_PID'], 'w').write(str(os.getpid()))\nos.kill(os.getppid(), signal.SIGTERM)\nsignal.signal(signal.SIGTERM, signal.SIG_IGN)\nwhile True: time.sleep(1)\n")
            child.chmod(0o700)
            profile = root / "profile.json"
            profile.write_text('{"mode":"realtime"}')
            profile.chmod(0o600)
            environment = root / ".env.local"
            environment.write_text("SPEECH_TEST_PID=" + str(pid_file) + "\n")
            environment.chmod(0o600)
            package = root / "src" / "speech_to_speech"
            package.mkdir(parents=True)
            (package / "__init__.py").write_text("")
            program = "import importlib.util,sys; s=importlib.util.spec_from_file_location('x',sys.argv[1]); m=importlib.util.module_from_spec(s); s.loader.exec_module(m); P=m.Path; m.SPEECH_ROOT=P(sys.argv[2]); m.SPEECH_EXECUTABLE_ROOT=P(sys.argv[2]); m.SPEECH_PROFILE=P(sys.argv[3]); m.SPEECH_EXECUTABLE=P(sys.argv[4]); m.SPEECH_ENVIRONMENT=P(sys.argv[5]); m.SPEECH_ENVIRONMENT_ROOT=P(sys.argv[2]); m.SPEECH_PYTHON=P(sys.executable); m.SPEECH_REVISION='test-revision'; m.validate_runtime_source=lambda:None; raise SystemExit(m.launch(P(sys.argv[3])))"
            process = subprocess.Popen([sys.executable, "-c", program, str(SCRIPT), str(root), str(profile), str(child), str(environment)], env={"PATH": os.environ.get("PATH", ""), "TMPDIR": str(root), "SPEECH_TEST_PID": str(pid_file)})
            process.wait(timeout=3)
            self.assertTrue(pid_file.exists())
            child_pid = int(pid_file.read_text())
            with self.assertRaises(ProcessLookupError):
                os.kill(child_pid, 0)
            self.assertEqual([], list(root.glob("gpt-live-operator-speech-*.json")))


if __name__ == "__main__":
    unittest.main()
