#!/usr/bin/env python3
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest


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
        }
        child = MODULE.build_child_environment(parent, {"DEEPSEEK_API_KEY": "speech-secret"})
        self.assertEqual("speech-secret", child["DEEPSEEK_API_KEY"])
        self.assertEqual("/usr/bin:/bin", child["PATH"])
        self.assertNotIn("POSTGRES_PASSWORD", child)
        self.assertNotIn("LIVE_STATIC_BEARER_TOKEN", child)

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


if __name__ == "__main__":
    unittest.main()
