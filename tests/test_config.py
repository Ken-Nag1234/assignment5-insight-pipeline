"""Offline tests for the credential presence check. Uses fake values only."""

import io
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from pipeline import config

FAKE = "fake-test-value-1234567890"


class ConfigTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.env = Path(self.tmp.name) / ".env"

    def tearDown(self):
        self.tmp.cleanup()

    def run_check(self, content):
        self.env.write_text(content)
        os.chmod(self.env, 0o600)
        out = io.StringIO()
        with mock.patch.object(config, "ENV_FILE", self.env), mock.patch.dict(os.environ, {}, clear=True), \
                redirect_stdout(out):
            code = config.main(["check"])
        return code, out.getvalue()

    def test_missing_key_is_not_ready(self):
        code, out = self.run_check("AI_GATEWAY_API_KEY=\n")
        self.assertEqual(code, 1)
        self.assertIn("AI_GATEWAY_API_KEY [required]: missing", out)

    def test_present_key_is_never_printed(self):
        code, out = self.run_check(f"# comment\nAI_GATEWAY_API_KEY=\"{FAKE}\"\n")
        self.assertIn("AI_GATEWAY_API_KEY [required]: present (from .env)", out)
        for i in range(len(FAKE) - 3):
            self.assertNotIn(FAKE[i:i + 4], out)

    def test_placeholder_and_whitespace_flagged(self):
        _, out = self.run_check("AI_GATEWAY_API_KEY=<paste here>\n")
        self.assertIn("contains whitespace", out)
        self.assertIn("looks like a placeholder", out)


if __name__ == "__main__":
    unittest.main()
