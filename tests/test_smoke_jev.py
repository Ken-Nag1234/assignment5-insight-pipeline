"""Offline tests for the Jev smoke test. The network is mocked; no real request is ever sent."""

import io
import json
import ssl
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

from pipeline import smoke_jev

FAKE_KEY = "fake-gateway-key-abcdef123456"


class FakeResponse:
    status = 200
    headers = {"x-ai-gateway-request-id": "test", "set-cookie": "should-not-be-saved"}

    def __init__(self, body):
        self.body = body

    def read(self):
        return self.body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


RESPONSE = {
    "model": "typesafe-ai/jev",
    "answers": {
        "topic": {"type": "choice", "choice": "access", "probabilities": {"access": 0.9, "other": 0.1},
                  "confidence": 0.8},
        "sentiment": {"type": "score", "score": 1.0},
    },
    "usage": {"input_tokens": 1000, "output_tokens": 20},
    "provider_metadata": {"gateway": {"cost": "0.000042", "generationId": "gen_test",
                                      "routing": {"finalProvider": "typesafe-ai"}}},
}


class SmokeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.patches = [
            mock.patch.object(smoke_jev, "LOCK_FILE", root / "work" / "lock.json"),
            mock.patch.object(smoke_jev, "OUT_DIR", root / "outputs"),
            mock.patch.object(smoke_jev, "REPO_ROOT", root),
            mock.patch.object(smoke_jev, "get_key", return_value=FAKE_KEY),
        ]
        for p in self.patches:
            p.start()
        self.root = root

    def tearDown(self):
        for p in self.patches:
            p.stop()
        self.tmp.cleanup()

    def run_main(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            code = smoke_jev.main(list(argv))
        return code, out.getvalue() + err.getvalue()

    def test_dry_run_sends_nothing(self):
        with mock.patch("urllib.request.urlopen") as urlopen:
            code, out = self.run_main()
        urlopen.assert_not_called()
        self.assertEqual(code, 0)
        self.assertIn("DRY RUN", out)
        self.assertNotIn(FAKE_KEY, out)

    def test_execute_sends_once_then_refuses(self):
        echoed = json.dumps(dict(RESPONSE, echo=FAKE_KEY)).encode()
        with mock.patch("urllib.request.urlopen", return_value=FakeResponse(echoed)) as urlopen:
            code, out = self.run_main("--execute")
            self.assertEqual(code, 0)
            self.assertEqual(urlopen.call_count, 1)
            code2, out2 = self.run_main("--execute")
            self.assertEqual(code2, 3)
            self.assertEqual(urlopen.call_count, 1)
        self.assertIn("topic: access", out)
        self.assertIn("Estimated cost (tokens x editable rate): $0.000042", out)
        saved = "".join(p.read_text() for p in (self.root / "outputs").glob("*.json"))
        for text in (out, out2, saved):
            self.assertNotIn(FAKE_KEY, text)
        self.assertNotIn("set-cookie", saved)

    def test_network_error_is_not_retried(self):
        with mock.patch("urllib.request.urlopen", side_effect=TimeoutError("timed out")) as urlopen:
            code, out = self.run_main("--execute")
        self.assertEqual(code, 1)
        self.assertEqual(urlopen.call_count, 1)
        self.assertTrue((self.root / "work" / "lock.json").exists())

    def test_https_uses_system_ca_bundle_with_full_verification(self):
        real_create = ssl.create_default_context
        with mock.patch("ssl.create_default_context", side_effect=real_create) as create, \
                mock.patch("urllib.request.urlopen", return_value=FakeResponse(json.dumps(RESPONSE).encode())) as urlopen:
            self.run_main("--execute")
        create.assert_called_once_with(cafile="/etc/ssl/cert.pem")
        context = urlopen.call_args.kwargs["context"]
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
        self.assertTrue(context.check_hostname)

    def test_system_ca_bundle_loads_offline(self):
        context = ssl.create_default_context(cafile=smoke_jev.CA_BUNDLE)
        self.assertGreater(context.cert_store_stats()["x509_ca"], 0)

    def test_cost_estimate(self):
        self.assertEqual(smoke_jev.estimate_cost({"input_tokens": 500, "output_tokens": 30}), "0.000021")
        self.assertIsNone(smoke_jev.estimate_cost(None))


if __name__ == "__main__":
    unittest.main()
