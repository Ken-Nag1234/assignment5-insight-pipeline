"""Offline tests for the Gateway chat model request shape (fake transport; no network, no key)."""

import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from pipeline import smoke_text
from pipeline.ledger import SpendLedger
from pipeline.textmodel import GatewayChatModel


def fake_transport(sent):
    def transport(body):
        sent.append(body)
        payload = {"id": "gen_fake", "model": body["model"],
                   "choices": [{"message": {"content": "{\"ok\": true}"}}],
                   "usage": {"prompt_tokens": 100, "completion_tokens": 20},
                   "providerMetadata": {"gateway": {"cost": "0.00002"}}}
        return 200, json.dumps(payload).encode()
    return transport


class GatewayChatModelTests(unittest.TestCase):
    def test_luna_requests_no_reasoning_and_no_temperature(self):
        sent = []
        model = GatewayChatModel("test-key", "openai/gpt-6-luna", "0.10", "0.50", transport=fake_transport(sent))
        response = model.generate([{"role": "user", "content": "hi"}], max_tokens=50)
        self.assertTrue(response.ok)
        self.assertEqual(sent[0]["reasoning_effort"], "none")
        self.assertNotIn("temperature", sent[0])
        self.assertEqual(sent[0]["response_format"], {"type": "json_object"})
        self.assertEqual(response.cost_usd, "0.00002")
        self.assertEqual(model.settings(), "reasoning_effort=none, json_object")
        self.assertNotIn("test-key", repr(model))

    def test_other_models_keep_temperature_zero(self):
        sent = []
        model = GatewayChatModel("k", "google/gemini-2.5-flash-lite", "0.10", "0.40", transport=fake_transport(sent))
        model.generate([{"role": "user", "content": "hi"}], max_tokens=50)
        self.assertEqual(sent[0]["temperature"], 0)
        self.assertNotIn("reasoning_effort", sent[0])

    def test_key_is_redacted_from_errors(self):
        model = GatewayChatModel("secret-key", "openai/gpt-6-luna", "0.10", "0.50",
                                 transport=lambda body: (401, b'{"error": {"message": "bad key secret-key"}}'))
        response = model.generate([{"role": "user", "content": "hi"}], max_tokens=50)
        self.assertFalse(response.ok)
        self.assertNotIn("secret-key", response.error)


class SmokeTextTests(unittest.TestCase):
    def test_single_call_logged_outside_export_and_capped(self):
        with tempfile.TemporaryDirectory() as tmp:
            sent = []
            model = GatewayChatModel("test-key-123", smoke_text.MODEL, "0.10", "0.50", transport=fake_transport(sent))
            ledger = SpendLedger(Decimal("0.001"), Path(tmp) / "ledger.jsonl")
            result = smoke_text.smoke(model, ledger, out_dir=Path(tmp), stamp="t1")
            self.assertTrue(result["json_ok"])
            self.assertEqual(len(sent), 1)
            self.assertEqual(sent[0]["max_tokens"], smoke_text.MAX_TOKENS)
            calls = [json.loads(x) for x in (Path(tmp) / "text_smoke_calls.jsonl").read_text().splitlines()]
            self.assertEqual([c["role"] for c in calls], ["smoke"])
            # a cap below the worst-case reservation refuses before sending anything
            tight = SpendLedger(Decimal("0.000001"), Path(tmp) / "ledger2.jsonl")
            from pipeline.textmodel import BudgetStop
            with self.assertRaises(BudgetStop):
                smoke_text.smoke(model, tight, out_dir=Path(tmp), stamp="t2")
            self.assertEqual(len(sent), 1)

    def test_model_check_is_metadata_only(self):
        urls = []
        status, entry = smoke_text.check_model("k", transport=lambda url: (urls.append(url) or 200,
                                                                           b'{"id": "openai/gpt-6-luna"}'))
        self.assertEqual((status, entry["id"]), (200, "openai/gpt-6-luna"))
        self.assertEqual(urls, ["https://ai-gateway.vercel.sh/v1/models/openai/gpt-6-luna"])


if __name__ == "__main__":
    unittest.main()
