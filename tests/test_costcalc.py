"""Offline tests for the cost calculator. Synthetic fixtures only (not measurements)."""

import csv
import json
import shutil
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from pipeline import costcalc
from pipeline.common import REPO_ROOT

CALLS = [
    {"run_kind": "cold", "request_id": "c1", "role": "enrich", "provider": "vercel-ai-gateway",
     "model": "typesafe-ai/jev", "review_ids": ["a"], "outcome": "succeeded", "attempt": 1,
     "input_tokens": 1000, "output_tokens": 20, "cached_input_tokens": 0},
    {"run_kind": "cold", "request_id": "c2", "role": "enrich", "provider": "vercel-ai-gateway",
     "model": "typesafe-ai/jev", "review_ids": ["b"], "outcome": "failed", "attempt": 1,
     "input_tokens": None, "output_tokens": None},
    {"run_kind": "cold", "request_id": "c3", "role": "enrich", "provider": "vercel-ai-gateway",
     "model": "typesafe-ai/jev", "review_ids": ["b"], "outcome": "succeeded", "attempt": 2,
     "input_tokens": 3000, "output_tokens": 20, "cached_input_tokens": 1000},
    {"run_kind": "cold", "request_id": "c4", "role": "memo", "provider": "vercel-ai-gateway",
     "model": "typesafe-ai/jev", "review_ids": [], "outcome": "succeeded", "attempt": 1,
     "input_tokens": 2000, "output_tokens": 0},
]
RUNS = {"cold": {"input_rows": 100, "completed": 99, "failed": 1, "wall_clock_s": 50.0,
                 "stage_seconds": {"enrich": 40.0, "memo": 5.0}, "input_sha256": "x"},
        "warm": {"input_rows": 100, "completed": 99, "failed": 1, "wall_clock_s": 2.0}}


class CostCalcTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        for name in ("rates.csv", "assumptions.json"):
            shutil.copy(REPO_ROOT / "cost" / name, self.dir / name)
        with (self.dir / "rates.csv").open("a", encoding="utf-8") as f:
            f.write("jev_cached,vercel-ai-gateway,typesafe-ai/jev,cached_input_tokens,0.010,1000000,USD,"
                    "2026-10-09,https://example.invalid,test-only cached rate\n")
        (self.dir / "pilot_calls.jsonl").write_text("".join(json.dumps(c) + "\n" for c in CALLS))
        (self.dir / "pilot_runs.json").write_text(json.dumps(RUNS))

    def tearDown(self):
        self.tmp.cleanup()

    def scale_rates(self, factor):
        path = self.dir / "rates.csv"
        with path.open(encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        for row in rows:
            row["price_usd"] = str(Decimal(row["price_usd"]) * factor)
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys(), lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)

    def test_item_arithmetic_and_cached_input_split(self):
        report = costcalc.replay(self.dir, write=False)
        enrich = report["cold"]["by_role"]["enrich"]
        # c1: 1000 x 0.042/1M; c3: 2000 uncached x 0.042/1M + 1000 cached x 0.010/1M; c2 usage unknown
        self.assertIsNone(enrich["cost_usd"])                      # unknown usage is never zero
        rows = costcalc.usage_table(CALLS, costcalc.load_rates(self.dir / "rates.csv"))
        c3 = {r["rate_item"]: r for r in rows if r["request_id"] == "c3"}
        self.assertEqual(c3["jev_input"]["billed_units"], 2000)
        self.assertEqual(c3["jev_cached"]["cost_usd"], Decimal("0.00001"))
        self.assertEqual(c3["jev_input"]["cost_usd"], Decimal("0.000084"))
        unknown = [r for r in rows if r["request_id"] == "c2"]
        self.assertTrue(all(r["status"] == "usage_unavailable" for r in unknown))

    def test_doubling_rates_doubles_api_cost_only(self):
        calls = [c for c in CALLS if c["input_tokens"] is not None]
        (self.dir / "pilot_calls.jsonl").write_text("".join(json.dumps(c) + "\n" for c in calls))
        before = costcalc.replay(self.dir, write=False)
        self.scale_rates(2)
        after = costcalc.replay(self.dir, write=False)
        self.assertEqual(after["cold"]["api_cost_usd"], before["cold"]["api_cost_usd"] * 2)
        self.assertEqual(after["cold"]["run_meta"]["wall_clock_s"], before["cold"]["run_meta"]["wall_clock_s"])
        self.assertEqual(after["local_compute"], before["local_compute"])

    def test_projection_target_does_not_change_measured_results(self):
        before = costcalc.replay(self.dir, write=False)
        assumptions = json.loads((self.dir / "assumptions.json").read_text())
        assumptions["projection_targets"] = [250000]
        (self.dir / "assumptions.json").write_text(json.dumps(assumptions))
        after = costcalc.replay(self.dir, write=False)
        self.assertEqual(before["cold"], after["cold"])
        self.assertEqual(after["projections"][0]["target_rows"], 250000)

    def test_fixed_roles_are_not_multiplied_and_reuse_reduces_requests(self):
        calls = [c for c in CALLS if c["input_tokens"] is not None]
        (self.dir / "pilot_calls.jsonl").write_text("".join(json.dumps(c) + "\n" for c in calls))
        report = costcalc.replay(self.dir, write=False)
        base = next(p for p in report["projections"] if p["scenario"] == "base" and p["target_rows"] == 660609)
        memo = report["cold"]["by_role"]["memo"]["cost_usd"]
        self.assertEqual(base["with_exact_text_reuse"]["fixed_overhead_usd"], memo)
        self.assertEqual(base["with_exact_text_reuse"]["distinct_texts_to_classify"], 484189)
        self.assertEqual(base["no_reuse"]["distinct_texts_to_classify"], 660609)
        self.assertEqual(base["with_exact_text_reuse"]["enrich_requests"], 484189)  # 1 review per request measured

    def test_budget_warning(self):
        assumptions = json.loads((self.dir / "assumptions.json").read_text())
        assumptions["controls"]["budget_usd"] = 0.0001
        (self.dir / "assumptions.json").write_text(json.dumps(assumptions))
        report = costcalc.replay(self.dir, write=False)
        self.assertTrue(any(p["with_exact_text_reuse"]["exceeds_budget"] for p in report["projections"]))
        self.assertIn("**YES**", costcalc.render_markdown(report))

    def test_no_pilot_is_labeled_and_uses_pre_pilot_assumptions(self):
        (self.dir / "pilot_calls.jsonl").unlink()
        (self.dir / "pilot_runs.json").unlink()
        report = costcalc.replay(self.dir, write=False)
        self.assertFalse(report["pilot_measured"])
        self.assertTrue(all(p["unit_cost_source"] == "pre_pilot_assumption" for p in report["projections"]))
        self.assertIn("NO MEASURED PILOT YET", costcalc.render_markdown(report))

    def test_replay_writes_outputs_without_network(self):
        costcalc.replay(self.dir)
        for name in ("usage.csv", "report.json", "report.md"):
            self.assertTrue((self.dir / name).exists())
        self.assertIn("unknown", (self.dir / "usage.csv").read_text())


if __name__ == "__main__":
    unittest.main()
