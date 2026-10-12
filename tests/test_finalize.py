"""Offline test of the final-run workflow (stage -> export -> provided checker -> serving -> promote).
Synthetic data and mock labels only; everything is written inside a temporary directory."""

import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

import check_submission
from pipeline import finalize, insights, runlock, trace
from pipeline.run import mock_text_responder
from pipeline.textmodel import MockTextModel
from pipeline.checkpoint import snapshot
from pipeline.common import atomic_write_json
from pipeline.enrich import Options, run_enrichment
from pipeline.ledger import SpendLedger
from pipeline.providers import MockProvider
from tests.test_enrich import make_db, no_sleep
from tests.test_export import MORE


class FinalizeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.dir = Path(self.tmp.name)
        conn = make_db(d, MORE)
        ledger = SpendLedger(Decimal("1"), d / "ledger.jsonl")
        run_enrichment(conn, MockProvider(max_batch=2), ledger, Options(stop_after_requests=2, sleep=no_sleep))
        (d / "checkpoints").mkdir()
        atomic_write_json(d / "checkpoints" / "checkpoint_before.json", snapshot(conn, "before"))
        run_enrichment(conn, MockProvider(max_batch=2), ledger, Options(sleep=no_sleep))
        atomic_write_json(d / "checkpoints" / "checkpoint_after.json", snapshot(conn, "after"))
        conn.close()
        self.db = d / "state.sqlite"
        check_submission.write_json(d / "ingestion.json", check_submission.profile(d / "input.csv"))
        self.root = d / "final"

    def tearDown(self):
        self.tmp.cleanup()

    def test_stage_export_check_serve_promote(self):
        summary = finalize.build_analysis(self.db, self.root, stamp="s1", allow_mock=True)
        self.assertGreater(summary["memberships"], 0)
        with self.assertRaises(finalize.FinalizeError):
            finalize.build_analysis(self.db, self.root, stamp="s1", allow_mock=True)   # never reuses a stage
        model, ledger = MockTextModel(mock_text_responder), SpendLedger(Decimal("1"), self.dir / "ledger.jsonl")
        analysis_dir = self.root / "s1" / "analysis"
        insights.name_issues(self.db, analysis_dir, model, ledger)
        insights.write_memo(self.db, analysis_dir, model, ledger)
        _, audit = finalize.build_export("s1", self.db, self.root, self.dir / "input.csv", self.dir / "checkpoints",
                                         self.dir / "ingestion.json", allow_mock=True,
                                         verify_dir=self.dir / "no_verify")
        # synthetic run without a verifier: only the verify role is missing, everything else passes
        self.assertEqual(set(audit["issue_counts"]), {"missing_model_roles"}, audit["examples"])
        self.assertIn("verify", audit["examples"][0]["detail"])
        self.assertNotIn("memo", audit["examples"][0]["detail"])
        base = self.root / "s1"
        self.assertTrue((base / "check_report.json").exists())
        manifest = json.loads((base / "analysis" / "run_manifest.json").read_text())
        self.assertEqual(len(manifest["code"]["code_sha256"]), 64)
        self.assertIn("grading/ranking.csv", manifest["outputs_sha256"])
        self.assertEqual(manifest["prompts"]["memo"], insights.MEMO_PROMPT_VERSION)
        self.assertTrue((base / "grading" / "records.jsonl.gz").exists())
        path, counts = finalize.build_serving("s1", self.db, self.root, allow_mock=True)
        self.assertEqual(counts["issues"], summary["issues"])
        self.assertGreater(counts["claims"], 0)   # claims.csv comes from the export, not the analysis stage
        grading, analysis = self.dir / "promoted_grading", self.dir / "promoted_analysis"
        grading.mkdir()
        (grading / "ingestion.json").write_bytes((self.dir / "ingestion.json").read_bytes())  # identical: allowed
        copied = finalize.promote("s1", self.root, grading, analysis)
        self.assertIn(str(grading / "ranking.csv"), copied)
        self.assertEqual((grading / "ranking.csv").read_text(), (base / "grading" / "ranking.csv").read_text())
        (grading / "ranking.csv").write_text("tampered\n")
        with self.assertRaises(finalize.FinalizeError):
            finalize.promote("s1", self.root, grading, analysis)     # refuses to overwrite a differing file
        self.assertEqual((grading / "ranking.csv").read_text(), "tampered\n")

    def test_trace_links_every_stage(self):
        finalize.build_analysis(self.db, self.root, stamp="t", allow_mock=True)
        analysis = self.root / "t" / "analysis"
        model, ledger = MockTextModel(mock_text_responder), SpendLedger(Decimal("1"), self.dir / "ledger.jsonl")
        insights.write_memo(self.db, analysis, model, ledger)
        rid = trace.pick(analysis, verify_dir=self.dir / "no-verify", db_path=self.db)
        text = trace.trace(rid, analysis, self.db, verify_dir=self.dir / "no-verify")
        for step in ("Source row", "Stored labels", "Issue membership", "Ranking", "Memo"):
            self.assertIn(step, text)
        self.assertIn("cited by recommendation", text)
        empty = trace.pick(analysis, failed=True, db_path=self.db)
        self.assertIn("Trace of review", trace.trace(empty, analysis, self.db, verify_dir=self.dir / "no-verify"))

    def test_refuses_while_enrichment_holds_the_lock(self):
        with runlock.hold_lock(self.db, "test"):
            with self.assertRaises(finalize.FinalizeError):
                finalize.build_analysis(self.db, self.root, stamp="s2", allow_mock=True)


if __name__ == "__main__":
    unittest.main()
