"""End-to-end offline test: ingest -> mock enrich (interrupt + resume) -> group -> rank -> export,
then the provided check_submission.py audit. Synthetic data and mock labels only."""

import csv
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

import check_submission
from pipeline import group, rank
from pipeline.checkpoint import snapshot
from pipeline.common import atomic_write_json
from pipeline.enrich import Options, run_enrichment
from pipeline.export import ExportError, export
from pipeline.ledger import SpendLedger
from pipeline.providers import MockProvider
from tests.test_enrich import TEXTS, make_db, no_sleep

MORE = TEXTS + ["The app crashes when I open my library", "Can't login after update, uninstalling now",
                "Ads every song, so annoying", "Shuffle only with premium, ridiculous price"]


class ExportTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.conn = make_db(self.dir, MORE)
        self.ledger = SpendLedger(Decimal("1"), self.dir / "ledger.jsonl")

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def enrich_with_resume(self):
        run_enrichment(self.conn, MockProvider(max_batch=2), self.ledger, Options(stop_after_requests=2, sleep=no_sleep))
        atomic_write_json(self.dir / "before.json", snapshot(self.conn, "before"))
        run_enrichment(self.conn, MockProvider(max_batch=2), self.ledger, Options(sleep=no_sleep))
        atomic_write_json(self.dir / "after.json", snapshot(self.conn, "after"))

    def test_export_passes_provided_checker_except_unbuilt_roles(self):
        self.enrich_with_resume()
        out = self.dir / "grading"
        result = export(self.conn, out, checkpoint_before=self.dir / "before.json",
                        checkpoint_after=self.dir / "after.json", allow_mock=True, gzip_records=True)
        top = result["ranking"][0]["issue_id"]
        export(self.conn, out, claim_selections=[(top, "complaint_count"), (top, "mean_severity")],
               checkpoint_before=self.dir / "before.json", checkpoint_after=self.dir / "after.json",
               allow_mock=True, gzip_records=True)
        check_submission.write_json(out / "ingestion.json", check_submission.profile(self.dir / "input.csv"))
        reference = check_submission.reference(self.dir / "input.csv", self.dir / "input.csv")
        audit = check_submission.audit(out, reference)
        # verify, group and memo model roles are not built yet; everything else must pass.
        self.assertEqual(set(audit["issue_counts"]), {"missing_model_roles"}, audit["examples"])
        self.assertEqual(audit["coverage"]["missing"], 0)
        self.assertEqual(audit["coverage"]["quarantined"], 1)
        self.assertGreater(audit["coverage"]["valid_cache_reuses"], 0)
        ours = [{k: str(v) for k, v in r.items()} for r in result["ranking"]]
        theirs = [{k: str(v) for k, v in r.items()} for r in audit["calculated_ranking"]]
        self.assertEqual(ours, theirs)

    def test_pending_reviews_are_not_relabeled_as_quarantined(self):
        run_enrichment(self.conn, MockProvider(max_batch=1), self.ledger, Options(max_requests=2, sleep=no_sleep))
        out = self.dir / "grading"
        result = export(self.conn, out, allow_mock=True, pending_path=self.dir / "pending.jsonl")
        self.assertGreater(result["counts"]["pending"], 0)
        self.assertEqual(result["counts"]["quarantined"], 1)        # still only the empty text
        lines = [json.loads(x) for x in (out / "records.jsonl").read_text().splitlines()]
        self.assertTrue(all(r["status"] in ("completed", "quarantined") for r in lines))
        pending = (self.dir / "pending.jsonl").read_text().splitlines()
        self.assertEqual(len(pending), result["counts"]["pending"])

    def test_mock_labels_refused_without_flag(self):
        run_enrichment(self.conn, MockProvider(max_batch=3), self.ledger, Options(sleep=no_sleep))
        with self.assertRaises(ExportError):
            export(self.conn, self.dir / "grading")

    def test_ranking_regenerates_identically_from_saved_files(self):
        run_enrichment(self.conn, MockProvider(max_batch=3), self.ledger, Options(sleep=no_sleep))
        out = self.dir / "grading"
        export(self.conn, out, allow_mock=True)
        first = (out / "ranking.csv").read_text()
        rows = rank.compute(rank.read_records(out / "records.jsonl"), rank.read_membership(out / "membership.csv"))
        rank.write_ranking(self.dir / "ranking_again.csv", rows)
        self.assertEqual(first, (self.dir / "ranking_again.csv").read_text())


class RankRuleTests(unittest.TestCase):
    def test_baseline_math_tie_break_and_half_up(self):
        records = {f"r{i}": {"intent": "complaint", "severity": s} for i, s in enumerate([4, 4, 3, 2, 2, 3, 1])}
        membership = [("b", "r0"), ("b", "r1"), ("a", "r2"), ("a", "r3"), ("a", "r4"), ("c", "r5"), ("c", "r6")]
        rows = rank.compute(records, membership)
        self.assertEqual([(r["rank"], r["issue_id"], r["priority_score"]) for r in rows],
                         [(1, "b", 8), (2, "a", 7), (3, "c", 4)])
        self.assertEqual(rows[1]["mean_severity"], "2.333333")
        self.assertEqual(rank.mean_string(5, 8), "0.625000")
        self.assertEqual(rank.mean_string(2, 3), "0.666667")

    def test_rejects_duplicates_and_noncomplaints(self):
        records = {"r1": {"intent": "complaint", "severity": 2}, "r2": {"intent": "praise", "severity": 1}}
        with self.assertRaises(ValueError):
            rank.compute(records, [("a", "r1"), ("a", "r1")])
        with self.assertRaises(ValueError):
            rank.compute(records, [("a", "r2")])

    def test_grouping_is_one_issue_per_complaint_and_stable(self):
        records = [
            {"review_id": "1", "status": "completed", "intent": "complaint", "topic": "playback", "entities": ["ads", "crash"]},
            {"review_id": "2", "status": "completed", "intent": "cancellation", "topic": "usability", "entities": ["ads"]},
            {"review_id": "3", "status": "completed", "intent": "praise", "topic": "catalog", "entities": []},
            {"review_id": "4", "status": "completed", "intent": "complaint", "topic": "other", "entities": []},
        ]
        membership, issues = group.assign(records)
        self.assertEqual(membership, [("other.general", "4"), ("playback.crash", "1"), ("usability.ads", "2")])
        self.assertEqual(group.assign(list(reversed(records)))[0], membership)
        self.assertEqual([i["issue_id"] for i in issues], ["other.general", "playback.crash", "usability.ads"])


if __name__ == "__main__":
    unittest.main()
