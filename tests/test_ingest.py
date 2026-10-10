"""Offline tests for ingestion. Uses small synthetic CSVs only (test fixtures, never business data).

Run: python3 -m unittest discover -s tests -v
"""

import csv
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from pipeline import db
from pipeline.common import FIELDS, row_sha
from pipeline.ingest import IngestError, ingest

ROWS = [
    # review_id, review_text, rating, likes, app_version, timestamp
    ["r1", "Songs keep stopping.\nPlease fix!", "1", "3", "8.8.0", "2023-01-01 10:00:00"],
    ["r2", "", "5", "0", "", "2023-01-02 10:00:00"],               # empty -> quarantined
    ["r3", "   \n ", "3", "0", "8.8.0", "2023-01-03 10:00:00"],    # whitespace only -> quarantined
    ["r4", "Great app", "5", "1", "", "2023-02-01 10:00:00"],
    ["r5", "Great app", "4", "0", "8.9.1", "2023-02-02 10:00:00"],  # exact duplicate text of r4
    ["r6", "great app", "5", "0", "8.9.1", "2023-02-03 10:00:00"],  # different case: not an exact duplicate
    ["r7", "Café ünïcode  spacing ", "2", "0", "", "2023-03-01 10:00:00"],
]


def write_csv(path, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(FIELDS)
        writer.writerows(rows)


class IngestTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.csv = self.dir / "input.csv"
        write_csv(self.csv, ROWS)
        self.db = self.dir / "state.sqlite"

    def tearDown(self):
        self.tmp.cleanup()

    def run_ingest(self, **kw):
        return ingest(self.csv, self.db, self.dir / "out", self.dir / "grading", **kw)

    def test_statuses_and_counts(self):
        report = self.run_ingest()
        self.assertEqual(report["statuses"], {"pending": 5, "completed": 0, "quarantined": 2})
        self.assertEqual(report["text"]["distinct_nonempty_texts"], 4)
        self.assertEqual(report["text"]["rows_reusable_via_exact_text_cache"], 1)
        self.assertEqual(report["field_quality"]["missing_app_version"], 3)
        self.assertEqual(report["field_quality"]["multiline_text"], 2)
        self.assertTrue(report["all_checks_pass"])
        self.assertEqual(report["model_calls"], 0)

    def test_quarantine_only_empty_texts_with_reason(self):
        self.run_ingest()
        lines = (self.dir / "out" / "quarantine.jsonl").read_text().splitlines()
        records = [json.loads(x) for x in lines]
        self.assertEqual([r["review_id"] for r in records], ["r2", "r3"])
        self.assertTrue(all(r["reason"] == "empty_review_text" for r in records))

    def test_source_hash_matches_provided_helper_without_normalization(self):
        self.run_ingest()
        conn = sqlite3.connect(self.db)
        saved = dict(conn.execute("SELECT review_id, source_sha256 FROM reviews"))
        conn.close()
        for row in ROWS:
            self.assertEqual(saved[row[0]], row_sha(dict(zip(FIELDS, row))))

    def test_exact_text_representative(self):
        self.run_ingest()
        conn = sqlite3.connect(self.db)
        rep = dict(conn.execute("SELECT review_id, text_rep_id FROM reviews"))
        conn.close()
        self.assertEqual(rep["r5"], "r4")
        self.assertEqual(rep["r6"], "r6")
        self.assertIsNone(rep["r2"])

    def test_grading_ingestion_json_equals_provided_profile(self):
        import check_submission
        self.run_ingest()
        saved = json.loads((self.dir / "grading" / "ingestion.json").read_text())
        self.assertEqual(saved, json.loads(json.dumps(check_submission.profile(self.csv))))

    def test_rerun_is_idempotent_and_preserves_completed_status(self):
        self.run_ingest()
        conn = db.connect(self.db)
        with conn:
            conn.execute("UPDATE reviews SET status = 'completed' WHERE review_id = 'r1'")
        conn.close()
        report = self.run_ingest()
        self.assertFalse(report["loaded_this_run"])
        self.assertEqual(report["statuses"], {"pending": 4, "completed": 1, "quarantined": 2})
        with self.assertRaises(IngestError):
            self.run_ingest(rebuild=True)

    def test_refuses_different_input_in_same_db(self):
        self.run_ingest()
        other = self.dir / "other.csv"
        write_csv(other, ROWS[:3])
        with self.assertRaises(IngestError):
            ingest(other, self.db, self.dir / "out", self.dir / "grading")

    def test_duplicate_ids_are_reported_not_merged_silently(self):
        write_csv(self.csv, ROWS + [["r1", "different text", "2", "0", "", "2023-04-01 10:00:00"]])
        report = self.run_ingest()
        self.assertEqual(report["field_quality"]["duplicate_review_ids"], 1)
        self.assertEqual(report["field_quality"]["unique_review_ids"], 7)

    def test_rejects_unexpected_header(self):
        with open(self.csv, "w", encoding="utf-8", newline="") as f:
            f.write("id,text\nx,y\n")
        with self.assertRaises(IngestError):
            self.run_ingest()


if __name__ == "__main__":
    unittest.main()
