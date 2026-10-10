"""Offline tests for the golden labeling helpers. Synthetic fixtures only."""

import csv
import tempfile
import unittest
from pathlib import Path

from pipeline.common import FIELDS
from pipeline.golden import COLUMNS, LABEL_FIELDS, create_template, validate


class GoldenTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.source = self.dir / "golden_source.csv"
        with self.source.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(list(FIELDS) + LABEL_FIELDS)
            w.writerow(["g1", "Can't log in since the update.", "1", "0", "", "2023-01-01 00:00:00"] + [""] * 7)
            w.writerow(["g2", "Great app", "5", "0", "", "2023-01-02 00:00:00"] + [""] * 7)
        self.working = self.dir / "work" / "labels.csv"

    def tearDown(self):
        self.tmp.cleanup()

    def edit(self, review_id, **values):
        rows = list(csv.DictReader(self.working.open(encoding="utf-8")))
        for row in rows:
            if row["review_id"] == review_id:
                row.update(values)
        with self.working.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=COLUMNS, lineterminator="\n")
            w.writeheader()
            w.writerows(rows)

    def test_template_is_blank_and_never_overwrites(self):
        self.assertEqual(create_template(self.source, self.working), 2)
        rows = list(csv.DictReader(self.working.open(encoding="utf-8")))
        self.assertTrue(all(not row[f] for row in rows for f in LABEL_FIELDS))
        self.assertNotIn("review_rating", rows[0])
        with self.assertRaises(SystemExit):
            create_template(self.source, self.working)

    def test_validate_counts_and_problems(self):
        create_template(self.source, self.working)
        report = validate(self.source, self.working)
        self.assertEqual((report["labeled"], report["unlabeled"]), (0, 2))
        self.edit("g1", topic="access", intent="complaint", sentiment="-0.5", severity="4", entities="[]",
                  evidence_quote="Can't log in since the update.", needs_review="false", ambiguous="false")
        self.edit("g2", topic="praise", intent="praise", sentiment="1", severity="1", entities="[]",
                  evidence_quote="Great app!", needs_review="false")
        report = validate(self.source, self.working)
        self.assertEqual((report["labeled"], report["valid"]), (2, 1))
        problems = " ".join(report["problems"]["g2"])
        self.assertIn("topic must be one of", problems)
        self.assertIn("not an exact substring", problems)


if __name__ == "__main__":
    unittest.main()
