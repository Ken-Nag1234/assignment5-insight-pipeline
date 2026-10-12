"""Offline tests for the golden evaluation. Synthetic reviews, labels and predictions only."""

import csv
import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from pipeline import db
from pipeline.common import FIELDS, row_sha
from pipeline.golden import COLUMNS, LABEL_FIELDS
from pipeline.golden_eval import (MISSING, EvalError, evaluate, load_human, load_predictions, macro_f1, make_input,
                                  run)
from pipeline.ingest import ingest

TEXTS = ["Premium Duo is great, no ads at all!", "Ads after every song, please reduce them",
         "Music stops after a few seconds, can't play anything", "Line one\nline two, with \"quotes\""]
#           topic        intent       sev  sent   entities          quote                    ambiguous accepted_topics
HUMAN = [("billing", "praise", 1, 1, '["premium"]', "Premium Duo is great", "true", "usability"),
         ("usability", "complaint", 2, -0.5, '["ads"]', "Ads after every song", "", ""),
         ("playback", "complaint", 4, -0.5, "[]", "Music stops", "false", ""),
         ("other", "unclear", 1, 0, "[]", "Line one", "", "")]


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class GoldenEvalTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.source = self.dir / "golden_source.csv"
        with self.source.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(list(FIELDS) + LABEL_FIELDS)
            for i, text in enumerate(TEXTS, 1):
                w.writerow([f"g{i}", text, "3", "0", "", "2023-01-01 00:00:00"] + [""] * 7)
        with self.source.open(encoding="utf-8", newline="") as f:
            self.source_rows = {r["review_id"]: r for r in csv.DictReader(f)}
        self.human_path = self.dir / "human.csv"
        with self.human_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(COLUMNS)
            for i, (text, h) in enumerate(zip(TEXTS, HUMAN), 1):
                topic, intent, sev, sent, ents, quote, amb, acc = h
                w.writerow([f"g{i}", row_sha(self.source_rows[f"g{i}"]), text, topic, intent, sent, sev, ents, quote,
                            "false", amb, acc, "", "", ""])
        self.human = load_human(self.source, self.human_path)

    def tearDown(self):
        self.tmp.cleanup()

    def pred(self, rid, topic, intent, severity, sentiment, entities=(), quote=None, needs_review=False,
             status="completed"):
        h = self.human[rid]
        return {"source_sha256": h["source_sha256"], "status": status, "reason": None, "label_config": "test+p1",
                "topic": topic, "intent": intent, "severity": severity, "sentiment": sentiment,
                "entities": list(entities), "evidence_quote": h["evidence_quote"] if quote is None else quote,
                "needs_review": needs_review, "adjustments": []}

    def perfect(self):
        return {rid: self.pred(rid, h["topic"], h["intent"], h["severity"], h["sentiment"], h["entities"])
                for rid, h in self.human.items()}

    def test_perfect_predictions_score_one(self):
        report = evaluate(self.human, self.perfect())
        m = report["metrics"]
        for field in ("topic", "intent", "severity"):
            self.assertEqual((m[field]["accuracy_strict"], m[field]["accuracy_accepted"]), (1.0, 1.0))
            self.assertEqual((m[field]["macro_f1_strict"], m[field]["macro_f1_accepted"]), (1.0, 1.0))
        self.assertEqual((m["severity"]["mae"], m["sentiment"]["within_tolerance"], m["sentiment"]["mae"]),
                         (0.0, 1.0, 0.0))
        self.assertEqual((m["entities"]["precision"], m["entities"]["recall"], m["needs_review"]["agreement"]),
                         (1.0, 1.0, 1.0))
        self.assertEqual(m["evidence_quote"], {"exact_substring": 1.0, "overlaps_human_quote": 1.0})
        self.assertEqual(report["mismatches"], [])

    def test_missing_and_quarantined_stay_in_denominator(self):
        preds = {"g1": self.pred("g1", "usability", "praise", 1, 1, ["premium"]),       # accepted alternative
                 "g2": self.pred("g2", "usability", "complaint", 2, -0.5, ["ads"]),     # correct
                 "g4": dict(self.pred("g4", "other", "unclear", 1, 0), status="quarantined",
                            reason="invalid_model_output: invalid topic")}             # g3 has no prediction
        del preds["g4"]["label_config"]
        report = evaluate(self.human, preds)
        topic = report["metrics"]["topic"]
        self.assertEqual(report["denominator"], 4)
        self.assertEqual(report["coverage"], {"predicted": 2, "missing": 1, "pending": 0, "quarantined": 1,
                                              "sha_mismatch": 0})
        self.assertEqual((topic["accuracy_strict"], topic["accuracy_accepted"]), (0.25, 0.5))
        # Strict: usability P=1/2 R=1 F1=0.6667; billing, playback, other F1=0 -> 0.6667/4.
        self.assertEqual(topic["macro_f1_strict"], 0.1667)
        # Accepted: g1 credited as billing -> billing 1, usability 1, playback 0, other 0.
        self.assertEqual(topic["macro_f1_accepted"], 0.5)
        self.assertEqual(topic["confusion_strict"]["matrix"]["playback"][MISSING], 1)
        self.assertEqual(topic["confusion_strict"]["matrix"]["other"][MISSING], 1)
        self.assertEqual(report["metrics"]["intent"]["accuracy_strict"], 0.5)
        self.assertEqual(report["metrics"]["sentiment"]["within_tolerance"], 0.5)
        # Human entities of unpredicted reviews still count toward recall (g3 and g4 have none).
        self.assertEqual(report["metrics"]["entities"]["recall"], 1.0)
        states = {r["review_id"]: r["state"] for r in report["mismatches"]}
        self.assertEqual(states, {"g1": "completed", "g3": "missing", "g4": "quarantined"})
        g1 = next(r for r in report["mismatches"] if r["review_id"] == "g1")
        self.assertEqual(g1["problems"], ["topic(accepted alternative)"])
        g4 = next(r for r in report["mismatches"] if r["review_id"] == "g4")
        self.assertEqual(g4["reason"], "invalid_model_output: invalid topic")

    def test_accepted_alternative_only_for_listed_values(self):
        preds = self.perfect()
        preds["g1"]["topic"] = "catalog"                    # not in accepted_topics
        report = evaluate(self.human, preds)
        self.assertEqual(report["metrics"]["topic"]["accuracy_accepted"], 0.75)
        self.assertEqual(report["ambiguous_cases"][0]["topic"]["accepted"], ["usability"])
        self.assertFalse(report["ambiguous_cases"][0]["topic"]["accepted_match"])
        preds["g1"]["topic"] = "usability"
        preds["g2"]["topic"] = "billing"                    # g2 has no alternatives
        report = evaluate(self.human, preds)
        self.assertEqual((report["metrics"]["topic"]["accuracy_strict"],
                          report["metrics"]["topic"]["accuracy_accepted"]), (0.5, 0.75))

    def test_severity_and_sentiment_errors(self):
        preds = self.perfect()
        preds["g2"].update(severity=3, sentiment=0.0)     # +1, |0.5| is within the tolerance
        preds["g3"].update(severity=2, sentiment=0.25)    # -2, |0.75| is outside
        m = evaluate(self.human, preds)["metrics"]
        self.assertEqual((m["severity"]["accuracy_strict"], m["severity"]["within_1"]), (0.5, 0.75))
        self.assertEqual((m["severity"]["mae"], m["severity"]["mean_signed_error"]), (0.75, -0.25))
        self.assertEqual(m["severity"]["error_distribution"], {"-2": 1, "0": 2, "1": 1})
        self.assertEqual((m["sentiment"]["within_tolerance"], m["sentiment"]["mae"]), (0.75, 0.3125))

    def test_entities_lowercase_and_trim_only(self):
        preds = self.perfect()
        preds["g1"]["entities"] = [" Premium "]           # matches after lowercase and trim
        preds["g2"]["entities"] = ["ad"]                  # singular/plural is not merged
        m = evaluate(self.human, preds)["metrics"]["entities"]
        self.assertEqual((m["true_positive"], m["predicted"], m["human"]), (1, 2, 2))
        self.assertEqual((m["precision"], m["recall"], m["exact_set_match"]), (0.5, 0.5, 0.75))

    def test_bad_quote_and_sha_mismatch(self):
        preds = self.perfect()
        preds["g2"]["evidence_quote"] = "ads after every song"   # case changed: not an exact substring
        preds["g3"]["source_sha256"] = "0" * 64
        report = evaluate(self.human, preds)
        self.assertEqual(report["coverage"]["sha_mismatch"], 1)
        self.assertEqual(report["metrics"]["evidence_quote"]["exact_substring"], 0.5)
        self.assertEqual(report["metrics"]["topic"]["accuracy_strict"], 0.75)

    def test_macro_f1_hand_computed(self):
        value, per_class = macro_f1([("a", "a"), ("a", "b"), ("b", "b"), ("c", MISSING)], ("a", "b", "c", "d"))
        self.assertEqual(per_class["a"]["f1"], 0.6667)   # P=1 R=0.5
        self.assertEqual(per_class["b"]["f1"], 0.6667)   # P=0.5 R=1
        self.assertEqual(per_class["c"]["f1"], 0.0)
        self.assertNotIn("d", per_class)
        self.assertEqual(value, 0.4445)

    def test_refuses_incomplete_human_labels(self):
        with self.human_path.open(encoding="utf-8", newline="") as f:
            rows = list(csv.reader(f))
        rows[2][COLUMNS.index("topic")] = ""
        bad = self.dir / "bad.csv"
        with bad.open("w", encoding="utf-8", newline="") as f:
            csv.writer(f, lineterminator="\n").writerows(rows)
        with self.assertRaises(EvalError):
            load_human(self.source, bad)

    def test_make_input_has_source_fields_only(self):
        out = self.dir / "input.csv"
        self.assertEqual(make_input(self.source, out), 4)
        with out.open(encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
        self.assertEqual(list(rows[0]), list(FIELDS))
        self.assertEqual([row_sha(r) for r in rows], [h["source_sha256"] for h in self.human.values()])
        with self.assertRaises(EvalError):
            make_input(self.source, out)

    def test_end_to_end_from_read_only_database(self):
        inp = self.dir / "input.csv"
        make_input(self.source, inp)
        db_path = self.dir / "eval.sqlite"
        ingest(inp, db_path, self.dir / "out", self.dir / "grading")
        conn = db.connect(db_path)
        for rid in ("g1", "g2", "g3"):
            h = self.human[rid]
            conn.execute("INSERT INTO labels (review_id, label_config, topic, intent, sentiment, severity, entities,"
                         " evidence_quote, needs_review, request_id, details, completed_at)"
                         " VALUES (?, 'test+p1', ?, ?, ?, ?, ?, ?, 0, 'req', ?, 'now')",
                         (rid, h["topic"], h["intent"], h["sentiment"], h["severity"], json.dumps(h["entities"]),
                          h["evidence_quote"], json.dumps({"adjustments": []})))
            conn.execute("UPDATE reviews SET status = 'completed' WHERE review_id = ?", (rid,))
        conn.commit()
        conn.close()
        before = (sha(db_path), sha(self.human_path))
        report = run(db_path, self.human_path, self.source, self.dir / "report")
        self.assertEqual(report["coverage"]["pending"], 1)                     # g4 never classified
        self.assertEqual(report["metrics"]["topic"]["accuracy_strict"], 0.75)
        self.assertEqual((sha(db_path), sha(self.human_path)), before)
        self.assertTrue((self.dir / "report" / "report.md").read_text().startswith("# Golden-50 evaluation"))
        with self.assertRaises(EvalError):
            run(db_path, self.human_path, self.source, self.dir / "report")    # no silent overwrite
        preds = load_predictions(db_path)
        self.assertEqual(preds["g4"]["status"], "pending")
        conn = sqlite3.connect(f"{db_path.resolve().as_uri()}?mode=ro", uri=True)
        with self.assertRaises(sqlite3.OperationalError):
            conn.execute("DELETE FROM labels")
        conn.close()


if __name__ == "__main__":
    unittest.main()
