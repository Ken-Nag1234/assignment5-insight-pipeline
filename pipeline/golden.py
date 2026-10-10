"""Golden-50 human labeling helpers. No model calls; never fills in labels.

Usage:
    python3 -m pipeline.golden template    # create the blank working file (refuses to overwrite)
    python3 -m pipeline.golden validate    # check the human labels entered so far

The working file shows only review_id, source_sha256 and review_text, so the labeler
is not anchored on star ratings or other metadata. Labels in this file are the human
reference answers: they must never be used as model inputs, examples or tuning data.
"""

import argparse
import csv
import json
import sys
from pathlib import Path

from pipeline.common import FIELDS, INTENTS, REPO_ROOT, TOPICS, csv_rows, row_sha

SOURCE = REPO_ROOT / "golden_50_to_label.csv"
WORKING = REPO_ROOT / "golden" / "golden_50_labels.csv"
LABEL_FIELDS = ["topic", "intent", "sentiment", "severity", "entities", "evidence_quote", "needs_review"]
EXTRA_FIELDS = ["ambiguous", "accepted_topics", "accepted_intents", "accepted_severities", "notes"]
COLUMNS = ["review_id", "source_sha256", "review_text"] + LABEL_FIELDS + EXTRA_FIELDS
BOOLS = {"true": True, "false": False}


def create_template(source=SOURCE, working=WORKING):
    working = Path(working)
    if working.exists():
        raise SystemExit(f"ERROR: {working} already exists; refusing to overwrite human labels.")
    rows = list(csv_rows(source))
    if any(row.get(f, "") for row in rows for f in LABEL_FIELDS):
        raise SystemExit("ERROR: source golden file already contains label values; expected blank columns.")
    working.parent.mkdir(parents=True, exist_ok=True)
    with working.open("x", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            source_row = {k: row[k] for k in FIELDS}
            writer.writerow({"review_id": row["review_id"], "source_sha256": row_sha(source_row),
                             "review_text": row["review_text"]})
    return len(rows)


def _split(value):
    return [x.strip() for x in value.split("|") if x.strip()]


def validate_row(row, source_text):
    """Return a list of problems for one row; an all-blank label row is reported as unlabeled."""
    if not any(row[f].strip() for f in LABEL_FIELDS):
        return ["unlabeled"]
    problems = []
    if row["topic"] not in TOPICS:
        problems.append(f"topic must be one of {TOPICS}")
    if row["intent"] not in INTENTS:
        problems.append(f"intent must be one of {INTENTS}")
    try:
        sentiment = float(row["sentiment"])
        if not -1 <= sentiment <= 1:
            problems.append("sentiment must be between -1 and 1")
    except ValueError:
        problems.append("sentiment must be a number between -1 and 1")
    if row["severity"] not in {"1", "2", "3", "4", "5"}:
        problems.append("severity must be an integer 1-5")
    try:
        entities = json.loads(row["entities"] or "[]")
        if not isinstance(entities, list) or not all(isinstance(x, str) and x.strip() for x in entities):
            raise ValueError
    except ValueError:
        problems.append('entities must be a JSON list of nonempty strings, e.g. [] or ["offline mode"]')
    if not row["evidence_quote"].strip():
        problems.append("evidence_quote is required")
    elif row["evidence_quote"] not in source_text:
        problems.append("evidence_quote is not an exact substring of review_text")
    for field in ("needs_review", "ambiguous"):
        if row[field].strip().lower() not in BOOLS and not (field == "ambiguous" and not row[field].strip()):
            problems.append(f"{field} must be true or false")
    for field, allowed in (("accepted_topics", TOPICS), ("accepted_intents", INTENTS),
                           ("accepted_severities", ("1", "2", "3", "4", "5"))):
        bad = [x for x in _split(row[field]) if x not in allowed]
        if bad:
            problems.append(f"{field} has invalid values {bad}")
    if row["ambiguous"].strip().lower() == "true" and not any(
            _split(row[f]) for f in ("accepted_topics", "accepted_intents", "accepted_severities")):
        problems.append("ambiguous=true needs at least one accepted_* alternative")
    return problems


def validate(source=SOURCE, working=WORKING):
    source_rows = {row["review_id"]: row for row in csv_rows(source)}
    rows = list(csv_rows(working))
    report = {"rows": len(rows), "labeled": 0, "valid": 0, "unlabeled": 0, "problems": {}}
    if [r["review_id"] for r in rows] != list(source_rows):
        report["problems"]["__file__"] = ["review_id list/order differs from golden_50_to_label.csv"]
    for row in rows:
        original = source_rows.get(row["review_id"])
        if original is None:
            report["problems"][row["review_id"]] = ["unknown review_id"]
            continue
        issues = []
        if row["review_text"] != original["review_text"]:
            issues.append("review_text was edited; it must match the source exactly")
        if row["source_sha256"] != row_sha({k: original[k] for k in FIELDS}):
            issues.append("source_sha256 does not match the source row")
        issues += validate_row(row, original["review_text"])
        if issues == ["unlabeled"]:
            report["unlabeled"] += 1
            continue
        report["labeled"] += 1
        if issues:
            report["problems"][row["review_id"]] = issues
        else:
            report["valid"] += 1
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["template", "validate"])
    parser.add_argument("--source", type=Path, default=SOURCE)
    parser.add_argument("--working", type=Path, default=WORKING)
    args = parser.parse_args(argv)
    if args.command == "template":
        n = create_template(args.source, args.working)
        print(f"Created blank labeling file with {n} rows: {args.working}")
        return 0
    report = validate(args.source, args.working)
    print(f"rows={report['rows']} labeled={report['labeled']} valid={report['valid']} unlabeled={report['unlabeled']}")
    for rid, issues in report["problems"].items():
        for issue in issues:
            print(f"  {rid}: {issue}")
    return 0 if not report["problems"] else 1


if __name__ == "__main__":
    sys.exit(main())
