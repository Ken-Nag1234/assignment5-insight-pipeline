"""Golden-50 human labeling helpers. No model calls; never fills in labels.

Usage:
    python3 -m pipeline.golden template    # create the blank working CSV (refuses to overwrite)
    python3 -m pipeline.golden xlsx        # create the Excel labeling workbook (refuses to overwrite)
    python3 -m pipeline.golden excel-csv   # create a UTF-8-with-BOM CSV copy for Excel (refuses to overwrite)
    python3 -m pipeline.golden from-xlsx   # convert the completed workbook to golden_50_labels_completed.csv
    python3 -m pipeline.golden validate [--working FILE]   # check the human labels entered so far

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
WORKBOOK = REPO_ROOT / "golden" / "golden_50_labels.xlsx"
EXCEL_CSV = REPO_ROOT / "golden" / "golden_50_labels_excel.csv"
COMPLETED = REPO_ROOT / "golden" / "golden_50_labels_completed.csv"
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


def create_excel_csv(working=WORKING, out=EXCEL_CSV):
    """Byte-for-byte copy of the working CSV with a UTF-8 BOM so Excel detects the encoding."""
    out = Path(out)
    if out.exists():
        raise SystemExit(f"ERROR: {out} already exists; refusing to overwrite.")
    data = Path(working).read_bytes()
    if data.startswith(b"\xef\xbb\xbf"):
        data = data[3:]
    with out.open("xb") as f:
        f.write(b"\xef\xbb\xbf" + data)


def create_workbook(working=WORKING, out=WORKBOOK):
    """Excel workbook: Labels sheet with dropdowns, plus Definitions and Instructions sheets.

    Identifier and text columns come from the working CSV unchanged; label cells are left blank.
    """
    from pipeline import xlsx_lite as x
    from pipeline.labels import INTENT_CRITERIA, SEVERITY_CRITERIA, TOPIC_CRITERIA
    out = Path(out)
    if out.exists():
        raise SystemExit(f"ERROR: {out} already exists; refusing to overwrite.")
    rows = list(csv_rows(working))
    if any(row.get(f, "") for row in rows for f in LABEL_FIELDS):
        raise SystemExit("ERROR: the working CSV already contains labels; the workbook must start blank.")
    body = [[row[c] for c in COLUMNS] for row in rows]
    n = len(body) + 1
    col = {name: x.col_letter(i) for i, name in enumerate(COLUMNS)}
    styles = [x.STYLE_READONLY_WRAP] * 3 + [x.STYLE_INPUT] * (len(COLUMNS) - 3)
    widths = [14, 12, 60, 12, 13, 11, 10, 22, 40, 13, 12, 18, 18, 18, 40]
    validations = [
        (f"{col['topic']}2:{col['topic']}{n}", TOPICS, "topic: " + ", ".join(TOPICS)),
        (f"{col['intent']}2:{col['intent']}{n}", INTENTS, "intent: " + ", ".join(INTENTS)),
        (f"{col['severity']}2:{col['severity']}{n}", ["1", "2", "3", "4", "5"], "severity: 1 to 5"),
        (f"{col['sentiment']}2:{col['sentiment']}{n}", ["-1", "-0.5", "0", "0.5", "1"], "sentiment: -1 to 1"),
        (f"{col['needs_review']}2:{col['needs_review']}{n}", ["true", "false"], "true or false"),
        (f"{col['ambiguous']}2:{col['ambiguous']}{n}", ["true", "false"], "true or false (optional)"),
    ]
    labels = x.sheet_xml([COLUMNS] + body, widths, styles, validations=validations)
    definitions = [["field", "value", "meaning"]]
    definitions += [["intent (first match wins)", k, v] for k, v in INTENT_CRITERIA.items()]
    definitions += [["topic", k, v] for k, v in TOPIC_CRITERIA.items()]
    definitions += [["severity", k, v] for k, v in SEVERITY_CRITERIA.items()]
    definitions += [["sentiment", v, m] for v, m in (("-1", "Strongly negative"), ("-0.5", "Negative"),
                                                     ("0", "Neutral, unclear or evenly mixed"), ("0.5", "Positive"),
                                                     ("1", "Strongly positive"))]
    definitions += [
        ["evidence_quote", "(copy-paste)", "Exact substring of review_text; never retype or edit"],
        ["entities", '[] or ["ads", "premium"]', "JSON list of features explicitly mentioned; [] when none"],
        ["needs_review", "true / false", "true when the label depends on missing context or weak evidence"],
        ["ambiguous", "true / false", "Optional: true when another answer is also defensible"],
        ["accepted_topics / accepted_intents / accepted_severities", "e.g. usability|billing or 3|4",
         "Optional: other defensible answers, separated by |"],
        ["notes", "free text", "Optional reasoning, especially for ambiguous or needs_review rows"],
    ]
    instructions = [["step", "instruction"]] + [[str(i), t] for i, t in enumerate([
        "Label from the review text only. Do not look at any model prediction before you finish.",
        "Do not edit review_id, source_sha256 or review_text (shaded grey).",
        "Fill the seven required columns: topic, intent, sentiment, severity, entities, evidence_quote, needs_review.",
        "Use the dropdowns for topic, intent, severity, sentiment, needs_review and ambiguous.",
        "Copy evidence_quote from review_text (click the cell, copy from the formula bar, paste).",
        "See the Definitions sheet and docs/GOLDEN_LABELING_GUIDE.md for the rules and worked examples.",
        "Save as .xlsx (File > Save). Do not rename the Labels sheet or reorder rows or columns.",
        "When finished, run: python3 -m pipeline.golden from-xlsx, then python3 -m pipeline.golden validate "
        "--working golden/golden_50_labels_completed.csv",
    ], 1)]
    x.write_workbook(out, [
        ("Labels", labels),
        ("Definitions", x.sheet_xml(definitions, [44, 28, 90])),
        ("Instructions", x.sheet_xml(instructions, [8, 120])),
    ])
    return len(body)


def import_workbook(workbook=WORKBOOK, out=COMPLETED, overwrite=False):
    """Convert the Labels sheet back to CSV. Identifiers and text are checked by validate()."""
    from pipeline import xlsx_lite as x
    out = Path(out)
    if out.exists() and not overwrite:
        raise SystemExit(f"ERROR: {out} already exists; pass --overwrite to replace it with a fresh conversion.")
    rows = x.read_sheet(workbook, "Labels")
    if rows[0][:len(COLUMNS)] != COLUMNS:
        raise SystemExit("ERROR: the Labels sheet header changed; columns must stay in the original order.")
    tmp = out.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(COLUMNS)
        for row in rows[1:]:
            if any(cell.strip() for cell in row):
                writer.writerow([cell.strip() if i >= 3 else cell for i, cell in enumerate(row[:len(COLUMNS)])])
    tmp.replace(out)
    return len(rows) - 1


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
    parser.add_argument("command", choices=["template", "xlsx", "excel-csv", "from-xlsx", "validate"])
    parser.add_argument("--source", type=Path, default=SOURCE)
    parser.add_argument("--working", type=Path, default=WORKING)
    parser.add_argument("--workbook", type=Path, default=WORKBOOK)
    parser.add_argument("--out", type=Path, help="Output path for xlsx / excel-csv / from-xlsx")
    parser.add_argument("--overwrite", action="store_true", help="from-xlsx only: replace an earlier conversion")
    args = parser.parse_args(argv)
    if args.command == "template":
        n = create_template(args.source, args.working)
        print(f"Created blank labeling file with {n} rows: {args.working}")
        return 0
    if args.command == "xlsx":
        n = create_workbook(args.working, args.out or WORKBOOK)
        print(f"Created labeling workbook with {n} rows: {args.out or WORKBOOK}")
        return 0
    if args.command == "excel-csv":
        create_excel_csv(args.working, args.out or EXCEL_CSV)
        print(f"Created UTF-8 BOM copy for Excel: {args.out or EXCEL_CSV}")
        return 0
    if args.command == "from-xlsx":
        n = import_workbook(args.workbook, args.out or COMPLETED, args.overwrite)
        print(f"Converted {n} workbook rows to {args.out or COMPLETED}; now run validate --working on it")
        return 0
    report = validate(args.source, args.working)
    print(f"rows={report['rows']} labeled={report['labeled']} valid={report['valid']} unlabeled={report['unlabeled']}")
    for rid, issues in report["problems"].items():
        for issue in issues:
            print(f"  {rid}: {issue}")
    return 0 if not report["problems"] else 1


if __name__ == "__main__":
    sys.exit(main())
