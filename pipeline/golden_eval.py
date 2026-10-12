"""Golden-50 evaluation: compare saved pipeline predictions with the human reference labels.

Usage:
    python3 -m pipeline.golden_eval make-input [--source FILE] [--out FILE]
        Write the six source columns of the golden set (no label columns) as a CSV that
        pipeline.ingest accepts. Refuses to overwrite.
    python3 -m pipeline.golden_eval evaluate --db FILE [--human FILE] [--out-dir DIR] [--overwrite]
        Score the labels saved in an evaluation database against the human labels.

Separation of model input and answers:
  - Predictions are produced earlier by pipeline.enrich from the review text only. This module
    never calls a model and opens the prediction database read-only.
  - Human labels are read only here, after predictions exist, and never written anywhere a
    classifier reads from.

Scoring rules (fixed before the first real evaluation):
  - Every golden review stays in every denominator. A review whose prediction is missing,
    pending, quarantined or attached to a different source_sha256 counts as incorrect.
  - Strict: only the primary human label is correct. Ambiguity-aware: the primary label or any
    value in accepted_topics / accepted_intents / accepted_severities is correct. For the
    ambiguity-aware macro F1, an accepted alternative is credited as the primary label.
  - Macro F1 averages per-class F1 over classes present in the human labels or the predictions.
    A missing prediction is a false negative for its human class and a false positive for no class.
  - Sentiment is correct when |predicted - human| <= SENTIMENT_TOLERANCE (0.5).
  - Entities are compared after lowercasing and trimming surrounding whitespace only;
    singular/plural forms and synonyms are not merged.
  - MAE values are averaged over reviews that have a prediction; the count is reported with them.
"""

import argparse
import csv
import json
import sqlite3
import sys
from collections import Counter
from pathlib import Path

from pipeline.common import FIELDS, INTENTS, REPO_ROOT, TOPICS, atomic_write_json, atomic_write_text, csv_rows, row_sha
from pipeline.golden import COMPLETED, SOURCE, validate

SENTIMENT_TOLERANCE = 0.5
SEVERITY_CLASSES = ("1", "2", "3", "4", "5")
MISSING = "(missing)"
DEFAULT_INPUT = REPO_ROOT / "work" / "golden_eval_input.csv"
DEFAULT_OUT_DIR = REPO_ROOT / "work" / "golden_eval"  # git-ignored: contains human labels and review text
EPS = 1e-9


class EvalError(Exception):
    pass


def make_input(source=SOURCE, out=DEFAULT_INPUT):
    """Write only the six source fields so the human label columns can never reach ingestion."""
    out = Path(out)
    if out.exists():
        raise EvalError(f"{out} already exists; refusing to overwrite")
    rows = list(csv_rows(source))
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("x", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(FIELDS)
        for row in rows:
            writer.writerow([row[k] for k in FIELDS])
    written = list(csv_rows(out))
    if [row_sha(r) for r in written] != [row_sha(r) for r in rows]:
        out.unlink()
        raise EvalError("source_sha256 changed while writing the evaluation input")
    return len(rows)


def _split(value):
    return [x.strip() for x in (value or "").split("|") if x.strip()]


def normalize_entities(values):
    return {v.strip().lower() for v in values if isinstance(v, str) and v.strip()}


def load_human(source=SOURCE, human=COMPLETED):
    """Validated human labels keyed by review_id, in golden order. Refuses an incomplete file."""
    report = validate(source, human)
    if report["problems"] or report["unlabeled"] or report["valid"] != report["rows"]:
        raise EvalError(f"human labels are not fully valid: {report}")
    rows = {}
    for row in csv_rows(human):
        rows[row["review_id"]] = {
            "review_id": row["review_id"], "source_sha256": row["source_sha256"], "review_text": row["review_text"],
            "topic": row["topic"], "intent": row["intent"], "severity": int(row["severity"]),
            "sentiment": float(row["sentiment"]), "entities": json.loads(row["entities"] or "[]"),
            "evidence_quote": row["evidence_quote"], "needs_review": row["needs_review"].strip().lower() == "true",
            "ambiguous": row["ambiguous"].strip().lower() == "true",
            "accepted_topics": _split(row["accepted_topics"]), "accepted_intents": _split(row["accepted_intents"]),
            "accepted_severities": [int(x) for x in _split(row["accepted_severities"])]}
    return rows


def load_predictions(db_path):
    """Read reviews and labels from an evaluation database without any write access."""
    db_path = Path(db_path)
    if not db_path.exists():
        raise EvalError(f"{db_path} does not exist")
    conn = sqlite3.connect(f"{db_path.resolve().as_uri()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT r.review_id, r.source_sha256, r.status, r.reason, l.label_config, l.topic, l.intent,"
            " l.sentiment, l.severity, l.entities, l.evidence_quote, l.needs_review, l.cache_source_id, l.details"
            " FROM reviews r LEFT JOIN labels l ON l.review_id = r.review_id").fetchall()
    finally:
        conn.close()
    out = {}
    for r in rows:
        pred = {"source_sha256": r["source_sha256"], "status": r["status"], "reason": r["reason"]}
        if r["label_config"] is not None:
            details = json.loads(r["details"] or "{}")
            pred.update(label_config=r["label_config"], topic=r["topic"], intent=r["intent"],
                        sentiment=r["sentiment"], severity=r["severity"], entities=json.loads(r["entities"]),
                        evidence_quote=r["evidence_quote"], needs_review=bool(r["needs_review"]),
                        cache_source_id=r["cache_source_id"], adjustments=details.get("adjustments", []))
        out[r["review_id"]] = pred
    return out


def _state(human, pred):
    """'completed' only when a label exists for the same source row; otherwise why it is missing."""
    if pred is None:
        return "missing"
    if pred["source_sha256"] != human["source_sha256"]:
        return "sha_mismatch"
    if pred["status"] == "quarantined":
        return "quarantined"
    if pred["status"] != "completed" or pred.get("label_config") is None:
        return "pending" if pred["status"] == "pending" else "missing"
    return "completed"


def macro_f1(pairs, classes):
    """pairs: (human, predicted-or-MISSING). Average F1 over classes seen in human or predicted values."""
    present = [c for c in classes if any(h == c or p == c for h, p in pairs)]
    per_class = {}
    for c in present:
        tp = sum(1 for h, p in pairs if h == c and p == c)
        fp = sum(1 for h, p in pairs if h != c and p == c)
        fn = sum(1 for h, p in pairs if h == c and p != c)
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / (tp + fn) if tp + fn else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        per_class[c] = {"precision": round(precision, 4), "recall": round(recall, 4), "f1": round(f1, 4),
                        "support": tp + fn}
    value = sum(v["f1"] for v in per_class.values()) / len(per_class) if per_class else 0.0
    return round(value, 4), per_class


def confusion(pairs, classes):
    columns = list(classes) + [MISSING]
    matrix = {h: {p: 0 for p in columns} for h in classes}
    for h, p in pairs:
        matrix[h][p if p in columns else MISSING] += 1
    return {"rows_human_columns_predicted": columns,
            "matrix": {h: row for h, row in matrix.items() if any(row.values())}}


def _rate(n, d):
    return round(n / d, 4) if d else 0.0


def _span(text, quote):
    start = text.find(quote) if quote else -1
    return (start, start + len(quote)) if start >= 0 else None


def evaluate(human, predictions, tolerance=SENTIMENT_TOLERANCE):
    """human: from load_human. predictions: review_id -> dict as from load_predictions."""
    n = len(human)
    states = Counter()
    reviews = []
    pairs = {"topic": [], "intent": [], "severity": []}
    lenient_pairs = {"topic": [], "intent": [], "severity": []}
    counts = Counter()
    sev_abs, sev_signed, sent_abs = [], [], []
    ent_tp = ent_pred = ent_human = 0
    configs = Counter()
    for rid, h in human.items():
        pred = predictions.get(rid)
        state = _state(h, pred)
        states[state] += 1
        ok = state == "completed"
        p = pred if ok else {}
        if ok:
            configs[p["label_config"]] += 1
        accepted = {"topic": [h["topic"]] + h["accepted_topics"], "intent": [h["intent"]] + h["accepted_intents"],
                    "severity": [h["severity"]] + h["accepted_severities"]}
        row = {"review_id": rid, "state": state, "ambiguous": h["ambiguous"], "review_text": h["review_text"]}
        if state in ("quarantined", "pending", "sha_mismatch"):
            row["reason"] = pred.get("reason") if state != "sha_mismatch" else "prediction source_sha256 differs"
        problems = [] if ok else [state]
        for field in ("topic", "intent", "severity"):
            value = p.get(field) if ok else None
            strict = ok and value == h[field]
            lenient = ok and value in accepted[field]
            counts[f"{field}_strict"] += strict
            counts[f"{field}_lenient"] += lenient
            shown = str(value) if ok else MISSING
            pairs[field].append((str(h[field]), shown))
            lenient_pairs[field].append((str(h[field]), str(h[field]) if lenient else shown))
            row[field] = {"human": h[field], "accepted": accepted[field][1:], "predicted": value,
                          "strict": strict, "accepted_match": lenient}
            if ok and not lenient:
                problems.append(field)
            elif ok and not strict:
                problems.append(f"{field}(accepted alternative)")
        if ok:
            counts["severity_within_1"] += abs(p["severity"] - h["severity"]) <= 1
            sev_abs.append(abs(p["severity"] - h["severity"]))
            sev_signed.append(p["severity"] - h["severity"])
            diff = abs(p["sentiment"] - h["sentiment"])
            sent_abs.append(diff)
            within = diff <= tolerance + EPS
            counts["sentiment_within"] += within
            if not within:
                problems.append("sentiment")
            row["sentiment"] = {"human": h["sentiment"], "predicted": p["sentiment"], "abs_error": round(diff, 4)}
            quote = p["evidence_quote"] or ""
            valid_quote = bool(quote.strip()) and quote in h["review_text"]
            counts["quote_valid"] += valid_quote
            hs, ps = _span(h["review_text"], h["evidence_quote"]), _span(h["review_text"], quote)
            overlap = bool(hs and ps and min(hs[1], ps[1]) > max(hs[0], ps[0]))
            counts["quote_overlap"] += overlap
            row["evidence_quote"] = {"human": h["evidence_quote"], "predicted": quote,
                                     "exact_substring": valid_quote, "overlaps_human": overlap}
            he, pe = normalize_entities(h["entities"]), normalize_entities(p["entities"])
            ent_tp += len(he & pe)
            ent_pred += len(pe)
            ent_human += len(he)
            counts["entities_exact"] += he == pe
            row["entities"] = {"human": sorted(he), "predicted": sorted(pe)}
            agree = p["needs_review"] == h["needs_review"]
            counts["needs_review_agree"] += agree
            row["needs_review"] = {"human": h["needs_review"], "predicted": p["needs_review"]}
            row["rule_adjustments"] = p.get("adjustments", [])
        else:
            ent_human += len(normalize_entities(h["entities"]))
            row["sentiment"] = {"human": h["sentiment"], "predicted": None, "abs_error": None}
        row["problems"] = problems
        reviews.append(row)

    predicted = states["completed"]
    ent_precision = _rate(ent_tp, ent_pred)
    ent_recall = _rate(ent_tp, ent_human)
    metrics = {}
    for field, classes in (("topic", TOPICS), ("intent", INTENTS), ("severity", SEVERITY_CLASSES)):
        strict_f1, strict_classes = macro_f1(pairs[field], classes)
        lenient_f1, lenient_classes = macro_f1(lenient_pairs[field], classes)
        metrics[field] = {
            "accuracy_strict": _rate(counts[f"{field}_strict"], n),
            "accuracy_accepted": _rate(counts[f"{field}_lenient"], n),
            "macro_f1_strict": strict_f1, "macro_f1_accepted": lenient_f1,
            "per_class_strict": strict_classes, "per_class_accepted": lenient_classes,
            "confusion_strict": confusion(pairs[field], classes)}
    metrics["severity"].update(
        within_1=_rate(counts["severity_within_1"], n),
        mae=round(sum(sev_abs) / predicted, 4) if predicted else None,
        mean_signed_error=round(sum(sev_signed) / predicted, 4) if predicted else None,
        error_distribution={str(k): v for k, v in sorted(Counter(sev_signed).items())})
    metrics["sentiment"] = {"tolerance": tolerance, "within_tolerance": _rate(counts["sentiment_within"], n),
                            "mae": round(sum(sent_abs) / predicted, 4) if predicted else None}
    metrics["evidence_quote"] = {"exact_substring": _rate(counts["quote_valid"], n),
                                 "overlaps_human_quote": _rate(counts["quote_overlap"], n)}
    metrics["entities"] = {
        "normalization": "lowercase and trim only", "precision": ent_precision, "recall": ent_recall,
        "f1": round(2 * ent_precision * ent_recall / (ent_precision + ent_recall), 4) if ent_precision + ent_recall else 0.0,
        "exact_set_match": _rate(counts["entities_exact"], n),
        "true_positive": ent_tp, "predicted": ent_pred, "human": ent_human}
    metrics["needs_review"] = {"agreement": _rate(counts["needs_review_agree"], n)}
    ambiguous = [r for r in reviews if r["ambiguous"]]
    return {
        "golden_reviews": n,
        "denominator": n,
        "coverage": {"predicted": predicted, "missing": states["missing"], "pending": states["pending"],
                     "quarantined": states["quarantined"], "sha_mismatch": states["sha_mismatch"]},
        "label_configs": dict(configs),
        "rules": {"sentiment_tolerance": tolerance, "entity_normalization": "lowercase and trim only",
                  "missing_predictions": "counted as incorrect in every rate; excluded only from MAE averages",
                  "accepted_alternatives": "accepted_* values count as correct in *_accepted metrics only"},
        "metrics": metrics,
        "ambiguous_cases": [{"review_id": r["review_id"], "state": r["state"],
                             "topic": r["topic"]} for r in ambiguous],
        "mismatches": [r for r in reviews if r["problems"]],
        "reviews": reviews,
    }


def _pct(x):
    return "n/a" if x is None else f"{100 * x:.1f}%"


def render_markdown(report, db_label):
    m = report["metrics"]
    c = report["coverage"]
    lines = [
        "# Golden-50 evaluation", "",
        f"Predictions: `{db_label}`; label_config: {', '.join(f'`{k}` ({v})' for k, v in report['label_configs'].items()) or 'none'}.",
        f"All {report['denominator']} golden reviews are in every denominator.", "",
        "## Coverage", "",
        "| Predicted | Missing | Pending | Quarantined | SHA mismatch |", "|---|---|---|---|---|",
        f"| {c['predicted']} | {c['missing']} | {c['pending']} | {c['quarantined']} | {c['sha_mismatch']} |", "",
        "## Classification", "",
        "| Field | Accuracy (strict) | Accuracy (accepted alternatives) | Macro F1 (strict) | Macro F1 (accepted) |",
        "|---|---|---|---|---|"]
    for field in ("topic", "intent", "severity"):
        f = m[field]
        lines.append(f"| {field} | {_pct(f['accuracy_strict'])} | {_pct(f['accuracy_accepted'])} | "
                     f"{f['macro_f1_strict']:.4f} | {f['macro_f1_accepted']:.4f} |")
    s = m["severity"]
    lines += ["", "## Severity and sentiment errors", "",
              f"- Severity within +/-1: {_pct(s['within_1'])}; MAE {s['mae']} and mean signed error "
              f"{s['mean_signed_error']} over {c['predicted']} predicted reviews (predicted minus human).",
              f"- Severity error distribution: {s['error_distribution']}",
              f"- Sentiment within +/-{m['sentiment']['tolerance']}: {_pct(m['sentiment']['within_tolerance'])}; "
              f"MAE {m['sentiment']['mae']}.", "",
              "## Evidence, entities and review flags", "",
              f"- Evidence quote is an exact substring: {_pct(m['evidence_quote']['exact_substring'])}; "
              f"overlaps the human quote: {_pct(m['evidence_quote']['overlaps_human_quote'])}.",
              f"- Entities ({m['entities']['normalization']}): precision {m['entities']['precision']}, recall "
              f"{m['entities']['recall']}, F1 {m['entities']['f1']}, exact set match "
              f"{_pct(m['entities']['exact_set_match'])}.",
              f"- needs_review agreement: {_pct(m['needs_review']['agreement'])}.", ""]
    for field in ("topic", "intent", "severity"):
        conf = m[field]["confusion_strict"]
        cols = conf["rows_human_columns_predicted"]
        lines += [f"## Confusion matrix: {field} (rows human, columns predicted, strict)", "",
                  "| human \\ predicted | " + " | ".join(cols) + " |", "|---" * (len(cols) + 1) + "|"]
        for h, row in conf["matrix"].items():
            lines.append(f"| {h} | " + " | ".join(str(row[k]) for k in cols) + " |")
        lines.append("")
    lines += ["## Ambiguous cases", "", "| review_id | state | human topic | accepted | predicted | strict | accepted match |",
              "|---|---|---|---|---|---|---|"]
    for a in report["ambiguous_cases"]:
        t = a["topic"]
        lines.append(f"| {a['review_id']} | {a['state']} | {t['human']} | {'/'.join(t['accepted'])} | "
                     f"{t['predicted']} | {t['strict']} | {t['accepted_match']} |")
    lines += ["", f"## Per-review mismatches ({len(report['mismatches'])})", "",
              "| review_id | state | problems | topic h/p | intent h/p | severity h/p | sentiment h/p |",
              "|---|---|---|---|---|---|---|"]
    for r in report["mismatches"]:
        lines.append(f"| {r['review_id']} | {r['state']} | {', '.join(r['problems'])} | "
                     f"{r['topic']['human']}/{r['topic']['predicted']} | {r['intent']['human']}/{r['intent']['predicted']} | "
                     f"{r['severity']['human']}/{r['severity']['predicted']} | "
                     f"{r['sentiment']['human']}/{r['sentiment']['predicted']} |")
    return "\n".join(lines) + "\n"


def run(db_path, human=COMPLETED, source=SOURCE, out_dir=DEFAULT_OUT_DIR, overwrite=False):
    out_dir = Path(out_dir)
    targets = [out_dir / "report.json", out_dir / "report.md"]
    if not overwrite and any(t.exists() for t in targets):
        raise EvalError(f"{out_dir} already has a report; pass --overwrite to replace it")
    report = evaluate(load_human(source, human), load_predictions(db_path))
    if any(cfg.startswith("mock/") for cfg in report["label_configs"]):
        report["warning"] = "mock labels: synthetic test predictions, not a real evaluation"
    atomic_write_json(targets[0], report)
    atomic_write_text(targets[1], render_markdown(report, Path(db_path).name))
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["make-input", "evaluate"])
    parser.add_argument("--source", type=Path, default=SOURCE)
    parser.add_argument("--out", type=Path, default=DEFAULT_INPUT, help="make-input: output CSV")
    parser.add_argument("--db", type=Path, help="evaluate: evaluation database with saved predictions")
    parser.add_argument("--human", type=Path, default=COMPLETED)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--overwrite", action="store_true", help="evaluate: replace an earlier report")
    args = parser.parse_args(argv)
    try:
        if args.command == "make-input":
            print(f"Wrote {make_input(args.source, args.out)} rows (source fields only) to {args.out}")
            return 0
        if not args.db:
            parser.error("evaluate needs --db")
        report = run(args.db, args.human, args.source, args.out_dir, args.overwrite)
    except EvalError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    m = report["metrics"]
    print(f"predicted={report['coverage']['predicted']}/{report['denominator']} "
          f"topic={m['topic']['accuracy_strict']}/{m['topic']['accuracy_accepted']} (strict/accepted) "
          f"intent={m['intent']['accuracy_strict']} severity={m['severity']['accuracy_strict']} "
          f"mismatches={len(report['mismatches'])} -> {args.out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
