"""Assemble the evals/ folder from saved evaluation outputs. No model calls; read-only on databases.

Usage:
    python3 -m pipeline.evals_export                      # aggregate results only
    python3 -m pipeline.evals_export --publish-golden     # also per-case human labels and predictions

Sources (whatever exists is included; missing parts are listed as not yet run):
    work/golden_eval/report.json      golden-50 comparison (real Jev predictions, pipeline.golden_eval)
    golden/golden_50_labels_completed.csv   human labels (copied only with --publish-golden)
    outputs/verify/                   independent verifier report, disagreements, planted-error test
    evals/injection/                  prompt-injection test (synthetic cases)
    outputs/checkpoints/ + database   interruption/resume evidence, checked with the grader's rules

The golden human labels are published only on request because they include the review texts and
are the answer key; the aggregate metrics never reveal per-case labels.
"""

import argparse
import csv
import json
import shutil
import sqlite3
import sys
from pathlib import Path

from pipeline.common import REPO_ROOT, atomic_write_json, atomic_write_text

EVALS = REPO_ROOT / "evals"


def _load(path):
    path = Path(path)
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def golden_section(report, out, publish):
    out.mkdir(parents=True, exist_ok=True)
    m = report["metrics"]
    summary = {"source": "pipeline.golden_eval on real typesafe-ai/jev predictions in a dedicated evaluation database",
               "label_configs": report["label_configs"], "denominator": report["denominator"],
               "coverage": report["coverage"], "rules": report["rules"],
               "ambiguous_cases": len(report["ambiguous_cases"]), "mismatching_reviews": len(report["mismatches"]),
               "metrics": {k: {kk: vv for kk, vv in v.items() if not kk.startswith("per_class") and kk != "confusion_strict"}
                           for k, v in m.items()},
               "per_class_f1_strict": {k: {c: x["f1"] for c, x in m[k]["per_class_strict"].items()}
                                       for k in ("topic", "intent", "severity")},
               "confusion_strict": {k: m[k]["confusion_strict"] for k in ("topic", "intent", "severity")}}
    atomic_write_json(out / "metrics.json", summary)
    patterns = {}
    for r in report["mismatches"]:
        for field in ("topic", "intent", "severity"):
            f = r[field]
            if f["predicted"] is not None and not f["accepted_match"]:
                key = f"{field}: human {f['human']} -> model {f['predicted']}"
                patterns[key] = patterns.get(key, 0) + 1
    atomic_write_json(out / "disagreement_patterns.json", dict(sorted(patterns.items(), key=lambda x: -x[1])))
    files = ["metrics.json", "disagreement_patterns.json"]
    if publish:
        with (out / "per_case.csv").open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(["review_id", "review_text", "ambiguous", "state", "human_topic", "accepted_topics", "model_topic",
                        "topic_ok", "human_intent", "model_intent", "intent_ok", "human_severity", "model_severity",
                        "severity_ok", "human_sentiment", "model_sentiment", "evidence_quote_model", "quote_exact",
                        "human_entities", "model_entities", "rule_adjustments", "problems"])
            for r in report["reviews"]:
                q = r.get("evidence_quote", {})
                e = r.get("entities", {})
                w.writerow([r["review_id"], r["review_text"], r["ambiguous"], r["state"], r["topic"]["human"],
                            "|".join(r["topic"]["accepted"]), r["topic"]["predicted"], r["topic"]["accepted_match"],
                            r["intent"]["human"], r["intent"]["predicted"], r["intent"]["accepted_match"],
                            r["severity"]["human"], r["severity"]["predicted"], r["severity"]["accepted_match"],
                            r["sentiment"]["human"], r["sentiment"]["predicted"], q.get("predicted"),
                            q.get("exact_substring"), "|".join(e.get("human", [])), "|".join(e.get("predicted", [])),
                            "; ".join(r.get("rule_adjustments", [])), "; ".join(r["problems"])])
        human = REPO_ROOT / "golden" / "golden_50_labels_completed.csv"
        if human.exists():
            shutil.copyfile(human, out / "golden_50_human_labels.csv")
        files += ["per_case.csv", "golden_50_human_labels.csv"]
    return files


def resume_section(db_path, checkpoint_dir, out):
    before = _load(Path(checkpoint_dir) / "checkpoint_before.json")
    after = _load(Path(checkpoint_dir) / "checkpoint_after.json")
    if not (before and after and Path(db_path).exists()):
        return None
    conn = sqlite3.connect(f"{Path(db_path).resolve().as_uri()}?mode=ro", uri=True)
    try:
        cached = {r for (r,) in conn.execute("SELECT review_id FROM labels WHERE cache_source_id IS NOT NULL")}
        calls = [(p, o, json.loads(ids)) for p, o, ids in conn.execute("SELECT phase, outcome, review_ids FROM calls")]
    finally:
        conn.close()
    b, a = set(before["completed_ids"]), set(after["completed_ids"])
    initial = {r for p, o, ids in calls if p == "initial" and o == "succeeded" for r in ids}
    resume_ok = {r for p, o, ids in calls if p == "resume" and o == "succeeded" for r in ids}
    resent = {r for p, _, ids in calls if p == "resume" for r in ids} & b
    result = {"checkpoint_before": len(b), "checkpoint_after": len(a), "new_after_resume": len(a - b),
              "new_via_resume_calls": len((a - b - cached) & resume_ok), "new_via_cache": len((a - b) & cached),
              "before_subset_of_after": b < a, "resume_calls_resending_checkpointed_ids": len(resent),
              "before_ids_without_initial_call_or_cache": len(b - cached - initial),
              "checks_pass": b < a and not resent and not (b - cached - initial) and bool((a - b - cached) & resume_ok),
              "taken_at": {"before": before.get("taken_at"), "after": after.get("taken_at")},
              "rules": "Same conditions as check_submission.py (resume_snapshot_mismatch, reprocessed_checkpoint, "
                       "resume_call_evidence)."}
    out.mkdir(parents=True, exist_ok=True)
    atomic_write_json(out / "resume_evidence.json", result)
    return result


def build(out=EVALS, publish_golden=False, golden_report=REPO_ROOT / "work" / "golden_eval" / "report.json",
          verify_dir=REPO_ROOT / "outputs" / "verify", db_path=REPO_ROOT / "work" / "pipeline.sqlite",
          checkpoint_dir=REPO_ROOT / "outputs" / "checkpoints"):
    out = Path(out)
    status = {}
    report = _load(golden_report)
    status["golden"] = golden_section(report, out / "golden", publish_golden) if report else "not yet run"
    copied = []
    for name in ("report.json", "report.md", "disagreements.csv", "planted_report.json", "sample.json"):
        if (Path(verify_dir) / name).exists():
            (out / "verification").mkdir(parents=True, exist_ok=True)
            shutil.copyfile(Path(verify_dir) / name, out / "verification" / name)
            copied.append(name)
    status["verification"] = copied or "not yet run (paid verifier run pending approval)"
    status["injection"] = "present" if (out / "injection" / "injection_report.json").exists() else \
        "not yet run (synthetic cases ready in evals/injection_cases.csv; paid run pending approval)"
    status["resume"] = resume_section(db_path, checkpoint_dir, out / "system") or "not available"
    status["offline_tests"] = ("python3 -m unittest discover -s tests (synthetic fixtures and mock models only; "
                               "see tests/test_resilience.py for simulated network outages)")
    atomic_write_json(out / "index.json", status)
    return status


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", type=Path, default=EVALS)
    parser.add_argument("--publish-golden", action="store_true",
                        help="Also write per-case human labels and predictions (answer key + review texts)")
    args = parser.parse_args(argv)
    print(json.dumps(build(args.out, args.publish_golden), indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
