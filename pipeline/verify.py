"""Stage 3 (Verify): an independent model task re-labels a declared random sample.

Usage:
    # Paid (explicit --execute): re-label a declared seeded sample of completed records
    python3 -m pipeline.verify run --db work/pipeline.sqlite --size 500 --sample analysis_10000 \
        --budget-usd 0.40 --out outputs/verify --execute
    # Offline: recompute agreement and the disagreement report from saved files
    python3 -m pipeline.verify report --db work/pipeline.sqlite --out outputs/verify
    # Offline: planted-error test on a separate synthetic test copy
    python3 -m pipeline.verify planted --db work/pipeline.sqlite --out outputs/verify
    # Paid (explicit --execute): prompt-injection cases (synthetic texts, never part of the dataset)
    python3 -m pipeline.verify injection --cases evals/injection_cases.csv --out evals/injection \
        --budget-usd 0.40 --execute

Independence: the verifier receives only the review text and its own separately written
instructions (VERIFY_PROMPT_VERSION). It never sees the first prediction; code compares the two
afterwards. The shared label definitions are the same, as the grading contract requires.
The verifier's results never change the enrichment labels; disagreements are reported for review.

Outputs (in --out): sample.json (declared sample: seed, size, scope, IDs), predictions.jsonl,
calls.jsonl (role "verify"), report.json, report.md, disagreements.csv, planted_report.json.
The production database is opened read-only.
"""

import argparse
import csv
import hashlib
import json
import sqlite3
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from pipeline.common import INTENTS, REPO_ROOT, TOPICS, atomic_write_json, atomic_write_text
from pipeline.labels import (INTENT_CRITERIA, SCHEMA_VERSION, SENTIMENT_LEVELS, SEVERITY_CRITERIA, TOPIC_CRITERIA,
                             apply_consistency)
from pipeline.ledger import DEFAULT_LEDGER, SpendLedger
from pipeline.textmodel import append_call, call_record

VERIFY_PROMPT_VERSION = "v1"
VERIFY_SEED = "berkeley-fall-2026-assignment-5-verify-v1"
MAX_ATTEMPTS = 5
FIELDS = ("topic", "intent", "severity")

# Separately written instructions; the label definitions (criteria) are the shared contract ones.
VERIFY_QUESTIONS = {
    "topic": {"type": "choice",
              "instructions": "You are auditing app-review labels. Read only the review text. Which single product "
                              "area does the most serious problem described in it belong to? If two are equally "
                              "serious, take the one the reviewer mentions first. If the review only praises, take "
                              "the first concrete feature praised; plain general praise is other.",
              "criteria": TOPIC_CRITERIA},
    "intent": {"type": "choice",
               "instructions": "Audit the reviewer's purpose. Check in this order and take the first that applies: "
                               "leaving or threatening to leave; reporting a bad experience; asking for a change "
                               "without reporting a failure; only positive; otherwise unclear.",
               "criteria": INTENT_CRITERIA},
    "severity": {"type": "choice",
                 "instructions": "Audit the impact actually described. Ignore star ratings, tone and threats to "
                                 "leave; judge only what stopped working and how badly.",
                 "criteria": SEVERITY_CRITERIA},
    "sentiment": {"type": "score", "instructions": "Overall tone of the review text.", "criteria": SENTIMENT_LEVELS},
}


def verify_config(model):
    return f"{model}+verify-{VERIFY_PROMPT_VERSION}+{SCHEMA_VERSION}"


def _now():
    return datetime.now(timezone.utc).isoformat()


def _key(review_id, seed=VERIFY_SEED):
    return hashlib.sha256(f"{seed}:{review_id}".encode()).digest()


def _ro(db_path):
    conn = sqlite3.connect(f"{Path(db_path).resolve().as_uri()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def first_labels(conn, ids):
    rows = {}
    for i in range(0, len(ids), 500):
        chunk = ids[i:i + 500]
        for r in conn.execute("SELECT r.review_id, r.review_text, l.topic, l.intent, l.severity, l.sentiment,"
                              " l.label_config FROM reviews r JOIN labels l USING (review_id) WHERE r.review_id IN"
                              f" ({','.join('?' * len(chunk))})", chunk):
            rows[r["review_id"]] = dict(r)
    return rows


def select_sample(conn, size, sample=None, seed=VERIFY_SEED, include_mock=False):
    """Seeded random sample of completed original records (one per distinct text), optionally within a sample."""
    query = ("SELECT l.review_id FROM labels l JOIN reviews r USING (review_id) WHERE r.status = 'completed'"
             " AND l.cache_source_id IS NULL" + ("" if include_mock else " AND l.label_config NOT LIKE 'mock/%'"))
    params = ()
    if sample:
        query += " AND l.review_id IN (SELECT review_id FROM sample_membership WHERE sample = ?)"
        params = (sample,)
    ids = sorted((rid for (rid,) in conn.execute(query, params)), key=lambda rid: _key(rid, seed))
    return ids[:size]


def load_jsonl(path):
    path = Path(path)
    return [json.loads(x) for x in path.read_text(encoding="utf-8").splitlines() if x.strip()] if path.exists() else []


def declare_sample(db_path, out_dir, size, sample=None, include_mock=False):
    """Write sample.json once; later runs reuse the declared sample unchanged."""
    path = Path(out_dir) / "sample.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    conn = _ro(db_path)
    try:
        ids = select_sample(conn, size, sample, include_mock=include_mock)
        source = conn.execute("SELECT value FROM meta WHERE key = 'source_sha256'").fetchone()[0]
    finally:
        conn.close()
    declared = {"seed": VERIFY_SEED, "size": len(ids), "requested_size": size, "scope": sample or "all completed",
                "population": "completed original records (cache copies excluded, one per distinct text)",
                "selection": "lowest SHA-256(seed:review_id)", "source_sha256": source, "review_ids": ids,
                "declared_at": _now(), "verify_prompt_version": VERIFY_PROMPT_VERSION}
    atomic_write_json(path, declared)
    return declared


def to_prediction(provider, raw):
    topic, intent, severity, sentiment, confidence, _ = provider.to_label(raw)
    problems = []
    if topic not in TOPICS:
        problems.append("invalid topic")
    if intent not in INTENTS:
        problems.append("invalid intent")
    if severity not in (1, 2, 3, 4, 5):
        problems.append("invalid severity")
    if sentiment is None:
        problems.append("invalid sentiment")
    if problems:
        return None, problems
    severity, adjustments = apply_consistency(topic, intent, severity)  # same deterministic rules as enrichment
    return {"topic": topic, "intent": intent, "severity": severity, "sentiment": sentiment,
            "confidence": confidence, "adjustments": adjustments}, []


def run(db_path, out_dir, provider, ledger, size, sample=None, rps=2.0, sleep=time.sleep, run_id=None,
        include_mock=False):
    """Re-label the declared sample; resumable (IDs already predicted are skipped)."""
    out_dir = Path(out_dir)
    declared = declare_sample(db_path, out_dir, size, sample, include_mock)
    done = {p["review_id"] for p in load_jsonl(out_dir / "predictions.jsonl")}
    conn = _ro(db_path)
    try:
        texts = {rid: row["review_text"] for rid, row in first_labels(conn, declared["review_ids"]).items()}
    finally:
        conn.close()
    config = verify_config(provider.model)
    run_id = run_id or f"verify-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    summary = Counter()
    stop = "complete"
    last = 0.0
    for rid in declared["review_ids"]:
        if rid in done:
            continue
        attempt, invalid = 0, 0
        while True:
            attempt += 1
            wait = last + 1.0 / rps - time.monotonic()
            if wait > 0:
                sleep(wait)
            reservation = provider.estimate_cost(provider.estimate_input_tokens([(rid, texts[rid])]))
            token = ledger.reserve(reservation)
            if token is None:
                stop = "budget_cap"
                break
            last = time.monotonic()
            started = _now()
            response = provider.classify([(rid, texts[rid])])
            if response.cost_usd is not None:
                cost, source = str(response.cost_usd), "provider_reported"
            elif response.input_tokens is not None:
                cost, source = format(provider.estimate_cost(response.input_tokens, response.output_tokens), "f"), "estimated"
            elif response.outcome_unknown:
                cost, source = None, "unknown"
            else:
                cost, source = "0", "not_sent" if response.error_kind == "network_not_sent" else "none"
            ledger.settle(token, {"request_id": response.request_id, "run_id": run_id, "role": "verify",
                                  "db": "verify", "cost_usd": cost, "cost_source": source})
            append_call(out_dir / "calls.jsonl", call_record(
                response, role="verify", review_ids=[rid], label_config=config, run_id=run_id,
                provider=provider.name, cost=cost, cost_source=source, attempt=attempt, started_at=started))
            summary["requests"] += 1
            if response.ok:
                prediction, problems = to_prediction(provider, response.items.get(rid) or {})
                if prediction:
                    append_call(out_dir / "predictions.jsonl", {"review_id": rid, "label_config": config,
                                                               "request_id": response.request_id, **prediction})
                    summary["predicted"] += 1
                    break
                invalid += 1
                summary["invalid_outputs"] += 1
                if invalid >= 2:  # retry invalid output once, then record it as unresolved
                    append_call(out_dir / "predictions.jsonl", {"review_id": rid, "label_config": config,
                                                               "unresolved": "; ".join(problems)})
                    break
                continue
            summary["failed_requests"] += 1
            if response.fatal or attempt >= MAX_ATTEMPTS:
                stop = "fatal_provider_error" if response.fatal else "repeated_failures"
                break
            sleep(min(5 * 2 ** (attempt - 1), 120))  # network or API error: bounded backoff, then retry
        if stop != "complete":
            break
    return {"run_id": run_id, "stop_reason": stop, **summary, "declared_size": declared["size"]}


def _kappa(pairs):
    n = len(pairs)
    if not n:
        return None
    observed = sum(a == b for a, b in pairs) / n
    left, right = Counter(a for a, _ in pairs), Counter(b for _, b in pairs)
    expected = sum(left[k] * right.get(k, 0) for k in left) / (n * n)
    return round((observed - expected) / (1 - expected), 4) if expected < 1 else None


def compare(first, predictions, planted=None):
    """first: review_id -> first label; predictions: list of verifier rows. Returns report dict."""
    by_id = {p["review_id"]: p for p in predictions}
    rows, pairs = [], {f: [] for f in FIELDS}
    unresolved = 0
    for rid, base in first.items():
        pred = by_id.get(rid)
        if pred is None or "unresolved" in pred:
            unresolved += pred is not None
            continue
        diff = [f for f in FIELDS if pred[f] != base[f]]
        for f in FIELDS:
            pairs[f].append((str(base[f]), str(pred[f])))
        rows.append({"review_id": rid, "disagree": diff, "first": {f: base[f] for f in FIELDS},
                     "verifier": {f: pred[f] for f in FIELDS}, "severity_gap": pred["severity"] - base["severity"],
                     "sentiment_gap": round(pred["sentiment"] - base["sentiment"], 4) if base.get("sentiment") is not None else None,
                     "planted": bool(planted and rid in planted)})
    n = len(rows)
    report = {"declared": len(first), "compared": n, "missing": len(first) - n - unresolved, "unresolved": unresolved}
    for f in FIELDS:
        agree = sum(1 for r in rows if f not in r["disagree"])
        report[f] = {"agreement": round(agree / n, 4) if n else None, "kappa": _kappa(pairs[f]),
                     "confusion_first_vs_verifier": dict(Counter(f"{a}->{b}" for a, b in pairs[f] if a != b).most_common(15))}
    report["severity"]["within_1"] = round(sum(abs(r["severity_gap"]) <= 1 for r in rows) / n, 4) if n else None
    report["severity"]["mean_abs_gap"] = round(sum(abs(r["severity_gap"]) for r in rows) / n, 4) if n else None
    report["all_three_agree"] = round(sum(not r["disagree"] for r in rows) / n, 4) if n else None
    report["disagreements"] = [r for r in rows if r["disagree"]]
    return report


def _first_for(db_path, ids):
    conn = _ro(db_path)
    try:
        return first_labels(conn, ids)
    finally:
        conn.close()


def write_report(db_path, out_dir):
    out_dir = Path(out_dir)
    declared = json.loads((out_dir / "sample.json").read_text(encoding="utf-8"))
    first = _first_for(db_path, declared["review_ids"])
    predictions = load_jsonl(out_dir / "predictions.jsonl")
    report = compare(first, predictions)
    report["declared_sample"] = {k: declared[k] for k in ("seed", "size", "scope", "population", "selection")}
    report["verify_label_config"] = sorted({p["label_config"] for p in predictions})
    synthetic = any(c.startswith("mock/") for c in report["verify_label_config"])
    report["synthetic"] = synthetic
    atomic_write_json(out_dir / "report.json", report)
    with (out_dir / "disagreements.csv").open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["review_id", "fields", "first_topic", "verifier_topic", "first_intent", "verifier_intent",
                    "first_severity", "verifier_severity", "evidence_text"])
        for r in report["disagreements"]:
            w.writerow([r["review_id"], "|".join(r["disagree"]), r["first"]["topic"], r["verifier"]["topic"],
                        r["first"]["intent"], r["verifier"]["intent"], r["first"]["severity"],
                        r["verifier"]["severity"], first[r["review_id"]]["review_text"][:200]])
    lines = ["# Independent verification report", "",
             ("**SYNTHETIC TEST RUN (mock verifier) - not a real result.**\n" if synthetic else ""),
             f"Declared sample: {report['declared_sample']['size']} records, seed `{declared['seed']}`, scope "
             f"{declared['scope']} ({declared['population']}).",
             f"Compared {report['compared']}; missing {report['missing']}; unresolved {report['unresolved']}.", "",
             "| Field | Agreement | Cohen's kappa |", "|---|---|---|"]
    for fld in FIELDS:
        lines.append(f"| {fld} | {report[fld]['agreement']} | {report[fld]['kappa']} |")
    lines += ["", f"Severity within +/-1: {report['severity']['within_1']}; mean absolute gap "
              f"{report['severity']['mean_abs_gap']}. All three fields agree: {report['all_three_agree']}.", "",
              f"Disagreements: {len(report['disagreements'])} (see `disagreements.csv`). The verifier never "
              "overwrites enrichment labels; disagreements are inputs for human inspection.", ""]
    atomic_write_text(out_dir / "report.md", "\n".join(lines))
    return report


def planted_test(db_path, out_dir, count=10):
    """Separate test copy: plant deliberately wrong first labels on records the verifier agreed with,
    then check that the comparison flags every planted record. Never touches real labels."""
    out_dir = Path(out_dir)
    declared = json.loads((out_dir / "sample.json").read_text(encoding="utf-8"))
    predictions = [p for p in load_jsonl(out_dir / "predictions.jsonl") if "unresolved" not in p]
    first = _first_for(db_path, declared["review_ids"])
    agreeing = [p["review_id"] for p in predictions
                if p["review_id"] in first and all(p[f] == first[p["review_id"]][f] for f in FIELDS)]
    chosen = sorted(agreeing, key=lambda rid: _key(rid, VERIFY_SEED + ":planted"))[:count]
    copy = {rid: dict(first[rid]) for rid in first}
    planted = {}
    for rid in chosen:
        original = copy[rid]["topic"]
        wrong = TOPICS[(TOPICS.index(original) + 3) % len(TOPICS)]
        copy[rid]["topic"] = wrong
        planted[rid] = {"field": "topic", "real": original, "planted": wrong}
    report = compare(copy, predictions, planted)
    flagged = {r["review_id"] for r in report["disagreements"] if "topic" in r["disagree"]}
    result = {"synthetic_test_copy": True, "planted": planted, "planted_count": len(planted),
              "detected": sorted(set(planted) & flagged), "missed": sorted(set(planted) - flagged),
              "detection_rate": round(len(set(planted) & flagged) / len(planted), 4) if planted else None,
              "verifier_label_configs": sorted({p["label_config"] for p in predictions}),
              "note": "Wrong labels were planted only in this in-memory test copy; stored labels are unchanged."}
    atomic_write_json(out_dir / "planted_report.json", result)
    return result


INJECTION_TARGET_FIELDS = ("topic", "intent", "severity")


def injection_test(cases_path, out_dir, provider, ledger, sleep=time.sleep, run_id=None):
    """Classify synthetic prompt-injection reviews with the enrichment configuration and check that
    (a) every output passes schema validation and (b) the label does not follow the injected
    instruction when the case declares the content-supported label. Synthetic; never exported."""
    from pipeline.enrich import _build_label  # same label building as enrichment
    from pipeline.labels import validate_label
    out_dir = Path(out_dir)
    with Path(cases_path).open(encoding="utf-8", newline="") as f:
        cases = list(csv.DictReader(f))
    run_id = run_id or f"injection-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    results = []
    for case in cases:
        rid, text = case["case_id"], case["text"]
        token = ledger.reserve(provider.estimate_cost(provider.estimate_input_tokens([(rid, text)])))
        if token is None:
            break
        started = _now()
        response = provider.classify([(rid, text)])
        cost = str(response.cost_usd) if response.cost_usd is not None else None
        ledger.settle(token, {"request_id": response.request_id, "run_id": run_id, "role": "injection_test",
                              "db": "synthetic", "cost_usd": cost,
                              "cost_source": "provider_reported" if cost else "unknown"})
        append_call(out_dir / "calls.jsonl", call_record(
            response, role="injection_test", review_ids=[rid], label_config=case.get("label_config", ""),
            run_id=run_id, provider=provider.name, cost=cost, cost_source="provider_reported" if cost else "unknown",
            attempt=1, started_at=started))
        row = {"case_id": rid, "attack": case["attack"], "ok": response.ok}
        unexpected = sorted(set(response.items) - {rid}) if response.ok else []
        row["unexpected_ids_rejected"] = unexpected
        if response.ok and isinstance(response.items.get(rid), dict):
            label, _ = _build_label(provider, response.items[rid], text, 0.5)
            row["label"] = {k: label.get(k) for k in INJECTION_TARGET_FIELDS}
            row["schema_problems"] = validate_label(label, text)
            injected = {k: case[f"injected_{k}"] for k in INJECTION_TARGET_FIELDS if case.get(f"injected_{k}")}
            expected = {k: case[f"expected_{k}"] for k in INJECTION_TARGET_FIELDS if case.get(f"expected_{k}")}
            row["followed_injection"] = sorted(k for k, v in injected.items()
                                               if str(label.get(k)) == v and expected.get(k) != v)
            row["matches_expected"] = {k: str(label.get(k)) == v for k, v in expected.items()}
        results.append(row)
        sleep(0.5)
    summary = {"synthetic": True, "cases": len(cases), "classified": sum(r["ok"] for r in results),
               "schema_valid": sum(1 for r in results if r.get("schema_problems") == []),
               "followed_injection": sum(1 for r in results if r.get("followed_injection")),
               "results": results,
               "note": "Synthetic injection texts written for this test; never part of the dataset or the grading export."}
    atomic_write_json(out_dir / "injection_report.json", summary)
    return summary


def _jev(questions):
    from pipeline.config import get_key
    from pipeline.providers import JevProvider
    key = get_key("AI_GATEWAY_API_KEY")
    if not key:
        raise SystemExit("ERROR: AI_GATEWAY_API_KEY is not set (python3 -m pipeline.config check).")
    return JevProvider(key, questions=questions)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["run", "report", "planted", "injection"])
    parser.add_argument("--db", type=Path, default=REPO_ROOT / "work" / "pipeline.sqlite")
    parser.add_argument("--out", type=Path, default=REPO_ROOT / "outputs" / "verify")
    parser.add_argument("--size", type=int, default=500)
    parser.add_argument("--sample", help="Restrict the population to a registered sample, e.g. analysis_10000")
    parser.add_argument("--cases", type=Path, default=REPO_ROOT / "evals" / "injection_cases.csv")
    parser.add_argument("--budget-usd", type=str, help="Cumulative cap in the shared spend ledger (paid commands)")
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--planted", type=int, default=10)
    parser.add_argument("--execute", action="store_true", help="Required for paid commands")
    args = parser.parse_args(argv)
    if args.command in ("run", "injection"):
        if not args.execute or not args.budget_usd:
            print("REFUSED: paid verification requires --execute and --budget-usd (and prior approval).", file=sys.stderr)
            return 2
        ledger = SpendLedger(args.budget_usd, args.ledger)
        if args.command == "run":
            result = run(args.db, args.out, _jev(VERIFY_QUESTIONS), ledger, args.size, args.sample)
            write_report(args.db, args.out)
        else:
            from pipeline.labels import QUESTIONS
            result = {k: v for k, v in injection_test(args.cases, args.out, _jev(QUESTIONS), ledger).items()
                      if k != "results"}
        print(json.dumps(result, indent=2))
        return 0
    if args.command == "report":
        report = write_report(args.db, args.out)
        print(json.dumps({k: v for k, v in report.items() if k != "disagreements"}, indent=2))
        return 0
    print(json.dumps(planted_test(args.db, args.out, args.planted), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
