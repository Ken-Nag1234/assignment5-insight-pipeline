"""Run every stage for one input CSV: ingest -> enrich -> verify -> group (+ naming) -> rank -> memo -> export.

Usage (paid; every model role needs explicit approval):
    python3 -m pipeline.run --input cost_100.csv --workdir work/pilot --budget-usd 0.10 \
        --text-model-id <provider/model> --input-price-per-m X --output-price-per-m Y --verify-size 20 --execute
Offline demonstration (mock models; SYNTHETIC outputs, never results):
    python3 -m pipeline.run --input cost_100.csv --workdir /tmp/demo --budget-usd 1 --mock

Each stage reads the previous stage's saved artifacts in --workdir and writes its own:
    pipeline.sqlite (ingested rows, enrichment labels, enrichment call log)
    runs/enrich-*/        enrichment progress and run logs
    verify/               declared sample, verifier predictions, verify calls, agreement report
    analysis/             membership.csv, issues.csv, ranking.csv, aggregates.csv, issue_names.json,
                          memo.md, claims_selection.json, model_calls.jsonl (group, memo), cache/
    grading/              the contract export (records, membership, ranking, claims, calls, run.json)
    run_report.json       stage timings, counts, calls per role, cost
Running the same command again on the same --workdir is a warm run: completed enrichment IDs are
not re-sent, verifier predictions are reused, and group/memo results come from the saved result
cache (keyed by model, prompt version and the exact input), so no model call repeats.
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from pipeline import db, export, group, insights, rank, verify
from pipeline.common import atomic_write_json
from pipeline.enrich import Options, run_enrichment
from pipeline.ingest import ingest
from pipeline.ledger import DEFAULT_LEDGER, SpendLedger


def mock_text_responder(messages):
    """SYNTHETIC answers for offline runs: names every issue and writes a rule-abiding memo."""
    payload = json.loads(messages[1]["content"])
    if "issues" in payload:
        return json.dumps({"names": {i["issue_id"]: f"Synthetic name for {i['topic']} {i['subtopic']}"
                                     for i in payload["issues"]}})
    top = payload["baseline_ranking_top"][0]
    cite = payload["evidence_pack"][0]["examples"][0]["review_id"]
    alt = payload["baseline_ranking_top"][1]["issue_id"] if len(payload["baseline_ranking_top"]) > 1 else top["issue_id"]
    md = ("## Recommendation\nSynthetic test memo: prioritize issue " + top["issue_id"] + " with {{C1}} complaints "
          "and a severity sum of {{C2}} [review:" + cite + "].\n\n## Evidence\nComplaint records: "
          "{{fact:complaint_records}} of {{fact:completed_reviews}} completed reviews.\n\n"
          "## Alternatives considered\nIssue " + alt + " has a priority score of {{C3}}.\n\n"
          "## Limitations\nSynthetic text from a mock model; {{fact:pending_reviews}} reviews are pending.")
    recs = [{"title": "Synthetic: fix the top issue first",
             "text": "Issue " + top["issue_id"] + " leads with a severity sum of {{C2}}.", "review_ids": [cite]},
            {"title": "Synthetic: monitor the runner-up",
             "text": "Issue " + alt + " follows with a priority score of {{C3}}.", "review_ids": [cite]}]
    return json.dumps({"title": "Synthetic test memo", "memo_markdown": md, "recommendations": recs, "claims": [
        {"ref": "C1", "issue_id": top["issue_id"], "metric": "complaint_count"},
        {"ref": "C2", "issue_id": top["issue_id"], "metric": "severity_sum"},
        {"ref": "C3", "issue_id": alt, "metric": "priority_score"}]})


def build_models(args):
    if args.mock:
        from pipeline.providers import MockProvider
        from pipeline.textmodel import MockTextModel
        return MockProvider(max_batch=1), MockProvider(max_batch=1), MockTextModel(mock_text_responder)
    if not args.execute:
        raise SystemExit("REFUSED: paid runs require --execute (and prior approval), or use --mock for a synthetic demo.")
    if not (args.text_model_id and args.input_price_per_m and args.output_price_per_m):
        raise SystemExit("ERROR: --text-model-id, --input-price-per-m and --output-price-per-m are required.")
    from pipeline.config import get_key
    from pipeline.labels import QUESTIONS
    from pipeline.providers import JevProvider
    from pipeline.textmodel import GatewayChatModel
    key = get_key("AI_GATEWAY_API_KEY")
    if not key:
        raise SystemExit("ERROR: AI_GATEWAY_API_KEY is not set.")
    return (JevProvider(key, questions=QUESTIONS), JevProvider(key, questions=verify.VERIFY_QUESTIONS),
            GatewayChatModel(key, args.text_model_id, args.input_price_per_m, args.output_price_per_m))


def _calls_by_role(workdir):
    counts = {}
    conn = db.connect(workdir / "pipeline.sqlite")
    try:
        for role, n in conn.execute("SELECT role, COUNT(*) FROM calls GROUP BY role"):
            counts[role] = n
    finally:
        conn.close()
    for path in (workdir / "verify" / "calls.jsonl", workdir / "analysis" / "model_calls.jsonl"):
        for call in verify.load_jsonl(path):
            counts[call["role"]] = counts.get(call["role"], 0) + 1
    return counts


def run_all(input_csv, workdir, enrich_provider, verify_provider, text_model, ledger, *, verify_size=20,
            workers=1, rps=2.0, allow_mock=False, sleep=time.sleep, manifest=None):
    workdir = Path(workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    db_path = workdir / "pipeline.sqlite"
    analysis = workdir / "analysis"
    stamp = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S%fZ}"  # unique even for back-to-back runs
    before = _calls_by_role(workdir) if db_path.exists() else {}
    spent_before = ledger.spent
    seconds = {}
    started = time.monotonic()

    t = time.monotonic()
    ingest(input_csv, db_path, workdir / "ingestion", workdir / "ingestion" / "grading", manifest)
    seconds["ingest"] = round(time.monotonic() - t, 3)

    t = time.monotonic()
    conn = db.connect(db_path)
    try:
        summary = run_enrichment(conn, enrich_provider, ledger, Options(
            workers=workers, requests_per_second=rps, sleep=sleep, run_id=f"enrich-{stamp}",
            log_dir=workdir / "runs" / f"enrich-{stamp}", echo_progress=False))
    finally:
        conn.close()
    seconds["enrich"] = round(time.monotonic() - t, 3)
    if summary.stop_reason != "queue_empty":
        raise RuntimeError(f"enrichment stopped: {summary.stop_reason}")

    t = time.monotonic()
    verify_result = verify.run(db_path, workdir / "verify", verify_provider, ledger, verify_size,
                               rps=rps, sleep=sleep, run_id=f"verify-{stamp}", include_mock=allow_mock)
    verify_report = verify.write_report(db_path, workdir / "verify")
    seconds["verify"] = round(time.monotonic() - t, 3)

    t = time.monotonic()
    conn = db.connect(db_path)
    try:
        records = list(group.records_from_db(conn, exclude_mock=not allow_mock))
    finally:
        conn.close()
    membership, issues = group.assign(records)
    group.write_membership(analysis / "membership.csv", membership)
    by_id = {r["review_id"]: r for r in records}
    ranking = rank.compute(by_id, membership)
    rank.write_ranking(analysis / "ranking.csv", ranking)
    rank.write_aggregates(analysis / "aggregates.csv", ranking, by_id, membership)
    group.write_issues(analysis / "issues.csv", issues)
    atomic_write_json(analysis / "verify_report.json", {k: v for k, v in verify_report.items() if k != "disagreements"})
    naming = insights.name_issues(db_path, analysis, text_model, ledger, run_id=f"group-{stamp}")
    names = json.loads((analysis / "issue_names.json").read_text(encoding="utf-8"))
    group.write_issues(analysis / "issues.csv", issues, names)
    seconds["group"] = round(time.monotonic() - t, 3)

    t = time.monotonic()
    memo = insights.write_memo(db_path, analysis, text_model, ledger, run_id=f"memo-{stamp}") if ranking else None
    seconds["memo"] = round(time.monotonic() - t, 3)

    t = time.monotonic()
    selections = [tuple(x) for x in json.loads((analysis / "claims_selection.json").read_text())] if memo else None
    conn = db.connect(db_path)
    try:
        exported = export.export(conn, workdir / "grading", selections, allow_mock=allow_mock,
                                 pending_path=workdir / "pending.jsonl",
                                 extra_calls=[p for p in (workdir / "verify" / "calls.jsonl",
                                                          analysis / "model_calls.jsonl") if p.exists()])
        status = db.status_counts(conn)
    finally:
        conn.close()
    seconds["export"] = round(time.monotonic() - t, 3)

    after = _calls_by_role(workdir)
    report = {
        "input": str(input_csv), "workdir": str(workdir), "stamp": stamp,
        "synthetic": allow_mock, "wall_clock_s": round(time.monotonic() - started, 3), "stage_seconds": seconds,
        "statuses": status, "enrich": {k: v for k, v in summary.__dict__.items() if k != "events"},
        "verify": verify_result, "naming": naming, "memo_accepted": bool(memo and memo["accepted"]),
        "new_calls_by_role": {r: after.get(r, 0) - before.get(r, 0) for r in sorted(set(after) | set(before))},
        "spend_usd": format(ledger.spent - spent_before, "f"),
        "exported": {k: v for k, v in exported.items() if k != "ranking"},
    }
    atomic_write_json(workdir / f"run_report-{stamp}.json", report)
    atomic_write_json(workdir / "run_report.json", report)
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--budget-usd", type=str, required=True, help="Cumulative cap in the shared spend ledger")
    parser.add_argument("--ledger", type=Path, default=None)
    parser.add_argument("--verify-size", type=int, default=20)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--rps", type=float, default=2.0)
    parser.add_argument("--text-model-id")
    parser.add_argument("--input-price-per-m")
    parser.add_argument("--output-price-per-m")
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--mock", action="store_true", help="Synthetic offline demo with mock models")
    args = parser.parse_args(argv)
    ledger_path = args.ledger or (args.workdir / "mock_ledger.jsonl" if args.mock else DEFAULT_LEDGER)
    enrich_provider, verify_provider, text_model = build_models(args)
    report = run_all(args.input, args.workdir, enrich_provider, verify_provider, text_model,
                     SpendLedger(Decimal(args.budget_usd), ledger_path), verify_size=args.verify_size,
                     workers=args.workers, rps=args.rps, allow_mock=args.mock, manifest=args.manifest)
    print(json.dumps({k: report[k] for k in ("synthetic", "wall_clock_s", "stage_seconds", "statuses",
                                             "new_calls_by_role", "spend_usd", "memo_accepted")}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
