"""Required 100-review cold/warm pilot (COST_CALCULATOR.md). Paid; separate from the offline replay.

Usage (explicit and approved only):
    python3 -m pipeline.pilot --execute --budget-usd 0.40 --workdir work/pilot \
        --text-model-id <provider/model> --input-price-per-m X --output-price-per-m Y --verify-size 20
Offline structure test (mock models; SYNTHETIC, written outside cost/ by default):
    python3 -m pipeline.pilot --mock --workdir /tmp/pilot --cost-dir /tmp/pilot-cost

Runs the full staged pipeline (pipeline.run) on cost_100.csv unchanged with an empty result cache
and one worker (cold), then repeats it on the same work directory (warm: zero new enrichment
calls expected, verifier and group/memo results reused). Then writes into --cost-dir:
    pilot_calls.jsonl    every attempted call of both runs in the calculator's format
    pilot_runs.json      measured wall clock, stage seconds, counts and settings per run
    pilot_records.jsonl  one grading-contract record per pilot ID (from the cold run's export)
and runs the offline calculator replay. Refuses to overwrite an earlier pilot.
"""

import argparse
import json
import shutil
import sys
from decimal import Decimal
from pathlib import Path

from pipeline import costcalc, verify
from pipeline.common import REPO_ROOT, atomic_write_json, file_sha256
from pipeline.ledger import DEFAULT_LEDGER, SpendLedger

COST_100 = REPO_ROOT / "cost_100.csv"
MANIFEST = REPO_ROOT / "manifest.json"


def pilot_calls(workdir, stamp, kind, settings):
    """All calls made by one pipeline.run invocation (identified by its stamp), calculator format."""
    import sqlite3
    workdir = Path(workdir)
    calls = []
    conn = sqlite3.connect(f"{(workdir / 'pipeline.sqlite').resolve().as_uri()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        for r in conn.execute("SELECT * FROM calls WHERE run_id = ? ORDER BY started_at, request_id", (f"enrich-{stamp}",)):
            calls.append({"request_id": r["request_id"], "role": r["role"], "phase": r["phase"],
                          "provider": r["provider"], "model": r["model"], "label_config": r["label_config"],
                          "review_ids": json.loads(r["review_ids"]), "outcome": r["outcome"], "error": r["error"],
                          "attempt": r["attempt"], "input_tokens": r["input_tokens"], "output_tokens": r["output_tokens"],
                          "cost_usd": r["cost_usd"], "cost_source": r["cost_source"], "run_id": r["run_id"],
                          "started_at": r["started_at"], "elapsed_s": r["elapsed_s"]})
    finally:
        conn.close()
    for path in (workdir / "verify" / "calls.jsonl", workdir / "analysis" / "model_calls.jsonl"):
        calls += [c for c in verify.load_jsonl(path) if c.get("run_id", "").endswith(stamp)]
    return [{**c, "run_kind": kind, "settings": settings.get(c["role"], ""), "cached_input_tokens": 0,
             "provider_cost_usd": c["cost_usd"] if c.get("cost_source") == "provider_reported" else None}
            for c in calls]


def _text_settings(model):
    return model.settings() if hasattr(model, "settings") else "temperature 0"


def run_meta(report, kind, input_csv, workers, rps, models):
    enrich = report["enrich"]
    return {"kind": kind, "input": Path(input_csv).name, "input_sha256": file_sha256(input_csv),
            "input_rows": sum(report["statuses"].values()), "completed": report["statuses"]["completed"],
            "failed": report["statuses"]["quarantined"] + report["statuses"]["pending"],
            "quarantined": report["statuses"]["quarantined"], "pending": report["statuses"]["pending"],
            "unique_texts": enrich["first_dispatches"], "cache_hits": enrich["completed_by_cache"],
            "wall_clock_s": report["wall_clock_s"], "stage_seconds": report["stage_seconds"],
            "new_calls_by_role": report["new_calls_by_role"], "workers": workers, "requests_per_second": rps,
            "batch_size": 1, "models": models, "synthetic": report["synthetic"], "stamp": report["stamp"]}


def run_pilot(workdir, cost_dir, providers, ledger, *, verify_size=20, input_csv=COST_100, allow_mock=False,
              sleep=None, settings=None):
    from pipeline.run import run_all
    workdir, cost_dir = Path(workdir), Path(cost_dir)
    if workdir.exists() and any(workdir.iterdir()):
        raise SystemExit(f"REFUSED: {workdir} is not empty; a cold pilot needs an empty result cache.")
    for name in ("pilot_calls.jsonl", "pilot_runs.json", "pilot_records.jsonl"):
        if (cost_dir / name).exists():
            raise SystemExit(f"REFUSED: {cost_dir / name} exists; an earlier pilot is never overwritten.")
    expected = json.loads(MANIFEST.read_text())["files"]["cost_100.csv"]["sha256"]
    if file_sha256(input_csv) != expected:
        raise SystemExit("REFUSED: the pilot input must be cost_100.csv unchanged (checksum mismatch).")
    enrich_provider, verify_provider, text_model = providers
    models = {"enrich": enrich_provider.model, "verify": verify_provider.model, "group": text_model.model,
              "memo": text_model.model}
    settings = settings or {"enrich": "prompt p1, schema s1, extraction x1, 1 review/request",
                            "verify": f"prompt verify-{verify.VERIFY_PROMPT_VERSION}, sample {verify_size}",
                            "group": f"prompt n1, {_text_settings(text_model)}, max_tokens 1500",
                            "memo": f"prompt m1, {_text_settings(text_model)}, max_tokens 2500"}
    kwargs = {"verify_size": verify_size, "workers": 1, "rps": 2.0, "allow_mock": allow_mock}
    if sleep is not None:
        kwargs["sleep"] = sleep
    runs, calls = {}, []
    for kind in ("cold", "warm"):
        report = run_all(input_csv, workdir, enrich_provider, verify_provider, text_model, ledger, **kwargs)
        runs[kind] = run_meta(report, kind, input_csv, 1, 2.0, models)
        calls += pilot_calls(workdir, report["stamp"], kind, settings)
    cost_dir.mkdir(parents=True, exist_ok=True)
    with (cost_dir / "pilot_calls.jsonl").open("x", encoding="utf-8") as f:
        for c in calls:
            f.write(json.dumps(c, sort_keys=True) + "\n")
    atomic_write_json(cost_dir / "pilot_runs.json", runs)
    shutil.copyfile(workdir / "grading" / "records.jsonl", cost_dir / "pilot_records.jsonl")
    for name in ("memo.md", "ranking.csv", "membership.csv"):
        if (workdir / "analysis" / name).exists():
            shutil.copyfile(workdir / "analysis" / name, cost_dir / f"pilot_{name}")
    report = costcalc.replay(cost_dir)
    return runs, report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--workdir", type=Path, default=REPO_ROOT / "work" / "pilot")
    parser.add_argument("--cost-dir", type=Path, default=None)
    parser.add_argument("--budget-usd", type=str)
    parser.add_argument("--ledger", type=Path, default=None)
    parser.add_argument("--verify-size", type=int, default=20)
    parser.add_argument("--text-model-id")
    parser.add_argument("--input-price-per-m")
    parser.add_argument("--output-price-per-m")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--mock", action="store_true")
    args = parser.parse_args(argv)
    from pipeline.run import build_models
    if args.mock:
        if args.cost_dir is None:
            raise SystemExit("ERROR: --mock needs an explicit --cost-dir outside cost/ (synthetic output).")
        args.budget_usd = args.budget_usd or "1"
    elif not args.budget_usd:
        raise SystemExit("ERROR: --budget-usd is required.")
    cost_dir = args.cost_dir or costcalc.COST_DIR
    if not args.mock:
        rates = costcalc.load_rates(cost_dir / "rates.csv")
        if not any(r["model"] == args.text_model_id for r in rates):
            raise SystemExit(f"REFUSED: add dated rate rows for {args.text_model_id} to {cost_dir / 'rates.csv'} first.")
    providers = build_models(args)
    ledger = SpendLedger(Decimal(args.budget_usd), args.ledger or (args.workdir.parent / "pilot_mock_ledger.jsonl"
                                                                   if args.mock else DEFAULT_LEDGER))
    runs, report = run_pilot(args.workdir, cost_dir, providers, ledger, verify_size=args.verify_size,
                             allow_mock=args.mock)
    print(json.dumps({k: {"wall_clock_s": v["wall_clock_s"], "new_calls_by_role": v["new_calls_by_role"]}
                      for k, v in runs.items()}, indent=2))
    print("warm run made zero new enrichment calls:", report["warm_zero_new_enrich_calls"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
