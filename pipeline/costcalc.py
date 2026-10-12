"""100-review cost and runtime calculator (COST_CALCULATOR.md). Offline by default.

Usage:
    python3 cost/calculator.py replay            # recompute everything from saved files; no API key
    python3 cost/calculator.py export-calls --db work/pilot_cold.sqlite --kind cold   # copy call log

Inputs (all in cost/, editable):
    rates.csv          dated prices in their billing units, with source links
    assumptions.json   budget, limits, projection scenarios, local-compute assumptions
    pilot_calls.jsonl  one line per attempted pilot call (cold and warm runs), real usage only
    pilot_runs.json    measured wall-clock and stage times per pilot run
Outputs:
    usage.csv          every billed item mapped to its call (billed_units x price_per_unit)
    report.json / report.md

Rules followed: item_cost = billed_units x price_per_unit (per-million prices are divided by
1,000,000 first); cached input is billed separately from uncached input; missing usage stays
"unknown" and is never turned into zero; estimates are labeled; API spend is shown separately
from local compute.
"""

import argparse
import csv
import json
import sqlite3
import sys
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

from pipeline.common import REPO_ROOT

COST_DIR = REPO_ROOT / "cost"
ROLES = ("enrich", "verify", "group", "memo")
FIXED_ROLES = ("group", "memo")           # once per run, never multiplied by review count


def money(value):
    return None if value is None else str(Decimal(value).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP))


def load_rates(path):
    rates = []
    with Path(path).open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            row["price_usd"] = Decimal(row["price_usd"])
            row["per_units"] = Decimal(row["per_units"])
            rates.append(row)
    return rates


def load_jsonl(path):
    path = Path(path)
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def billed_items(call, rates):
    """Mutually exclusive billing items for one call: uncached input, cached input, output.

    Returns a list of dicts; billed_units is None when the provider did not report usage.
    """
    items = []
    matching = [r for r in rates if r["provider"] == call["provider"] and r["model"] == call["model"]]
    if not matching:
        return [{"rate_item": None, "unit": None, "billed_units": None, "cost_usd": None,
                 "status": "no_rate_for_model"}]
    input_tokens = call.get("input_tokens")
    cached = call.get("cached_input_tokens") or 0
    units = {
        "input_tokens": None if input_tokens is None else input_tokens - cached,
        "cached_input_tokens": None if input_tokens is None else cached,
        "output_tokens": call.get("output_tokens"),
        "requests": 1,
    }
    for rate in matching:
        unit = rate["unit"]
        billed = units.get(unit)
        if billed is None:
            items.append({"rate_item": rate["item_id"], "unit": unit, "billed_units": None, "cost_usd": None,
                          "status": "usage_unavailable"})
            continue
        cost = Decimal(billed) * (rate["price_usd"] / rate["per_units"])
        items.append({"rate_item": rate["item_id"], "unit": unit, "billed_units": billed, "cost_usd": cost,
                      "status": "computed"})
    return items


def usage_table(calls, rates):
    rows = []
    for call in calls:
        for item in billed_items(call, rates):
            rows.append({"run_kind": call["run_kind"], "request_id": call["request_id"], "role": call["role"],
                         "provider": call["provider"], "model": call["model"], "outcome": call["outcome"],
                         **item})
    return rows


def _sum(values):
    values = list(values)
    if any(v is None for v in values):
        return None
    return sum(values, Decimal("0"))


def summarize_run(kind, calls, usage, run_meta):
    run_calls = [c for c in calls if c["run_kind"] == kind]
    run_usage = [u for u in usage if u["run_kind"] == kind]
    by_role = {}
    for role in ROLES:
        role_calls = [c for c in run_calls if c["role"] == role]
        role_usage = [u for u in run_usage if u["role"] == role]
        by_role[role] = {
            "requests": len(role_calls),
            "failed": sum(c["outcome"] == "failed" for c in role_calls),
            "retries": sum((c.get("attempt") or 1) > 1 for c in role_calls),
            "fallbacks": sum(bool(c.get("fallback")) for c in role_calls),
            "reviews_sent": sum(len(c.get("review_ids") or []) for c in role_calls),
            "input_tokens": _sum(c.get("input_tokens") for c in role_calls),
            "output_tokens": _sum(c.get("output_tokens") for c in role_calls),
            "cost_usd": _sum(u["cost_usd"] for u in role_usage),
            "provider_reported_cost_usd": _sum(Decimal(c["provider_cost_usd"]) if c.get("provider_cost_usd") else None
                                               for c in role_calls) if role_calls else Decimal("0"),
            "models": sorted({c["model"] for c in role_calls}),
            "settings": sorted({c.get("settings", "") for c in role_calls}),
            "seconds": (run_meta.get("stage_seconds") or {}).get(role),
        }
    api_total = _sum(r["cost_usd"] for r in by_role.values())
    inputs = run_meta.get("input_rows")
    completed = run_meta.get("completed")
    wall = run_meta.get("wall_clock_s")
    return {
        "kind": kind, "measured": bool(run_calls) or bool(run_meta), "run_meta": run_meta, "by_role": by_role,
        "api_cost_usd": api_total,
        "cost_per_1000_inputs_usd": api_total / Decimal(inputs) * 1000 if api_total is not None and inputs else None,
        "cost_per_completed_usd": api_total / Decimal(completed) if api_total is not None and completed else None,
        "throughput_rows_per_s": Decimal(inputs) / Decimal(str(wall)) if inputs and wall else None,
        "new_enrich_calls": by_role["enrich"]["requests"],
    }


def project(assumptions, cold, scenario_name, target_rows):
    """Full-run (or partial-target) projection for one scenario from measured cold-pilot unit costs."""
    s = assumptions["scenarios"][scenario_name]
    full = assumptions["corpus"]
    measured = cold["by_role"]["enrich"]
    source = "measured_cold_pilot"
    if measured["requests"] and measured["cost_usd"] is not None:
        per_request = measured["cost_usd"] / measured["requests"]
        reviews_per_request = Decimal(measured["reviews_sent"]) / measured["requests"]
        seconds_per_request = (Decimal(str(measured["seconds"])) / measured["requests"]) if measured["seconds"] else None
    else:
        pre = assumptions["pre_pilot_assumptions"]
        per_request = (Decimal(pre["enrich_input_tokens_per_request"]) * Decimal(pre["enrich_input_price_per_m"])
                       / Decimal(1_000_000))
        reviews_per_request = Decimal(pre["reviews_per_request"])
        seconds_per_request = Decimal(str(pre["seconds_per_request"]))
        source = "pre_pilot_assumption"
    multiplier = Decimal(str(s.get("token_multiplier", 1)))
    retry = Decimal(str(s["retry_rate"]))
    reuse = Decimal(full["distinct_nonempty_texts"]) / Decimal(full["nonempty_rows"])
    out = {"scenario": scenario_name, "unit_cost_source": source, "target_rows": target_rows}
    for label, distinct_ratio in (("with_exact_text_reuse", reuse), ("no_reuse", Decimal(1))):
        texts = (Decimal(target_rows) * distinct_ratio).to_integral_value(rounding=ROUND_HALF_UP)
        requests = (texts / reviews_per_request).to_integral_value(rounding="ROUND_CEILING")
        enrich = requests * per_request * multiplier * (1 + retry)
        verify_rows = (Decimal(target_rows) * Decimal(str(s["verification_rate"]))).to_integral_value(rounding="ROUND_CEILING")
        verify_unit = _unit_cost(cold, "verify")
        verify = None if verify_unit is None else verify_rows * verify_unit * multiplier * (1 + retry)
        fallback_rate = min(Decimal(str(s["fallback_rate"])), Decimal(str(assumptions["controls"]["max_fallback_fraction"])))
        fallback_unit = Decimal(str(assumptions["controls"]["fallback_cost_per_review_usd"])) \
            if assumptions["controls"].get("fallback_cost_per_review_usd") is not None else None
        fallback = Decimal(0) if fallback_rate == 0 else (None if fallback_unit is None
                                                          else texts * fallback_rate * fallback_unit)
        fixed = _sum(cold["by_role"][r]["cost_usd"] for r in FIXED_ROLES) if cold["measured"] else None
        total = _sum([enrich, verify, fallback, fixed])
        budget = Decimal(str(assumptions["controls"]["budget_usd"]))
        workers = Decimal(assumptions["controls"]["max_workers"])
        seconds = None if seconds_per_request is None else requests * seconds_per_request * (1 + retry) / workers
        out[label] = {
            "distinct_texts_to_classify": int(texts), "enrich_requests": int(requests),
            "enrich_usd": enrich, "verify_rows": int(verify_rows), "verify_usd": verify,
            "fallback_usd": fallback, "fixed_overhead_usd": fixed, "total_api_usd": total,
            "enrich_hours": None if seconds is None else seconds / 3600,
            "exceeds_budget": _exceeds(budget, [enrich, verify, fallback, fixed]),
        }
    return out


def _exceeds(budget, parts):
    """True if the known parts alone exceed the budget; None if unknown parts could decide it."""
    known = sum((p for p in parts if p is not None), Decimal("0"))
    if known > budget:
        return True
    return None if any(p is None for p in parts) else False


def _unit_cost(cold, role):
    r = cold["by_role"][role]
    if not r["requests"] or r["cost_usd"] is None or not r["reviews_sent"]:
        return None
    return r["cost_usd"] / r["reviews_sent"]


def replay(cost_dir=COST_DIR, write=True):
    cost_dir = Path(cost_dir)
    rates = load_rates(cost_dir / "rates.csv")
    assumptions = json.loads((cost_dir / "assumptions.json").read_text(encoding="utf-8"))
    calls = load_jsonl(cost_dir / "pilot_calls.jsonl")
    runs_path = cost_dir / "pilot_runs.json"
    runs = json.loads(runs_path.read_text(encoding="utf-8")) if runs_path.exists() else {}
    usage = usage_table(calls, rates)
    cold = summarize_run("cold", calls, usage, runs.get("cold", {}))
    warm = summarize_run("warm", calls, usage, runs.get("warm", {}))
    projections = [project(assumptions, cold, name, target)
                   for target in assumptions["projection_targets"] for name in assumptions["scenarios"]]
    report = {
        "pilot_measured": cold["measured"],
        "rates_checked": sorted({r["checked"] for r in rates}),
        "cold": cold, "warm": warm,
        "warm_zero_new_enrich_calls": warm["measured"] and warm["new_enrich_calls"] == 0,
        "projections": projections,
        "controls": assumptions["controls"],
        "local_compute": assumptions["local_compute"],
        "corpus": assumptions["corpus"],
    }
    if write:
        _write_usage(cost_dir / "usage.csv", usage)
        (cost_dir / "report.json").write_text(json.dumps(report, indent=2, default=_json_default) + "\n", encoding="utf-8")
        (cost_dir / "report.md").write_text(render_markdown(report), encoding="utf-8")
    return report


def _json_default(value):
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(type(value))


def _write_usage(path, usage):
    fields = ["run_kind", "request_id", "role", "provider", "model", "outcome", "rate_item", "unit",
              "billed_units", "cost_usd", "status"]
    with Path(path).open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for row in usage:
            writer.writerow({**row, "billed_units": "unknown" if row["billed_units"] is None else row["billed_units"],
                             "cost_usd": "unknown" if row["cost_usd"] is None else money(row["cost_usd"])})


def _fmt(value, digits=6):
    if value is None:
        return "unknown"
    if isinstance(value, Decimal):
        return f"{value:.{digits}f}"
    return str(value)


def render_markdown(report):
    lines = ["# Cost and runtime calculator report", ""]
    if not report["pilot_measured"]:
        lines += ["> **NO MEASURED PILOT YET.** Measured tables are empty. Projections below use the labeled",
                  "> pre-pilot assumptions in `assumptions.json` and are estimates only.", ""]
    lines += [f"Rates checked: {', '.join(report['rates_checked'])}. Recompute with `python3 cost/calculator.py replay`.", ""]
    for run in (report["cold"], report["warm"]):
        meta = run["run_meta"]
        lines += [f"## Measured {run['kind']} run", ""]
        if not run["measured"]:
            lines += ["Not measured.", ""]
            continue
        lines += [f"- Input checksum: `{meta.get('input_sha256', 'unknown')}`; IDs: {meta.get('input_rows', 'unknown')}; "
                  f"completed: {meta.get('completed', 'unknown')}; failed/quarantined: {meta.get('failed', 'unknown')}; "
                  f"unique texts: {meta.get('unique_texts', 'unknown')}; result-cache hits: {meta.get('cache_hits', 'unknown')}",
                  f"- Batch size: {meta.get('batch_size', 'unknown')}; workers: {meta.get('workers', 'unknown')}; "
                  f"wall clock: {meta.get('wall_clock_s', 'unknown')} s",
                  f"- API cost: ${_fmt(run['api_cost_usd'], 8)}; per 1,000 inputs: ${_fmt(run['cost_per_1000_inputs_usd'])}; "
                  f"per completed record: ${_fmt(run['cost_per_completed_usd'], 8)}; throughput: "
                  f"{_fmt(run['throughput_rows_per_s'], 3)} rows/s", "",
                  "| Role | Model / settings | Requests | Failed | Retries | Fallbacks | Input tok | Output tok | Cost (USD) | Seconds |",
                  "|---|---|---|---|---|---|---|---|---|---|"]
        for role, r in run["by_role"].items():
            lines.append(f"| {role} | {', '.join(r['models']) or '-'} {' '.join(x for x in r['settings'] if x)} | "
                         f"{r['requests']} | {r['failed']} | {r['retries']} | {r['fallbacks']} | {_fmt(r['input_tokens'])} | "
                         f"{_fmt(r['output_tokens'])} | {_fmt(r['cost_usd'], 8)} | {_fmt(r['seconds'])} |")
        lines.append("")
    if report["warm"]["measured"]:
        lines += [f"Warm run new enrichment calls: {report['warm']['new_enrich_calls']} "
                  f"({'zero, as required' if report['warm_zero_new_enrich_calls'] else 'NOT zero'}).", ""]
    c = report["controls"]
    lines += ["## Controls", "",
              f"Budget ${c['budget_usd']}; output-token cap {c['output_token_cap']}; max workers {c['max_workers']}; "
              f"max fallback fraction {c['max_fallback_fraction']}.", "",
              "## Projections (estimates)", "",
              "| Target rows | Scenario | Unit cost source | Reuse | Texts | Requests | Enrich USD | Verify USD | Fallback USD | Fixed USD | Total USD | Enrich hours | Over budget |",
              "|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for p in report["projections"]:
        for label in ("with_exact_text_reuse", "no_reuse"):
            r = p[label]
            flag = "unknown" if r["exceeds_budget"] is None else ("**YES**" if r["exceeds_budget"] else "no")
            lines.append(f"| {p['target_rows']:,} | {p['scenario']} | {p['unit_cost_source']} | {label} | "
                         f"{r['distinct_texts_to_classify']:,} | {r['enrich_requests']:,} | {_fmt(r['enrich_usd'], 4)} | "
                         f"{_fmt(r['verify_usd'], 4)} | {_fmt(r['fallback_usd'], 4)} | {_fmt(r['fixed_overhead_usd'], 4)} | "
                         f"{_fmt(r['total_api_usd'], 4)} | {_fmt(r['enrich_hours'], 2)} | {flag} |")
    lc = report["local_compute"]
    lines += ["", "## Local compute (separate from API spend)", "",
              f"{lc['description']} Cost: {lc['cost_usd'] if lc['cost_usd'] is not None else 'unknown (not measured)'}.", "",
              "Projections scale each stage by its own work count; group and memo are fixed once-per-run costs.",
              "Partial targets assume the full-corpus exact-text reuse ratio; the seeded queue can differ.", ""]
    return "\n".join(lines)


def export_calls(db_path, kind, out_path, settings=""):
    """Append a pilot database's call log to pilot_calls.jsonl in the calculator's format."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM calls ORDER BY started_at, request_id").fetchall()
    conn.close()
    with Path(out_path).open("a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps({
                "run_kind": kind, "run_id": r["run_id"], "request_id": r["request_id"], "role": r["role"],
                "phase": r["phase"], "provider": r["provider"], "model": r["model"], "settings": settings,
                "label_config": r["label_config"], "review_ids": json.loads(r["review_ids"]),
                "outcome": r["outcome"], "error": r["error"], "attempt": r["attempt"],
                "input_tokens": r["input_tokens"], "output_tokens": r["output_tokens"], "cached_input_tokens": 0,
                "provider_cost_usd": r["cost_usd"] if r["cost_source"] == "provider_reported" else None,
                "started_at": r["started_at"], "elapsed_s": r["elapsed_s"]}) + "\n")
    return len(rows)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("replay", help="Offline recomputation (default)")
    p = sub.add_parser("export-calls", help="Copy a pilot database's call log into pilot_calls.jsonl")
    p.add_argument("--db", type=Path, required=True)
    p.add_argument("--kind", choices=["cold", "warm"], required=True)
    p.add_argument("--settings", default="")
    parser.add_argument("--cost-dir", type=Path, default=COST_DIR)
    args = parser.parse_args(argv)
    if args.command == "export-calls":
        n = export_calls(args.db, args.kind, args.cost_dir / "pilot_calls.jsonl", args.settings)
        print(f"Appended {n} {args.kind} calls to {args.cost_dir / 'pilot_calls.jsonl'}")
        return 0
    report = replay(args.cost_dir)
    print(render_markdown(report))
    return 0


if __name__ == "__main__":
    sys.exit(main())
