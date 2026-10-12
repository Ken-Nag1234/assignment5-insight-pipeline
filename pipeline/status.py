"""Read-only status of a pipeline database, its latest run and its supervisor. No model calls.

Usage:
    python3 -m pipeline.status --db work/pipeline.sqlite
    python3 -m pipeline.status --db work/pipeline.sqlite --strata --json

Opens the database read-only, never creates lock files and never writes anything, so it is safe
to run while an enrichment job is active.
"""

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from pipeline.common import REPO_ROOT
from pipeline.ledger import DEFAULT_LEDGER
from pipeline.runlock import is_locked, read_holder

STALE_AFTER_S = 180  # a running job writes a snapshot at least every 30 s by default


def _latest(paths):
    paths = [p for p in paths if p.exists()]
    return max(paths, key=lambda p: p.stat().st_mtime) if paths else None


def _age_s(timestamp):
    try:
        return round((datetime.now(timezone.utc) - datetime.fromisoformat(timestamp)).total_seconds())
    except (TypeError, ValueError):
        return None


def collect(db_path, runs_dir, ledger_path, strata=False):
    info = {"database": str(db_path)}
    conn = sqlite3.connect(f"{Path(db_path).resolve().as_uri()}?mode=ro", uri=True)
    try:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        info["reviews"] = dict(conn.execute("SELECT status, COUNT(*) FROM reviews GROUP BY status").fetchall())
        if "calls" in tables:
            info["calls"] = [{"phase": p, "outcome": o, "count": n} for p, o, n in conn.execute(
                "SELECT phase, outcome, COUNT(*) FROM calls GROUP BY phase, outcome ORDER BY phase, outcome")]
        if "labels" in tables:
            info["label_configs"] = dict(conn.execute("SELECT label_config, COUNT(*) FROM labels GROUP BY 1"))
        if strata:
            from pipeline.enrich import scope_counts
            info["strata"] = {s: scope_counts(conn, stratum=s) for s in ("duplicates", "singletons")}
    finally:
        conn.close()
    if Path(ledger_path).exists():
        lines = [json.loads(x) for x in Path(ledger_path).read_text(encoding="utf-8").splitlines() if x.strip()]
        info["ledger"] = {"path": str(ledger_path), "lines": len(lines),
                          "total_usd": format(sum((Decimal(x["charged_usd"]) for x in lines), Decimal("0")), "f")}
    running = is_locked(db_path)
    info["enrich_process"] = {"running": running, "holder": read_holder(db_path) if running else None}
    supervising = is_locked(db_path, ".supervisor.lock")
    info["supervisor_process"] = {"running": supervising,
                                  "holder": read_holder(db_path, ".supervisor.lock") if supervising else None}
    status_file = _latest(Path(runs_dir).glob("enrich-*/status.json"))
    if status_file:
        snap = json.loads(status_file.read_text(encoding="utf-8"))
        age = _age_s(snap.get("at"))
        warning = None
        if snap.get("state") in ("running", "waiting_for_network"):
            if not running:
                warning = "the run did not finish cleanly (no process holds the lock); restart to continue"
            elif age is not None and age > STALE_AFTER_S:
                warning = f"no progress snapshot for {age} s"
        info["latest_run"] = {"dir": str(status_file.parent), "seconds_since_update": age, "warning": warning,
                              **{k: snap.get(k) for k in ("run_id", "state", "stop_reason", "elapsed_s", "scope",
                                                          "reviews_in_scope", "requests", "errors", "network",
                                                          "spend_usd", "requests_per_second", "eta_s")}}
    log = _latest(Path(runs_dir).glob("supervisor*/supervisor.jsonl"))
    if log:
        info["supervisor_log"] = {"path": str(log),
                                  "last_events": [json.loads(x) for x in log.read_text().splitlines()[-5:]]}
    return info


def render(info):
    lines = [f"Database: {info['database']}", f"Reviews: {info['reviews']}"]
    for c in info.get("calls", []):
        lines.append(f"  calls {c['phase']}/{c['outcome']}: {c['count']}")
    if "label_configs" in info:
        lines.append(f"Label configs: {info['label_configs']}")
    if "strata" in info:
        lines.append(f"Strata: {info['strata']}")
    if "ledger" in info:
        lines.append(f"Ledger: ${info['ledger']['total_usd']} over {info['ledger']['lines']} lines")
    e, s = info["enrich_process"], info["supervisor_process"]
    lines.append(f"Enrichment process running: {e['running']}" + (f" (pid {e['holder'].get('pid')})" if e["holder"] else ""))
    lines.append(f"Supervisor running: {s['running']}" + (f" (pid {s['holder'].get('pid')})" if s["holder"] else ""))
    run = info.get("latest_run")
    if run:
        lines += [f"Latest run: {run['run_id']} state={run['state']} stop_reason={run.get('stop_reason')}"
                  f" updated {run['seconds_since_update']} s ago",
                  f"  in scope: {run['reviews_in_scope']}", f"  requests: {run['requests']}",
                  f"  errors: {run['errors']}", f"  network: {run['network']}", f"  spend: {run['spend_usd']}",
                  f"  rate: {run['requests_per_second']} req/s, eta {run['eta_s']} s"]
        if run["warning"]:
            lines.append(f"  WARNING: {run['warning']}")
    sup = info.get("supervisor_log")
    if sup:
        lines.append(f"Supervisor log: {sup['path']}")
        lines += [f"  {x['at'][:19]} {x['event']} {x.get('reason') or x.get('result') or ''}" for x in sup["last_events"]]
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", type=Path, default=REPO_ROOT / "work" / "pipeline.sqlite")
    parser.add_argument("--runs", type=Path, default=REPO_ROOT / "outputs" / "runs")
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--strata", action="store_true", help="Also count both strata (takes a few seconds)")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    info = collect(args.db, args.runs, args.ledger, args.strata)
    print(json.dumps(info, indent=2, default=str) if args.json else render(info))
    return 0


if __name__ == "__main__":
    sys.exit(main())
