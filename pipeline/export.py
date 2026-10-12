"""Write the standardized grading/ export (GRADING_CONTRACT.md "Export one grading folder").

Usage:
    python3 -m pipeline.export --db work/full.sqlite --out grading \
        --checkpoint-before grading/checkpoint_before.json --checkpoint-after grading/checkpoint_after.json \
        --claims outputs/analysis/claims_selection.json

Writes run.json, records.jsonl (or .jsonl.gz), membership.csv, ranking.csv, claims.csv (when claim
selections are given), calls.jsonl, and copies the checkpoint snapshots. ingestion.json is produced
by pipeline.ingest with the provided profile helper.

records.jsonl holds exactly one record per source ID that is completed or quarantined. Reviews
still pending (never classified) are NOT relabeled as quarantined: they are listed in a separate
pending file outside the grading folder and reported as incomplete coverage.
"""

import argparse
import gzip
from collections import Counter
import json
import shutil
import sys
from pathlib import Path

from pipeline import db, group, rank
from pipeline.common import REPO_ROOT, atomic_write_json, atomic_write_text
from pipeline.labels import record_from_row

CONTRACT_VERSION = "a5-audit-v1"


class ExportError(Exception):
    pass


def iter_final_records(conn, allow_mock=False):
    """Yield (status, record) in source order; pending rows yield (pending, minimal dict)."""
    rows = conn.execute(
        "SELECT r.review_id, r.source_sha256, r.status, r.reason, r.attempts, l.label_config, l.topic, l.intent,"
        " l.sentiment, l.severity, l.entities, l.evidence_quote, l.needs_review, l.cache_source_id"
        " FROM reviews r LEFT JOIN labels l USING (review_id) ORDER BY r.source_row")
    for row in rows:
        if row["status"] == "completed":
            if row["label_config"].startswith("mock/") and not allow_mock:
                raise ExportError("Mock (synthetic) labels found; they must not be exported as results.")
            yield "completed", record_from_row(row)
        elif row["status"] == "quarantined":
            yield "quarantined", {"review_id": row["review_id"], "source_sha256": row["source_sha256"],
                                  "status": "quarantined", "reason": row["reason"]}
        else:
            yield "pending", {"review_id": row["review_id"], "source_sha256": row["source_sha256"],
                              "attempts": row["attempts"]}


EXTRA_ROLES = ("verify", "group", "memo")


def _usage(value, cost_source):
    # A request that never left this machine used no tokens; other missing usage stays null (unknown).
    return 0 if value is None and cost_source == "not_sent" else value


def call_lines(conn):
    for row in conn.execute("SELECT * FROM calls ORDER BY started_at, request_id"):
        yield {"request_id": row["request_id"], "role": row["role"], "review_ids": json.loads(row["review_ids"]),
               "model": row["model"], "phase": row["phase"], "outcome": row["outcome"],
               "label_config": row["label_config"], "input_tokens": _usage(row["input_tokens"], row["cost_source"]),
               "output_tokens": _usage(row["output_tokens"], row["cost_source"]), "provider": row["provider"],
               "run_id": row["run_id"], "attempt": row["attempt"], "error": row["error"], "cost_usd": row["cost_usd"],
               "cost_source": row["cost_source"]}


def extra_call_lines(paths, known_ids, allow_mock=False):
    """Calls of the verify/group/memo roles from their saved call logs. Refuses foreign review IDs."""
    for path in paths or ():
        for line in Path(path).read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            call = json.loads(line)
            if call.get("role") not in EXTRA_ROLES:
                raise ExportError(f"{path}: role {call.get('role')!r} is not an exportable model role")
            if call.get("model", "").startswith("mock/") and not allow_mock:
                raise ExportError(f"{path}: mock (synthetic) calls must not be exported")
            unknown = [rid for rid in call.get("review_ids", []) if rid not in known_ids]
            if unknown:
                raise ExportError(f"{path}: review IDs not in this input: {unknown[:3]}")
            for field in ("input_tokens", "output_tokens"):
                call[field] = _usage(call.get(field), call.get("cost_source"))
            yield call


def _jsonl(rows):
    return "".join(json.dumps(r, ensure_ascii=False, sort_keys=True, allow_nan=False) + "\n" for r in rows)


def _write_jsonl(out_dir, name, rows, compress):
    if compress:
        with gzip.open(out_dir / f"{name}.jsonl.gz", "wt", encoding="utf-8") as f:
            f.write(_jsonl(rows))
        (out_dir / f"{name}.jsonl").unlink(missing_ok=True)
    else:
        atomic_write_text(out_dir / f"{name}.jsonl", _jsonl(rows))
        (out_dir / f"{name}.jsonl.gz").unlink(missing_ok=True)


def export(conn, out_dir, claim_selections=None, checkpoint_before=None, checkpoint_after=None,
           allow_mock=False, gzip_records=False, pending_path=None, extra_calls=None, gzip_calls=None):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    final, pending, counts = [], [], {"completed": 0, "quarantined": 0, "pending": 0, "cache_reuse": 0}
    for status, record in iter_final_records(conn, allow_mock):
        counts[status] += 1
        if status == "pending":
            pending.append(record)
            continue
        counts["cache_reuse"] += "cache_source_id" in record
        final.append(record)

    atomic_write_json(out_dir / "run.json", {
        "version": CONTRACT_VERSION,
        "analysis_count": conn.execute("SELECT COUNT(*) FROM reviews").fetchone()[0],
        "analysis_sha256": db.get_meta(conn, "source_sha256"),
        "classification_input_fields": ["review_text"],
        "allow_multi_issue": False,
    })
    _write_jsonl(out_dir, "records", final, gzip_records)

    membership, issues = group.assign(final)
    records_by_id = {r["review_id"]: r for r in final if r["status"] == "completed"}
    ranking = rank.compute(records_by_id, membership)
    group.write_membership(out_dir / "membership.csv", membership)
    rank.write_ranking(out_dir / "ranking.csv", ranking)
    if claim_selections:
        rank.write_claims(out_dir / "claims.csv", rank.claims_for(ranking, claim_selections))
    known = {r[0] for r in conn.execute("SELECT review_id FROM reviews")}
    calls = list(call_lines(conn)) + list(extra_call_lines(extra_calls, known, allow_mock))
    _write_jsonl(out_dir, "calls", calls, gzip_records if gzip_calls is None else gzip_calls)
    for source, name in ((checkpoint_before, "checkpoint_before.json"), (checkpoint_after, "checkpoint_after.json")):
        if source and Path(source).resolve() != (out_dir / name).resolve():
            shutil.copyfile(source, out_dir / name)
    if pending_path:
        atomic_write_text(pending_path, _jsonl(pending))
    return {"counts": counts, "issues": len(issues), "memberships": len(membership), "ranking": ranking,
            "calls_by_role": dict(sorted(Counter(c["role"] for c in calls).items()))}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--out", type=Path, default=REPO_ROOT / "grading")
    parser.add_argument("--claims", type=Path, help="JSON list of [issue_id, metric] pairs cited in the memo")
    parser.add_argument("--checkpoint-before", type=Path)
    parser.add_argument("--checkpoint-after", type=Path)
    parser.add_argument("--gzip", action="store_true", help="Write records.jsonl.gz and calls.jsonl.gz")
    parser.add_argument("--extra-calls", type=Path, nargs="*",
                        help="Saved call logs of the verify/group/memo roles to include in calls.jsonl")
    parser.add_argument("--pending-out", type=Path, default=REPO_ROOT / "outputs" / "analysis" / "pending.jsonl")
    parser.add_argument("--allow-mock", action="store_true", help="Tests only: export synthetic labels")
    args = parser.parse_args(argv)
    selections = [tuple(x) for x in json.loads(args.claims.read_text(encoding="utf-8"))] if args.claims else None
    conn = db.connect(args.db)
    try:
        result = export(conn, args.out, selections, args.checkpoint_before, args.checkpoint_after,
                        args.allow_mock, args.gzip, args.pending_out, args.extra_calls)
    except ExportError as e:
        print("ERROR:", e, file=sys.stderr)
        return 2
    finally:
        conn.close()
    print(json.dumps({k: v for k, v in result.items() if k != "ranking"}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
