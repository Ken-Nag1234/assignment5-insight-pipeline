"""Final-run workflow: stage every deliverable in a new directory, self-check it, then promote.

Nothing here calls a model. Every step writes only inside a fresh staging directory
(work/final/<stamp>/) until `promote`, which copies into grading/ and outputs/analysis/ and refuses
to overwrite any existing file that differs. Existing checkpoints, ledgers and run logs are only read.

Steps (run in order after enrichment has stopped; each refuses while an enrichment run holds the lock):
    python3 -m pipeline.finalize analysis            # -> work/final/<stamp>/analysis (membership, ranking, weighted)
    # paid, separately approved (bounded text-model calls on saved aggregates):
    python3 -m pipeline.insights name-issues --analysis work/final/<stamp>/analysis ... --execute
    python3 -m pipeline.insights memo        --analysis work/final/<stamp>/analysis ... --execute
    python3 -m pipeline.finalize export   --stamp <stamp>   # grading/ export + provided checker audit
    python3 -m pipeline.finalize serving  --stamp <stamp>   # dashboard serving database (SQLite)
    python3 -m pipeline.finalize promote  --stamp <stamp>   # copy into grading/ and outputs/analysis/

The checker audit uses the provided check_submission.py unmodified: it builds the reference from the
full source CSV (the same file serves as the analysis file for a full-corpus run) and audits the staged
grading folder. Its report is saved next to the export so that the README can link it.
"""

import argparse
import filecmp
import json
import shutil
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

from pipeline import group, rank, runlock, serving, trends, weighted
from pipeline.common import REPO_ROOT, atomic_write_json

FINAL_ROOT = REPO_ROOT / "work" / "final"
DB_PATH = REPO_ROOT / "work" / "pipeline.sqlite"
SOURCE_CSV = REPO_ROOT / "spotify_reviews_18months.csv"
CHECKPOINTS = REPO_ROOT / "outputs" / "checkpoints"
GRADING = REPO_ROOT / "grading"
ANALYSIS = REPO_ROOT / "outputs" / "analysis"
VERIFY_DIR = REPO_ROOT / "outputs" / "verify"


class FinalizeError(Exception):
    pass


def require_idle(db_path):
    if runlock.is_locked(Path(db_path)):
        raise FinalizeError(f"an enrichment run holds {db_path}.lock; finalize after it stops")


def stage_dir(stamp, root=FINAL_ROOT):
    path = Path(root) / stamp
    if not path.is_dir():
        raise FinalizeError(f"no staging directory {path}; run `analysis` first")
    return path


def build_analysis(db_path=DB_PATH, root=FINAL_ROOT, stamp=None, allow_mock=False):
    require_idle(db_path)
    stamp = stamp or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = Path(root) / stamp / "analysis"
    if out.exists():
        raise FinalizeError(f"{out} already exists")
    out.mkdir(parents=True)
    conn = sqlite3.connect(f"{Path(db_path).resolve().as_uri()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        records = list(group.records_from_db(conn, exclude_mock=not allow_mock))
        status = dict(conn.execute("SELECT status, COUNT(*) FROM reviews GROUP BY status").fetchall())
    finally:
        conn.close()
    membership, issues = group.assign(records)
    by_id = {r["review_id"]: r for r in records}
    ranking = rank.compute(by_id, membership)
    group.write_membership(out / "membership.csv", membership)
    group.write_issues(out / "issues.csv", issues)
    rank.write_ranking(out / "ranking.csv", ranking)
    rank.write_aggregates(out / "aggregates.csv", ranking, by_id, membership)
    atomic_write_json(out / "weighted_comparison.json", weighted.compare(db_path, out))
    trends.write(db_path, out)
    # evaluation summaries the memo may cite as facts (aggregates only; per-case human labels and
    # review texts are never copied into publishable outputs)
    verify_report = VERIFY_DIR / "report.json"
    if verify_report.exists():
        report = json.loads(verify_report.read_text(encoding="utf-8"))
        atomic_write_json(out / "verify_report.json", {k: v for k, v in report.items() if k != "disagreements"})
    golden_report = REPO_ROOT / "work" / "golden_eval" / "report.json"
    if golden_report.exists():
        metrics = json.loads(golden_report.read_text(encoding="utf-8"))["metrics"]
        atomic_write_json(out / "golden_report.json", {"source": "pipeline.golden_eval (aggregate only)", "metrics": {
            field: {k: v for k, v in m.items() if k in ("accuracy_accepted", "accuracy_strict", "macro_f1_accepted",
                                                        "macro_f1_strict")}
            for field, m in metrics.items() if isinstance(m, dict)}})
    summary = {"stamp": stamp, "db": str(Path(db_path).name), "statuses": status, "completed_records": len(records),
               "memberships": len(membership), "issues": len(issues),
               "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    atomic_write_json(out.parent / "analysis_summary.json", summary)
    return summary


def build_export(stamp, db_path=DB_PATH, root=FINAL_ROOT, source_csv=SOURCE_CSV, checkpoints=CHECKPOINTS,
                 ingestion=GRADING / "ingestion.json", allow_mock=False, verify_dir=VERIFY_DIR):
    import check_submission
    from pipeline import db, export
    require_idle(db_path)
    base = stage_dir(stamp, root)
    analysis, out = base / "analysis", base / "grading"
    if out.exists():
        raise FinalizeError(f"{out} already exists")
    claims = analysis / "claims_selection.json"
    selections = [tuple(x) for x in json.loads(claims.read_text())] if claims.exists() else None
    extra = [p for p in (analysis / "model_calls.jsonl", Path(verify_dir) / "calls.jsonl") if p.exists()]
    conn = db.connect(db_path)
    try:
        result = export.export(conn, out, selections, Path(checkpoints) / "checkpoint_before.json",
                               Path(checkpoints) / "checkpoint_after.json", allow_mock, gzip_records=True,
                               pending_path=base / "pending_ids.jsonl", extra_calls=extra)
    finally:
        conn.close()
    shutil.copyfile(ingestion, out / "ingestion.json")
    reference = check_submission.reference(source_csv, source_csv)
    audit = check_submission.audit(out, reference)
    atomic_write_json(base / "check_report.json", audit)
    summary = {"export": {k: v for k, v in result.items() if k != "ranking"}, "claims_included": bool(selections),
               "extra_call_logs": [str(p.relative_to(REPO_ROOT)) if p.is_relative_to(REPO_ROOT) else str(p) for p in extra],
               "check_report": "check_report.json"}
    atomic_write_json(base / "export_summary.json", summary)
    write_manifest(stamp, db_path, root)
    return summary, audit


def code_version():
    """Content hash of the pipeline and dashboard code (stable without a git commit), plus git HEAD if any."""
    import hashlib
    import subprocess
    digest = hashlib.sha256()
    for path in sorted(list((REPO_ROOT / "pipeline").glob("*.py")) + list((REPO_ROOT / "dashboard").rglob("*.py"))
                       + [REPO_ROOT / "dashboard" / "static" / "index.html"]):
        digest.update(path.relative_to(REPO_ROOT).as_posix().encode() + b"\0" + path.read_bytes() + b"\0")
    try:
        head = subprocess.run(["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"], capture_output=True, text=True,
                              timeout=10).stdout.strip() or None
    except (OSError, subprocess.SubprocessError):
        head = None
    return {"code_sha256": digest.hexdigest(), "git_head": head}


def write_manifest(stamp, db_path=DB_PATH, root=FINAL_ROOT):
    """Tie source checksum, code version, prompts, models, settings and output hashes together."""
    from pipeline import extract, insights, labels, verify
    from pipeline.common import file_sha256
    base = stage_dir(stamp, root)
    conn = sqlite3.connect(f"{Path(db_path).resolve().as_uri()}?mode=ro", uri=True)
    try:
        meta = dict(conn.execute("SELECT key, value FROM meta"))
        label_configs = dict(conn.execute("SELECT label_config, COUNT(*) FROM labels GROUP BY label_config"))
        models = sorted({m for (m,) in conn.execute("SELECT DISTINCT model FROM calls")})
        runs = sorted({r for (r,) in conn.execute("SELECT DISTINCT run_id FROM calls")})
    finally:
        conn.close()
    outputs = {}
    for sub in ("analysis", "grading"):
        for path in sorted((base / sub).rglob("*")) if (base / sub).exists() else []:
            if path.is_file() and "cache" not in path.parts:
                outputs[path.relative_to(base).as_posix()] = file_sha256(path)
    text_calls = verify.load_jsonl(base / "analysis" / "model_calls.jsonl")
    manifest = {
        "run_id": f"final-{stamp}",
        "source": {k: meta.get(k) for k in ("source_name", "source_sha256", "source_bytes", "ingest_code_version")},
        "code": code_version(),
        "prompts": {"enrich": labels.PROMPT_VERSION, "schema": labels.SCHEMA_VERSION,
                    "extraction": extract.EXTRACTION_VERSION, "verify": verify.VERIFY_PROMPT_VERSION,
                    "naming": insights.NAMING_PROMPT_VERSION, "memo": insights.MEMO_PROMPT_VERSION,
                    "grouping": group.GROUPING_VERSION},
        "label_configs": label_configs,
        "models": {"enrich_and_verify": models, "text": sorted({c.get("model") for c in text_calls if c.get("model")})},
        "settings": {"enrich": "1 review per request, exact-text cache, shared cumulative spend cap",
                     "text": sorted({c.get("label_config") for c in text_calls if c.get("label_config")})},
        "enrichment_run_ids": runs,
        "outputs_sha256": outputs,
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    atomic_write_json(base / "analysis" / "run_manifest.json", manifest)
    return manifest


def build_serving(stamp, db_path=DB_PATH, root=FINAL_ROOT, allow_mock=False):
    base = stage_dir(stamp, root)
    tables = serving.collect(db_path, base / "analysis", allow_mock=allow_mock, all_records=True,
                             claims_csv=base / "grading" / "claims.csv")
    return serving.write_sqlite(base / "serving.sqlite", tables), {t: len(tables[t]) for t in serving.TABLES}


def promote(stamp, root=FINAL_ROOT, grading_dir=GRADING, analysis_dir=ANALYSIS):
    """Copy staged files; refuse if any destination file exists with different content."""
    base = stage_dir(stamp, root)
    plan = []
    for src_dir, dst_dir in ((base / "grading", Path(grading_dir)), (base / "analysis", Path(analysis_dir))):
        if not src_dir.is_dir():
            raise FinalizeError(f"{src_dir} is missing")
        for src in sorted(p for p in src_dir.rglob("*") if p.is_file() and "cache" not in p.relative_to(src_dir).parts):
            dst = dst_dir / src.relative_to(src_dir)
            if dst.exists() and not filecmp.cmp(src, dst, shallow=False):
                raise FinalizeError(f"refusing to overwrite {dst} (differs from the staged file)")
            plan.append((src, dst))
    for src, dst in plan:
        dst.parent.mkdir(parents=True, exist_ok=True)
        if not dst.exists():
            shutil.copyfile(src, dst)
    return [str(dst) for _, dst in plan]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("step", choices=["analysis", "export", "serving", "promote"])
    parser.add_argument("--stamp")
    parser.add_argument("--db", type=Path, default=DB_PATH)
    args = parser.parse_args(argv)
    try:
        if args.step == "analysis":
            print(json.dumps(build_analysis(args.db, stamp=args.stamp), indent=2))
            return 0
        if not args.stamp:
            raise FinalizeError("--stamp is required")
        if args.step == "export":
            summary, audit = build_export(args.stamp, args.db)
            print(json.dumps(summary, indent=2, default=str))
            print(json.dumps(audit, indent=2, default=str)[:4000])
        elif args.step == "serving":
            path, counts = build_serving(args.stamp, args.db)
            print("wrote", path, json.dumps(counts))
        else:
            print("\n".join(promote(args.stamp)))
    except FinalizeError as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
