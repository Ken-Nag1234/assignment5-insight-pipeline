"""Stage 1 (Prepare): ingest, profile and queue an input CSV. No model calls.

Usage:
    python3 -m pipeline.ingest --input spotify_reviews_18months.csv --manifest manifest.json

Outputs:
    <out>/ingestion_report.json  full-file profile, data-quality checks, text dedup stats
    <out>/data_manifest.json     source identity, code hashes and output hashes for this run
    <out>/quarantine.jsonl       genuinely quarantined records (empty text) with reasons
    <grading>/ingestion.json     exact output of the provided profile helper (grading contract)

Nonempty reviews are queued as status "pending"; they are never quarantined
just because they have not been classified yet.
"""

import argparse
import platform
import statistics
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from pipeline import db
from pipeline.common import (EMPTY_TEXT_REASON, FIELDS, REPO_ROOT, VALID_RATINGS, atomic_write_json,
                             atomic_write_jsonl, csv_rows, file_sha256, grading_profile, is_empty_text,
                             provided, row_sha, text_sha256)

CODE_VERSION = "ingest-v1"
BATCH_ROWS = 5000


class IngestError(Exception):
    pass


def _parse_timestamp(value):
    try:
        datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        return True
    except ValueError:
        return False


def _percentile(sorted_values, q):
    if not sorted_values:
        return None
    index = min(len(sorted_values) - 1, max(0, round(q * (len(sorted_values) - 1))))
    return sorted_values[index]


def _text_stats(stats, group_sizes, lengths):
    lengths = sorted(lengths)
    dup_groups = [n for n in group_sizes.values() if n > 1]
    return {
        "nonempty_rows": stats["nonempty_review_text"],
        "empty_rows_quarantined": stats["empty_review_text"],
        "distinct_nonempty_texts": len(group_sizes),
        "rows_reusable_via_exact_text_cache": stats["nonempty_review_text"] - len(group_sizes),
        "duplicate_text_groups": len(dup_groups),
        "rows_in_duplicate_text_groups": sum(dup_groups),
        "largest_duplicate_text_group": max(dup_groups, default=0),
        "length_chars": {
            "mean": round(statistics.fmean(lengths), 2) if lengths else None,
            "p50": _percentile(lengths, 0.50), "p90": _percentile(lengths, 0.90),
            "p99": _percentile(lengths, 0.99), "max": lengths[-1] if lengths else None,
            "total": sum(lengths),
        },
    }


def load_rows(conn, input_path):
    """Stream the CSV into SQLite. Returns per-file statistics gathered in the same pass."""
    stats = Counter()
    duplicate_ids = []
    seen_ids = set()
    text_rep = {}  # text_sha256 -> first review_id with that exact text
    group_sizes = Counter()
    lengths = []
    batch = []
    insert = ("INSERT INTO reviews (review_id, source_row, review_text, review_rating, review_likes, app_version,"
              " review_timestamp, source_sha256, text_sha256, text_rep_id, status, reason)"
              " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

    with open(input_path, encoding="utf-8-sig", newline="") as f:
        header = f.readline().rstrip("\r\n").split(",")
    if header != list(FIELDS):
        raise IngestError(f"Unexpected CSV header {header!r}; expected {list(FIELDS)!r}")

    for source_row, row in enumerate(csv_rows(input_path), 1):
        stats["records"] += 1
        rid = row["review_id"]
        if not rid:
            stats["blank_review_id"] += 1
        if rid in seen_ids:
            # Keep the first occurrence; later duplicates are reported, not silently merged.
            duplicate_ids.append({"review_id": rid, "source_row": source_row})
            continue
        seen_ids.add(rid)

        text = row["review_text"]
        stats["invalid_rating"] += row["review_rating"] not in VALID_RATINGS
        stats["missing_app_version"] += not row["app_version"].strip()
        stats["nonnumeric_likes"] += not row["review_likes"].isdigit()
        stats["unparseable_timestamp"] += not _parse_timestamp(row["review_timestamp"])
        stats["multiline_text"] += ("\n" in text or "\r" in text)

        if is_empty_text(text):
            stats["empty_review_text"] += 1
            t_sha, rep, status, reason = None, None, "quarantined", EMPTY_TEXT_REASON
        else:
            stats["nonempty_review_text"] += 1
            t_sha = text_sha256(text)
            rep = text_rep.setdefault(t_sha, rid)
            group_sizes[t_sha] += 1
            lengths.append(len(text))
            status, reason = "pending", None

        batch.append((rid, source_row, text, row["review_rating"], row["review_likes"], row["app_version"],
                      row["review_timestamp"], row_sha(row), t_sha, rep, status, reason))
        if len(batch) >= BATCH_ROWS:
            conn.executemany(insert, batch)
            batch.clear()
    if batch:
        conn.executemany(insert, batch)

    text_stats = _text_stats(stats, group_sizes, lengths)
    return stats, duplicate_ids, text_stats


def manifest_checks(manifest_path, input_path, file_bytes, file_hash, profile, text_stats):
    """Compare the profile with the dataset manifest, when the input is a manifest-listed file."""
    import json
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    checks = []

    def check(name, expected, actual):
        checks.append({"check": name, "expected": expected, "actual": actual, "pass": expected == actual})

    entry = manifest.get("files", {}).get(Path(input_path).name)
    if entry is None:
        return [{"check": "input_listed_in_manifest", "expected": True, "actual": False, "pass": False}]
    check("file_sha256", entry["sha256"], file_hash)
    check("file_bytes", entry["bytes"], file_bytes)
    if Path(input_path).name == manifest.get("assignment_scope", {}).get("final_input"):
        expected_profile = manifest["profile"]
        for key in ("records", "duplicate_review_ids", "empty_review_text", "missing_app_version"):
            check("profile." + key, expected_profile[key], profile["counts"][key])
        check("reviews_by_month", manifest["reviews_by_month"], dict(sorted(profile["reviews_by_month"].items())))
        check("reviews_by_rating", manifest["reviews_by_rating"], dict(sorted(profile["reviews_by_rating"].items())))
        check("first_review", manifest["window"]["first_review"], profile["first_review"])
        check("last_review", manifest["window"]["last_review"], profile["last_review"])
        scope = manifest["assignment_scope"]
        check("nonempty_to_classify", scope["nonempty_to_classify"], text_stats["nonempty_rows"])
        check("empty_text_quarantines", scope["empty_text_quarantines"], text_stats["empty_rows_quarantined"])
        # Published in GRADING_CONTRACT.md / COST_CALCULATOR.md rather than manifest.json.
        check("distinct_nonempty_texts (assignment docs)", 484189, text_stats["distinct_nonempty_texts"])
    return checks


def _display_path(path):
    """Repository-relative path for saved artifacts, so local home directories are not published."""
    path = Path(path).resolve()
    return str(path.relative_to(REPO_ROOT)) if path.is_relative_to(REPO_ROOT) else path.name


def ingest(input_path, db_path, out_dir, grading_dir, manifest_path=None, rebuild=False):
    started = time.monotonic()
    started_at = datetime.now(timezone.utc)
    input_path, out_dir, grading_dir = Path(input_path), Path(out_dir), Path(grading_dir)
    file_hash = file_sha256(input_path)
    file_bytes = input_path.stat().st_size

    conn = db.connect(db_path)
    try:
        existing = db.get_meta(conn, "source_sha256")
        if existing and existing != file_hash:
            raise IngestError(f"{db_path} already holds a different input ({existing[:12]}...). "
                              "Use a separate --db for each input file.")
        if existing and rebuild:
            if conn.execute("SELECT COUNT(*) FROM reviews WHERE status = 'completed'").fetchone()[0]:
                raise IngestError("Refusing --rebuild: the database contains completed classifications.")
            conn.execute("DELETE FROM sample_membership")
            conn.execute("DELETE FROM reviews")
            conn.execute("DELETE FROM meta")
            conn.commit()
            existing = None

        if existing:
            # Re-running is idempotent: keep all saved statuses and only regenerate reports.
            stats, duplicate_ids, text_stats = _stats_from_db(conn)
            loaded = False
        else:
            with conn:
                stats, duplicate_ids, text_stats = load_rows(conn, input_path)
                db.set_meta(conn, "schema_version", db.SCHEMA_VERSION)
                db.set_meta(conn, "source_sha256", file_hash)
                db.set_meta(conn, "source_name", input_path.name)
                db.set_meta(conn, "source_bytes", file_bytes)
                db.set_meta(conn, "ingest_code_version", CODE_VERSION)
                db.set_meta(conn, "duplicate_review_ids", len(duplicate_ids))
                db.set_meta(conn, "ingested_at", started_at.isoformat())
            loaded = True

        load_seconds = round(time.monotonic() - started, 3)
        profile = grading_profile(input_path)  # exact grading-contract profile (second full read)
        statuses = db.status_counts(conn)
        unique_ids = conn.execute("SELECT COUNT(*) FROM reviews").fetchone()[0]

        checks = [
            {"check": "profile.records == rows read", "expected": profile["counts"]["records"],
             "actual": stats["records"], "pass": profile["counts"]["records"] == stats["records"]},
            {"check": "profile.empty_review_text == quarantined empty rows", "expected": profile["counts"]["empty_review_text"],
             "actual": text_stats["empty_rows_quarantined"],
             "pass": profile["counts"]["empty_review_text"] == text_stats["empty_rows_quarantined"]},
            {"check": "every unique source ID has exactly one status", "expected": unique_ids,
             "actual": sum(statuses.values()), "pass": unique_ids == sum(statuses.values())},
            {"check": "pending == nonempty rows", "expected": text_stats["nonempty_rows"],
             "actual": statuses["pending"] + statuses["completed"],
             "pass": text_stats["nonempty_rows"] == statuses["pending"] + statuses["completed"]},
            {"check": "no quarantine other than empty_review_text at ingestion",
             "expected": 0, "actual": conn.execute(
                 "SELECT COUNT(*) FROM reviews WHERE status = 'quarantined' AND reason != ?",
                 (EMPTY_TEXT_REASON,)).fetchone()[0]},
        ]
        checks[-1]["pass"] = checks[-1]["actual"] == 0
        if manifest_path:
            checks += manifest_checks(manifest_path, input_path, file_bytes, file_hash, profile, text_stats)

        finished_at = datetime.now(timezone.utc)
        run_id = f"ingest-{started_at.strftime('%Y%m%dT%H%M%SZ')}-{file_hash[:8]}"
        report = {
            "run_id": run_id,
            "stage": "ingest",
            "code_version": CODE_VERSION,
            "model_calls": 0,
            "loaded_this_run": loaded,
            "source": {"name": input_path.name, "bytes": file_bytes, "sha256": file_hash,
                       "parsed_rows_sha256": profile["parsed_rows_sha256"]},
            "profile": profile,
            "field_quality": {
                "records": stats["records"],
                "unique_review_ids": unique_ids,
                "duplicate_review_ids": len(duplicate_ids),
                "duplicate_review_id_examples": duplicate_ids[:10],
                "blank_review_id": stats["blank_review_id"],
                "empty_review_text": stats["empty_review_text"],
                "missing_app_version": stats["missing_app_version"],
                "invalid_rating": stats["invalid_rating"],
                "nonnumeric_likes": stats["nonnumeric_likes"],
                "unparseable_timestamp": stats["unparseable_timestamp"],
                "multiline_text": stats["multiline_text"],
            },
            "text": text_stats,
            "statuses": statuses,
            "checks": checks,
            "all_checks_pass": all(c["pass"] for c in checks),
            "timing": {"started_at": started_at.isoformat(), "finished_at": finished_at.isoformat(),
                       "load_seconds": load_seconds,
                       "total_seconds": round(time.monotonic() - started, 3)},
            "notes": [
                "Nonempty reviews are queued as 'pending'; pending is not a quarantine and not a completed classification.",
                "Only empty or whitespace-only texts are quarantined at ingestion (reason empty_review_text).",
                "Missing app_version and star ratings do not affect classification eligibility.",
            ],
        }

        quarantined = [{"review_id": rid, "source_sha256": sha, "source_row": srow, "status": "quarantined",
                        "reason": reason, "attempts": attempts, "stage": "ingest"}
                       for rid, sha, srow, reason, attempts in conn.execute(
                           "SELECT review_id, source_sha256, source_row, reason, attempts FROM reviews"
                           " WHERE status = 'quarantined' ORDER BY source_row")]

        atomic_write_json(grading_dir / "ingestion.json", profile)
        atomic_write_json(out_dir / "ingestion_report.json", report)
        atomic_write_jsonl(out_dir / "quarantine.jsonl", quarantined)

        outputs = [grading_dir / "ingestion.json", out_dir / "ingestion_report.json", out_dir / "quarantine.jsonl"]
        code_files = sorted((REPO_ROOT / "pipeline").glob("*.py"))
        data_manifest = {
            "run_id": run_id,
            "source": report["source"],
            "database": {"path": _display_path(db_path), "schema_version": db.SCHEMA_VERSION},
            "provided_helper": {"file": "check_submission.py", "sha256": file_sha256(provided.__file__),
                                "functions_used": ["row_sha", "csv_rows", "profile", "sha"]},
            "code": {str(p.relative_to(REPO_ROOT)): file_sha256(p) for p in code_files},
            "outputs": {_display_path(p): file_sha256(p) for p in outputs},
            "environment": {"python": sys.version.split()[0], "platform": platform.platform()},
        }
        atomic_write_json(out_dir / "data_manifest.json", data_manifest)
        return report
    finally:
        conn.close()


def _stats_from_db(conn):
    """Rebuild the single-pass statistics from saved rows (used on idempotent re-runs)."""
    stats = Counter()
    group_sizes = Counter()
    lengths = []
    for text, rating, likes, version, ts, t_sha in conn.execute(
            "SELECT review_text, review_rating, review_likes, app_version, review_timestamp, text_sha256 FROM reviews"):
        stats["records"] += 1
        stats["invalid_rating"] += rating not in VALID_RATINGS
        stats["missing_app_version"] += not version.strip()
        stats["nonnumeric_likes"] += not likes.isdigit()
        stats["unparseable_timestamp"] += not _parse_timestamp(ts)
        stats["multiline_text"] += ("\n" in text or "\r" in text)
        if t_sha is None:
            stats["empty_review_text"] += 1
        else:
            stats["nonempty_review_text"] += 1
            group_sizes[t_sha] += 1
            lengths.append(len(text))
    stats["records"] += int(db.get_meta(conn, "duplicate_review_ids", "0"))
    text_stats = _text_stats(stats, group_sizes, lengths)
    return stats, [], text_stats


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", required=True, type=Path, help="Input CSV path")
    parser.add_argument("--db", type=Path, default=REPO_ROOT / "work" / "pipeline.sqlite")
    parser.add_argument("--out", type=Path, default=REPO_ROOT / "outputs" / "ingestion")
    parser.add_argument("--grading-dir", type=Path, default=REPO_ROOT / "grading")
    parser.add_argument("--manifest", type=Path, help="Optional dataset manifest.json to verify against")
    parser.add_argument("--rebuild", action="store_true",
                        help="Reload the input from scratch (refused if any classification is completed)")
    args = parser.parse_args(argv)
    try:
        report = ingest(args.input, args.db, args.out, args.grading_dir, args.manifest, args.rebuild)
    except IngestError as e:
        print("ERROR:", e, file=sys.stderr)
        return 2
    failed = [c for c in report["checks"] if not c["pass"]]
    print(f"run_id={report['run_id']} records={report['field_quality']['records']} statuses={report['statuses']}")
    print(f"distinct_nonempty_texts={report['text']['distinct_nonempty_texts']} "
          f"checks_passed={len(report['checks']) - len(failed)}/{len(report['checks'])}")
    for c in failed:
        print(f"FAILED {c['check']}: expected={c['expected']!r} actual={c['actual']!r}", file=sys.stderr)
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
