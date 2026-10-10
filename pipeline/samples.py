"""Register and verify the fixed development samples against the ingested full file. No model calls.

Usage:
    python3 -m pipeline.samples --manifest manifest.json

Checks every sample file's checksum, that every sample row is byte-identical to the
full-file row (same grading row hash), the documented nesting
(cost_100 = first 100 of checkpoint_500 = first 500 of analysis_10000), and that the
golden 50 is disjoint from all development samples.

Golden human-label columns are never read into the database: only the six source
fields are used, so golden answers cannot leak into prompts or tuning inputs.
"""

import argparse
import json
import sys
from pathlib import Path

from pipeline import db
from pipeline.common import FIELDS, REPO_ROOT, atomic_write_json, csv_rows, file_sha256, row_sha

SAMPLES = {
    "cost_100": "cost_100.csv",
    "checkpoint_500": "checkpoint_500.csv",
    "analysis_10000": "analysis_10000.csv",
    "golden_50": "golden_50_to_label.csv",
}
DEV_SAMPLES = ("cost_100", "checkpoint_500", "analysis_10000")


def read_sample(path):
    ids = []
    hashes = {}
    for row in csv_rows(path):
        source = {k: row[k] for k in FIELDS}  # drop any label columns
        ids.append(source["review_id"])
        hashes[source["review_id"]] = row_sha(source)
    return ids, hashes


def verify_samples(db_path, data_dir, manifest_path, out_path):
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    conn = db.connect(db_path)
    checks = []

    def check(name, expected, actual):
        checks.append({"check": name, "expected": expected, "actual": actual, "pass": expected == actual})

    try:
        if not db.get_meta(conn, "source_sha256"):
            raise SystemExit("ERROR: run pipeline.ingest on the full CSV first.")
        ids = {}
        with conn:
            conn.execute("DELETE FROM sample_membership")
            for name, filename in SAMPLES.items():
                path = Path(data_dir) / filename
                check(f"{name}.sha256", manifest["files"][filename]["sha256"], file_sha256(path))
                sample_ids, hashes = read_sample(path)
                ids[name] = sample_ids
                check(f"{name}.unique_ids", len(sample_ids), len(set(sample_ids)))
                mismatched, missing = 0, 0
                for position, rid in enumerate(sample_ids, 1):
                    found = conn.execute("SELECT source_sha256 FROM reviews WHERE review_id = ?", (rid,)).fetchone()
                    if found is None:
                        missing += 1
                        continue
                    mismatched += found[0] != hashes[rid]
                    conn.execute("INSERT INTO sample_membership (sample, review_id, sample_row) VALUES (?, ?, ?)",
                                 (name, rid, position))
                check(f"{name}.ids_missing_from_full_file", 0, missing)
                check(f"{name}.rows_differing_from_full_file", 0, mismatched)

        check("rows.cost_100", 100, len(ids["cost_100"]))
        check("rows.checkpoint_500", 500, len(ids["checkpoint_500"]))
        check("rows.analysis_10000", 10000, len(ids["analysis_10000"]))
        check("rows.golden_50", 50, len(ids["golden_50"]))
        check("cost_100 == first 100 of checkpoint_500", True, ids["cost_100"] == ids["checkpoint_500"][:100])
        check("checkpoint_500 == first 500 of analysis_10000", True,
              ids["checkpoint_500"] == ids["analysis_10000"][:500])
        dev_ids = set().union(*(ids[s] for s in DEV_SAMPLES))
        check("golden_50 disjoint from development samples", 0, len(set(ids["golden_50"]) & dev_ids))
        status_of_golden = dict(conn.execute(
            "SELECT r.status, COUNT(*) FROM sample_membership m JOIN reviews r USING (review_id)"
            " WHERE m.sample = 'golden_50' GROUP BY r.status").fetchall())
        check("golden_50 texts remain in the full run (nonempty, queued)", 50,
              status_of_golden.get("pending", 0) + status_of_golden.get("completed", 0))
    finally:
        conn.close()

    report = {"stage": "sample_verification", "model_calls": 0,
              "samples": {name: {"file": SAMPLES[name], "rows": len(v)} for name, v in ids.items()},
              "golden_label_columns_loaded": False,
              "checks": checks, "all_checks_pass": all(c["pass"] for c in checks)}
    atomic_write_json(out_path, report)
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", type=Path, default=REPO_ROOT / "work" / "pipeline.sqlite")
    parser.add_argument("--data-dir", type=Path, default=REPO_ROOT)
    parser.add_argument("--manifest", type=Path, default=REPO_ROOT / "manifest.json")
    parser.add_argument("--out", type=Path, default=REPO_ROOT / "outputs" / "ingestion" / "samples_report.json")
    args = parser.parse_args(argv)
    report = verify_samples(args.db, args.data_dir, args.manifest, args.out)
    failed = [c for c in report["checks"] if not c["pass"]]
    print(f"sample checks passed={len(report['checks']) - len(failed)}/{len(report['checks'])}")
    for c in failed:
        print(f"FAILED {c['check']}: expected={c['expected']!r} actual={c['actual']!r}", file=sys.stderr)
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
