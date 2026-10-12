"""Checkpoint snapshots for the interruption/resume demonstration. No model calls.

Usage:
    python3 -m pipeline.checkpoint before --db work/full.sqlite --out grading/checkpoint_before.json
    python3 -m pipeline.checkpoint after  --db work/full.sqlite --out grading/checkpoint_after.json

"before" saves the completed IDs at the interruption point and switches later enrichment
calls in this database to phase "resume". "after" saves the completed IDs after resuming.
"""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

from pipeline import db
from pipeline.common import atomic_write_json
from pipeline.runlock import LockHeld, hold_lock


def snapshot(conn, which):
    ids = [r[0] for r in conn.execute(
        "SELECT review_id FROM reviews WHERE status = 'completed' ORDER BY review_id")]
    configs = sorted({r[0] for r in conn.execute("SELECT DISTINCT label_config FROM labels")})
    data = {"completed_ids": ids, "snapshot": which, "completed_count": len(ids), "label_configs": configs,
            "taken_at": datetime.now(timezone.utc).isoformat(), "statuses": db.status_counts(conn)}
    if which == "before":
        with conn:
            db.set_meta(conn, "enrich_phase", "resume")
            db.set_meta(conn, "checkpoint_before_count", len(ids))
    return data


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("which", choices=["before", "after"])
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        # Refuse while an enrichment run holds the database: a snapshot must not race saved work.
        with hold_lock(args.db, f"pipeline.checkpoint {args.which}"):
            conn = db.connect(args.db)
            try:
                if args.which == "before" and db.get_meta(conn, "enrich_phase") == "resume":
                    print("REFUSED: a 'before' checkpoint was already taken for this database.", file=sys.stderr)
                    return 2
                data = snapshot(conn, args.which)
            finally:
                conn.close()
    except LockHeld as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return 2
    atomic_write_json(args.out, data)
    print(f"Saved {args.which} checkpoint with {data['completed_count']} completed IDs to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
