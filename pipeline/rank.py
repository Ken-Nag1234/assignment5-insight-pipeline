"""Stage 5 (Rank): reproducible baseline ranking from saved outputs. No model calls.

Usage (regenerates ranking.csv and aggregates.csv from saved records and membership):
    python3 -m pipeline.rank --records grading/records.jsonl.gz --membership grading/membership.csv --out grading

Baseline (GRADING_CONTRACT.md): complaint_count = membership count; severity_sum = sum of member
severity; mean_severity = severity_sum / complaint_count, six decimals, half-up; priority_score =
severity_sum (exactly count x mean). Order: descending score, then ascending issue_id; ranks from 1.
"""

import argparse
import csv
import gzip
import json
import sys
from collections import defaultdict
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

from pipeline.common import atomic_write_text

RANKING_FIELDS = ["rank", "issue_id", "complaint_count", "severity_sum", "mean_severity", "priority_score"]
METRICS = ("complaint_count", "severity_sum", "mean_severity", "priority_score")


def mean_string(total, count):
    return str((Decimal(total) / Decimal(count)).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP))


def compute(records, membership):
    """records: dict review_id -> record; membership: iterable of (issue_id, review_id)."""
    severities = defaultdict(list)
    seen = set()
    for issue_id, review_id in membership:
        if (issue_id, review_id) in seen:
            raise ValueError(f"Duplicate membership ({issue_id}, {review_id})")
        seen.add((issue_id, review_id))
        record = records[review_id]
        if record["intent"] not in ("complaint", "cancellation"):
            raise ValueError(f"Non-complaint record in ranking: {review_id}")
        severities[issue_id].append(record["severity"])
    rows = [{"issue_id": iid, "complaint_count": len(v), "severity_sum": sum(v),
             "mean_severity": mean_string(sum(v), len(v)), "priority_score": sum(v)}
            for iid, v in severities.items()]
    rows.sort(key=lambda r: (-r["priority_score"], r["issue_id"]))
    for rank, row in enumerate(rows, 1):
        row["rank"] = rank
    return rows


def read_records(path):
    path = Path(path)
    opener = gzip.open if path.suffix == ".gz" else open
    records = {}
    with opener(path, "rt", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                record = json.loads(line)
                if record.get("status") == "completed":
                    records[record["review_id"]] = record
    return records


def read_membership(path):
    with Path(path).open(encoding="utf-8", newline="") as f:
        return [(row["issue_id"], row["review_id"]) for row in csv.DictReader(f)]


def write_ranking(path, rows):
    lines = [",".join(RANKING_FIELDS) + "\n"]
    lines += [",".join(str(row[k]) for k in RANKING_FIELDS) + "\n" for row in rows]
    atomic_write_text(path, "".join(lines))


def write_aggregates(path, rows, records, membership):
    """Ranking plus descriptive counts (cancellations, needs_review) for analysis and the dashboard."""
    extra = defaultdict(lambda: {"cancellation_count": 0, "needs_review_count": 0})
    for issue_id, review_id in membership:
        record = records[review_id]
        extra[issue_id]["cancellation_count"] += record["intent"] == "cancellation"
        extra[issue_id]["needs_review_count"] += bool(record["needs_review"])
    fields = RANKING_FIELDS + ["cancellation_count", "needs_review_count"]
    lines = [",".join(fields) + "\n"]
    for row in rows:
        merged = {**row, **extra[row["issue_id"]]}
        lines.append(",".join(str(merged[k]) for k in fields) + "\n")
    atomic_write_text(path, "".join(lines))


def claims_for(rows, selections):
    """Build claims.csv rows for (issue_id, metric) selections, values taken from the ranking."""
    by_id = {r["issue_id"]: r for r in rows}
    claims = []
    for n, (issue_id, metric) in enumerate(selections, 1):
        if metric not in METRICS:
            raise ValueError(f"Unsupported claim metric {metric}")
        claims.append({"claim_id": f"C{n:03d}", "issue_id": issue_id, "metric": metric,
                       "value": str(by_id[issue_id][metric])})
    return claims


def write_claims(path, claims):
    lines = ["claim_id,issue_id,metric,value\n"]
    lines += [f"{c['claim_id']},{c['issue_id']},{c['metric']},{c['value']}\n" for c in claims]
    atomic_write_text(path, "".join(lines))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--records", type=Path, required=True)
    parser.add_argument("--membership", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    records = read_records(args.records)
    membership = read_membership(args.membership)
    rows = compute(records, membership)
    write_ranking(args.out / "ranking.csv", rows)
    write_aggregates(args.out / "aggregates.csv", rows, records, membership)
    print(f"Ranked {len(rows)} issues from {len(membership)} memberships")
    for row in rows[:5]:
        print(f"  {row['rank']}. {row['issue_id']} score={row['priority_score']} count={row['complaint_count']} "
              f"mean={row['mean_severity']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
