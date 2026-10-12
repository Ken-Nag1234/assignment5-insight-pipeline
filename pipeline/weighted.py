"""Stratum-weighted sensitivity check for the baseline ranking. No model calls; read-only on the DB.

Usage:
    python3 -m pipeline.weighted --db work/pipeline.sqlite --analysis outputs/analysis
    python3 -m pipeline.weighted --db work/pipeline.sqlite --analysis outputs/analysis_preview_10000 --sample analysis_10000

Why: the full run classifies every review whose exact text occurs more than once (the duplicates
stratum, mostly short generic texts) but only a seeded random part of the reviews whose text occurs
once (singletons). Unweighted counts therefore over-represent duplicated texts. This module
reweights each completed review by population / completed within its stratum (Horvitz-Thompson
style), recomputes complaint counts and severity sums per issue with the same membership, and
compares the weighted order with the baseline. The baseline ranking (GRADING_CONTRACT.md) is
unchanged; this is a disclosed sensitivity analysis only.
"""

import argparse
import csv
import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

from pipeline.common import atomic_write_json

METHOD = ("Stratum weights = nonempty reviews / completed reviews within each exact-text stratum "
          "(duplicated text vs. text occurring once); weighted rank orders issues by weighted severity sum, "
          "then issue_id. The baseline ranking is unchanged.")


def _read_csv(path):
    with Path(path).open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def strata(conn, sample=None):
    """Return ({review_id: stratum} for completed reviews, {stratum: (population, completed)})."""
    scope, params = "", ()
    if sample:
        scope = " AND review_id IN (SELECT review_id FROM sample_membership WHERE sample = ?)"
        params = (sample,)
    query = ("WITH s AS (SELECT review_id, status, text_sha256 FROM reviews WHERE status != 'quarantined'" + scope + "), "
             "g AS (SELECT text_sha256, COUNT(*) AS n FROM reviews WHERE status != 'quarantined' GROUP BY text_sha256) "
             "SELECT s.review_id, s.status, CASE WHEN g.n > 1 THEN 'duplicates' ELSE 'singletons' END "
             "FROM s JOIN g USING (text_sha256)")
    member, counts = {}, defaultdict(lambda: [0, 0])
    for review_id, status, stratum in conn.execute(query, params):
        counts[stratum][0] += 1
        if status == "completed":
            counts[stratum][1] += 1
            member[review_id] = stratum
    return member, {k: tuple(v) for k, v in counts.items()}


def spearman(xs, ys):
    n = len(xs)
    if n < 2:
        return None
    d2 = sum((x - y) ** 2 for x, y in zip(xs, ys))
    return round(1 - 6 * d2 / (n * (n * n - 1)), 6)


def compare(db_path, analysis_dir, sample=None, top=10):
    analysis_dir = Path(analysis_dir)
    ranking = _read_csv(analysis_dir / "ranking.csv")
    membership = _read_csv(analysis_dir / "membership.csv")
    conn = sqlite3.connect(f"{Path(db_path).resolve().as_uri()}?mode=ro", uri=True)
    try:
        stratum_of, counts = strata(conn, sample)
        severity = dict(conn.execute("SELECT review_id, severity FROM labels"))
    finally:
        conn.close()
    weights = {s: (pop / done if done else None) for s, (pop, done) in counts.items()}
    totals = defaultdict(lambda: [0.0, 0.0])
    missing = 0
    for row in membership:
        stratum = stratum_of.get(row["review_id"])
        if stratum is None or weights.get(stratum) is None:
            missing += 1
            continue
        w = weights[stratum]
        totals[row["issue_id"]][0] += w
        totals[row["issue_id"]][1] += w * severity[row["review_id"]]
    weighted_order = sorted(totals, key=lambda iid: (-round(totals[iid][1], 9), iid))
    weighted_rank = {iid: n for n, iid in enumerate(weighted_order, 1)}
    baseline_rank = {r["issue_id"]: int(r["rank"]) for r in ranking}
    issues = [{"issue_id": iid, "baseline_rank": baseline_rank[iid], "weighted_rank": weighted_rank[iid],
               "weighted_complaint_count": round(totals[iid][0], 3), "weighted_severity_sum": round(totals[iid][1], 3)}
              for iid in weighted_order]
    ids = [i["issue_id"] for i in issues]
    base_top = {r["issue_id"] for r in ranking[:top]}
    return {
        "method": METHOD,
        "scope": f"sample {sample}" if sample else "full corpus",
        "strata": {s: {"nonempty_reviews": pop, "completed_reviews": done,
                       "weight": round(weights[s], 6) if weights[s] else None} for s, (pop, done) in sorted(counts.items())},
        "memberships_without_weight": missing,
        "spearman_rank_correlation": spearman([baseline_rank[i] for i in ids], [weighted_rank[i] for i in ids]),
        f"top{top}_overlap": len(base_top & set(weighted_order[:top])),
        "top_changes": [i for i in issues if i["baseline_rank"] != i["weighted_rank"]][:top],
        "issues": issues,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--analysis", type=Path, required=True)
    parser.add_argument("--sample")
    args = parser.parse_args(argv)
    result = compare(args.db, args.analysis, args.sample)
    atomic_write_json(args.analysis / "weighted_comparison.json", result)
    print(json.dumps({k: v for k, v in result.items() if k not in ("issues", "top_changes")}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
