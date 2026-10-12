"""Complaint-share trend by calendar month with denominators. No model calls; read-only on the DB.

Usage:
    python3 -m pipeline.trends --db work/pipeline.sqlite --analysis outputs/analysis [--sample analysis_10000]

Writes trends.csv (one row per month) and trends.json (the comparison of comparable periods). The
share is complaint-or-cancellation records / completed records in that month; both counts are
reported. The first and last months of the source window are partial and flagged. Comparable
periods are the same complete calendar months one year apart (June-October 2022 vs. 2023), so
seasonality does not drive the comparison. Classification coverage is partial when the run did not
complete every review, so each month also reports how many of its reviews are completed.
"""

import argparse
import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

from pipeline.common import atomic_write_json, atomic_write_text

COMPARE = (["2022-06", "2022-07", "2022-08", "2022-09", "2022-10"],
           ["2023-06", "2023-07", "2023-08", "2023-09", "2023-10"])


def monthly(conn, sample=None):
    scope, params = "", ()
    if sample:
        scope = " AND r.review_id IN (SELECT review_id FROM sample_membership WHERE sample = ?)"
        params = (sample,)
    rows = conn.execute(
        "WITH g AS (SELECT text_sha256, COUNT(*) AS n FROM reviews WHERE status != 'quarantined' GROUP BY text_sha256) "
        "SELECT substr(r.review_timestamp, 1, 7) AS month, r.status, l.intent, "
        "CASE WHEN g.n > 1 THEN 'duplicates' ELSE 'singletons' END FROM reviews r JOIN g USING (text_sha256) "
        "LEFT JOIN labels l USING (review_id) WHERE r.status != 'quarantined'" + scope, params)
    counts = defaultdict(lambda: {"nonempty_reviews": 0, "completed": 0, "complaint_or_cancellation": 0})
    cells = defaultdict(lambda: [0, 0, 0])   # (month, stratum) -> [population, completed, complaints]
    for month, status, intent, stratum in rows:
        c, cell = counts[month], cells[(month, stratum)]
        c["nonempty_reviews"] += 1
        cell[0] += 1
        if status == "completed":
            hit = intent in ("complaint", "cancellation")
            c["completed"] += 1
            c["complaint_or_cancellation"] += hit
            cell[1] += 1
            cell[2] += hit
    months = sorted(counts)
    out = []
    for m in months:
        c = counts[m]
        out.append({"month": m, **c,
                    "complaint_share": round(c["complaint_or_cancellation"] / c["completed"], 6) if c["completed"] else None,
                    "weighted_complaint_share": _weighted([cells[k] for k in cells if k[0] == m]),
                    "classified_fraction": round(c["completed"] / c["nonempty_reviews"], 6),
                    "partial_month": m in (months[0], months[-1])})
    return out


def _weighted(cells):
    """Post-stratified share: each exact-text stratum's share weighted by its population.

    The run classified every duplicated text but only a seeded subset of singleton texts, so the
    unweighted share over-represents duplicated (often short, generic) reviews. Returns None when a
    stratum with reviews has no completed ones, because its share cannot be estimated.
    """
    population = sum(c[0] for c in cells)
    if not population or any(c[0] and not c[1] for c in cells):
        return None
    return round(sum(c[0] * c[2] / c[1] for c in cells if c[0]) / population, 6)


def compare_periods(rows, periods=COMPARE):
    by_month = {r["month"]: r for r in rows}
    result = {}
    for label, months in zip(("2022", "2023"), periods):
        present = [by_month[m] for m in months if m in by_month]
        completed = sum(r["completed"] for r in present)
        complaints = sum(r["complaint_or_cancellation"] for r in present)
        population = sum(r["nonempty_reviews"] for r in present)
        weighted = None
        if population and all(r.get("weighted_complaint_share") is not None for r in present):
            weighted = round(sum(r["weighted_complaint_share"] * r["nonempty_reviews"] for r in present) / population, 6)
        result[label] = {"months": months, "nonempty_reviews": population, "completed": completed,
                         "complaint_or_cancellation": complaints,
                         "complaint_share": round(complaints / completed, 6) if completed else None,
                         "weighted_complaint_share": weighted}
    for key, field in (("change_in_share_points", "complaint_share"),
                       ("weighted_change_in_share_points", "weighted_complaint_share")):
        a, b = result["2022"][field], result["2023"][field]
        result[key] = round((b - a) * 100, 3) if a is not None and b is not None else None
    result["note"] = ("Same complete months one year apart; partial first/last months excluded. complaint_share uses "
                      "completed classifications as the denominator; weighted_complaint_share re-weights the "
                      "duplicated-text and single-text strata to their populations within each month, because the "
                      "run classified all duplicated texts but only part of the single texts. Reviews are "
                      "self-selected and not a customer census.")
    return result


def write(db_path, analysis_dir, sample=None):
    conn = sqlite3.connect(f"{Path(db_path).resolve().as_uri()}?mode=ro", uri=True)
    try:
        rows = monthly(conn, sample)
    finally:
        conn.close()
    analysis_dir = Path(analysis_dir)
    fields = ["month", "nonempty_reviews", "completed", "classified_fraction", "complaint_or_cancellation",
              "complaint_share", "weighted_complaint_share", "partial_month"]
    atomic_write_text(analysis_dir / "trends.csv", ",".join(fields) + "\n" + "".join(
        ",".join("" if r[f] is None else str(r[f]).lower() if isinstance(r[f], bool) else str(r[f]) for f in fields)
        + "\n" for r in rows))
    comparison = compare_periods(rows)
    atomic_write_json(analysis_dir / "trends.json", {"scope": f"sample {sample}" if sample else "full corpus",
                                                     "comparison": comparison})
    return rows, comparison


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--analysis", type=Path, required=True)
    parser.add_argument("--sample")
    args = parser.parse_args(argv)
    _, comparison = write(args.db, args.analysis, args.sample)
    print(json.dumps(comparison, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
