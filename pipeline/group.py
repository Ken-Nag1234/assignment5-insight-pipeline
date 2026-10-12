"""Stage 4 (Group): deterministic issue membership for complaint and cancellation records.

Usage:
    python3 -m pipeline.group --db work/full.sqlite --out outputs/analysis

Each completed complaint/cancellation record belongs to exactly one issue
(allow_multi_issue = false). The issue is the record's topic plus a subtopic: the first
explicitly mentioned entity (in order of appearance in the text) that belongs to that topic's
keyword list, otherwise "general". Issue IDs are stable strings such as "playback.crash", so
re-running on the same records always yields the same membership. A model may later suggest
display names from bounded examples; the accepted mapping is saved, and ranking never needs a model.
"""

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path

from pipeline import db
from pipeline.common import REPO_ROOT, atomic_write_text
from pipeline.extract import TOPIC_KEYWORDS

GROUPED_INTENTS = ("complaint", "cancellation")
GROUPING_VERSION = "g1"


def issue_for(topic, entities):
    keywords = TOPIC_KEYWORDS.get(topic, ())
    subtopic = next((e for e in entities if e in keywords), "general")
    return f"{topic}.{subtopic.replace(' ', '_')}", subtopic


def default_name(topic, subtopic):
    return f"{topic.capitalize()}: {subtopic}"


def assign(records):
    """records: iterable of grading-contract record dicts. Returns (membership, issues)."""
    membership, issues = [], {}
    for record in records:
        if record.get("status") != "completed" or record["intent"] not in GROUPED_INTENTS:
            continue
        issue_id, subtopic = issue_for(record["topic"], record["entities"])
        membership.append((issue_id, record["review_id"]))
        issue = issues.setdefault(issue_id, {"issue_id": issue_id, "topic": record["topic"], "subtopic": subtopic,
                                             "name": default_name(record["topic"], subtopic), "members": 0})
        issue["members"] += 1
    membership.sort()
    return membership, [issues[k] for k in sorted(issues)]


def write_membership(path, membership):
    lines = ["issue_id,review_id\n"] + [f"{iid},{rid}\n" for iid, rid in membership]
    atomic_write_text(path, "".join(lines))


def write_issues(path, issues, names=None):
    names = names or {}
    rows = [{**issue, "name": names.get(issue["issue_id"], issue["name"]), "grouping_version": GROUPING_VERSION}
            for issue in issues]
    fields = ["issue_id", "topic", "subtopic", "name", "members", "grouping_version"]
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def records_from_db(conn, exclude_mock=True, sample=None):
    from pipeline.labels import record_from_row
    query = ("SELECT r.review_id, r.source_sha256, l.* FROM labels l JOIN reviews r USING (review_id)"
             " WHERE r.status = 'completed'")
    params = ()
    if sample:
        query += " AND r.review_id IN (SELECT review_id FROM sample_membership WHERE sample = ?)"
        params = (sample,)
    rows = conn.execute(query + " ORDER BY r.review_id", params)
    for row in rows:
        if exclude_mock and row["label_config"].startswith("mock/"):
            continue
        yield record_from_row(row)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--out", type=Path, default=REPO_ROOT / "outputs" / "analysis")
    parser.add_argument("--names", type=Path, help="Optional accepted issue-name mapping (JSON)")
    parser.add_argument("--allow-mock", action="store_true", help="Include synthetic mock labels (tests only)")
    parser.add_argument("--sample", help="Only records of a registered sample, e.g. analysis_10000 (preliminary analysis)")
    parser.add_argument("--rank", action="store_true", help="Also write ranking.csv and aggregates.csv")
    args = parser.parse_args(argv)
    import sqlite3
    conn = sqlite3.connect(f"{Path(args.db).resolve().as_uri()}?mode=ro", uri=True)  # never writes the database
    conn.row_factory = sqlite3.Row
    try:
        records = list(records_from_db(conn, exclude_mock=not args.allow_mock, sample=args.sample))
    finally:
        conn.close()
    membership, issues = assign(records)
    names = json.loads(args.names.read_text(encoding="utf-8")) if args.names else None
    write_membership(args.out / "membership.csv", membership)
    write_issues(args.out / "issues.csv", issues, names)
    if args.rank:
        from pipeline import rank
        by_id = {r["review_id"]: r for r in records}
        ranking = rank.compute(by_id, membership)
        rank.write_ranking(args.out / "ranking.csv", ranking)
        rank.write_aggregates(args.out / "aggregates.csv", ranking, by_id, membership)
    counts = Counter(iid.split(".")[0] for iid, _ in membership)
    print(f"{len(membership)} memberships in {len(issues)} issues; by topic: {dict(sorted(counts.items()))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
