"""Build the dashboard's serving database from saved pipeline outputs. No model calls.

Usage:
    # local SQLite serving database (what the offline dashboard and tests use)
    python3 -m pipeline.serving --db work/pipeline.sqlite --analysis outputs/analysis --out work/serving.sqlite
    # preliminary preview restricted to a registered sample
    python3 -m pipeline.serving --db work/pipeline.sqlite --sample analysis_10000 \
        --analysis outputs/analysis_preview_10000 --out work/serving_preview.sqlite
    # hosted Postgres (requires the psycopg package and DATABASE_URL; loads the same tables)
    python3 -m pipeline.serving ... --postgres

The pipeline database is opened read-only. Every number the dashboard shows is copied from a
saved artifact (ranking.csv, aggregates, claims, memo/recommendation reports) or counted here
from the same completed records, so the dashboard never recomputes or invents a metric.

Tables (portable SQL, identical in SQLite and Postgres):
    meta              key/value provenance: run, scope, build time, synthetic flags
    metrics           overall counts with their denominators
    breakdown         label distributions (topic, intent, severity, sentiment band) of completed records
    issues            baseline ranking joined with issue names and topic
    memberships       issue_id -> review_id
    reviews           processed records of complaint/cancellation members and every cited review
    claims            claim_id -> issue metric value (from the memo)
    recommendations   AI-generated recommendations with cited claim and review IDs
    weighted_issues   optional stratum-weighted comparison (pipeline.weighted)
"""

import argparse
import csv
import json
import os
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from pipeline.common import REPO_ROOT

SERVING_VERSION = "d1"

SCHEMA = [
    "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)",
    "CREATE TABLE metrics (name TEXT PRIMARY KEY, value DOUBLE PRECISION, label TEXT, denominator TEXT, position INTEGER)",
    "CREATE TABLE breakdown (dimension TEXT, value TEXT, n INTEGER, PRIMARY KEY (dimension, value))",
    "CREATE TABLE issues (rank INTEGER, issue_id TEXT PRIMARY KEY, name TEXT, topic TEXT, subtopic TEXT, "
    "complaint_count INTEGER, severity_sum INTEGER, mean_severity TEXT, priority_score INTEGER, "
    "cancellation_count INTEGER, needs_review_count INTEGER)",
    "CREATE TABLE memberships (issue_id TEXT, review_id TEXT, PRIMARY KEY (issue_id, review_id))",
    "CREATE TABLE reviews (review_id TEXT PRIMARY KEY, review_timestamp TEXT, rating INTEGER, topic TEXT, "
    "intent TEXT, severity INTEGER, sentiment DOUBLE PRECISION, evidence_quote TEXT, review_text TEXT, "
    "needs_review INTEGER, label_config TEXT, cache_source_id TEXT)",
    "CREATE TABLE claims (claim_id TEXT PRIMARY KEY, issue_id TEXT, metric TEXT, value TEXT)",
    "CREATE TABLE recommendations (position INTEGER PRIMARY KEY, title TEXT, body_md TEXT, claim_ids TEXT, "
    "review_ids TEXT, model TEXT, synthetic INTEGER)",
    "CREATE TABLE weighted_issues (issue_id TEXT PRIMARY KEY, baseline_rank INTEGER, weighted_rank INTEGER, "
    "weighted_complaint_count DOUBLE PRECISION, weighted_severity_sum DOUBLE PRECISION)",
    "CREATE TABLE trends (month TEXT PRIMARY KEY, nonempty_reviews INTEGER, completed INTEGER, "
    "complaint_or_cancellation INTEGER, complaint_share DOUBLE PRECISION, weighted_complaint_share DOUBLE PRECISION, "
    "partial_month INTEGER)",
    "CREATE INDEX memberships_review ON memberships (review_id)",
]
TABLES = ["meta", "metrics", "breakdown", "issues", "memberships", "reviews", "claims", "recommendations",
          "weighted_issues", "trends"]


def _read_csv(path):
    path = Path(path)
    if not path.exists():
        return []
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _load_json(path):
    path = Path(path)
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def sentiment_band(value):
    if value is None:
        return "missing"
    for upper, name in ((-0.6, "very negative"), (-0.2, "negative"), (0.2, "neutral or mixed"), (0.6, "positive")):
        if value < upper:
            return name
    return "very positive"


def collect(db_path, analysis_dir, sample=None, allow_mock=False, all_records=False, claims_csv=None):
    """all_records=True stores every completed record (the processed review data); otherwise only issue
    members and cited reviews (the minimum the dashboard needs for drill-down and evidence)."""
    """Read saved outputs and the pipeline DB (read-only). Returns {table: rows} plus meta."""
    analysis_dir = Path(analysis_dir)
    ranking = _read_csv(analysis_dir / "ranking.csv")
    if not ranking:
        raise SystemExit(f"ERROR: {analysis_dir / 'ranking.csv'} is missing; run pipeline.group --rank first.")
    aggregates = {r["issue_id"]: r for r in _read_csv(analysis_dir / "aggregates.csv")}
    issue_rows = {r["issue_id"]: r for r in _read_csv(analysis_dir / "issues.csv")}
    names = _load_json(analysis_dir / "issue_names.json") or {}
    membership = [(r["issue_id"], r["review_id"]) for r in _read_csv(analysis_dir / "membership.csv")]
    recs = _load_json(analysis_dir / "recommendations.json")
    memo_report = _load_json(analysis_dir / "memo_report.json") or {}
    # claims.csv is written by the export (grading/), not the analysis stage; an explicit path wins
    claims_path = Path(claims_csv) if claims_csv else analysis_dir / "claims.csv"
    claims = _read_csv(claims_path)
    weighted = _load_json(analysis_dir / "weighted_comparison.json")
    trend_rows = [(r["month"], int(r["nonempty_reviews"]), int(r["completed"]), int(r["complaint_or_cancellation"]),
                   float(r["complaint_share"]) if r["complaint_share"] else None,
                   float(r["weighted_complaint_share"]) if r.get("weighted_complaint_share") else None,
                   int(r["partial_month"] == "true"))
                  for r in _read_csv(analysis_dir / "trends.csv")]
    trend_note = ((_load_json(analysis_dir / "trends.json") or {}).get("comparison") or {}).get("note", "")

    cited = set()
    for rec in (recs or {}).get("recommendations", []):
        cited.update(rec.get("review_ids", []))
    member_ids = {rid for _, rid in membership}
    wanted = member_ids | cited

    conn = sqlite3.connect(f"{Path(db_path).resolve().as_uri()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        scope = ""
        params = ()
        if sample:
            scope = " AND r.review_id IN (SELECT review_id FROM sample_membership WHERE sample = ?)"
            params = (sample,)
        status = Counter()
        for row in conn.execute("SELECT r.status, COUNT(*) FROM reviews r WHERE 1=1" + scope + " GROUP BY r.status",
                                params):
            status[row[0]] = row[1]
        breakdown = Counter()
        cache_reused = 0
        reviews = []
        query = ("SELECT r.review_id, r.review_timestamp, r.review_rating, r.review_text, l.topic, l.intent, "
                 "l.severity, l.sentiment, l.evidence_quote, l.needs_review, l.label_config, l.cache_source_id "
                 "FROM labels l JOIN reviews r USING (review_id) WHERE r.status = 'completed'" + scope)
        for row in conn.execute(query, params):
            if row["label_config"].startswith("mock/") and not allow_mock:
                continue
            breakdown[("topic", row["topic"])] += 1
            breakdown[("intent", row["intent"])] += 1
            breakdown[("severity", str(row["severity"]))] += 1
            breakdown[("sentiment", sentiment_band(row["sentiment"]))] += 1
            if row["cache_source_id"]:
                cache_reused += 1
            if all_records or row["review_id"] in wanted:
                reviews.append((row["review_id"], row["review_timestamp"], row["review_rating"], row["topic"],
                                row["intent"], row["severity"], row["sentiment"], row["evidence_quote"],
                                row["review_text"], int(bool(row["needs_review"])), row["label_config"],
                                row["cache_source_id"]))
        label_configs = sorted({r[0] for r in conn.execute(
            "SELECT DISTINCT l.label_config FROM labels l JOIN reviews r USING (review_id) WHERE 1=1" + scope, params)})
    finally:
        conn.close()

    total = sum(status.values())
    completed = status.get("completed", 0)
    complaint_like = breakdown[("intent", "complaint")] + breakdown[("intent", "cancellation")]
    metrics = [
        ("source_reviews", total, "Source reviews in scope", "", 1),
        ("completed_reviews", completed, "Classified (completed)", "source_reviews", 2),
        ("pending_reviews", status.get("pending", 0), "Not yet classified (pending)", "source_reviews", 3),
        ("quarantined_reviews", status.get("quarantined", 0), "Quarantined (empty text)", "source_reviews", 4),
        ("cache_reused_reviews", cache_reused, "Completed by exact-text cache reuse", "completed_reviews", 5),
        ("complaint_or_cancellation", complaint_like, "Complaint or cancellation records", "completed_reviews", 6),
        ("grouped_records", len(membership), "Records in ranked issues", "complaint_or_cancellation", 7),
        ("ranked_issues", len(ranking), "Ranked issues", "", 8),
    ]
    issues = []
    for r in ranking:
        iid = r["issue_id"]
        agg = aggregates.get(iid, {})
        info = issue_rows.get(iid, {})
        issues.append((int(r["rank"]), iid, names.get(iid) or info.get("name") or iid, iid.split(".")[0],
                       info.get("subtopic") or iid.split(".", 1)[-1], int(r["complaint_count"]), int(r["severity_sum"]),
                       r["mean_severity"], int(r["priority_score"]), _int(agg.get("cancellation_count")),
                       _int(agg.get("needs_review_count"))))
    rec_rows = []
    synthetic_recs = bool((recs or {}).get("synthetic"))
    for n, rec in enumerate((recs or {}).get("recommendations", []), 1):
        rec_rows.append((n, rec["title"], rec["body_md"], json.dumps(rec.get("claim_ids", [])),
                         json.dumps(rec.get("review_ids", [])), (recs or {}).get("model"), int(synthetic_recs)))
    cited = {c for rec in (recs or {}).get("recommendations", []) for c in rec.get("claim_ids", [])}
    missing = sorted(cited - {c["claim_id"] for c in claims})
    if missing:
        raise ValueError(f"recommendations cite claims not in {claims_path}: {missing[:5]} (pass the export's claims.csv)")
    weighted_rows = [(w["issue_id"], w["baseline_rank"], w["weighted_rank"], w["weighted_complaint_count"],
                      w["weighted_severity_sum"]) for w in (weighted or {}).get("issues", [])]
    meta = {
        "serving_version": SERVING_VERSION,
        "built_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "scope": f"sample {sample}" if sample else "full corpus",
        "analysis_dir": str(analysis_dir.resolve().relative_to(REPO_ROOT)) if analysis_dir.resolve().is_relative_to(REPO_ROOT)
        else str(analysis_dir),
        "label_configs": ", ".join(label_configs),
        "recommendations_model": (recs or {}).get("model") or "",
        "recommendations_synthetic": str(synthetic_recs).lower(),
        "recommendations_present": str(bool(rec_rows)).lower(),
        "memo_model": memo_report.get("label_config", ""),
        "weighting_note": (weighted or {}).get("method", ""),
        "trend_note": trend_note,
        "preliminary": str(bool(sample)).lower(),
        "reviews_table": "all completed records" if all_records else "issue members and cited reviews",
    }
    return {
        "meta": sorted(meta.items()),
        "metrics": metrics,
        "breakdown": sorted((d, v, n) for (d, v), n in breakdown.items()),
        "issues": issues,
        "memberships": sorted(membership),
        "reviews": sorted(reviews),
        "claims": [(c["claim_id"], c["issue_id"], c["metric"], c["value"]) for c in claims],
        "recommendations": rec_rows,
        "weighted_issues": weighted_rows,
        "trends": trend_rows,
    }


def _int(value):
    return int(value) if value not in (None, "") else None


def write_sqlite(path, tables):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    conn = sqlite3.connect(tmp)
    try:
        for statement in SCHEMA:
            conn.execute(statement)
        for table in TABLES:
            rows = tables[table]
            if rows:
                marks = ",".join("?" * len(rows[0]))
                conn.executemany(f"INSERT INTO {table} VALUES ({marks})", rows)
        conn.commit()
    finally:
        conn.close()
    os.replace(tmp, path)
    return path


def estimate_bytes(tables):
    """Rough on-disk size: raw value bytes plus ~40 bytes of row overhead, plus ~35% for indexes."""
    sizes = {}
    for table in TABLES:
        raw = sum(sum(len(str(v).encode("utf-8")) if v is not None else 0 for v in row) + 40 for row in tables[table])
        sizes[table] = int(raw * 1.35)
    return {"tables": sizes, "total_bytes": sum(sizes.values()), "total_mb": round(sum(sizes.values()) / 1e6, 1)}


class MigrationError(Exception):
    pass


READER_ROLE = "dashboard_reader"   # read-only login role the deployed app uses (see dashboard/README.md)


def write_postgres(url, tables, connect=None, reader_role=READER_ROLE):
    """Replace the serving tables in a hosted Postgres database in ONE transaction, verify, then commit.

    Postgres DDL is transactional: readers keep seeing the previous tables until the commit, and any
    failure (including a verification mismatch) rolls back to the previous state. Only the serving
    tables listed in TABLES are touched. The connection string is never printed. Recreating a table
    drops its grants, so SELECT is re-granted to the read-only reader role (when it exists) in the
    same transaction; the app never sees a moment without read access.
    """
    if connect is None:
        try:
            import psycopg  # noqa: PLC0415 - optional dependency, only for loading the hosted database
        except ImportError:
            raise SystemExit("ERROR: loading Postgres needs the psycopg package (not installed).")
        connect = psycopg.connect
    conn = connect(url)
    try:
        with conn.cursor() as cur:
            for table in TABLES:
                cur.execute(f"DROP TABLE IF EXISTS {table}")
            for statement in SCHEMA:
                cur.execute(statement)
            cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (reader_role,))
            if cur.fetchone():
                cur.execute(f'GRANT SELECT ON {", ".join(TABLES)} TO "{reader_role}"')
            for table in TABLES:
                rows = tables[table]
                if rows:
                    with cur.copy(f"COPY {table} FROM STDIN") as copy:
                        for row in rows:
                            copy.write_row(row)
            counts = {}
            for table in TABLES:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                counts[table] = cur.fetchone()[0]
            expected = {t: len(tables[t]) for t in TABLES}
            if counts != expected:
                raise MigrationError(f"row counts after load {counts} != expected {expected}")
            cur.execute("SELECT rank, issue_id, priority_score FROM issues ORDER BY rank")
            served = [tuple(r) for r in cur.fetchall()]
            if served != [(i[0], i[1], i[8]) for i in sorted(tables["issues"])]:
                raise MigrationError("ranking read back from Postgres differs from the saved ranking")
        conn.commit()
        return counts
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", type=Path, default=REPO_ROOT / "work" / "pipeline.sqlite")
    parser.add_argument("--analysis", type=Path, required=True)
    parser.add_argument("--sample", help="Restrict to a registered sample (preliminary preview)")
    parser.add_argument("--claims", type=Path, help="claims.csv from the export (default: <analysis>/claims.csv)")
    parser.add_argument("--out", type=Path, help="SQLite serving database to write")
    parser.add_argument("--postgres", action="store_true", help="Load DATABASE_URL instead of a SQLite file")
    parser.add_argument("--all-records", action="store_true",
                        help="Store every completed record, not only issue members and cited reviews")
    parser.add_argument("--estimate", action="store_true", help="Only print the estimated database size")
    args = parser.parse_args(argv)
    tables = collect(args.db, args.analysis, args.sample, all_records=args.all_records, claims_csv=args.claims)
    if args.estimate:
        print(json.dumps(estimate_bytes(tables), indent=2))
        return 0
    if args.postgres:
        from pipeline.config import get_key  # noqa: PLC0415 - environment first, then the git-ignored .env
        url = get_key("DATABASE_URL")
        if not url:
            raise SystemExit("ERROR: set DATABASE_URL (environment or .env) for --postgres.")
        try:
            counts = write_postgres(url, tables)
        except MigrationError:
            raise
        except Exception as e:  # report the error type only: driver messages can echo connection details
            raise SystemExit(f"ERROR: Postgres load failed and was rolled back ({type(e).__name__}).") from None
        print("loaded hosted Postgres and verified row counts and ranking:", json.dumps(counts))
    else:
        if not args.out:
            raise SystemExit("ERROR: --out is required without --postgres.")
        print("wrote", write_sqlite(args.out, tables))
    print(json.dumps({t: len(tables[t]) for t in TABLES}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
