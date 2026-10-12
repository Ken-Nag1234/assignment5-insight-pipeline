"""Dashboard backend: a read-only JSON API over the serving database, plus the static page.

Standard library only (WSGI). The database is chosen by environment:
    DATABASE_URL=postgres://...   hosted Postgres (needs psycopg; listed in requirements.txt)
    SERVING_DB=path/to.sqlite     local SQLite file built by `python3 -m pipeline.serving`

Run locally:
    SERVING_DB=work/serving_preview.sqlite python3 dashboard/app.py --port 8000

Endpoints (GET only; every value comes from the serving tables, nothing is computed from a model):
    /api/health                   database kind and table row counts
    /api/overview                 provenance, overall metrics, label distributions
    /api/issues?limit=N           baseline ranking (rank, counts, severity) with issue names
    /api/issues/<issue_id>        one issue and its member reviews (most severe first, paged)
    /api/recommendations          AI-generated recommendations with resolved claims and cited reviews
    /api/reviews/<review_id>      one processed review and its issue membership
    /api/weighted                 optional stratum-weighted ranking comparison
    /api/trends                   monthly complaint share with denominators (partial months flagged)
"""

import json
import os
import sqlite3
import sys
from pathlib import Path
from urllib.parse import parse_qs

HERE = Path(__file__).resolve().parent
INDEX = HERE / "static" / "index.html"
MAX_PAGE = 100


class Database:
    def __init__(self, url=None, sqlite_path=None):
        self.url = url
        self.sqlite_path = sqlite_path
        self.kind = "postgres" if url else "sqlite"
        self._pg = None

    def query(self, sql, params=()):
        if self.url:
            if self._pg is None:  # one connection per request: each new TLS handshake to the host costs ~0.3 s
                import psycopg  # noqa: PLC0415 - only needed when deployed against Postgres
                from psycopg.rows import dict_row  # noqa: PLC0415
                self._pg = psycopg.connect(self.url, row_factory=dict_row, autocommit=True)
            return list(self._pg.execute(sql.replace("?", "%s"), params))
        conn = sqlite3.connect(f"{Path(self.sqlite_path).resolve().as_uri()}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        try:
            return [dict(r) for r in conn.execute(sql, params)]
        finally:
            conn.close()

    def close(self):
        if self._pg is not None:
            self._pg.close()
            self._pg = None


def database_from_env():
    url = os.environ.get("DATABASE_URL") or os.environ.get("POSTGRES_URL")
    if url:
        return Database(url=url)
    path = os.environ.get("SERVING_DB")
    if not path or not Path(path).exists():
        raise RuntimeError("No database configured: set DATABASE_URL or SERVING_DB.")
    return Database(sqlite_path=path)


def _limit(params, default, maximum=MAX_PAGE):
    try:
        return max(1, min(int(params.get("limit", [default])[0]), maximum))
    except ValueError:
        return default


def _offset(params):
    try:
        return max(0, int(params.get("offset", [0])[0]))
    except ValueError:
        return 0


def health(db, params):
    counts = {t: db.query(f"SELECT COUNT(*) AS n FROM {t}")[0]["n"]
              for t in ("issues", "reviews", "memberships", "recommendations")}
    return {"database": db.kind, "rows": counts}


def overview(db, params):
    meta = {r["key"]: r["value"] for r in db.query("SELECT key, value FROM meta")}
    metrics = db.query("SELECT name, value, label, denominator FROM metrics ORDER BY position")
    breakdown = {}
    for r in db.query("SELECT dimension, value, n FROM breakdown ORDER BY dimension, n DESC, value"):
        breakdown.setdefault(r["dimension"], []).append({"value": r["value"], "n": r["n"]})
    return {"meta": meta, "metrics": metrics, "breakdown": breakdown}


ISSUE_COLUMNS = ("rank, issue_id, name, topic, subtopic, complaint_count, severity_sum, mean_severity, "
                 "priority_score, cancellation_count, needs_review_count")


def issues(db, params):
    return {"issues": db.query(f"SELECT {ISSUE_COLUMNS} FROM issues ORDER BY rank LIMIT ?",
                               (_limit(params, 50, 500),))}


def issue_detail(db, params, issue_id):
    rows = db.query(f"SELECT {ISSUE_COLUMNS} FROM issues WHERE issue_id = ?", (issue_id,))
    if not rows:
        return None
    limit, offset = _limit(params, 20), _offset(params)
    reviews = db.query(
        "SELECT r.review_id, r.review_timestamp, r.rating, r.intent, r.severity, r.sentiment, r.evidence_quote, "
        "r.review_text, r.needs_review, r.cache_source_id FROM memberships m JOIN reviews r ON r.review_id = m.review_id "
        "WHERE m.issue_id = ? ORDER BY r.severity DESC, r.review_id LIMIT ? OFFSET ?", (issue_id, limit, offset))
    return {"issue": rows[0], "reviews": reviews, "limit": limit, "offset": offset,
            "order": "severity descending, then review_id"}


def recommendations(db, params):
    claims = {c["claim_id"]: c for c in db.query("SELECT claim_id, issue_id, metric, value FROM claims")}
    out = []
    for rec in db.query("SELECT position, title, body_md, claim_ids, review_ids, model, synthetic "
                        "FROM recommendations ORDER BY position"):
        claim_ids, review_ids = json.loads(rec["claim_ids"]), json.loads(rec["review_ids"])
        found = {r["review_id"]: r for r in db.query(
            "SELECT review_id, rating, severity, evidence_quote FROM reviews WHERE review_id IN "
            f"({','.join('?' * len(review_ids))})", tuple(review_ids))} if review_ids else {}
        cited = [found.get(rid, {"review_id": rid, "missing": True}) for rid in review_ids]
        out.append({**rec, "claim_ids": claim_ids, "review_ids": review_ids, "synthetic": bool(rec["synthetic"]),
                    "claims": [claims.get(c, {"claim_id": c, "missing": True}) for c in claim_ids],
                    "reviews": cited})
    return {"recommendations": out}


def review_detail(db, params, review_id):
    rows = db.query("SELECT * FROM reviews WHERE review_id = ?", (review_id,))
    if not rows:
        return None
    memberships = [r["issue_id"] for r in db.query("SELECT issue_id FROM memberships WHERE review_id = ?", (review_id,))]
    return {"review": rows[0], "issues": memberships}


def weighted(db, params):
    return {"issues": db.query("SELECT w.*, i.name FROM weighted_issues w LEFT JOIN issues i USING (issue_id) "
                               "ORDER BY w.weighted_rank")}


def trends(db, params):
    return {"months": db.query("SELECT month, nonempty_reviews, completed, complaint_or_cancellation, complaint_share, "
                               "weighted_complaint_share, partial_month FROM trends ORDER BY month")}


def route(db, path, params):
    """Return (status, payload)."""
    simple = {"/api/health": health, "/api/overview": overview, "/api/issues": issues,
              "/api/recommendations": recommendations, "/api/weighted": weighted, "/api/trends": trends}
    if path in simple:
        return 200, simple[path](db, params)
    for prefix, handler in (("/api/issues/", issue_detail), ("/api/reviews/", review_detail)):
        if path.startswith(prefix) and len(path) > len(prefix):
            result = handler(db, params, path[len(prefix):])
            return (200, result) if result is not None else (404, {"error": "not found"})
    return 404, {"error": "not found"}


def app(environ, start_response):
    method = environ.get("REQUEST_METHOD", "GET")
    path = environ.get("PATH_INFO", "/") or "/"
    if method not in ("GET", "HEAD"):
        start_response("405 Method Not Allowed", [("Content-Type", "application/json"), ("Allow", "GET, HEAD")])
        return [b'{"error": "read-only API"}']
    if not path.startswith("/api/"):
        body = INDEX.read_bytes()
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8"), ("Cache-Control", "no-cache")])
        return [body]
    params = parse_qs(environ.get("QUERY_STRING", ""))
    db = None
    try:
        db = database_from_env()
        status, payload = route(db, path, params)
    except Exception as e:  # noqa: BLE001 - report a clean JSON error, never a traceback or connection string
        status, payload = 500, {"error": f"{type(e).__name__}"}
        print(f"dashboard error on {path}: {type(e).__name__}", file=sys.stderr)  # messages can echo connection details
    finally:
        if db is not None:
            db.close()
    body = json.dumps(payload, default=str).encode("utf-8")
    reason = {200: "OK", 404: "Not Found", 500: "Internal Server Error"}[status]
    start_response(f"{status} {reason}", [("Content-Type", "application/json"), ("Cache-Control", "public, max-age=60")])
    return [body]


if __name__ == "__main__":
    import argparse
    from wsgiref.simple_server import make_server
    parser = argparse.ArgumentParser(description="Run the dashboard locally")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    with make_server("127.0.0.1", args.port, app) as server:
        print(f"Dashboard on http://127.0.0.1:{args.port}/")
        server.serve_forever()
