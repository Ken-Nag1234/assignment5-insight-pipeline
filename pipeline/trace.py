"""Trace one review through every stage, from saved artifacts only (README evidence). No model calls.

Usage:
    python3 -m pipeline.trace --analysis outputs/analysis --review-id <ID>
    python3 -m pipeline.trace --analysis outputs/analysis --pick            # a memo-cited review, verified if possible
    python3 -m pipeline.trace --analysis outputs/analysis --pick-failed     # a quarantined or needs_review case

Prints Markdown: source ID and row hash -> enrichment call (or exact-text cache source and its call)
-> stored labels -> independent verifier answer (if sampled) -> issue membership -> ranking row ->
memo claims and recommendations that use the issue or cite the review. The pipeline database is
opened read-only.
"""

import argparse
import csv
import json
import sqlite3
import sys
from pathlib import Path

from pipeline.common import REPO_ROOT

DB_PATH = REPO_ROOT / "work" / "pipeline.sqlite"
VERIFY_DIR = REPO_ROOT / "outputs" / "verify"


def _csv(path):
    path = Path(path)
    if not path.exists():
        return []
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _jsonl(path):
    path = Path(path)
    if not path.exists():
        return []
    return [json.loads(x) for x in path.read_text(encoding="utf-8").splitlines() if x.strip()]


def _ro(db_path):
    conn = sqlite3.connect(f"{Path(db_path).resolve().as_uri()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _calls_for(conn, review_id):
    rows = conn.execute("SELECT c.* FROM calls c, json_each(c.review_ids) j WHERE j.value = ? "
                        "ORDER BY c.started_at", (review_id,)).fetchall()
    return [dict(r) for r in rows]


def trace(review_id, analysis_dir, db_path=DB_PATH, verify_dir=VERIFY_DIR):
    analysis_dir = Path(analysis_dir)
    conn = _ro(db_path)
    try:
        review = conn.execute("SELECT * FROM reviews WHERE review_id = ?", (review_id,)).fetchone()
        if review is None:
            raise SystemExit(f"unknown review_id {review_id}")
        label = conn.execute("SELECT * FROM labels WHERE review_id = ?", (review_id,)).fetchone()
        calls = _calls_for(conn, review_id)
        source_calls = _calls_for(conn, label["cache_source_id"]) if label and label["cache_source_id"] else []
    finally:
        conn.close()
    lines = [f"### Trace of review `{review_id}`", "",
             f"1. **Source row** (ingest): status `{review['status']}`"
             + (f", reason `{review['reason']}`" if review["reason"] else "")
             + f"; row SHA-256 `{review['source_sha256']}`; rating {review['review_rating']} (metadata only); "
             f"text: \"{(review['review_text'] or '')[:160]}\""]
    if label is None:
        lines.append("2. **Enrichment**: none - " + ("quarantined before any model call (no text to classify)."
                                                     if review["status"] == "quarantined" else "not yet classified."))
        return "\n".join(lines) + "\n"
    if calls:
        c = calls[-1]
        lines.append(f"2. **Enrichment call** (role `{c['role']}`, phase `{c['phase']}`): request `{c['request_id']}`, "
                     f"model `{c['model']}`, `{c['label_config']}`, {c['input_tokens']} input tokens, "
                     f"cost ${c['cost_usd']} ({c['cost_source']}), outcome `{c['outcome']}`; "
                     f"{len(calls)} attempt(s) logged.")
    elif source_calls:
        c = source_calls[-1]
        lines.append(f"2. **Exact-text cache reuse**: no new call; `cache_source_id` = `{label['cache_source_id']}`, "
                     f"whose call was request `{c['request_id']}` (`{c['label_config']}`).")
    lines.append(f"3. **Stored labels** (code-validated): topic `{label['topic']}`, intent `{label['intent']}`, "
                 f"severity {label['severity']}, sentiment {label['sentiment']}, entities {label['entities']}, "
                 f"needs_review {bool(label['needs_review'])}; evidence quote \"{label['evidence_quote'][:120]}\".")
    verified = [p for p in _jsonl(Path(verify_dir) / "predictions.jsonl") if p.get("review_id") == review_id]
    if verified:
        v = verified[-1]
        lines.append(f"4. **Independent verifier** (separate instructions, blind): topic `{v.get('topic')}`, "
                     f"intent `{v.get('intent')}`, severity {v.get('severity')}.")
    else:
        lines.append("4. **Independent verifier**: not in the declared verification sample.")
    issues = [r["issue_id"] for r in _csv(analysis_dir / "membership.csv") if r["review_id"] == review_id]
    if not issues:
        lines.append(f"5. **Grouping**: not grouped (intent `{label['intent']}` is not complaint/cancellation).")
        return "\n".join(lines) + "\n"
    issue_id = issues[0]
    names = json.loads((analysis_dir / "issue_names.json").read_text()) if (analysis_dir / "issue_names.json").exists() else {}
    lines.append(f"5. **Issue membership** (code rule topic + first explicit entity): `{issue_id}`"
                 + (f" - \"{names[issue_id]}\"" if issue_id in names else "") + ".")
    row = next(r for r in _csv(analysis_dir / "ranking.csv") if r["issue_id"] == issue_id)
    lines.append(f"6. **Ranking**: rank {row['rank']}, complaint_count {row['complaint_count']}, severity_sum "
                 f"{row['severity_sum']} (this review adds {label['severity']}), mean {row['mean_severity']}.")
    selections = json.loads((analysis_dir / "claims_selection.json").read_text()) \
        if (analysis_dir / "claims_selection.json").exists() else []
    claims = [f"C{n:03d} ({metric} = {row[metric]})" for n, (iid, metric) in enumerate(selections, 1) if iid == issue_id]
    memo = (analysis_dir / "memo.md").read_text(encoding="utf-8") if (analysis_dir / "memo.md").exists() else ""
    recs = json.loads((analysis_dir / "recommendations.json").read_text())["recommendations"] \
        if (analysis_dir / "recommendations.json").exists() else []
    cited_in = [f"recommendation {n} (\"{r['title']}\")" for n, r in enumerate(recs, 1) if review_id in r["review_ids"]]
    lines.append("7. **Memo**: " + (f"claims {', '.join(claims)} use this issue" if claims else "no claim uses this issue")
                 + ("; the memo cites this review" if f"`{review_id}`" in memo else "")
                 + (f"; cited by {', '.join(cited_in)}" if cited_in else "") + ".")
    return "\n".join(lines) + "\n"


def pick(analysis_dir, verify_dir=VERIFY_DIR, failed=False, db_path=DB_PATH):
    analysis_dir = Path(analysis_dir)
    conn = _ro(db_path)
    try:
        if failed:
            row = conn.execute("SELECT l.review_id FROM labels l JOIN reviews r USING (review_id) "
                               "WHERE r.status = 'completed' AND l.needs_review = 1 AND l.cache_source_id IS NULL "
                               "ORDER BY l.review_id LIMIT 1").fetchone()
            row = row or conn.execute("SELECT review_id FROM reviews WHERE status = 'quarantined' "
                                      "ORDER BY review_id LIMIT 1").fetchone()
            return row[0] if row else None
    finally:
        conn.close()
    recs = json.loads((analysis_dir / "recommendations.json").read_text())["recommendations"] \
        if (analysis_dir / "recommendations.json").exists() else []
    cited = [rid for r in recs for rid in r["review_ids"]]
    verified = {p.get("review_id") for p in _jsonl(Path(verify_dir) / "predictions.jsonl")}
    for rid in cited:
        if rid in verified:
            return rid
    if cited:
        return cited[0]
    membership = _csv(analysis_dir / "membership.csv")
    return membership[0]["review_id"] if membership else None


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--analysis", type=Path, required=True)
    parser.add_argument("--db", type=Path, default=DB_PATH)
    parser.add_argument("--review-id")
    parser.add_argument("--pick", action="store_true")
    parser.add_argument("--pick-failed", action="store_true")
    args = parser.parse_args(argv)
    review_id = args.review_id or pick(args.analysis, failed=args.pick_failed, db_path=args.db)
    if not review_id:
        raise SystemExit("no review to trace")
    print(trace(review_id, args.analysis, args.db))
    return 0


if __name__ == "__main__":
    sys.exit(main())
