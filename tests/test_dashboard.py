"""Offline tests for the serving database, the dashboard API and the weighted comparison.

Synthetic fixtures and the mock provider only; mock labels are relabeled inside the temporary
database so the serving builder (which skips mock labels) can be exercised.
"""

import csv
import io
import json
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from pipeline import group, rank, serving, trends, weighted
from pipeline.common import REPO_ROOT
from pipeline.enrich import Options, run_enrichment
from pipeline.ledger import SpendLedger
from pipeline.providers import MockProvider
from tests.test_enrich import make_db, no_sleep

sys.path.insert(0, str(REPO_ROOT / "dashboard"))
import app as dashboard  # noqa: E402


def call(path, query="", method="GET"):
    captured = {}

    def start_response(status, headers):
        captured["status"], captured["headers"] = status, dict(headers)
    body = b"".join(dashboard.app({"REQUEST_METHOD": method, "PATH_INFO": path, "QUERY_STRING": query},
                                  start_response))
    return int(captured["status"].split()[0]), captured["headers"], body


class DashboardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        d = Path(cls.tmp.name)
        conn = make_db(d)
        run_enrichment(conn, MockProvider(max_batch=3), SpendLedger(Decimal("1"), d / "ledger.jsonl"),
                       Options(sleep=no_sleep))
        conn.execute("UPDATE labels SET label_config = 'test/fixture'")
        conn.commit()
        records = {r["review_id"]: r for r in group.records_from_db(conn, exclude_mock=False)}
        conn.close()
        cls.db_path = d / "state.sqlite"
        cls.analysis = d / "analysis"
        membership, issues = group.assign(records.values())
        group.write_membership(cls.analysis / "membership.csv", membership)
        group.write_issues(cls.analysis / "issues.csv", issues)
        ranking = rank.compute(records, membership)
        rank.write_ranking(cls.analysis / "ranking.csv", ranking)
        rank.write_aggregates(cls.analysis / "aggregates.csv", ranking, records, membership)
        cls.ranking = ranking
        cited = membership[0][1]
        (cls.analysis / "claims.csv").write_text(
            f"claim_id,issue_id,metric,value\nC001,{ranking[0]['issue_id']},priority_score,{ranking[0]['priority_score']}\n")
        (cls.analysis / "recommendations.json").write_text(json.dumps({
            "model": "mock/text-v1", "synthetic": True,
            "recommendations": [{"title": "Fix the top issue", "body_md": "Because of C001.", "claim_ids": ["C001"],
                                 "review_ids": [cited]}]}))
        result = weighted.compare(cls.db_path, cls.analysis)
        (cls.analysis / "weighted_comparison.json").write_text(json.dumps(result))
        cls.weighted = result
        trends.write(cls.db_path, cls.analysis)
        cls.serving_db = d / "serving.sqlite"
        serving.write_sqlite(cls.serving_db, serving.collect(cls.db_path, cls.analysis))
        cls.old_env = {k: dashboard.os.environ.pop(k, None) for k in ("DATABASE_URL", "POSTGRES_URL", "SERVING_DB")}
        dashboard.os.environ["SERVING_DB"] = str(cls.serving_db)

    @classmethod
    def tearDownClass(cls):
        dashboard.os.environ.pop("SERVING_DB", None)
        for k, v in cls.old_env.items():
            if v is not None:
                dashboard.os.environ[k] = v
        cls.tmp.cleanup()

    def get(self, path, query=""):
        status, headers, body = call(path, query)
        self.assertEqual(headers["Content-Type"], "application/json")
        return status, json.loads(body)

    def test_ranking_served_matches_saved_ranking_csv(self):
        status, data = self.get("/api/issues", "limit=100")
        self.assertEqual(status, 200)
        with (self.analysis / "ranking.csv").open() as f:
            saved = list(csv.DictReader(f))
        served = [(str(i["rank"]), i["issue_id"], str(i["complaint_count"]), str(i["severity_sum"]),
                   i["mean_severity"], str(i["priority_score"])) for i in data["issues"]]
        self.assertEqual(served, [tuple(r[k] for k in rank.RANKING_FIELDS) for r in saved])

    def test_overview_metrics_and_provenance(self):
        status, data = self.get("/api/overview")
        self.assertEqual(status, 200)
        m = {x["name"]: x["value"] for x in data["metrics"]}
        self.assertEqual(m["source_reviews"], 10)
        self.assertEqual(m["quarantined_reviews"], 1)
        self.assertEqual(m["completed_reviews"], 9)
        self.assertEqual(m["cache_reused_reviews"], 2)  # two "Great app" copies reuse the first label
        self.assertEqual(m["grouped_records"], sum(r["complaint_count"] for r in self.ranking))
        self.assertEqual(sum(b["n"] for b in data["breakdown"]["topic"]), 9)
        self.assertEqual(data["meta"]["recommendations_synthetic"], "true")

    def test_issue_detail_lists_member_reviews_and_404s(self):
        top = self.ranking[0]["issue_id"]
        status, data = self.get(f"/api/issues/{top}", "limit=5")
        self.assertEqual(status, 200)
        self.assertEqual(len(data["reviews"]), min(5, self.ranking[0]["complaint_count"]))
        self.assertTrue(all(r["evidence_quote"] for r in data["reviews"]))
        self.assertEqual(self.get("/api/issues/no.such")[0], 404)
        self.assertEqual(self.get("/api/reviews/missing")[0], 404)

    def test_recommendations_resolve_claims_and_cited_reviews(self):
        status, data = self.get("/api/recommendations")
        rec = data["recommendations"][0]
        self.assertTrue(rec["synthetic"])
        self.assertEqual(rec["claims"][0]["value"], str(self.ranking[0]["priority_score"]))
        self.assertFalse(rec["reviews"][0].get("missing", False))

    def test_read_only_and_static_page(self):
        status, _, _ = call("/api/health", method="POST")
        self.assertEqual(status, 405)
        status, headers, body = call("/")
        self.assertEqual(status, 200)
        self.assertIn(b"Spotify Review Insights", body)
        before = self.serving_db.read_bytes()
        self.get("/api/overview")
        self.assertEqual(before, self.serving_db.read_bytes())

    def test_errors_do_not_leak_details(self):
        dashboard.os.environ["SERVING_DB"] = str(self.serving_db) + ".missing"
        try:
            err = io.StringIO()
            old, sys.stderr = sys.stderr, err
            try:
                status, data = self.get("/api/overview")
            finally:
                sys.stderr = old
        finally:
            dashboard.os.environ["SERVING_DB"] = str(self.serving_db)
        self.assertEqual(status, 500)
        self.assertNotIn(".missing", json.dumps(data))

    def test_weighted_comparison_full_coverage_matches_baseline(self):
        # every nonempty review is completed, so all weights are 1 and the order equals the baseline
        self.assertEqual({s["weight"] for s in self.weighted["strata"].values()}, {1.0})
        self.assertTrue(all(i["baseline_rank"] == i["weighted_rank"] for i in self.weighted["issues"]))
        status, data = self.get("/api/weighted")
        self.assertEqual(len(data["issues"]), len(self.ranking))


class DashboardDetailTests(DashboardTests):
    """More API behaviour on the same synthetic fixture (inherits the class-level setup)."""

    def test_paging_covers_every_member_exactly_once(self):
        top = self.ranking[0]
        seen, offset = [], 0
        while True:
            _, page = self.get(f"/api/issues/{top['issue_id']}", f"limit=1&offset={offset}")
            if not page["reviews"]:
                break
            seen += [r["review_id"] for r in page["reviews"]]
            offset += 1
        self.assertEqual(len(seen), top["complaint_count"])
        self.assertEqual(len(set(seen)), len(seen))
        severities = [self.get(f"/api/reviews/{rid}")[1]["review"]["severity"] for rid in seen]
        self.assertEqual(severities, sorted(severities, reverse=True))      # most severe first
        self.assertEqual(sum(severities), top["severity_sum"])               # drill-down adds up to the ranking

    def test_invalid_or_extreme_parameters_are_clamped(self):
        top = self.ranking[0]["issue_id"]
        for query in ("limit=abc", "limit=-5", "limit=100000", "offset=-3", "offset=zz"):
            status, data = self.get(f"/api/issues/{top}", query)
            self.assertEqual(status, 200, query)
            self.assertLessEqual(len(data["reviews"]), dashboard.MAX_PAGE)
        status, data = self.get("/api/issues", "limit=1")
        self.assertEqual(len(data["issues"]), 1)
        status, data = self.get(f"/api/issues/{top}", "offset=999999")
        self.assertEqual((status, data["reviews"]), (200, []))

    def test_review_detail_lists_its_issue(self):
        issue_id, review_id = next(iter(self.membership_pairs()))
        status, data = self.get(f"/api/reviews/{review_id}")
        self.assertEqual(status, 200)
        self.assertEqual(data["issues"], [issue_id])
        self.assertEqual(data["review"]["review_id"], review_id)

    def membership_pairs(self):
        with (self.analysis / "membership.csv").open() as f:
            return [(r["issue_id"], r["review_id"]) for r in csv.DictReader(f)]

    def test_breakdowns_and_metrics_are_consistent(self):
        _, data = self.get("/api/overview")
        m = {x["name"]: x["value"] for x in data["metrics"]}
        for dimension in ("topic", "intent", "severity", "sentiment"):
            self.assertEqual(sum(b["n"] for b in data["breakdown"][dimension]), m["completed_reviews"], dimension)
        self.assertEqual(m["source_reviews"], m["completed_reviews"] + m["pending_reviews"] + m["quarantined_reviews"])
        intents = {b["value"]: b["n"] for b in data["breakdown"]["intent"]}
        self.assertEqual(m["complaint_or_cancellation"], intents.get("complaint", 0) + intents.get("cancellation", 0))
        self.assertEqual(m["grouped_records"], m["complaint_or_cancellation"])   # every complaint belongs to an issue

    def test_unknown_routes_and_head(self):
        self.assertEqual(self.get("/api/nothing")[0], 404)
        self.assertEqual(self.get("/api/issues/")[0], 404)
        status, _, _ = call("/api/health", method="HEAD")
        self.assertEqual(status, 200)

    def test_page_escapes_stored_text(self):
        page = (REPO_ROOT / "dashboard" / "static" / "index.html").read_text()
        self.assertIn("function esc(", page)
        # every interpolation of stored text in the page goes through esc(); no raw innerHTML of review fields
        for field in ("evidence_quote", "review_text", "body_md", "title", "name"):
            self.assertNotIn("${v." + field + "}", page)
            self.assertNotIn("${r." + field + "}", page)
            self.assertNotIn("${i." + field + "}", page)


class TrendTests(DashboardTests):
    def test_trend_rows_have_denominators_and_flags(self):
        status, data = self.get("/api/trends")
        self.assertEqual(status, 200)
        months = data["months"]
        self.assertEqual(sum(m["completed"] for m in months), 9)
        self.assertTrue(months[0]["partial_month"] and months[-1]["partial_month"])
        for m in months:
            if m["completed"]:
                self.assertAlmostEqual(m["complaint_share"], m["complaint_or_cancellation"] / m["completed"], places=6)

    def test_period_comparison(self):
        rows = [{"month": "2022-06", "nonempty_reviews": 40, "completed": 10, "complaint_or_cancellation": 2,
                 "weighted_complaint_share": 0.3},
                {"month": "2023-06", "nonempty_reviews": 40, "completed": 20, "complaint_or_cancellation": 10,
                 "weighted_complaint_share": 0.6}]
        result = trends.compare_periods(rows, (["2022-06"], ["2023-06"]))
        self.assertEqual(result["2022"]["complaint_share"], 0.2)
        self.assertEqual(result["2023"]["complaint_share"], 0.5)
        self.assertEqual(result["change_in_share_points"], 30.0)
        self.assertEqual(result["weighted_change_in_share_points"], 30.0)

    def test_weighted_share_corrects_stratum_imbalance(self):
        # duplicates: 100 reviews, all completed, 10% complaints; singletons: 300 reviews, 30 completed, 60%
        self.assertEqual(trends._weighted([[100, 100, 10], [300, 30, 18]]), round((100 * 0.1 + 300 * 0.6) / 400, 6))
        self.assertIsNone(trends._weighted([[100, 100, 10], [300, 0, 0]]))   # a stratum with no estimate
        self.assertIsNone(trends._weighted([]))


class EmptyStateTests(unittest.TestCase):
    """A serving database without recommendations or weighting still serves every endpoint."""

    def test_missing_optional_outputs_give_empty_lists(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            conn = make_db(d)
            run_enrichment(conn, MockProvider(max_batch=3), SpendLedger(Decimal("1"), d / "l.jsonl"),
                           Options(sleep=no_sleep))
            records = {r["review_id"]: r for r in group.records_from_db(conn, exclude_mock=False)}
            conn.close()
            membership, issues = group.assign(records.values())
            group.write_membership(d / "a" / "membership.csv", membership)
            group.write_issues(d / "a" / "issues.csv", issues)
            rank.write_ranking(d / "a" / "ranking.csv", rank.compute(records, membership))
            db_file = serving.write_sqlite(d / "s.sqlite", serving.collect(d / "state.sqlite", d / "a", allow_mock=True))
            old = dashboard.os.environ.get("SERVING_DB")
            dashboard.os.environ["SERVING_DB"] = str(db_file)
            try:
                for path in ("/api/recommendations", "/api/weighted"):
                    status, _, body = call(path)
                    self.assertEqual(status, 200)
                    self.assertEqual(list(json.loads(body).values())[0], [])
                status, _, body = call("/api/overview")
                self.assertEqual(json.loads(body)["meta"]["recommendations_present"], "false")
            finally:
                if old is None:
                    dashboard.os.environ.pop("SERVING_DB", None)
                else:
                    dashboard.os.environ["SERVING_DB"] = old

    def test_no_database_configured_returns_clean_500(self):
        saved = {k: dashboard.os.environ.pop(k, None) for k in ("DATABASE_URL", "POSTGRES_URL", "SERVING_DB")}
        try:
            stderr, sys.stderr = sys.stderr, io.StringIO()
            try:
                status, _, body = call("/api/health")
            finally:
                sys.stderr = stderr
            self.assertEqual(status, 500)
            self.assertEqual(json.loads(body), {"error": "RuntimeError"})
            status, _, _ = call("/")          # the page itself still loads and shows the error banner
            self.assertEqual(status, 200)
        finally:
            for k, v in saved.items():
                if v is not None:
                    dashboard.os.environ[k] = v


class WeightTests(unittest.TestCase):
    def test_partial_singleton_coverage_upweights_singletons(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            conn = make_db(d)
            run_enrichment(conn, MockProvider(max_batch=3), SpendLedger(Decimal("1"), d / "l.jsonl"),
                           Options(sleep=no_sleep))
            # pretend two singleton reviews were never classified
            ids = [r[0] for r in conn.execute(
                "SELECT review_id FROM reviews WHERE status='completed' AND text_sha256 IN "
                "(SELECT text_sha256 FROM reviews GROUP BY text_sha256 HAVING COUNT(*) = 1) ORDER BY review_id LIMIT 2")]
            conn.executemany("DELETE FROM labels WHERE review_id = ?", [(i,) for i in ids])
            conn.executemany("UPDATE reviews SET status='pending' WHERE review_id = ?", [(i,) for i in ids])
            conn.commit()
            member, counts = weighted.strata(conn)
            conn.close()
            self.assertEqual(counts["duplicates"], (3, 3))
            pop, done = counts["singletons"]
            self.assertEqual(pop - done, 2)
            self.assertNotIn(ids[0], member)

    def test_spearman(self):
        self.assertEqual(weighted.spearman([1, 2, 3], [1, 2, 3]), 1.0)
        self.assertEqual(weighted.spearman([1, 2, 3], [3, 2, 1]), -1.0)


if __name__ == "__main__":
    unittest.main()


class FakePostgres:
    """Minimal psycopg-like connection over SQLite with explicit transactions (Postgres DDL is
    transactional too), so the migration can be tested without any external database."""

    def __init__(self, path, fail_on_table=None, roles=()):
        import sqlite3
        self.db = sqlite3.connect(path, isolation_level=None)
        self.db.execute("BEGIN")
        self.fail_on_table = fail_on_table
        self.closed = False
        self.roles, self.grants = set(roles), []

    def cursor(self):
        outer = self

        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def execute(self, sql, params=()):
                if "pg_roles" in sql:
                    self.result = outer.db.execute("SELECT 1 WHERE ?", (params[0] in outer.roles,))
                elif sql.startswith("GRANT"):
                    outer.grants.append(sql)
                else:
                    self.result = outer.db.execute(sql, params)

            def fetchone(self):
                return self.result.fetchone()

            def fetchall(self):
                return self.result.fetchall()

            def copy(self, statement):
                table = statement.split()[1]

                class Copy:
                    def __enter__(self):
                        return self

                    def __exit__(self, *exc):
                        return False

                    def write_row(self, row):
                        if table == outer.fail_on_table:
                            raise RuntimeError("simulated network drop during COPY")
                        marks = ",".join("?" * len(row))
                        outer.db.execute(f"INSERT INTO {table} VALUES ({marks})", row)
                return Copy()
        return Cursor()

    def commit(self):
        self.db.execute("COMMIT")

    def rollback(self):
        self.db.execute("ROLLBACK")

    def close(self):
        self.closed = True
        self.db.close()


class MigrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        DashboardTests.setUpClass.__func__(DashboardTests)  # reuse the synthetic analysis fixture
        self.tables = serving.collect(DashboardTests.db_path, DashboardTests.analysis, all_records=True)
        self.target = Path(self.tmp.name) / "fake_pg.sqlite"

    def tearDown(self):
        DashboardTests.tearDownClass.__func__(DashboardTests)
        self.tmp.cleanup()

    def test_reload_regrants_reader_role(self):
        fake = FakePostgres(self.target, roles={"dashboard_reader"})
        serving.write_postgres("x", self.tables, connect=lambda url: fake)
        self.assertEqual(len(fake.grants), 1)
        self.assertTrue(fake.grants[0].startswith("GRANT SELECT ON meta, metrics,"))
        self.assertTrue(fake.grants[0].endswith('TO "dashboard_reader"'))
        for table in serving.TABLES:
            self.assertIn(table, fake.grants[0])
        no_role = FakePostgres(Path(self.tmp.name) / "fake_pg2.sqlite")
        serving.write_postgres("x", self.tables, connect=lambda url: no_role)
        self.assertEqual(no_role.grants, [])   # no reader role yet: nothing to grant

    def test_load_verifies_and_commits(self):
        counts = serving.write_postgres("postgres://user:secret@example/db", self.tables,
                                        connect=lambda url: FakePostgres(self.target))
        self.assertEqual(counts["issues"], len(self.tables["issues"]))
        self.assertEqual(counts["reviews"], 9)        # all completed records of the fixture
        import sqlite3
        conn = sqlite3.connect(self.target)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM memberships").fetchone()[0], len(self.tables["memberships"]))
        conn.close()

    def test_failure_mid_load_keeps_previous_tables(self):
        serving.write_postgres("x", self.tables, connect=lambda url: FakePostgres(self.target))
        changed = dict(self.tables, meta=[("serving_version", "NEW")])
        with self.assertRaises(RuntimeError):
            serving.write_postgres("x", changed, connect=lambda url: FakePostgres(self.target, fail_on_table="reviews"))
        import sqlite3
        conn = sqlite3.connect(self.target)
        self.assertEqual(dict(conn.execute("SELECT key, value FROM meta"))["serving_version"], serving.SERVING_VERSION)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM reviews").fetchone()[0], 9)
        conn.close()

    def test_estimate_reports_every_table(self):
        estimate = serving.estimate_bytes(self.tables)
        self.assertEqual(set(estimate["tables"]), set(serving.TABLES))
        self.assertGreater(estimate["total_bytes"], 0)
