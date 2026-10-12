"""Offline tests for the enrichment orchestrator. Mock provider and synthetic reviews only."""

import csv
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from pipeline import db, extract
from pipeline.checkpoint import snapshot
from pipeline.common import FIELDS
from pipeline.enrich import STRATA, Options, build_queue, order_key, run_enrichment, scope_counts
from pipeline.ingest import ingest
from pipeline.labels import apply_consistency, validate_label
from pipeline.ledger import SpendLedger
from pipeline.providers import JevProvider, MockProvider

LONG = ("I have used this app for years. The interface used to be clean and simple. "
        "Now every time I open a downloaded playlist offline the songs disappear and I have to download them again. "
        "Please fix this because it ruins my commute and I pay every month for this.")
TEXTS = [
    "Great app", "Great app", "Great app",                       # exact duplicates -> one request
    "I can't log in, password reset never arrives",
    "Songs keep stopping and the app crashes",
    "I was charged twice for premium, uninstalling",
    "Please add lyrics for more songs",
    "Too many ads",
    LONG,
    "",                                                          # empty -> quarantined at ingestion
]


def make_db(directory, texts=TEXTS):
    path = Path(directory) / "input.csv"
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(FIELDS)
        for i, text in enumerate(texts, 1):
            writer.writerow([f"id{i:03d}", text, "3", "0", "", f"2023-01-{i % 28 + 1:02d} 10:00:00"])
    db_path = Path(directory) / "state.sqlite"
    ingest(path, db_path, Path(directory) / "out", Path(directory) / "grading")
    return db.connect(db_path)


def no_sleep(_):
    return None


class EnrichTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.conn = make_db(self.dir)
        self.ledger_path = self.dir / "ledger.jsonl"

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def ledger(self, cap="1.00"):
        return SpendLedger(Decimal(cap), self.ledger_path)

    def run_mock(self, provider=None, cap="1.00", **options):
        provider = provider or MockProvider(max_batch=3)
        return run_enrichment(self.conn, provider, self.ledger(cap), Options(sleep=no_sleep, **options)), provider

    def labels(self):
        return {r["review_id"]: dict(r) for r in self.conn.execute("SELECT * FROM labels")}

    def calls(self):
        return [dict(r) for r in self.conn.execute("SELECT * FROM calls ORDER BY started_at, request_id")]

    def test_full_run_one_request_per_distinct_text_with_cache_provenance(self):
        summary, provider = self.run_mock()
        self.assertEqual(db.status_counts(self.conn), {"pending": 0, "completed": 9, "quarantined": 1})
        self.assertEqual(summary.completed_by_model, 7)
        self.assertEqual(summary.completed_by_cache, 2)
        labels = self.labels()
        cached = {rid: l for rid, l in labels.items() if l["cache_source_id"]}
        great = {"id001", "id002", "id003"}
        self.assertEqual(len(cached), 2)
        self.assertTrue(set(cached) < great)
        self.assertEqual({l["cache_source_id"] for l in cached.values()}, great - set(cached))
        for rid, label in cached.items():
            source = labels[label["cache_source_id"]]
            self.assertIsNone(source["cache_source_id"])           # direct original, no chains
            self.assertIsNotNone(source["request_id"])             # original has call evidence
            self.assertIsNone(label["request_id"])                 # reuse is not a new call
            for field in ("topic", "intent", "sentiment", "severity", "entities", "evidence_quote",
                          "needs_review", "label_config"):
                self.assertEqual(label[field], source[field])
        sent = [rid for call in self.calls() for rid in json.loads(call["review_ids"])]
        self.assertEqual(len(sent), len(set(sent)))                 # nothing sent twice
        self.assertEqual(len(great & set(sent)), 1)                 # one request for three identical texts
        self.assertTrue(all(len(json.loads(c["review_ids"])) <= 3 for c in self.calls()))
        texts = dict(self.conn.execute("SELECT review_id, review_text FROM reviews"))
        for rid, label in labels.items():
            record = dict(label, entities=json.loads(label["entities"]), needs_review=bool(label["needs_review"]))
            self.assertEqual(validate_label(record, texts[rid]), [])

    def test_calls_and_ledger_record_usage_and_cost(self):
        summary, _ = self.run_mock()
        calls = self.calls()
        self.assertEqual(len(calls), summary.requests)
        self.assertTrue(all(c["outcome"] == "succeeded" and c["input_tokens"] > 0 for c in calls))
        self.assertTrue(all(c["role"] == "enrich" and c["phase"] == "initial" for c in calls))
        ledger_lines = [json.loads(x) for x in self.ledger_path.read_text().splitlines()]
        self.assertEqual(len(ledger_lines), len(calls))
        total = sum(Decimal(c["cost_usd"]) for c in calls)
        self.assertEqual(Decimal(summary.charged_usd), total)

    def test_limit_takes_seeded_sample_and_later_run_continues(self):
        first, _ = self.run_mock(limit=3)
        self.assertGreaterEqual(first.completed_by_model + first.completed_by_cache, 3)
        before = {r[0] for r in self.conn.execute("SELECT review_id FROM reviews WHERE status = 'completed'")}
        self.assertLess(len(before), 9)
        self.run_mock()
        later_sent = [rid for c in self.calls()[first.requests:] for rid in json.loads(c["review_ids"])]
        self.assertFalse(before & set(later_sent))
        self.assertEqual(db.status_counts(self.conn)["pending"], 0)

    def test_queue_order_is_deterministic(self):
        self.assertEqual(build_queue(self.conn), build_queue(self.conn))

    def test_invalid_output_retried_once_then_quarantined(self):
        provider = MockProvider(max_batch=3, malformed_texts={"Too many ads"})
        summary, _ = self.run_mock(provider)
        row = self.conn.execute("SELECT status, reason, invalid_outputs FROM reviews WHERE review_text = 'Too many ads'").fetchone()
        self.assertEqual(row["status"], "quarantined")
        self.assertTrue(row["reason"].startswith("invalid_model_output"))
        self.assertEqual(row["invalid_outputs"], 2)
        self.assertEqual(summary.invalid_outputs, 2)

    def test_missing_id_in_response_is_retried_then_quarantined(self):
        provider = MockProvider(max_batch=3, drop_texts={"Please add lyrics for more songs"})
        self.run_mock(provider)
        row = self.conn.execute("SELECT status, reason FROM reviews WHERE review_text LIKE 'Please add lyrics%'").fetchone()
        self.assertEqual(row["status"], "quarantined")
        self.assertIn("id missing from response", row["reason"])

    def test_transient_failure_is_retried_with_backoff_and_logged(self):
        provider = MockProvider(max_batch=3, fail_requests={1})
        summary, _ = self.run_mock(provider)
        self.assertEqual(summary.failed_requests, 1)
        self.assertEqual(db.status_counts(self.conn)["pending"], 0)
        failed = [c for c in self.calls() if c["outcome"] == "failed"]
        self.assertEqual(len(failed), 1)
        retried = [c for c in self.calls() if c["review_ids"] == failed[0]["review_ids"] and c["outcome"] == "succeeded"]
        self.assertEqual(retried[0]["attempt"], 2)

    def test_fatal_error_stops_run_and_leaves_reviews_pending(self):
        provider = MockProvider(max_batch=3, fatal_requests={1})
        summary, _ = self.run_mock(provider)
        self.assertEqual(summary.stop_reason, "fatal_provider_error")
        counts = db.status_counts(self.conn)
        self.assertEqual(counts["quarantined"], 1)                  # only the empty text
        self.assertGreater(counts["pending"], 0)

    def test_timeout_charges_full_reservation_as_unknown(self):
        provider = MockProvider(max_batch=3, timeout_requests={1})
        self.run_mock(provider)
        first = json.loads(self.ledger_path.read_text().splitlines()[0])
        self.assertEqual(first["cost_source"], "unknown")
        self.assertEqual(first["charged_usd"], first["reserved_usd"])

    def test_budget_cap_stops_before_exceeding(self):
        provider = MockProvider(max_batch=1, overhead_tokens=10_000)   # about $0.00042 per request
        summary, _ = self.run_mock(provider, cap="0.001")
        self.assertEqual(summary.stop_reason, "budget_cap")
        self.assertLessEqual(SpendLedger(Decimal("0.001"), self.ledger_path).spent, Decimal("0.001"))
        self.assertGreater(db.status_counts(self.conn)["pending"], 0)

    def test_ledger_is_shared_across_runs(self):
        self.run_mock(limit=2)
        spent = SpendLedger(Decimal("1"), self.ledger_path).spent
        self.assertGreater(spent, 0)
        summary, _ = self.run_mock(cap=str(spent))                      # nothing left under this cap
        self.assertEqual(summary.stop_reason, "budget_cap")

    def test_interruption_and_resume_never_resend_completed_ids(self):
        provider = MockProvider(max_batch=2)
        first, _ = self.run_mock(provider, stop_after_requests=2)
        self.assertEqual(first.stop_reason, "simulated_interruption")
        before = snapshot(self.conn, "before")
        resumed, _ = self.run_mock(MockProvider(max_batch=2))
        self.assertEqual(resumed.phase, "resume")
        after = snapshot(self.conn, "after")
        self.assertTrue(set(before["completed_ids"]) < set(after["completed_ids"]))
        for call in self.calls():
            if call["phase"] == "resume":
                self.assertFalse(set(json.loads(call["review_ids"])) & set(before["completed_ids"]))

    def test_changed_label_config_does_not_reuse_results(self):
        self.run_mock(limit=1)
        other = MockProvider(max_batch=3)
        other.model = "mock/keyword-rules-v2"
        summary, _ = self.run_mock(other)
        configs = {r[0] for r in self.conn.execute("SELECT DISTINCT label_config FROM labels")}
        self.assertEqual(len(configs), 2)
        for label in self.labels().values():
            if label["cache_source_id"]:
                self.assertEqual(label["label_config"], self.labels()[label["cache_source_id"]]["label_config"])

    def test_parallel_workers_complete_everything_once(self):
        provider = MockProvider(max_batch=1, latency_s=0.01)
        summary, _ = self.run_mock(provider, workers=3, requests_per_second=1000)
        self.assertEqual(db.status_counts(self.conn)["pending"], 0)
        sent = [rid for c in self.calls() for rid in json.loads(c["review_ids"])]
        self.assertEqual(len(sent), len(set(sent)))

    def test_rejects_batches_over_fifty(self):
        with self.assertRaises(ValueError):
            self.run_mock(MockProvider(max_batch=51))


class SampleScopeTests(unittest.TestCase):
    def test_sample_scope_only_completes_sample_members(self):
        with tempfile.TemporaryDirectory() as d:
            conn = make_db(d)
            with conn:
                conn.executemany("INSERT INTO sample_membership VALUES ('pilot', ?, ?)", [("id001", 1), ("id004", 2)])
            run_enrichment(conn, MockProvider(max_batch=3), SpendLedger(Decimal("1"), Path(d) / "l.jsonl"),
                           Options(sample="pilot", sleep=no_sleep))
            done = {r[0] for r in conn.execute("SELECT review_id FROM reviews WHERE status = 'completed'")}
            self.assertEqual(done, {"id001", "id004"})                # duplicates outside the sample untouched
            conn.close()


class JevProviderTests(unittest.TestCase):
    KEY = "fake-key-should-never-appear-0123"

    def provider(self, status, payload):
        body = json.dumps(payload).encode()
        return JevProvider(self.KEY, transport=lambda request_body: (status, body))

    def test_maps_answers_usage_and_gateway_cost(self):
        payload = {"model": "typesafe-ai/jev", "usage": {"input_tokens": 512, "output_tokens": 30},
                   "answers": {"topic": {"type": "choice", "choice": "access", "confidence": 0.9,
                                         "probabilities": {"access": 0.95}},
                               "intent": {"type": "choice", "choice": "complaint", "confidence": 0.8},
                               "severity": {"type": "choice", "choice": "4", "confidence": 0.7},
                               "sentiment": {"type": "score", "score": 0.5}},
                   "provider_metadata": {"gateway": {"cost": "0.0000215", "generationId": "gen_abc"}}}
        provider = self.provider(200, payload)
        response = provider.classify([("r1", "cannot log in")])
        self.assertTrue(response.ok)
        self.assertEqual((response.request_id, response.input_tokens, response.cost_usd), ("gen_abc", 512, "0.0000215"))
        topic, intent, severity, sentiment, confidence, _ = provider.to_label(response.items["r1"])
        self.assertEqual((topic, intent, severity, sentiment), ("access", "complaint", 4, -0.75))

    def test_error_classification_and_key_redaction(self):
        forbidden = self.provider(403, {"error": {"message": f"denied {self.KEY}"}}).classify([("r1", "x")])
        self.assertTrue(forbidden.fatal)
        self.assertNotIn(self.KEY, forbidden.error)
        limited = self.provider(429, {"error": {"message": "slow down"}}).classify([("r1", "x")])
        self.assertTrue(limited.retryable)
        self.assertNotIn(self.KEY, repr(JevProvider(self.KEY)))

    def test_timeout_is_unknown_outcome(self):
        def boom(_):
            raise TimeoutError("timed out")
        response = JevProvider(self.KEY, transport=boom).classify([("r1", "x")])
        self.assertTrue(response.retryable and response.outcome_unknown)

    def test_reservation_is_at_least_1200_tokens_per_review(self):
        provider = JevProvider(self.KEY, transport=lambda body: (500, b"{}"))
        self.assertGreaterEqual(provider.estimate_input_tokens([("r1", "")]), 1200)
        self.assertGreaterEqual(provider.estimate_input_tokens([("r1", "x" * 486)]), 1200 + 486 // 3)
        self.assertEqual(provider.estimate_cost(1200), Decimal("0.0000504"))

    def test_one_review_per_request(self):
        with self.assertRaises(ValueError):
            self.provider(200, {}).classify([("a", "x"), ("b", "y")])


class RuleTests(unittest.TestCase):
    def test_consistency_rules(self):
        self.assertEqual(apply_consistency("other", "praise", 3)[0], 1)
        self.assertEqual(apply_consistency("other", "complaint", 1)[0], 2)
        self.assertEqual(apply_consistency("playback", "cancellation", 2), (2, []))

    def test_quote_is_exact_substring_and_entities_explicit(self):
        quote, method = extract.evidence_quote(LONG, "downloads")
        self.assertIn(quote, LONG)
        self.assertEqual(method, "topic_keyword_sentence")
        self.assertIn("offline", quote)
        self.assertEqual(extract.evidence_quote("Short text", "other"), ("Short text", "full_text"))
        self.assertEqual(extract.find_entities("Too many ads on premium"), ["ads", "premium"])
        self.assertEqual(extract.find_entities("I love music"), [])


STRATUM_TEXTS = [
    "Great app", "Great app", "Great app",                       # duplicate group of three
    "Too many ads", "Too many ads",                              # duplicate group of two
    "great app", "Great app ",                                   # exact text differs: singletons
    "I can't log in, password reset never arrives",
    "Songs keep stopping and the app crashes",
    "Please add lyrics for more songs",
    LONG,
    "", "   ",                                                   # empty -> outside every stratum
]
DUPLICATE_IDS = {"id001", "id002", "id003", "id004", "id005"}
SINGLETON_IDS = {f"id{i:03d}" for i in range(6, 12)}


class StratumTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.conn = make_db(self.dir, STRATUM_TEXTS)
        self.ledger_path = self.dir / "ledger.jsonl"

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def run_mock(self, provider=None, **options):
        provider = provider or MockProvider(max_batch=1)
        ledger = SpendLedger(Decimal("1"), self.ledger_path)
        return run_enrichment(self.conn, provider, ledger, Options(sleep=no_sleep, **options))

    def completed(self):
        return {r[0] for r in self.conn.execute("SELECT review_id FROM reviews WHERE status = 'completed'")}

    def sent(self):
        return [rid for (ids,) in self.conn.execute("SELECT review_ids FROM calls") for rid in json.loads(ids)]

    def members(self, stratum):
        return {m for _, _, ms in build_queue(self.conn, stratum=stratum) for m in ms}

    def test_strata_are_disjoint_exact_text_and_cover_every_nonempty_review(self):
        duplicates, singletons = self.members("duplicates"), self.members("singletons")
        self.assertEqual(duplicates, DUPLICATE_IDS)
        self.assertEqual(singletons, SINGLETON_IDS)                  # case/space variants are not merged
        self.assertFalse(duplicates & singletons)
        nonempty = {r[0] for r in self.conn.execute("SELECT review_id FROM reviews WHERE text_sha256 IS NOT NULL")}
        self.assertEqual(duplicates | singletons, nonempty)
        self.assertEqual(len(nonempty), len(STRATUM_TEXTS) - 2)
        self.assertEqual(scope_counts(self.conn, stratum="duplicates")["total"], 5)
        self.assertEqual(scope_counts(self.conn, stratum="singletons")["total"], 6)
        self.assertEqual(len(build_queue(self.conn, stratum="duplicates")), 2)   # two distinct texts
        with self.assertRaises(ValueError):
            build_queue(self.conn, stratum="normalized")

    def test_membership_does_not_change_as_reviews_complete(self):
        self.run_mock(stratum="duplicates", max_requests=1)
        self.assertEqual(scope_counts(self.conn, stratum="duplicates")["total"], 5)
        self.assertEqual(scope_counts(self.conn, stratum="singletons")["total"], 6)

    def test_singletons_follow_seeded_order_deterministically(self):
        queue = build_queue(self.conn, stratum="singletons")
        self.assertEqual(queue, build_queue(self.conn, stratum="singletons"))
        self.assertEqual([rep for rep, _, _ in queue], sorted(SINGLETON_IDS, key=order_key))
        first_three = [rep for rep, _, _ in build_queue(self.conn, stratum="singletons", max_groups=3)]
        self.assertEqual(first_three, sorted(SINGLETON_IDS, key=order_key)[:3])
        self.run_mock(stratum="singletons", max_requests=3)
        self.assertEqual(self.sent(), first_three)                   # the uniform seeded prefix
        texts = dict(self.conn.execute("SELECT review_id, review_text FROM reviews"))
        self.assertEqual([t for _, t, _ in queue], [texts[r] for r, _, _ in queue])

    def test_duplicates_use_one_request_per_text_and_cache_the_rest(self):
        summary = self.run_mock(stratum="duplicates")
        self.assertEqual(self.completed(), DUPLICATE_IDS)
        self.assertEqual((summary.requests, summary.completed_by_model, summary.completed_by_cache), (2, 2, 3))
        cached = self.conn.execute("SELECT COUNT(*) FROM labels WHERE cache_source_id IS NOT NULL").fetchone()[0]
        self.assertEqual(cached, 3)
        self.assertEqual(scope_counts(self.conn, stratum="singletons")["completed"], 0)

    def test_both_strata_complete_everything_once_without_skips(self):
        self.run_mock(stratum="duplicates")
        self.run_mock(stratum="singletons")
        self.assertEqual(db.status_counts(self.conn), {"pending": 0, "completed": 11, "quarantined": 2})
        sent = self.sent()
        self.assertEqual(len(sent), len(set(sent)))
        self.assertEqual(len(sent), 2 + 6)                           # one per distinct text
        self.assertEqual(self.run_mock().requests, 0)                # nothing left to send

    def test_cache_fills_stratum_members_left_by_an_earlier_sample_run(self):
        with self.conn:
            self.conn.execute("INSERT INTO sample_membership VALUES ('pilot', 'id002', 1)")
        self.run_mock(sample="pilot")
        self.assertEqual(self.completed(), {"id002"})
        summary = self.run_mock(stratum="duplicates")
        self.assertEqual(summary.completed_by_cache, 3)              # id001, id003 reuse id002 (direct)
        self.assertEqual(summary.requests, 1)                        # only "Too many ads"
        sources = {r[0] for r in self.conn.execute("SELECT cache_source_id FROM labels WHERE review_id IN"
                                                   " ('id001', 'id003')")}
        self.assertEqual(sources, {"id002"})

    def test_sample_and_stratum_combine_as_intersection(self):
        with self.conn:
            self.conn.executemany("INSERT INTO sample_membership VALUES ('pilot', ?, ?)",
                                  [("id001", 1), ("id006", 2), ("id007", 3)])
        self.run_mock(sample="pilot", stratum="singletons")
        self.assertEqual(self.completed(), {"id006", "id007"})
        self.run_mock(sample="pilot")                                # sample alone behaves as before
        self.assertEqual(self.completed(), {"id001", "id006", "id007"})

    def test_interrupt_and_resume_within_a_stratum(self):
        first = self.run_mock(stratum="singletons", stop_after_requests=2)
        self.assertEqual(first.stop_reason, "simulated_interruption")
        before = snapshot(self.conn, "before")
        self.assertEqual(set(before["completed_ids"]), set(sorted(SINGLETON_IDS, key=order_key)[:2]))
        resumed = self.run_mock(stratum="singletons")
        self.assertEqual((resumed.phase, resumed.requests), ("resume", 4))
        resume_sent = [rid for (ids,) in self.conn.execute("SELECT review_ids FROM calls WHERE phase = 'resume'")
                       for rid in json.loads(ids)]
        self.assertFalse(set(resume_sent) & set(before["completed_ids"]))
        self.assertEqual(scope_counts(self.conn, stratum="singletons")["pending"], 0)

    def test_limit_still_counts_source_reviews(self):
        summary = self.run_mock(stratum="duplicates", limit=3)
        self.assertGreaterEqual(summary.completed_by_model + summary.completed_by_cache, 3)
        self.assertEqual(self.completed() - DUPLICATE_IDS, set())


class ProgressLogTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.conn = make_db(self.dir, STRATUM_TEXTS)
        self.ledger_path = self.dir / "ledger.jsonl"
        self.log_dir = self.dir / "run"

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def run_mock(self, provider=None, **options):
        provider = provider or MockProvider(max_batch=1)
        options.setdefault("log_dir", self.log_dir)
        options.setdefault("echo_progress", False)
        return run_enrichment(self.conn, provider, SpendLedger(Decimal("1"), self.ledger_path),
                              Options(sleep=no_sleep, **options))

    def lines(self, name):
        return [json.loads(x) for x in (self.log_dir / name).read_text().splitlines()]

    def test_progress_is_written_incrementally_with_counts_and_spend(self):
        summary = self.run_mock(stratum="singletons", progress_every_responses=2, run_id="enrich-test")
        progress = self.lines("progress.jsonl")
        self.assertEqual(progress[0]["state"], "running")
        self.assertEqual(progress[0]["reviews_in_scope"]["pending"], 6)
        self.assertGreaterEqual(len(progress), 2 + 6 // 2)           # start, every 2 responses, finish
        last = progress[-1]
        self.assertEqual((last["state"], last["stop_reason"]), ("finished", "queue_empty"))
        self.assertEqual(last["reviews_in_scope"], {"pending": 0, "completed": 6, "quarantined": 0, "total": 6})
        self.assertEqual(last["requests"]["sent"], 6)
        self.assertEqual(last["requests"]["first_dispatches"], 6)
        self.assertEqual(last["spend_usd"]["this_run"], summary.charged_usd)
        ledger_total = sum(Decimal(json.loads(x)["charged_usd"]) for x in self.ledger_path.read_text().splitlines())
        self.assertEqual(Decimal(last["spend_usd"]["ledger_total"]), ledger_total)
        self.assertEqual(json.loads((self.log_dir / "status.json").read_text()), last)
        completed = [p["reviews_in_scope"]["completed"] for p in progress]
        self.assertEqual(completed, sorted(completed))               # monotonic
        events = self.lines("run_log.jsonl")
        self.assertEqual([e["event"] for e in events][0], "queue_built")
        self.assertEqual(events[-1]["event"], "run_finished")
        self.assertEqual(summary.events, [])                         # streamed, not held in memory

    def test_errors_and_quarantines_are_reported(self):
        provider = MockProvider(max_batch=1, fail_requests={1}, malformed_texts={"Please add lyrics for more songs"})
        self.run_mock(provider)
        last = self.lines("progress.jsonl")[-1]
        self.assertEqual(last["errors"], {"failed_requests": 1, "invalid_outputs": 2, "quarantined_this_run": 1})
        self.assertEqual(last["requests"]["retries"], 2)             # one transient, one invalid-output retry
        self.assertEqual(last["reviews_in_scope"]["quarantined"], 1)  # empty texts are outside the scope counts

    def test_progress_survives_a_crash_and_resume_appends(self):
        class Crashing(MockProvider):
            def classify(self, items):
                if self.calls == 3:
                    raise KeyboardInterrupt  # stands in for an abrupt stop inside the run loop
                return super().classify(items)

        with self.assertRaises(KeyboardInterrupt):
            self.run_mock(Crashing(max_batch=1))
        progress = self.lines("progress.jsonl")
        self.assertEqual(progress[-1]["state"], "error")
        saved = progress[-1]["reviews_in_scope"]["completed"]
        self.assertEqual(saved, len({r[0] for r in self.conn.execute(
            "SELECT review_id FROM reviews WHERE status = 'completed'")}))
        self.assertEqual(self.conn.execute("PRAGMA integrity_check").fetchone()[0], "ok")
        for line in self.ledger_path.read_text().splitlines():
            json.loads(line)                                         # every ledger line is complete JSON
        self.run_mock()                                              # resume into the same directory
        progress = self.lines("progress.jsonl")
        self.assertEqual(progress[-1]["state"], "finished")
        self.assertEqual(progress[-1]["reviews_in_scope"]["pending"], 0)
        sent = [rid for (ids,) in self.conn.execute("SELECT review_ids FROM calls WHERE outcome = 'succeeded'")
                for rid in json.loads(ids)]
        self.assertEqual(len(sent), len(set(sent)))


class ThroughputTests(unittest.TestCase):
    """Real clock: the rate limiter must let several workers overlap and still cap requests/second."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.conn = make_db(self.dir, [f"distinct review number {i}" for i in range(16)])

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def run_timed(self, workers, rps, latency):
        import time as clock
        provider = MockProvider(max_batch=1, latency_s=latency)
        started = clock.monotonic()
        summary = run_enrichment(self.conn, provider, SpendLedger(Decimal("1"), self.dir / "l.jsonl"),
                                 Options(workers=workers, requests_per_second=rps, sleep=clock.sleep))
        return summary, clock.monotonic() - started

    def test_workers_overlap_under_a_rate_limit(self):
        summary, elapsed = self.run_timed(workers=4, rps=100, latency=0.1)
        self.assertEqual(summary.requests, 16)
        self.assertLess(elapsed, 16 * 0.1 / 2)                       # serial would take >= 1.6 s

    def test_rate_limit_is_still_enforced(self):
        summary, elapsed = self.run_timed(workers=4, rps=20, latency=0.0)
        self.assertEqual(summary.requests, 16)
        self.assertGreaterEqual(elapsed, 15 / 20 - 0.05)              # 16 dispatches need >= 15 gaps of 50 ms

    def test_request_limit_reports_max_requests(self):
        summary = run_enrichment(self.conn, MockProvider(max_batch=1), SpendLedger(Decimal("1"), self.dir / "l.jsonl"),
                                 Options(max_requests=5, sleep=no_sleep, requests_per_second=1000))
        self.assertEqual((summary.requests, summary.stop_reason), (5, "max_requests"))
        rest = run_enrichment(self.conn, MockProvider(max_batch=1), SpendLedger(Decimal("1"), self.dir / "l.jsonl"),
                              Options(sleep=no_sleep, requests_per_second=1000))
        self.assertEqual((rest.requests, rest.stop_reason), (11, "queue_empty"))


if __name__ == "__main__":
    unittest.main()
