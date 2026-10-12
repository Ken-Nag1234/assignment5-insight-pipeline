"""Offline tests for network-outage recovery, the run lock, the supervisor and status reporting.

Network failures are simulated with a scripted fake transport behind the real JevProvider; no
socket is opened and no API is called. All reviews and answers are synthetic.
"""

import hashlib
import http.client
import json
import os
import signal
import socket
import sys
import tempfile
import unittest
import urllib.error
from decimal import Decimal
from pathlib import Path

from pipeline import checkpoint, db, enrich, status
from pipeline.enrich import Options, run_enrichment
from pipeline.ledger import SpendLedger
from pipeline.providers import NETWORK_NOT_SENT, NETWORK_UNKNOWN, JevProvider
from pipeline.runlock import LockHeld, hold_lock, is_locked
from pipeline.supervise import Supervisor
from tests.test_enrich import make_db

KEY = "fake-key-never-sent-0000"
TEXTS = ["Great app", "Great app", "Ads after every song", "Cannot log in since update",
         "Offline downloads vanish", "Love the playlists"]
DISTINCT = 5


def answer(n):
    payload = {"model": "typesafe-ai/jev", "usage": {"input_tokens": 900, "output_tokens": 0},
               "answers": {"topic": {"type": "choice", "choice": "other", "confidence": 0.9},
                           "intent": {"type": "choice", "choice": "praise", "confidence": 0.9},
                           "severity": {"type": "choice", "choice": "1", "confidence": 0.9},
                           "sentiment": {"type": "score", "score": 3.0}},
               "provider_metadata": {"gateway": {"cost": "0.0000378", "generationId": f"gen_{n:04d}"}}}
    return 200, json.dumps(payload).encode(), {}


class ScriptedNetwork:
    """Plays a list of outcomes, one per HTTP attempt; 'ok' after the script runs out."""

    def __init__(self, script=()):
        self.script = list(script)
        self.attempts = 0

    def __call__(self, body):
        self.attempts += 1
        step = self.script.pop(0) if self.script else "ok"
        if step == "ok":
            return answer(self.attempts)
        if step == "dns":
            raise urllib.error.URLError(socket.gaierror(8, "nodename nor servname provided, or not known"))
        if step == "unreachable":
            raise urllib.error.URLError(OSError(51, "Network is unreachable"))
        if step == "timeout":
            raise TimeoutError("The read operation timed out")
        if step == "reset":
            raise ConnectionResetError(54, "Connection reset by peer")
        if step == "incomplete":
            raise http.client.IncompleteRead(b'{"answers": {"to')
        if step == "truncated":
            return 200, b'{"model": "typesafe-ai/jev", "answers": {"topic": {"type": "cho', {}
        if step == "429":
            return 429, b'{"error": {"message": "rate limited"}}', {"Retry-After": "7"}
        if step == "500":
            return 500, b'{"error": {"message": "server error"}}', {}
        if step == "402":
            return 402, b'{"error": {"message": "insufficient credit"}}', {}
        raise AssertionError(step)


class Probe:
    """Connectivity probe that fails a given number of times, then succeeds (or never does)."""

    def __init__(self, failures=0, never=False, on_call=None):
        self.failures, self.never, self.on_call, self.calls = failures, never, on_call, 0

    def __call__(self):
        self.calls += 1
        if self.on_call:
            self.on_call()
        if self.never:
            return False
        if self.failures > 0:
            self.failures -= 1
            return False
        return True


class Sleeps:
    def __init__(self):
        self.calls = []

    def __call__(self, seconds):
        self.calls.append(seconds)


class NetworkRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.conn = make_db(self.dir, TEXTS)
        self.ledger_path = self.dir / "ledger.jsonl"

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def run_jev(self, network, probe=None, cap="1", **options):
        self.sleeps = Sleeps()
        provider = JevProvider(KEY, transport=network)
        options.setdefault("requests_per_second", 1000)
        return run_enrichment(self.conn, provider, SpendLedger(Decimal(cap), self.ledger_path),
                              Options(sleep=self.sleeps, network_probe=probe or Probe(), **options))

    def counts(self):
        return db.status_counts(self.conn)

    def calls(self):
        return [dict(r) for r in self.conn.execute("SELECT * FROM calls ORDER BY started_at, request_id")]

    def ledger(self):
        return [json.loads(x) for x in self.ledger_path.read_text().splitlines()]

    def assert_no_damage(self):
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM reviews WHERE status = 'quarantined'"
                                           " AND reason != 'empty_review_text'").fetchone()[0], 0)
        self.assertEqual(self.conn.execute("SELECT SUM(invalid_outputs) FROM reviews").fetchone()[0], 0)
        sent_ok = [rid for c in self.calls() if c["outcome"] == "succeeded" for rid in json.loads(c["review_ids"])]
        self.assertEqual(len(sent_ok), len(set(sent_ok)))            # no review classified twice

    def test_wifi_off_before_send_pauses_probes_with_backoff_and_recovers(self):
        probe = Probe(failures=2)
        summary = self.run_jev(ScriptedNetwork(["ok", "dns"]), probe)
        self.assertEqual(summary.stop_reason, "queue_empty")
        self.assertEqual(self.counts()["pending"], 0)
        self.assert_no_damage()
        self.assertEqual((summary.network_outages, summary.not_sent_failures, summary.unknown_outcomes), (1, 1, 0))
        self.assertEqual(probe.calls, 3)
        self.assertTrue(35 <= sum(self.sleeps.calls) <= 35 * 1.25 + 1)   # 5 + 10 + 20 s with <=25% jitter
        self.assertTrue(all(s <= 1.0 for s in self.sleeps.calls))       # waits are sliced for Ctrl+C
        failed = [c for c in self.calls() if c["outcome"] == "failed"]
        self.assertEqual([(c["cost_usd"], c["cost_source"]) for c in failed], [("0", "not_sent")])
        self.assertEqual([x["charged_usd"] for x in self.ledger() if x["cost_source"] == "not_sent"], ["0"])

    def test_repeated_outages_never_trip_the_breaker_or_quarantine(self):
        summary = self.run_jev(ScriptedNetwork(["dns", "unreachable"] * 4 + ["reset"]))
        self.assertEqual(summary.stop_reason, "queue_empty")
        self.assertEqual(summary.network_outages, 9)
        self.assertEqual(self.counts()["pending"], 0)
        self.assert_no_damage()

    def test_long_outage_stops_cleanly_and_a_restart_finishes_without_repeats(self):
        summary = self.run_jev(ScriptedNetwork(["ok", "ok", "dns"]), Probe(never=True), max_network_wait_s=900)
        self.assertEqual(summary.stop_reason, "network_unavailable")
        self.assertAlmostEqual(summary.network_wait_s, 900, places=6)
        counts = self.counts()
        self.assertEqual(counts["completed"], 3)                     # two requests, one cached duplicate
        self.assertEqual(counts["quarantined"], 0)
        done = {r[0] for r in self.conn.execute("SELECT review_id FROM reviews WHERE status = 'completed'")}
        resumed = self.run_jev(ScriptedNetwork())
        self.assertEqual(resumed.stop_reason, "queue_empty")
        self.assertEqual(self.counts()["pending"], 0)
        self.assert_no_damage()
        later = [rid for c in self.calls()[-resumed.requests:] for rid in json.loads(c["review_ids"])]
        self.assertFalse(done & set(later))

    def test_lost_response_is_charged_at_reservation_and_resent(self):
        summary = self.run_jev(ScriptedNetwork(["ok", "timeout"]))
        self.assertEqual(self.counts()["pending"], 0)
        self.assert_no_damage()
        self.assertEqual(summary.unknown_outcomes, 1)
        unknown = [x for x in self.ledger() if x["cost_source"] == "unknown"]
        self.assertEqual(len(unknown), 1)
        self.assertEqual(unknown[0]["charged_usd"], unknown[0]["reserved_usd"])
        self.assertGreater(Decimal(unknown[0]["charged_usd"]), Decimal("0.0000378"))  # never under-counted
        self.assertEqual(summary.requests, DISTINCT + 1)               # exactly one extra (possibly double) charge

    def test_resends_after_lost_responses_are_bounded_per_batch(self):
        summary = self.run_jev(ScriptedNetwork(["timeout"] * 4), max_unknown_resends=3)
        self.assertEqual(summary.unknown_outcomes, 4)
        self.assertEqual(self.counts()["pending"], 1)                 # left pending, not quarantined
        self.assert_no_damage()
        self.assertIn("batch_left_pending", [e["event"] for e in summary.events])

    def test_run_stops_after_too_many_unknown_outcomes(self):
        summary = self.run_jev(ScriptedNetwork(["timeout", "reset", "incomplete"]), max_unknown_outcomes=2)
        self.assertEqual(summary.stop_reason, "unknown_outcome_limit")
        self.assert_no_damage()

    def test_truncated_or_cut_responses_never_quarantine(self):
        script = ["truncated", "ok", "truncated", "incomplete", "ok", "reset", "ok", "truncated"]
        summary = self.run_jev(ScriptedNetwork(script))
        self.assertEqual(summary.stop_reason, "queue_empty")
        self.assertEqual(self.counts()["pending"], 0)
        self.assertEqual(summary.invalid_outputs, 0)
        self.assert_no_damage()

    def test_rate_limit_waits_for_retry_after(self):
        summary = self.run_jev(ScriptedNetwork(["429"]))
        self.assertEqual(self.counts()["pending"], 0)
        self.assertGreaterEqual(max(self.sleeps.calls), 6.9)
        self.assertEqual(summary.network_outages, 0)

    def test_api_errors_still_trip_the_circuit_breaker(self):
        summary = self.run_jev(ScriptedNetwork(["500"] * 5))
        self.assertEqual(summary.stop_reason, "circuit_breaker")
        self.assertEqual(self.counts()["quarantined"], 0)

    def test_billing_error_is_fatal_without_retry(self):
        summary = self.run_jev(ScriptedNetwork(["402"]))
        self.assertEqual((summary.stop_reason, summary.requests), ("fatal_provider_error", 1))
        self.assertEqual(self.counts()["completed"], 0)

    def test_budget_cap_holds_through_an_outage(self):
        cap = Decimal("0.00015")                                       # room for about two reservations
        summary = self.run_jev(ScriptedNetwork(["ok", "timeout"]), cap=str(cap))
        self.assertEqual(summary.stop_reason, "budget_cap")
        self.assertLessEqual(SpendLedger(cap, self.ledger_path).spent, cap)

    def test_ctrl_c_during_a_network_wait_stops_promptly(self):
        if hasattr(signal, "pthread_kill"):
            probe = Probe(never=True, on_call=lambda: os.kill(os.getpid(), signal.SIGINT))
            summary = self.run_jev(ScriptedNetwork(["dns"]), probe)
            self.assertEqual(summary.stop_reason, "interrupted")
            self.assertEqual(probe.calls, 1)
            self.assertEqual(self.counts()["quarantined"], 0)

    def test_nohup_ignored_hangup_stays_ignored(self):
        previous = signal.signal(signal.SIGHUP, signal.SIG_IGN)
        try:
            self.run_jev(ScriptedNetwork())
            self.assertEqual(signal.getsignal(signal.SIGHUP), signal.SIG_IGN)
        finally:
            signal.signal(signal.SIGHUP, previous)

    def test_progress_shows_network_state(self):
        log_dir = self.dir / "run"
        self.run_jev(ScriptedNetwork(["dns"]), Probe(failures=1), log_dir=log_dir, echo_progress=False,
                     progress_every_s=0)
        progress = [json.loads(x) for x in (log_dir / "progress.jsonl").read_text().splitlines()]
        self.assertIn("waiting_for_network", [p["state"] for p in progress])
        self.assertEqual(progress[-1]["network"]["outages"], 1)
        self.assertFalse(progress[-1]["network"]["down"])
        events = [json.loads(x)["event"] for x in (log_dir / "run_log.jsonl").read_text().splitlines()]
        self.assertLess(events.index("network_down"), events.index("network_restored"))


class ProviderClassificationTests(unittest.TestCase):
    def classify(self, step):
        return JevProvider(KEY, transport=ScriptedNetwork([step])).classify([("r1", "text")])

    def test_failure_kinds(self):
        for step, kind, unknown in (("dns", NETWORK_NOT_SENT, False), ("unreachable", NETWORK_NOT_SENT, False),
                                    ("timeout", NETWORK_UNKNOWN, True), ("reset", NETWORK_UNKNOWN, True),
                                    ("incomplete", NETWORK_UNKNOWN, True), ("truncated", NETWORK_UNKNOWN, True)):
            response = self.classify(step)
            self.assertEqual((response.ok, response.error_kind, response.outcome_unknown, response.retryable),
                             (False, kind, unknown, True), step)
            self.assertNotIn(KEY, response.error)
        limited = self.classify("429")
        self.assertEqual((limited.error_kind, limited.retry_after_s), ("rate_limited", 7.0))
        self.assertTrue(self.classify("402").fatal)

    def test_valid_json_without_answers_is_still_a_model_output_problem(self):
        provider = JevProvider(KEY, transport=lambda body: (200, b'{"model": "typesafe-ai/jev"}', {}))
        response = provider.classify([("r1", "text")])
        self.assertTrue(response.ok)
        self.assertEqual(response.items, {})

    def test_raised_http_error_is_classified_by_status(self):
        def raise_403(body):
            raise urllib.error.HTTPError("https://example.invalid", 403, "Forbidden", {}, None)
        self.assertTrue(JevProvider(KEY, transport=raise_403).classify([("r1", "x")]).fatal)


class LockAndCliTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        conn = make_db(self.dir, TEXTS)
        conn.close()
        self.db = self.dir / "state.sqlite"

    def tearDown(self):
        self.tmp.cleanup()

    def cli(self, *extra):
        return enrich.main(["--db", str(self.db), "--provider", "mock", "--allow-mock", "--budget-usd", "1",
                            "--ledger", str(self.dir / "ledger.jsonl"), "--out", str(self.dir / "runs"),
                            "--rps", "1000", *extra])

    def test_second_process_is_refused_while_the_first_holds_the_database(self):
        with hold_lock(self.db, "test holder"):
            self.assertTrue(is_locked(self.db))
            with self.assertRaises(LockHeld):
                with hold_lock(self.db, "second"):
                    pass
            self.assertEqual(self.cli(), enrich.EXIT_LOCKED)
            self.assertEqual(checkpoint.main(["before", "--db", str(self.db), "--out", str(self.dir / "b.json")]), 2)
        self.assertFalse(is_locked(self.db))                          # released when the holder exits
        conn = db.connect(self.db)
        self.assertEqual(db.status_counts(conn)["completed"], 0)      # the refused run changed nothing
        conn.close()

    def test_cli_exit_codes_and_summary(self):
        self.assertEqual(self.cli("--max-requests", "1"), 0)
        self.assertEqual(self.cli(), 0)
        summary = json.loads(sorted((self.dir / "runs").glob("enrich-*/run_summary.json"))[-1].read_text())
        self.assertEqual((summary["stop_reason"], summary["exit_code"]), ("queue_empty", 0))
        self.assertEqual(enrich.EXIT_CODES["budget_cap"], 4)
        self.assertFalse(is_locked(self.db))

    def test_status_is_read_only_and_reports_runs(self):
        self.cli()
        before = hashlib.sha256(self.db.read_bytes()).hexdigest()
        info = status.collect(self.db, self.dir / "runs", self.dir / "ledger.jsonl", strata=True)
        self.assertEqual(info["latest_run"]["state"], "finished")
        self.assertFalse(info["enrich_process"]["running"])
        self.assertEqual(info["strata"]["duplicates"]["total"], 2)
        self.assertEqual(Decimal(info["ledger"]["total_usd"]) > 0, True)
        self.assertEqual(hashlib.sha256(self.db.read_bytes()).hexdigest(), before)
        self.assertFalse((self.dir / "state.sqlite.supervisor.lock").exists())
        self.assertIn("Latest run", status.render(info))
        # A run that died mid-way leaves state "running" with no lock holder: status warns.
        run_dir = sorted((self.dir / "runs").glob("enrich-*"))[-1]
        snap = json.loads((run_dir / "status.json").read_text())
        (run_dir / "status.json").write_text(json.dumps(dict(snap, state="running")))
        self.assertIn("did not finish cleanly", status.collect(self.db, self.dir / "runs",
                                                               self.dir / "ledger.jsonl")["latest_run"]["warning"])


CHILD = """
import sys
counter, codes = sys.argv[1], [int(c) for c in sys.argv[2:]]
try:
    n = int(open(counter).read())
except FileNotFoundError:
    n = 0
open(counter, "w").write(str(n + 1))
sys.exit(codes[min(n, len(codes) - 1)])
"""


class SupervisorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def supervise(self, codes, max_restarts=10, sleep=None):
        command = [sys.executable, "-c", CHILD, str(self.dir / "count"), *map(str, codes)]
        self.supervisor = Supervisor(command, self.dir / "sup", max_restarts=max_restarts, restart_delay_s=600,
                                     sleep=sleep or (lambda s: None), echo=False)
        return self.supervisor.run()

    def runs(self):
        return int((self.dir / "count").read_text())

    def events(self):
        return [json.loads(x) for x in (self.dir / "sup" / "supervisor.jsonl").read_text().splitlines()]

    def test_restarts_after_network_breaker_and_crash_then_finishes(self):
        final, code, restarts = self.supervise([6, 7, 1, 0])
        self.assertEqual((final, code, restarts, self.runs()), ("finished", 0, 3, 4))
        self.assertEqual([e["event"] for e in self.events()].count("restart_scheduled"), 3)

    def test_never_restarts_after_terminal_stops(self):
        for code, reason in ((4, "budget_cap"), (5, "fatal_provider_error"), (3, "interrupted"), (2, "refused"),
                             (8, "unknown_outcome_limit")):
            (self.dir / "count").unlink(missing_ok=True)
            final, last, restarts = self.supervise([code, 0])
            self.assertEqual((final, last, restarts, self.runs()), (reason, code, 0, 1), reason)

    def test_restart_limit_is_enforced(self):
        final, code, restarts = self.supervise([6] * 10, max_restarts=2)
        self.assertEqual((final, code, restarts, self.runs()), ("restart_limit_reached", 6, 2, 3))

    def test_stop_request_during_restart_delay_prevents_restart(self):
        def sleep(_):
            self.supervisor.request_stop()
        final, _, restarts = self.supervise([6, 0], sleep=sleep)
        self.assertEqual((final, restarts, self.runs()), ("stopped_by_user", 1, 1))

    def test_one_supervisor_per_database(self):
        db_path = self.dir / "x.sqlite"
        with hold_lock(db_path, "first", suffix=".supervisor.lock"):
            with self.assertRaises(LockHeld):
                with hold_lock(db_path, "second", suffix=".supervisor.lock"):
                    pass
            self.assertFalse(is_locked(db_path))                     # separate from the enrichment lock


if __name__ == "__main__":
    unittest.main()
