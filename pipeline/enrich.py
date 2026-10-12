"""Stage 2 (Classify): bounded, resumable enrichment of pending reviews.

Usage (paid provider; requires explicit --execute):
    python3 -m pipeline.enrich --db work/pilot.sqlite --provider jev --budget-usd 0.50 --execute
Offline demonstration with the mock provider (synthetic labels, test databases only):
    python3 -m pipeline.enrich --db work/mock.sqlite --provider mock --allow-mock --budget-usd 1
Stratified selection for partial-corpus runs:
    --stratum duplicates   reviews whose exact text occurs more than once in the input
    --stratum singletons   reviews whose exact text occurs exactly once, in seeded order

What the orchestrator (code) owns:
  - the pending queue, ordered by SHA-256(seed:review_id); a distinct-text group is placed by its
    earliest member, so heavily duplicated texts come first unless a stratum is selected
  - optional strata over exact-text identity (the same text_sha256 used by the cache, with no
    case or whitespace normalization): disjoint, and together they cover every nonempty review
  - exact-text cache: one request per distinct text; other identical texts reuse the saved,
    validated result with a direct cache_source_id (same label_config only)
  - batches of at most provider.max_batch (never more than 50) reviews per request
  - validation of every returned key against the keys sent, and of every label field
  - one retry for invalid model output, then quarantine with a reason
  - bounded retries with exponential backoff and jitter for transient API errors
  - spend reservation before dispatch and a hard budget cap (shared ledger)
  - an atomic database transaction after each response (results, statuses and the call log)
  - interruption and resume: completed IDs are never sent again under the same label_config
  - durable progress: run_log.jsonl and progress.jsonl are appended while the run proceeds and
    status.json is replaced atomically, so a stopped run still leaves its progress on disk
  - network outages: a network failure pauses dispatch and puts the batch back in the queue; a
    free connectivity probe (DNS + TCP, no API call) is retried with capped exponential backoff,
    and dispatch resumes when it succeeds. Network failures never count as invalid output, never
    quarantine a review and never trip the circuit breaker. A run stops cleanly with
    network_unavailable after max_network_wait_s of cumulative waiting.
  - a request that failed after it was sent (outcome unknown) is charged at its reservation and
    re-sent at most max_unknown_resends times per batch; the run stops after max_unknown_outcomes
  - one process per database (a lock file), and distinct exit codes so pipeline.supervise can
    restart only after recoverable stops
What the model owns: reading the review text and answering the fixed questions.
"""

import argparse
import concurrent.futures as futures
import hashlib
import json
import os
import random
import signal
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from pipeline import db, extract
from pipeline.common import REPO_ROOT, atomic_write_json
from pipeline.labels import apply_consistency, label_config as make_label_config, validate_label
from pipeline.ledger import DEFAULT_LEDGER, SpendLedger
from pipeline.providers import (MAX_REVIEWS_PER_REQUEST, MOCK_MODEL, NETWORK_KINDS, NETWORK_NOT_SENT, RATE_LIMITED,
                                JevProvider, MockProvider, ProviderResponse, probe_network)
from pipeline.runlock import LockHeld, hold_lock

ORDER_SEED = "berkeley-fall-2026-assignment-5-v1"  # same seed as the course sampling (manifest.json)
MAX_INVALID_OUTPUTS = 2          # first invalid output is retried once; the second quarantines
MAX_TRANSIENT_RETRIES = 3        # per batch, for rate limits / server errors
MAX_CONSECUTIVE_FAILURES = 5     # circuit breaker for API (not network) failures: stop the run
BACKOFF_BASE_S = 1.0
RATE_LIMIT_BACKOFF_BASE_S = 5.0
BACKOFF_MAX_S = 120.0
# Exit codes of the CLI. pipeline.supervise restarts only after the recoverable ones.
EXIT_CODES = {"queue_empty": 0, "max_requests": 0, "simulated_interruption": 0, "interrupted": 3,
              "budget_cap": 4, "fatal_provider_error": 5, "network_unavailable": 6, "circuit_breaker": 7,
              "unknown_outcome_limit": 8}
EXIT_LOCKED = 2
INVALID_OUTPUT_REASON = "invalid_model_output"
TEXT_GROUPS = "SELECT text_sha256 FROM reviews WHERE text_sha256 IS NOT NULL GROUP BY text_sha256"
STRATA = {  # membership depends only on the ingested texts, never on status, so it is stable
    "duplicates": f"r.text_sha256 IN ({TEXT_GROUPS} HAVING COUNT(*) > 1)",
    "singletons": f"r.text_sha256 IN ({TEXT_GROUPS} HAVING COUNT(*) = 1)",
}
TEXT_FETCH_CHUNK = 500


@dataclass
class Options:
    limit: int | None = None             # target number of source reviews to complete in this run
    sample: str | None = None            # restrict to a registered sample (e.g. cost_100)
    workers: int = 1
    max_requests: int | None = None
    stop_after_requests: int | None = None  # simulate an interruption after N processed responses
    requests_per_second: float = 5.0
    confidence_threshold: float = 0.5
    run_id: str | None = None
    stratum: str | None = None           # "duplicates" or "singletons" (exact-text strata)
    log_dir: Path | None = None          # run_log.jsonl, progress.jsonl and status.json go here
    progress_every_s: float = 30.0
    progress_every_responses: int = 200
    echo_progress: bool = True           # also print each snapshot to stderr when log_dir is set
    network_probe: object = None         # callable -> bool; None assumes the network is back (tests)
    network_backoff_base_s: float = 5.0
    network_backoff_max_s: float = 300.0
    max_network_wait_s: float = 6 * 3600.0   # cumulative waiting for the network in one run
    max_unknown_resends: int = 3         # per batch: re-sends after an unknown-outcome failure
    max_unknown_outcomes: int = 200      # per run: stop instead of paying for more lost responses
    sleep: object = time.sleep           # injectable for tests


@dataclass
class Batch:
    items: list                          # [(review_id, text)]
    transient_attempts: int = 0
    not_before: float = 0.0
    is_retry: bool = False               # re-sent after an invalid output
    network_failures: int = 0
    unknown_resends: int = 0


@dataclass
class Summary:
    run_id: str
    label_config: str
    phase: str
    started_at: str
    requests: int = 0
    succeeded_requests: int = 0
    failed_requests: int = 0
    completed_by_model: int = 0
    completed_by_cache: int = 0
    invalid_outputs: int = 0
    quarantined: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    charged_usd: str = "0"
    stop_reason: str = "queue_empty"
    elapsed_s: float = 0.0
    first_dispatches: int = 0            # distinct texts sent for the first time in this run
    network_outages: int = 0
    network_wait_s: float = 0.0
    not_sent_failures: int = 0
    unknown_outcomes: int = 0
    events: list = field(default_factory=list)   # kept in memory only when no log_dir is given


def order_key(review_id):
    return hashlib.sha256(f"{ORDER_SEED}:{review_id}".encode()).digest()


def _now():
    return datetime.now(timezone.utc).isoformat()


def _scope_clause(sample=None, stratum=None):
    clause, params = "", ()
    if sample:
        clause += " AND r.review_id IN (SELECT review_id FROM sample_membership WHERE sample = ?)"
        params += (sample,)
    if stratum:
        if stratum not in STRATA:
            raise ValueError(f"Unknown stratum {stratum!r}; expected one of {sorted(STRATA)}")
        clause += " AND " + STRATA[stratum]
    return clause, params


def scope_counts(conn, sample=None, stratum=None):
    """Status counts of nonempty reviews in scope (empty texts are outside every stratum)."""
    clause, params = _scope_clause(sample, stratum)
    counts = {"pending": 0, "completed": 0, "quarantined": 0}
    counts.update(conn.execute("SELECT r.status, COUNT(*) FROM reviews r WHERE r.text_sha256 IS NOT NULL"
                               + clause + " GROUP BY r.status", params).fetchall())
    counts["total"] = counts["pending"] + counts["completed"] + counts["quarantined"]
    return counts


def _originals(conn, config):
    """text_sha256 -> review_id of an original (model-produced) completed record under config."""
    rows = conn.execute(
        "SELECT r.text_sha256, l.review_id FROM labels l JOIN reviews r USING (review_id)"
        " WHERE l.label_config = ? AND l.cache_source_id IS NULL AND l.request_id IS NOT NULL"
        " ORDER BY l.completed_at, l.review_id", (config,))
    found = {}
    for t_sha, rid in rows:
        found.setdefault(t_sha, rid)
    return found


def _copy_label(conn, source_id, target_id):
    conn.execute(
        "INSERT INTO labels (review_id, label_config, topic, intent, sentiment, severity, entities,"
        " evidence_quote, needs_review, cache_source_id, request_id, details, completed_at)"
        " SELECT ?, label_config, topic, intent, sentiment, severity, entities, evidence_quote, needs_review,"
        " ?, NULL, json_object('cache_reuse', 1), ? FROM labels WHERE review_id = ?",
        (target_id, source_id, _now(), source_id))
    conn.execute("UPDATE reviews SET status = 'completed' WHERE review_id = ? AND status = 'pending'", (target_id,))


def apply_cache(conn, config, sample=None, limit=None, stratum=None):
    """Complete pending reviews whose exact text already has an original result. Returns count."""
    originals = _originals(conn, config)
    if not originals:
        return 0
    clause, params = _scope_clause(sample, stratum)
    rows = conn.execute("SELECT r.review_id, r.text_sha256 FROM reviews r WHERE r.status = 'pending'"
                        " AND r.text_sha256 IS NOT NULL" + clause, params).fetchall()
    rows.sort(key=lambda r: order_key(r[0]))
    reused = 0
    with conn:
        for rid, t_sha in rows:
            if limit is not None and reused >= limit:
                break
            source = originals.get(t_sha)
            if source and source != rid:
                _copy_label(conn, source, rid)
                reused += 1
    return reused


def build_queue(conn, sample=None, limit=None, stratum=None, max_groups=None):
    """Distinct-text groups of pending reviews, in seeded order.

    Returns a list of (representative_id, text, member_ids). With a limit, groups are taken
    until their combined member count reaches it (a group is never split). max_groups caps the
    number of groups (a run never sends more requests than that). Texts are read only for the
    selected groups, so a large corpus does not keep every review text in memory.
    """
    clause, params = _scope_clause(sample, stratum)
    groups = {}
    for rid, t_sha in conn.execute(
            "SELECT r.review_id, r.text_sha256 FROM reviews r WHERE r.status = 'pending'"
            " AND r.text_sha256 IS NOT NULL" + clause, params):
        groups.setdefault(t_sha, []).append(rid)
    ordered = [sorted(members, key=order_key) for members in groups.values()]
    del groups
    ordered.sort(key=lambda members: order_key(members[0]))
    selected, total = [], 0
    for members in ordered:
        if (limit is not None and total >= limit) or (max_groups is not None and len(selected) >= max_groups):
            break
        selected.append(members)
        total += len(members)
    del ordered
    texts = {}
    reps = [members[0] for members in selected]
    for i in range(0, len(reps), TEXT_FETCH_CHUNK):
        chunk = reps[i:i + TEXT_FETCH_CHUNK]
        texts.update(conn.execute("SELECT review_id, review_text FROM reviews WHERE review_id IN (%s)"
                                  % ",".join("?" * len(chunk)), chunk).fetchall())
    return [(members[0], texts[members[0]], members) for members in selected]


class RunLog:
    """Append-only run evidence that survives an interrupted or crashed run.

    run_log.jsonl: one line per event, flushed immediately.
    progress.jsonl: one line per progress snapshot, flushed and fsynced.
    status.json: the latest snapshot, replaced atomically.
    Without a directory, events stay in memory (tests) and only the last snapshot is kept.
    """

    def __init__(self, directory=None):
        self.directory = Path(directory) if directory else None
        self.memory_events = []
        self.last_progress = None
        if self.directory:
            self.directory.mkdir(parents=True, exist_ok=True)

    def _append(self, name, record, sync=False):
        with (self.directory / name).open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, sort_keys=True) + "\n")
            f.flush()
            if sync:
                os.fsync(f.fileno())

    def event(self, record):
        if self.directory:
            self._append("run_log.jsonl", record)
        else:
            self.memory_events.append(record)

    def progress(self, record):
        self.last_progress = record
        if self.directory:
            self._append("progress.jsonl", record, sync=True)
            atomic_write_json(self.directory / "status.json", record)


def _record_call(conn, run_id, phase, provider, config, response, review_ids, attempt, started_at, cost, source):
    request_id = response.request_id
    if conn.execute("SELECT 1 FROM calls WHERE request_id = ?", (request_id,)).fetchone():
        request_id = f"{request_id}#{attempt}-{started_at}"
    conn.execute(
        "INSERT INTO calls (request_id, run_id, role, phase, provider, model, label_config, review_ids, outcome,"
        " error, input_tokens, output_tokens, cost_usd, cost_source, started_at, elapsed_s, attempt)"
        " VALUES (?, ?, 'enrich', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (request_id, run_id, phase, provider.name, response.model or provider.model, config,
         json.dumps(review_ids), "succeeded" if response.ok else "failed", response.error,
         response.input_tokens, response.output_tokens, cost, source, started_at, response.elapsed_s, attempt))
    return request_id


def _charge(provider, response, reservation_amount):
    """Return (cost_usd or None, cost_source) for the call record."""
    if response.cost_usd is not None:
        return str(response.cost_usd), "provider_reported"
    if response.input_tokens is not None:
        return format(provider.estimate_cost(response.input_tokens, response.output_tokens), "f"), "estimated"
    if response.outcome_unknown:
        return None, "unknown"
    if response.error_kind == NETWORK_NOT_SENT:
        return "0", "not_sent"
    return "0", "none"


def _build_label(provider, raw, text, threshold):
    topic, intent, severity, sentiment, confidence, probabilities = provider.to_label(raw)
    label = {"topic": topic, "intent": intent, "severity": severity, "sentiment": sentiment}
    adjustments = []
    if isinstance(severity, int) and intent is not None:
        label["severity"], adjustments = apply_consistency(topic, intent, severity)
    quote, quote_method = extract.evidence_quote(text, topic) if topic else (None, None)
    low_confidence = [k for k, v in confidence.items() if isinstance(v, (int, float)) and v < threshold]
    label.update(entities=extract.find_entities(text), evidence_quote=quote,
                 needs_review=bool(low_confidence or adjustments))
    details = {"confidence": confidence, "probabilities": probabilities, "adjustments": adjustments,
               "low_confidence": low_confidence, "quote_method": quote_method}
    return label, details


def run_enrichment(conn, provider, ledger, options=None):
    options = options or Options()
    if provider.max_batch > MAX_REVIEWS_PER_REQUEST:
        raise ValueError("Batch size exceeds the 50-review request limit")
    config = make_label_config(provider.model, extract.EXTRACTION_VERSION)
    phase = db.get_meta(conn, "enrich_phase", "initial")
    started = time.monotonic()
    summary = Summary(run_id=options.run_id or f"enrich-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}",
                      label_config=config, phase=phase, started_at=_now())

    log = RunLog(options.log_dir)
    summary.events = log.memory_events

    def event(kind, **data):
        log.event(dict(data, event=kind, at=_now(), run_id=summary.run_id))

    scope = {"sample": options.sample, "stratum": options.stratum, "limit": options.limit,
             "max_requests": options.max_requests, "workers": options.workers,
             "requests_per_second": options.requests_per_second}
    progress_state = {"at": time.monotonic(), "responses": 0}
    network = {"down": False, "delay": options.network_backoff_base_s}
    probe = options.network_probe or (lambda: True)

    def report_progress(state, queue_left=0):
        elapsed = time.monotonic() - started
        counts = scope_counts(conn, options.sample, options.stratum)
        rate = summary.requests / elapsed if elapsed > 0 else 0.0
        record = {
            "at": _now(), "run_id": summary.run_id, "state": state, "label_config": config, "phase": phase,
            "scope": scope, "elapsed_s": round(elapsed, 1),
            "reviews_in_scope": counts,
            "requests": {"sent": summary.requests, "first_dispatches": summary.first_dispatches,
                         "retries": summary.requests - summary.first_dispatches,
                         "succeeded": summary.succeeded_requests, "failed": summary.failed_requests,
                         "queued": queue_left},
            "completed_this_run": {"by_model": summary.completed_by_model, "by_cache": summary.completed_by_cache},
            "errors": {"failed_requests": summary.failed_requests, "invalid_outputs": summary.invalid_outputs,
                       "quarantined_this_run": summary.quarantined},
            "spend_usd": {"this_run": summary.charged_usd, "ledger_total": format(ledger.spent, "f"),
                          "reserved_in_flight": format(ledger.reserved, "f"), "cap": format(ledger.cap, "f")},
            "network": {"down": network["down"], "outages": summary.network_outages,
                        "waited_s": round(summary.network_wait_s, 1), "not_sent_failures": summary.not_sent_failures,
                        "unknown_outcomes": summary.unknown_outcomes},
            "requests_per_second": round(rate, 3),
            "eta_s": round(queue_left / rate) if rate > 0 and queue_left else None,
        }
        if state != "running":
            record["stop_reason"] = summary.stop_reason
        log.progress(record)
        progress_state.update(at=time.monotonic(), responses=0)
        if log.directory and options.echo_progress:
            print(f"[{record['at'][:19]}] {state} completed={counts['completed']} pending={counts['pending']}"
                  f" quarantined={counts['quarantined']} requests={summary.requests} failed={summary.failed_requests}"
                  f" spent_run=${summary.charged_usd} ledger=${record['spend_usd']['ledger_total']}"
                  f" elapsed={record['elapsed_s']}s", file=sys.stderr, flush=True)

    summary.completed_by_cache += apply_cache(conn, config, options.sample, options.limit, options.stratum)
    remaining_target = None if options.limit is None else max(0, options.limit - summary.completed_by_cache)
    max_groups = None if options.max_requests is None else options.max_requests * provider.max_batch
    groups = build_queue(conn, options.sample, remaining_target, options.stratum, max_groups) \
        if remaining_target != 0 else []
    members = {rep: ids for rep, _, ids in groups}
    queue = deque(Batch([(rep, text) for rep, text, _ in groups[i:i + provider.max_batch]])
                  for i in range(0, len(groups), provider.max_batch))
    event("queue_built", groups=len(groups), batches=len(queue), cache_reused=summary.completed_by_cache,
          scope=scope)
    del groups

    inflight = {}
    consecutive_failures = 0
    stop = None
    last_dispatch = 0.0
    interrupted = {"flag": False}
    previous_handlers = {}
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        try:
            if sig == signal.SIGHUP and signal.getsignal(sig) == signal.SIG_IGN:
                continue  # started under nohup: keep ignoring terminal hang-ups
            previous_handlers[sig] = signal.signal(sig, lambda *_: interrupted.__setitem__("flag", True))
        except ValueError:
            pass  # not in the main thread (tests); signal handling is only needed for the CLI
    report_progress("running", len(queue))

    def pause(seconds):
        """Sleep in short slices so Ctrl+C / SIGTERM is honored during long waits. False if interrupted."""
        remaining = seconds
        while remaining > 0:
            if interrupted["flag"]:
                return False
            step = min(1.0, remaining)
            options.sleep(step)
            remaining -= step
        return not interrupted["flag"]

    try:
        with futures.ThreadPoolExecutor(max_workers=options.workers) as pool:
            while (queue or inflight) and not (stop and not inflight):
                if interrupted["flag"] and not stop:
                    stop = "interrupted"
                if network["down"] and not stop and not inflight:
                    if summary.network_wait_s >= options.max_network_wait_s:
                        stop = "network_unavailable"
                        event("network_unavailable", waited_s=round(summary.network_wait_s, 1))
                        continue
                    delay = min(network["delay"] * (1 + 0.25 * random.random()),
                                options.max_network_wait_s - summary.network_wait_s)
                    if time.monotonic() - progress_state["at"] >= options.progress_every_s:
                        report_progress("waiting_for_network", len(queue))
                    if not pause(delay):
                        continue
                    summary.network_wait_s += delay
                    if probe():
                        network.update(down=False, delay=options.network_backoff_base_s)
                        event("network_restored", waited_s=round(summary.network_wait_s, 1))
                    else:
                        network["delay"] = min(network["delay"] * 2, options.network_backoff_max_s)
                    continue
                # Dispatch while capacity allows (never while the network is down). When the rate limit
                # or a retry backoff delays the next dispatch while requests are in flight, wait for a
                # response for at most that delay instead of blocking until a response arrives, so
                # several workers really overlap.
                dispatch_wait = None
                while not stop and not network["down"] and queue and len(inflight) < options.workers:
                    if options.max_requests is not None and summary.requests >= options.max_requests:
                        stop = "max_requests"
                        break
                    batch = queue[0]
                    wait = max(batch.not_before - time.monotonic(),
                               last_dispatch + 1.0 / options.requests_per_second - time.monotonic())
                    if wait > 0:
                        if inflight:
                            dispatch_wait = wait
                            break
                        options.sleep(wait)
                    reservation = provider.estimate_cost(provider.estimate_input_tokens(batch.items))
                    token = ledger.reserve(reservation)
                    if token is None:
                        stop = "budget_cap"
                        event("budget_cap", spent=str(ledger.spent), reserved=str(ledger.reserved),
                              next_reservation=str(reservation), cap=str(ledger.cap))
                        break
                    queue.popleft()
                    summary.requests += 1
                    if batch.transient_attempts == 0 and not batch.is_retry:
                        summary.first_dispatches += 1
                    last_dispatch = time.monotonic()
                    fut = pool.submit(provider.classify, batch.items)
                    inflight[fut] = (batch, token, reservation, _now())
                if not inflight:
                    continue
                done, _ = futures.wait(inflight, timeout=dispatch_wait, return_when=futures.FIRST_COMPLETED)
                for fut in done:
                    batch, token, reservation, started_at = inflight.pop(fut)
                    try:
                        response = fut.result()
                    except Exception as e:  # a provider bug must not lose the batch or the call record
                        response = ProviderResponse(f"local-error-{summary.requests}-{time.time_ns()}", provider.model,
                                                    False, error=f"{type(e).__name__}: {e}"[:300], fatal=True)
                    sent_ids = [rid for rid, _ in batch.items]
                    attempt = batch.transient_attempts + batch.network_failures + 1
                    cost, source = _charge(provider, response, reservation)
                    with conn:  # one atomic transaction per response
                        request_id = _record_call(conn, summary.run_id, phase, provider, config, response, sent_ids,
                                                  attempt, started_at, cost, source)
                        conn.executemany("UPDATE reviews SET attempts = attempts + 1 WHERE review_id = ?",
                                         [(rid,) for rid in sent_ids])
                        if response.ok:
                            consecutive_failures = 0
                            summary.succeeded_requests += 1
                            unexpected = set(response.items) - set(sent_ids)
                            if unexpected:
                                event("unexpected_ids", request_id=request_id, ids=sorted(unexpected)[:10])
                            retry_items = []
                            for rid, text in batch.items:
                                raw = response.items.get(rid)
                                label, details = _build_label(provider, raw, text, options.confidence_threshold) \
                                    if isinstance(raw, dict) else ({}, {"error": "missing from response"})
                                problems = validate_label(label, text) if label else ["id missing from response"]
                                if problems:
                                    summary.invalid_outputs += 1
                                    conn.execute("UPDATE reviews SET invalid_outputs = invalid_outputs + 1"
                                                 " WHERE review_id = ?", (rid,))
                                    count = conn.execute("SELECT invalid_outputs FROM reviews WHERE review_id = ?",
                                                         (rid,)).fetchone()[0]
                                    event("invalid_output", review_id=rid, problems=problems, count=count)
                                    if count >= MAX_INVALID_OUTPUTS:
                                        reason = f"{INVALID_OUTPUT_REASON}: {'; '.join(problems)}"
                                        conn.execute("UPDATE reviews SET status = 'quarantined', reason = ?"
                                                     " WHERE review_id = ?", (reason, rid))
                                        summary.quarantined += 1
                                    else:
                                        retry_items.append((rid, text))
                                    continue
                                conn.execute(
                                    "INSERT INTO labels (review_id, label_config, topic, intent, sentiment, severity,"
                                    " entities, evidence_quote, needs_review, cache_source_id, request_id, details,"
                                    " completed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)",
                                    (rid, config, label["topic"], label["intent"], label["sentiment"], label["severity"],
                                     json.dumps(label["entities"]), label["evidence_quote"], int(label["needs_review"]),
                                     request_id, json.dumps(details), _now()))
                                conn.execute("UPDATE reviews SET status = 'completed' WHERE review_id = ?", (rid,))
                                summary.completed_by_model += 1
                                for other in members.get(rid, [rid])[1:]:
                                    _copy_label(conn, rid, other)
                                    summary.completed_by_cache += 1
                            if retry_items:
                                queue.append(Batch(retry_items, is_retry=True))
                        else:
                            summary.failed_requests += 1
                            kind = response.error_kind
                            event("request_failed", request_id=request_id, error=response.error, attempt=attempt,
                                  error_kind=kind)
                            if response.fatal:
                                consecutive_failures += 1
                                stop = stop or "fatal_provider_error"
                            elif kind in NETWORK_KINDS:
                                # The review stays pending: no invalid-output count, no quarantine, no breaker.
                                batch.network_failures += 1
                                if kind == NETWORK_NOT_SENT:
                                    summary.not_sent_failures += 1
                                else:
                                    summary.unknown_outcomes += 1
                                    batch.unknown_resends += 1
                                if not network["down"]:
                                    network.update(down=True, delay=options.network_backoff_base_s)
                                    summary.network_outages += 1
                                    event("network_down", error=response.error, error_kind=kind)
                                if batch.unknown_resends > options.max_unknown_resends:
                                    event("batch_left_pending", ids=sent_ids, error="unknown-outcome resend limit")
                                else:
                                    queue.appendleft(batch)
                                if summary.unknown_outcomes > options.max_unknown_outcomes:
                                    stop = stop or "unknown_outcome_limit"
                            elif response.retryable and batch.transient_attempts < MAX_TRANSIENT_RETRIES:
                                consecutive_failures += 1
                                batch.transient_attempts += 1
                                base = RATE_LIMIT_BACKOFF_BASE_S if kind == RATE_LIMITED else BACKOFF_BASE_S
                                delay = min(base * 2 ** (batch.transient_attempts - 1), BACKOFF_MAX_S)
                                delay *= 1 + random.random()
                                if response.retry_after_s:
                                    delay = max(delay, response.retry_after_s)
                                batch.not_before = time.monotonic() + delay
                                queue.appendleft(batch)
                            else:
                                consecutive_failures += 1
                                event("batch_left_pending", ids=sent_ids, error=response.error)
                            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                                stop = stop or "circuit_breaker"
                    charged = ledger.settle(token, {"request_id": request_id, "run_id": summary.run_id, "role": "enrich",
                                                    "db": db.get_meta(conn, "source_name", ""), "cost_usd": cost,
                                                    "cost_source": source})
                    summary.charged_usd = format(Decimal(summary.charged_usd) + charged, "f")
                    summary.input_tokens += response.input_tokens or 0
                    summary.output_tokens += response.output_tokens or 0
                    processed = summary.succeeded_requests + summary.failed_requests
                    if options.stop_after_requests is not None and processed >= options.stop_after_requests:
                        stop = stop or "simulated_interruption"
                    progress_state["responses"] += 1
                if (progress_state["responses"] >= options.progress_every_responses
                        or time.monotonic() - progress_state["at"] >= options.progress_every_s):
                    report_progress("running", len(queue) + len(inflight))
    except BaseException as e:  # leave a final snapshot; saved work is already committed
        summary.stop_reason = f"error: {type(e).__name__}"
        event("run_error", error=f"{type(e).__name__}: {e}"[:300])
        try:
            report_progress("error", len(queue))
        except Exception:
            pass  # the original error matters more than a missing final snapshot
        for sig, handler in previous_handlers.items():
            signal.signal(sig, handler)
        raise
    for sig, handler in previous_handlers.items():
        signal.signal(sig, handler)
    if not stop and options.max_requests is not None and summary.requests >= options.max_requests \
            and scope_counts(conn, options.sample, options.stratum)["pending"]:
        stop = "max_requests"  # the queue was built only up to the request limit; work remains
    if stop:
        summary.stop_reason = stop
    summary.elapsed_s = round(time.monotonic() - started, 3)
    event("run_finished", stop_reason=summary.stop_reason, requests=summary.requests)
    report_progress("finished", len(queue))
    return summary


def make_provider(name, allow_mock):
    if name == "mock":
        if not allow_mock:
            raise SystemExit("ERROR: the mock provider writes synthetic labels; pass --allow-mock and use a test database.")
        return MockProvider()
    from pipeline.config import get_key
    key = get_key("AI_GATEWAY_API_KEY")
    if not key:
        raise SystemExit("ERROR: AI_GATEWAY_API_KEY is not set (python3 -m pipeline.config check).")
    return JevProvider(key)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--provider", choices=["jev", "mock"], required=True)
    parser.add_argument("--budget-usd", type=Decimal, required=True, help="Hard cap on total recorded spend")
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--limit", type=int, help="Target number of source reviews to complete in this run")
    parser.add_argument("--sample", help="Restrict to a registered sample, e.g. cost_100")
    parser.add_argument("--stratum", choices=sorted(STRATA),
                        help="Restrict to reviews whose exact text is duplicated / occurs once")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--max-requests", type=int)
    parser.add_argument("--stop-after-requests", type=int, help="Simulate an interruption after N responses")
    parser.add_argument("--rps", type=float, default=5.0, help="Maximum requests per second")
    parser.add_argument("--execute", action="store_true", help="Required for paid providers")
    parser.add_argument("--allow-mock", action="store_true")
    parser.add_argument("--out", type=Path, default=REPO_ROOT / "outputs" / "runs")
    parser.add_argument("--progress-every-s", type=float, default=30.0, help="Seconds between progress snapshots")
    parser.add_argument("--max-network-wait-min", type=float, default=360.0,
                        help="Stop with network_unavailable after this much cumulative waiting for the network")
    args = parser.parse_args(argv)
    if args.provider != "mock" and not args.execute:
        print("REFUSED: paid enrichment requires --execute (and prior approval).", file=sys.stderr)
        return EXIT_LOCKED
    provider = make_provider(args.provider, args.allow_mock)
    try:
        with hold_lock(args.db, "pipeline.enrich " + " ".join(argv if argv is not None else sys.argv[1:])):
            return _run_cli(args, provider)
    except LockHeld as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return EXIT_LOCKED


def _run_cli(args, provider):
    conn = db.connect(args.db)
    try:
        if not db.get_meta(conn, "source_sha256"):
            raise SystemExit("ERROR: ingest the input CSV into this database first (pipeline.ingest).")
        ledger = SpendLedger(args.budget_usd, args.ledger)
        run_id = f"enrich-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
        out_dir = args.out / run_id
        print(f"Run {run_id}: progress in {out_dir}/progress.jsonl and status.json", file=sys.stderr)
        summary = run_enrichment(conn, provider, ledger, Options(
            limit=args.limit, sample=args.sample, stratum=args.stratum, workers=args.workers,
            max_requests=args.max_requests, stop_after_requests=args.stop_after_requests,
            requests_per_second=args.rps, run_id=run_id, log_dir=out_dir, progress_every_s=args.progress_every_s,
            network_probe=probe_network if args.provider == "jev" else None,
            max_network_wait_s=args.max_network_wait_min * 60))
        statuses = db.status_counts(conn)
    finally:
        conn.close()
    result = {k: v for k, v in summary.__dict__.items() if k != "events"}
    result.update(statuses_after=statuses, provider=provider.name, model=provider.model,
                  ledger_spent_usd=format(ledger.spent, "f"), budget_cap_usd=format(ledger.cap, "f"),
                  synthetic_labels=provider.model == MOCK_MODEL, sample=args.sample, stratum=args.stratum)
    result["exit_code"] = EXIT_CODES.get(summary.stop_reason, 1)
    atomic_write_json(out_dir / "run_summary.json", result)
    print(json.dumps(result, indent=2))
    return result["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
