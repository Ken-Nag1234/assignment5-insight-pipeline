"""Classifier providers behind one interface.

A provider receives a batch of (key, text) items (at most 50) and returns a ProviderResponse
with one raw answer per key it chose to return. The orchestrator validates every returned key
against the keys it sent, maps answers into the shared schema and records usage and cost.

Providers:
  MockProvider  deterministic keyword rules with failure injection, for offline tests ONLY.
                Its labels are synthetic and must never be exported as business results.
  JevProvider   TypeSafe Jev through Vercel AI Gateway (one review per request).
"""

import http.client
import json
import socket
import ssl
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass, field
from decimal import Decimal

from pipeline.labels import QUESTIONS

MAX_REVIEWS_PER_REQUEST = 50  # grading contract limit for enrichment requests
MOCK_MODEL = "mock/keyword-rules-v1"

# Failure kinds. Network kinds never count as invalid model output and never quarantine a review.
NETWORK_NOT_SENT = "network_not_sent"   # connection never established / request not fully sent: no charge
NETWORK_UNKNOWN = "network_unknown"     # failed after the request was sent: it may have been billed
RATE_LIMITED = "rate_limited"           # HTTP 429
SERVER_ERROR = "server_error"           # HTTP 408 / 5xx / 529
FATAL = "fatal"                         # auth, billing, bad request: stop the run
NETWORK_KINDS = (NETWORK_NOT_SENT, NETWORK_UNKNOWN)


@dataclass
class ProviderResponse:
    request_id: str
    model: str
    ok: bool
    items: dict = field(default_factory=dict)   # key -> raw answer dict
    input_tokens: int | None = None
    output_tokens: int | None = None
    cost_usd: str | None = None                 # provider-reported, decimal string
    error: str | None = None
    retryable: bool = False                     # transient: rate limit, overload, server error
    fatal: bool = False                         # stop the run: auth, permission, bad request
    outcome_unknown: bool = False               # e.g. timeout: the call may still be billed
    elapsed_s: float | None = None
    error_kind: str | None = None               # one of the failure kinds above
    retry_after_s: float | None = None          # from a Retry-After header, when given


class Provider:
    name = "base"
    model = "base"
    max_batch = 1
    input_rate_per_m = Decimal("0")
    output_rate_per_m = Decimal("0")

    def classify(self, items):  # pragma: no cover - interface
        raise NotImplementedError

    def to_label(self, raw):
        """Map one raw answer to (topic, intent, severity, sentiment, confidence dict)."""
        raise NotImplementedError

    def estimate_input_tokens(self, items):
        raise NotImplementedError

    def estimate_cost(self, input_tokens, output_tokens=0):
        return (Decimal(input_tokens) * self.input_rate_per_m
                + Decimal(output_tokens or 0) * self.output_rate_per_m) / Decimal(1_000_000)


def _choice_answer(answer):
    return answer.get("choice"), answer.get("confidence")


def _retry_after(value):
    """Seconds from a numeric Retry-After header (HTTP dates are ignored), capped at 10 minutes."""
    try:
        return min(max(float(value), 0.0), 600.0) if value is not None else None
    except ValueError:
        return None


def probe_network(host="ai-gateway.vercel.sh", port=443, timeout_s=5.0):
    """Free connectivity check: DNS lookup plus a TCP connection, no HTTP request and no API charge."""
    try:
        with socket.create_connection((host, port), timeout=timeout_s):
            return True
    except OSError:
        return False


class MockProvider(Provider):
    """Offline test double. Rules are crude on purpose; they exercise the pipeline, not quality."""

    name = "mock"
    model = MOCK_MODEL
    input_rate_per_m = Decimal("0.042")

    RULES = (
        ("billing", ("charged", "refund", "price", "premium", "subscription")),
        ("access", ("log in", "login", "password", "sign in", "account")),
        ("downloads", ("download", "offline")),
        ("playback", ("crash", "stops", "lag", "buffer", "skips")),
        ("catalog", ("lyrics", "search", "recommend", "missing song")),
        ("support", ("support", "customer service")),
        ("usability", ("ads", "shuffle", "queue", "button")),
    )

    def __init__(self, max_batch=10, overhead_tokens=300, fail_requests=(), malformed_texts=(),
                 drop_texts=(), timeout_requests=(), fatal_requests=(), latency_s=0.0):
        self.max_batch = max_batch
        self.overhead_tokens = overhead_tokens
        self.fail_requests = set(fail_requests)        # request numbers (1-based) returning HTTP 529
        self.timeout_requests = set(timeout_requests)  # request numbers that time out
        self.fatal_requests = set(fatal_requests)      # request numbers returning HTTP 403
        self.malformed_texts = set(malformed_texts)    # texts answered with an invalid topic
        self.drop_texts = set(drop_texts)              # texts whose key is omitted from the response
        self.latency_s = latency_s
        self.calls = 0

    def estimate_input_tokens(self, items):
        return self.overhead_tokens + sum(len(text) // 4 + 1 for _, text in items)

    def classify(self, items):
        self.calls += 1
        n = self.calls
        rid = f"mock-{uuid.uuid4().hex[:12]}"
        if self.latency_s:
            time.sleep(self.latency_s)
        if n in self.timeout_requests:
            return ProviderResponse(rid, self.model, False, error="timeout", retryable=True, outcome_unknown=True,
                                    error_kind=NETWORK_UNKNOWN)
        if n in self.fail_requests:
            return ProviderResponse(rid, self.model, False, error="HTTP 529 overloaded", retryable=True,
                                    error_kind=SERVER_ERROR)
        if n in self.fatal_requests:
            return ProviderResponse(rid, self.model, False, error="HTTP 403 forbidden", fatal=True, error_kind=FATAL)
        answers = {}
        for key, text in items:
            if text in self.drop_texts:
                continue
            lower = text.lower()
            topic = next((t for t, words in self.RULES if any(w in lower for w in words)), "other")
            negative = any(w in lower for w in ("bad", "hate", "worst", "not", "can't", "cannot", "stop", "crash"))
            leaving = any(w in lower for w in ("uninstall", "cancel", "leaving"))
            intent = "cancellation" if leaving else "complaint" if negative else "praise"
            severity = "4" if "can't" in lower or "cannot" in lower else "3" if negative else "1"
            if text in self.malformed_texts:
                topic = "not-a-topic"
            answers[key] = {
                "topic": {"type": "choice", "choice": topic, "confidence": 0.9},
                "intent": {"type": "choice", "choice": intent, "confidence": 0.9},
                "severity": {"type": "choice", "choice": severity, "confidence": 0.9},
                "sentiment": {"type": "score", "score": 1.0 if negative else 3.0},
            }
        tokens = self.estimate_input_tokens(items)
        return ProviderResponse(rid, self.model, True, items=answers, input_tokens=tokens, output_tokens=0,
                                cost_usd=format(self.estimate_cost(tokens), "f"), elapsed_s=self.latency_s)

    def to_label(self, raw):
        return JevProvider.to_label(self, raw)


class JevProvider(Provider):
    """TypeSafe Jev via Vercel AI Gateway (TypeSafe-compatible API). One review per request.

    Shapes follow https://vercel.com/docs/ai-gateway/sdks-and-apis/typesafe (checked 2026-10-09).
    """

    name = "vercel-ai-gateway"
    model = "typesafe-ai/jev"
    endpoint = "https://ai-gateway.vercel.sh/typesafe/v1/systemone"
    ca_bundle = "/etc/ssl/cert.pem"
    max_batch = 1
    input_rate_per_m = Decimal("0.042")   # https://docs.typesafe.ai/models.md (checked 2026-10-09)
    output_rate_per_m = Decimal("0")
    timeout_s = 30

    def __init__(self, api_key, overhead_tokens=1200, transport=None, questions=None):
        # overhead_tokens is a conservative per-review reservation floor (the smoke test measured
        # 820 input tokens for a 66-character review); actual usage comes from each response.
        # questions defaults to the enrichment questions (prompt p1); the verifier passes its own.
        self._api_key = api_key
        self.overhead_tokens = overhead_tokens
        self._transport = transport or self._http_post
        self.questions = questions or QUESTIONS

    def __repr__(self):
        return f"JevProvider(model={self.model!r})"  # never include the key

    def estimate_input_tokens(self, items):
        return self.overhead_tokens + sum(len(text) // 3 + 1 for _, text in items)

    def _http_post(self, body):
        """Return (status, body, headers).

        urllib raises URLError for failures while connecting or sending the request (DNS, no route,
        refused, connect timeout): the request did not reach the API. Errors while waiting for or
        reading the response (read timeout, reset, IncompleteRead) are raised unwrapped: the API
        may have processed and billed the request.
        """
        request = urllib.request.Request(
            self.endpoint, data=json.dumps(body).encode("utf-8"), method="POST",
            headers={"Authorization": "Bearer " + self._api_key, "Content-Type": "application/json",
                     "User-Agent": "mba290t-a5-enrich/1"})
        context = ssl.create_default_context(cafile=self.ca_bundle)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_s, context=context) as response:
                return response.status, response.read(), dict(response.headers)
        except urllib.error.HTTPError as e:
            return e.code, e.read(), dict(e.headers or {})

    def classify(self, items):
        if len(items) != 1:
            raise ValueError("JevProvider sends exactly one review per request")
        key, text = items[0]
        body = {"model": self.model, "state": text, "questions": self.questions}
        started = time.monotonic()
        local_id = f"local-{uuid.uuid4().hex[:12]}"
        try:
            result = self._transport(body)
        except urllib.error.HTTPError as e:  # transports normally return HTTP errors; handle a raised one too
            result = (e.code, e.read() if e.fp else b"", dict(e.headers or {}))
        except urllib.error.URLError as e:
            return self._network_failure(local_id, e, started, NETWORK_NOT_SENT)
        except (OSError, http.client.HTTPException) as e:  # TimeoutError and ssl/socket errors are OSError
            return self._network_failure(local_id, e, started, NETWORK_UNKNOWN)
        status, raw = result[0], result[1]
        headers = {k.lower(): v for k, v in (result[2] if len(result) > 2 else {}).items()}
        elapsed = round(time.monotonic() - started, 3)
        text_body = raw.decode("utf-8", errors="replace").replace(self._api_key, "[REDACTED]")
        try:
            payload = json.loads(text_body)
            if not isinstance(payload, dict):
                raise ValueError("not a JSON object")
        except ValueError:
            if status == 200:
                # A 200 whose body is not complete JSON was cut off in transit (the API returns JSON).
                # Treat it as a network failure after sending: retry later, never quarantine.
                return ProviderResponse(local_id, self.model, False, error="truncated or unparseable 200 response",
                                        retryable=True, outcome_unknown=True, elapsed_s=elapsed,
                                        error_kind=NETWORK_UNKNOWN)
            payload = {}
        gateway = (payload.get("provider_metadata") or payload.get("providerMetadata") or {}).get("gateway", {})
        request_id = gateway.get("generationId") or local_id
        if status != 200:
            message = (payload.get("error") or {}).get("message") if isinstance(payload.get("error"), dict) \
                else payload.get("message")
            fatal = status in (400, 401, 402, 403, 404, 422)
            retryable = status in (408, 429, 500, 502, 503, 504, 529)
            kind = FATAL if fatal else RATE_LIMITED if status == 429 else SERVER_ERROR if retryable else None
            return ProviderResponse(request_id, self.model, False, error=f"HTTP {status}: {message or text_body[:200]}",
                                    retryable=retryable, fatal=fatal, elapsed_s=elapsed, error_kind=kind,
                                    retry_after_s=_retry_after(headers.get("retry-after")))
        usage = payload.get("usage") or {}
        answers = payload.get("answers")
        return ProviderResponse(
            request_id, payload.get("model") or self.model, True,
            items={key: answers} if isinstance(answers, dict) else {},
            input_tokens=usage.get("input_tokens") if isinstance(usage.get("input_tokens"), int) else None,
            output_tokens=usage.get("output_tokens") if isinstance(usage.get("output_tokens"), int) else None,
            cost_usd=str(gateway["cost"]) if gateway.get("cost") is not None else None, elapsed_s=elapsed)

    def _network_failure(self, local_id, error, started, kind):
        message = f"{type(error).__name__}: {error}".replace(self._api_key, "[REDACTED]")[:300]
        return ProviderResponse(local_id, self.model, False, error=message, retryable=True,
                                outcome_unknown=kind == NETWORK_UNKNOWN, error_kind=kind,
                                elapsed_s=round(time.monotonic() - started, 3))

    def to_label(self, raw):
        topic, topic_conf = _choice_answer(raw.get("topic", {}))
        intent, intent_conf = _choice_answer(raw.get("intent", {}))
        severity_choice, severity_conf = _choice_answer(raw.get("severity", {}))
        score = raw.get("sentiment", {}).get("score")
        severity = int(severity_choice) if isinstance(severity_choice, str) and severity_choice.isdigit() else None
        sentiment = None
        if isinstance(score, (int, float)):
            # Score is a 0-based position on 5 levels: sentiment = score / 2 - 1 maps 0..4 to -1..1.
            sentiment = round(max(-1.0, min(1.0, score / 2 - 1)), 4)
        confidence = {"topic": topic_conf, "intent": intent_conf, "severity": severity_conf}
        probabilities = {k: raw.get(k, {}).get("probabilities") for k in ("topic", "intent", "severity")}
        return topic, intent, severity, sentiment, confidence, probabilities
