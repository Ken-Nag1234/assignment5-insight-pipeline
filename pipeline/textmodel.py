"""Text-generation models for the bounded language roles (issue naming and memo writing).

Classes:
  MockTextModel     offline test double; its output is synthetic and is labeled as such.
  GatewayChatModel  Vercel AI Gateway OpenAI-compatible Chat Completions
                    (POST https://ai-gateway.vercel.sh/v1/chat/completions, checked 2026-10-11).

run_model_call() is the single entry point used by the roles. It
  1. returns a saved result when the exact same request (model, role config, messages, output cap)
     was already answered (downstream result cache; no new call, nothing billed),
  2. otherwise reserves the worst-case cost in the shared spend ledger, calls the model once,
     settles the actual cost, and appends one contract-style call record to the role's call log.
Keys are never written to logs. A model must be chosen and its prices confirmed before any paid use.
"""

import hashlib
import json
import ssl
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from pipeline.common import atomic_write_json

MOCK_TEXT_MODEL = "mock/text-v1"

# Per-model request settings. GPT-6 Luna defaults to reasoning effort "medium"; "none" is the
# cheapest supported setting (https://developers.openai.com/api/docs/models/gpt-6-luna, checked
# 2026-10-11). Reasoning models may reject a temperature, so it is sent only for other models.
MODEL_SETTINGS = {"openai/gpt-6-luna": {"reasoning_effort": "none"}}


@dataclass
class TextResponse:
    request_id: str
    model: str
    ok: bool
    text: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    cost_usd: str | None = None
    error: str | None = None
    outcome_unknown: bool = False
    elapsed_s: float | None = None


class MockTextModel:
    """Deterministic offline model. responder(messages) -> str; raising makes a failed call."""

    name = "mock"

    def __init__(self, responder, model=MOCK_TEXT_MODEL):
        self.responder = responder
        self.model = model
        self.calls = 0

    def estimate_cost(self, input_tokens, output_tokens):
        return Decimal(input_tokens + output_tokens) * Decimal("0.0000001")

    def generate(self, messages, max_tokens):
        self.calls += 1
        rid = f"mock-text-{uuid.uuid4().hex[:12]}"
        prompt_chars = sum(len(m["content"]) for m in messages)
        try:
            text = self.responder(messages)
        except Exception as e:  # noqa: BLE001 - simulated failure
            return TextResponse(rid, self.model, False, error=f"{type(e).__name__}: {e}")
        tokens_in, tokens_out = prompt_chars // 4 + 1, len(text) // 4 + 1
        return TextResponse(rid, self.model, True, text=text, input_tokens=tokens_in, output_tokens=tokens_out,
                            cost_usd=format(self.estimate_cost(tokens_in, tokens_out), "f"), elapsed_s=0.0)


class GatewayChatModel:
    """OpenAI-compatible chat completions through Vercel AI Gateway. Prices are passed in explicitly."""

    name = "vercel-ai-gateway"
    endpoint = "https://ai-gateway.vercel.sh/v1/chat/completions"
    ca_bundle = "/etc/ssl/cert.pem"
    timeout_s = 120

    def __init__(self, api_key, model, input_price_per_m, output_price_per_m, transport=None, reasoning_effort=None):
        self._api_key = api_key
        self.model = model
        self.reasoning_effort = reasoning_effort or MODEL_SETTINGS.get(model, {}).get("reasoning_effort")
        self.input_price_per_m = Decimal(str(input_price_per_m))
        self.output_price_per_m = Decimal(str(output_price_per_m))
        self._transport = transport or self._http_post

    def __repr__(self):
        return f"GatewayChatModel(model={self.model!r})"

    def settings(self):
        """Request settings recorded with each call (never includes the key)."""
        if self.reasoning_effort:
            return f"reasoning_effort={self.reasoning_effort}, json_object"
        return "temperature=0, json_object"

    def estimate_cost(self, input_tokens, output_tokens):
        return (Decimal(input_tokens) * self.input_price_per_m
                + Decimal(output_tokens) * self.output_price_per_m) / Decimal(1_000_000)

    def _http_post(self, body):
        request = urllib.request.Request(
            self.endpoint, data=json.dumps(body).encode("utf-8"), method="POST",
            headers={"Authorization": "Bearer " + self._api_key, "Content-Type": "application/json",
                     "User-Agent": "mba290t-a5-roles/1"})
        context = ssl.create_default_context(cafile=self.ca_bundle)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_s, context=context) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()

    def generate(self, messages, max_tokens):
        body = {"model": self.model, "messages": messages, "max_tokens": max_tokens,
                "response_format": {"type": "json_object"}}
        if self.reasoning_effort:
            body["reasoning_effort"] = self.reasoning_effort  # Gateway alias of reasoning.effort
        else:
            body["temperature"] = 0
        local_id = f"local-{uuid.uuid4().hex[:12]}"
        started = time.monotonic()
        try:
            status, raw = self._transport(body)
        except urllib.error.URLError as e:
            return TextResponse(local_id, self.model, False, error=f"not sent: {type(e).__name__}: {e}"[:300])
        except OSError as e:
            return TextResponse(local_id, self.model, False, error=f"{type(e).__name__}: {e}"[:300],
                                outcome_unknown=True)
        elapsed = round(time.monotonic() - started, 3)
        text = raw.decode("utf-8", errors="replace").replace(self._api_key, "[REDACTED]")
        try:
            payload = json.loads(text)
        except ValueError:
            payload = None
        if status != 200 or not isinstance(payload, dict):
            message = ((payload or {}).get("error") or {}).get("message") if isinstance(payload, dict) else None
            return TextResponse(local_id, self.model, False, error=f"HTTP {status}: {message or text[:200]}",
                                outcome_unknown=status == 200, elapsed_s=elapsed)
        usage = payload.get("usage") or {}
        meta = (payload.get("providerMetadata") or payload.get("provider_metadata") or {}).get("gateway") or {}
        choices = payload.get("choices") or [{}]
        content = (choices[0].get("message") or {}).get("content")
        return TextResponse(payload.get("id") or meta.get("generationId") or local_id, payload.get("model") or self.model,
                            isinstance(content, str), text=content,
                            input_tokens=usage.get("prompt_tokens"), output_tokens=usage.get("completion_tokens"),
                            cost_usd=str(meta["cost"]) if meta.get("cost") is not None else None,
                            error=None if isinstance(content, str) else "no message content", elapsed_s=elapsed)


def _now():
    return datetime.now(timezone.utc).isoformat()


def request_key(model, label_config, messages, max_tokens):
    blob = json.dumps({"model": model, "label_config": label_config, "messages": messages,
                       "max_tokens": max_tokens}, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def append_call(log_path, record):
    log_path = Path(log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, sort_keys=True, ensure_ascii=False) + "\n")


def call_record(response, *, role, review_ids, label_config, run_id, provider, cost, cost_source, attempt,
                started_at, phase=None):
    """One calls.jsonl line in the grading-contract shape (plus provider/cost fields)."""
    return {"request_id": response.request_id, "role": role, "review_ids": list(review_ids),
            "model": response.model, "phase": phase, "outcome": "succeeded" if response.ok else "failed",
            "label_config": label_config, "input_tokens": response.input_tokens,
            "output_tokens": response.output_tokens, "provider": provider, "run_id": run_id, "attempt": attempt,
            "error": response.error, "cost_usd": cost, "cost_source": cost_source, "started_at": started_at,
            "elapsed_s": response.elapsed_s}


def run_model_call(model, messages, *, role, review_ids, label_config, ledger, log_path, cache_dir, run_id,
                   max_tokens, attempt=1):
    """Return (text or None, from_cache, record or None). Never calls the model for a cached request."""
    key = request_key(model.model, label_config, messages, max_tokens)
    cache_file = Path(cache_dir) / role / f"{key}.json"
    if cache_file.exists():
        cached = json.loads(cache_file.read_text(encoding="utf-8"))
        return cached["text"], True, None
    input_estimate = sum(len(m["content"]) for m in messages) // 3 + 50
    reservation = model.estimate_cost(input_estimate, max_tokens)
    token = ledger.reserve(reservation)
    if token is None:
        raise BudgetStop(f"{role}: spend cap reached (spent {ledger.spent}, cap {ledger.cap})")
    started_at = _now()
    response = model.generate(messages, max_tokens)
    if response.cost_usd is not None:
        cost, source = str(response.cost_usd), "provider_reported"
    elif response.input_tokens is not None:
        cost = format(model.estimate_cost(response.input_tokens, response.output_tokens or 0), "f")
        source = "estimated"
    elif response.outcome_unknown:
        cost, source = None, "unknown"
    else:
        cost, source = "0", "none"
    record = call_record(response, role=role, review_ids=review_ids, label_config=label_config, run_id=run_id,
                         provider=model.name, cost=cost, cost_source=source, attempt=attempt, started_at=started_at)
    ledger.settle(token, {"request_id": response.request_id, "run_id": run_id, "role": role, "db": "",
                          "cost_usd": cost, "cost_source": source})
    append_call(log_path, record)
    if response.ok:
        atomic_write_json(cache_file, {"key": key, "role": role, "model": response.model,
                                       "label_config": label_config, "request_id": response.request_id,
                                       "text": response.text, "saved_at": _now()})
    return (response.text if response.ok else None), False, record


class BudgetStop(Exception):
    pass
