"""Jev connectivity smoke test through Vercel AI Gateway (TypeSafe-compatible API).

Sends at most ONE request about ONE short synthetic review (not from the dataset).
No automatic retries. Default mode is a dry run that sends nothing.

Usage:
    python3 -m pipeline.smoke_jev               # dry run: show request body, send nothing
    python3 -m pipeline.smoke_jev --execute     # send exactly one paid request

Request/response shapes follow the official docs (checked 2026-10-09):
  https://vercel.com/docs/ai-gateway/sdks-and-apis/typesafe
  https://docs.typesafe.ai/primitives/choice , https://docs.typesafe.ai/primitives/score
The API key is read from AI_GATEWAY_API_KEY (environment or git-ignored .env) and is never
printed, logged or saved.
"""

import argparse
import json
import ssl
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal

from pipeline.common import REPO_ROOT, atomic_write_json
from pipeline.config import get_key

ENDPOINT = "https://ai-gateway.vercel.sh/typesafe/v1/systemone"
MODEL = "typesafe-ai/jev"
# Editable rate, USD per 1M tokens. Source: https://docs.typesafe.ai/models.md and
# https://vercel.com/ai-gateway/models (checked 2026-10-09). Output tokens are not charged.
RATE_INPUT_PER_M = Decimal("0.042")
RATE_OUTPUT_PER_M = Decimal("0")
TIMEOUT_SECONDS = 30
# python.org macOS builds ship without CA roots; use the system bundle. Verification stays fully on.
CA_BUNDLE = "/etc/ssl/cert.pem"
LOCK_FILE = REPO_ROOT / "work" / "smoke_jev_attempt.json"
OUT_DIR = REPO_ROOT / "outputs" / "smoke"

# Synthetic test text written for this check; it is not a dataset review.
SYNTHETIC_REVIEW = "The app keeps logging me out and now I can't sign back in at all."

QUESTIONS = {
    "topic": {
        "type": "choice",
        "instructions": "Which product area is the main problem or praised feature in this app review?",
        "criteria": {
            "access": "Login, signup, password or account access",
            "usability": "Navigation, controls, layout, queue/playlist management, ad interruptions",
            "playback": "Playback failure, crashes, lag, connection failures, audio quality, resource use",
            "downloads": "Downloading, saved music, offline listening, disappearing downloads",
            "catalog": "Missing songs/artists, search/discovery, recommendations, lyrics availability",
            "billing": "Price, charges, subscriptions, paywalls, premium entitlement",
            "support": "Contacting support and the support response",
            "other": "General praise/criticism, unrelated content, or no specific topic",
        },
    },
    "intent": {
        "type": "choice",
        "instructions": "What is the reviewer's intent?",
        "criteria": {
            "cancellation": "Explicitly leaving, uninstalling, cancelling, or threatening to",
            "complaint": "Reports a negative experience, including mixed praise and criticism",
            "request": "Asks for a change without reporting a failure",
            "praise": "Positive experience only",
            "unclear": "Unrelated, meaningless, or a bare slogan",
        },
    },
    "severity": {
        "type": "choice",
        "instructions": "How severe is the reported impact?",
        "criteria": {
            "1": "No reported problem: praise, unclear content, or a pure feature request",
            "2": "Dislike, generic criticism, or minor annoyance; no functional loss",
            "3": "A degraded or restricted function; some use remains",
            "4": "A core task is clearly blocked, such as logging in or playing music",
            "5": "Explicit serious financial, privacy, or data harm",
        },
    },
    "sentiment": {
        "type": "score",
        "instructions": "What is the overall sentiment of the review?",
        "criteria": ["Very negative", "Negative", "Neutral or mixed", "Positive", "Very positive"],
    },
}


def build_request():
    return {"model": MODEL, "state": SYNTHETIC_REVIEW, "questions": QUESTIONS}


def estimate_cost(usage):
    """Cost from usage and the editable rates; None when the provider did not report usage."""
    if not usage or not isinstance(usage.get("input_tokens"), int):
        return None
    output = usage.get("output_tokens") if isinstance(usage.get("output_tokens"), int) else 0
    cost = (Decimal(usage["input_tokens"]) * RATE_INPUT_PER_M + Decimal(output) * RATE_OUTPUT_PER_M) / Decimal(1_000_000)
    return format(cost, "f")


def _redact(text, secret):
    return text.replace(secret, "[REDACTED]") if secret else text


def send_once(body, key):
    """Exactly one HTTP attempt. Returns (status, parsed_json_or_text, safe_headers, elapsed_seconds)."""
    request = urllib.request.Request(
        ENDPOINT, data=json.dumps(body).encode("utf-8"), method="POST",
        headers={"Authorization": "Bearer " + key, "Content-Type": "application/json",
                 "User-Agent": "mba290t-a5-smoke/1"})
    started = time.monotonic()
    try:
        context = ssl.create_default_context(cafile=CA_BUNDLE)
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS, context=context) as response:
            status, raw, headers = response.status, response.read(), response.headers
    except urllib.error.HTTPError as e:
        status, raw, headers = e.code, e.read(), e.headers
    elapsed = round(time.monotonic() - started, 3)
    text = _redact(raw.decode("utf-8", errors="replace"), key)
    try:
        payload = json.loads(text)
    except ValueError:
        payload = text
    safe = {k: v for k, v in (headers or {}).items()
            if k.lower().startswith(("x-ai-gateway", "x-typesafe", "x-vercel-id", "content-type", "retry-after"))}
    return status, payload, safe, elapsed


def summarize(payload):
    lines = []
    answers = payload.get("answers", {}) if isinstance(payload, dict) else {}
    for name, answer in answers.items():
        if answer.get("type") == "choice":
            probs = answer.get("probabilities") or {}
            top = ", ".join(f"{k}={v:.3f}" for k, v in sorted(probs.items(), key=lambda kv: -kv[1])[:3])
            lines.append(f"  {name}: {answer.get('choice')} (confidence={answer.get('confidence')}; top: {top})")
        elif answer.get("type") == "score":
            score = answer.get("score")
            mapped = f"; sentiment={score / 2 - 1:+.3f}" if isinstance(score, (int, float)) else ""
            lines.append(f"  {name}: score={score}{mapped}")
        else:
            lines.append(f"  {name}: {json.dumps(answer)}")
    return lines


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--execute", action="store_true", help="Actually send the single paid request")
    args = parser.parse_args(argv)
    body = build_request()

    if not args.execute:
        print("DRY RUN: nothing sent. Request that --execute would send (no credentials shown):")
        print(f"POST {ENDPOINT}")
        print(json.dumps(body, indent=2))
        print(f"Rates: input ${RATE_INPUT_PER_M}/1M tokens, output ${RATE_OUTPUT_PER_M}/1M tokens")
        return 0

    if LOCK_FILE.exists():
        print(f"REFUSED: a smoke-test request was already attempted (see {LOCK_FILE.relative_to(REPO_ROOT)}). "
              "Delete that file only if you approve another paid request.", file=sys.stderr)
        return 3
    key = get_key("AI_GATEWAY_API_KEY")
    if not key:
        print("REFUSED: AI_GATEWAY_API_KEY is not set. Run: python3 -m pipeline.config check", file=sys.stderr)
        return 2

    attempted_at = datetime.now(timezone.utc).isoformat()
    # Record the attempt BEFORE sending, so a crash or timeout can never lead to an unnoticed second request.
    atomic_write_json(LOCK_FILE, {"attempted_at": attempted_at, "endpoint": ENDPOINT, "model": MODEL})
    try:
        status, payload, headers, elapsed = send_once(body, key)
        error = None
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        status, payload, headers, elapsed, error = None, None, {}, None, _redact(f"{type(e).__name__}: {e}", key)

    usage = payload.get("usage") if isinstance(payload, dict) else None
    gateway = (payload.get("provider_metadata") or {}).get("gateway", {}) if isinstance(payload, dict) else {}
    record = {
        "attempted_at": attempted_at, "endpoint": ENDPOINT, "model_requested": MODEL,
        "outcome": "succeeded" if status == 200 else "failed", "http_status": status, "error": error,
        "elapsed_seconds": elapsed, "retries": 0, "request_body": body, "response": payload,
        "response_headers": headers, "usage": usage,
        "estimated_cost_usd": estimate_cost(usage),
        "gateway_reported_cost_usd": gateway.get("cost"),
        "generation_id": gateway.get("generationId"),
        "rates": {"input_per_1m_usd": str(RATE_INPUT_PER_M), "output_per_1m_usd": str(RATE_OUTPUT_PER_M),
                  "checked": "2026-10-09"},
        "note": "Synthetic single-review connectivity check; not part of the dataset run or the cost pilot.",
    }
    out = OUT_DIR / ("jev_smoke_" + attempted_at[:19].replace(":", "").replace("-", "") + "Z.json")
    atomic_write_json(out, record)

    print(f"Outcome: {record['outcome']} (HTTP {status}) in {elapsed}s, retries: 0")
    if error:
        print("Error:", error)
    if status != 200:
        print("Response:", json.dumps(payload, indent=2) if isinstance(payload, dict) else payload)
    else:
        print(f"Model: {payload.get('model')}  provider: "
              f"{(gateway.get('routing') or {}).get('finalProvider')}  generation: {record['generation_id']}")
        print("Answers:")
        print("\n".join(summarize(payload)))
    print(f"Usage: {usage}")
    print(f"Estimated cost (tokens x editable rate): ${record['estimated_cost_usd']}")
    print(f"Gateway-reported cost: ${record['gateway_reported_cost_usd']}")
    print(f"Saved: {out.relative_to(REPO_ROOT)}")
    return 0 if status == 200 else 1


if __name__ == "__main__":
    sys.exit(main())
