"""Text-model availability check and a single smoke-test call through Vercel AI Gateway.

Usage:
    python3 -m pipeline.smoke_text                       # dry run: show the request, send nothing
    python3 -m pipeline.smoke_text --check-model         # free metadata lookup (GET /v1/models/<id>), no generation
    python3 -m pipeline.smoke_text --execute --cap-increment-usd 0.001   # exactly one paid request

The paid request is one tiny synthetic prompt (no dataset text) with a 40-token output cap. It goes
through the shared spend ledger with a cumulative cap of (current ledger total + increment), is
logged to outputs/smoke/text_smoke_calls.jsonl (role "smoke", never exported to grading/), and its
result is saved to outputs/smoke/text_smoke_<stamp>.json. No retries. The key is never printed.
"""

import argparse
import json
import ssl
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from pipeline.common import REPO_ROOT, atomic_write_json
from pipeline.ledger import DEFAULT_LEDGER, SpendLedger
from pipeline.textmodel import GatewayChatModel, run_model_call

MODEL = "openai/gpt-6-luna"
MODELS_URL = "https://ai-gateway.vercel.sh/v1/models/"
OUT = REPO_ROOT / "outputs" / "smoke"
MESSAGES = [{"role": "system", "content": "You are a connectivity test. Return only JSON."},
            {"role": "user", "content": "Return exactly this JSON object: {\"ok\": true}"}]
MAX_TOKENS = 40


def check_model(api_key, model=MODEL, transport=None):
    """Free metadata request; returns the parsed model entry (never generates text)."""
    def default(url):
        request = urllib.request.Request(url, headers={"Authorization": "Bearer " + api_key,
                                                       "User-Agent": "mba290t-a5-roles/1"})
        context = ssl.create_default_context(cafile=GatewayChatModel.ca_bundle)
        try:
            with urllib.request.urlopen(request, timeout=30, context=context) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()
    status, raw = (transport or default)(MODELS_URL + model)
    text = raw.decode("utf-8", errors="replace").replace(api_key, "[REDACTED]")
    return status, json.loads(text) if text.strip().startswith("{") else {"raw": text[:300]}


def smoke(model, ledger, out_dir=OUT, stamp=None):
    stamp = stamp or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(out_dir)
    text, cached, record = run_model_call(model, MESSAGES, role="smoke", review_ids=[], label_config=f"{model.model}+smoke",
                                          ledger=ledger, log_path=out_dir / "text_smoke_calls.jsonl",
                                          cache_dir=out_dir / "text_smoke_cache" / stamp,
                                          run_id=f"text-smoke-{stamp}", max_tokens=MAX_TOKENS)
    try:
        parsed_ok = json.loads(text or "").get("ok") is True
    except (ValueError, AttributeError):
        parsed_ok = False
    result = {"stamp": stamp, "model": model.model, "settings": model.settings() if hasattr(model, "settings") else "",
              "synthetic_prompt": True, "from_cache": cached, "call": record, "response_text": text,
              "json_ok": parsed_ok, "ledger_total_after": str(ledger.spent)}
    atomic_write_json(out_dir / f"text_smoke_{stamp}.json", result)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--model-id", default=MODEL)
    parser.add_argument("--input-price-per-m", default="0.10")
    parser.add_argument("--output-price-per-m", default="0.50")
    parser.add_argument("--cap-increment-usd", default="0.001")
    parser.add_argument("--check-model", action="store_true")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    if not (args.check_model or args.execute):
        model = GatewayChatModel("dry-run", args.model_id, args.input_price_per_m, args.output_price_per_m,
                                 transport=lambda body: (_ for _ in ()).throw(SystemExit(json.dumps(body, indent=2))))
        print("DRY RUN - this request would be sent once:")
        try:
            model.generate(MESSAGES, MAX_TOKENS)
        except SystemExit as shown:
            print(shown)
        return 0
    from pipeline.config import get_key
    key = get_key("AI_GATEWAY_API_KEY")
    if not key:
        raise SystemExit("ERROR: AI_GATEWAY_API_KEY is not set.")
    if args.check_model:
        status, entry = check_model(key, args.model_id)
        print(status, json.dumps(entry, indent=2)[:2000])
        return 0 if status == 200 else 1
    current = SpendLedger(Decimal("0"), DEFAULT_LEDGER).spent
    ledger = SpendLedger(current + Decimal(args.cap_increment_usd), DEFAULT_LEDGER)
    model = GatewayChatModel(key, args.model_id, args.input_price_per_m, args.output_price_per_m)
    result = smoke(model, ledger)
    print(json.dumps({k: v for k, v in result.items() if k != "call"}, indent=2))
    print(json.dumps({k: result["call"].get(k) for k in ("outcome", "input_tokens", "output_tokens", "cost_usd",
                                                          "cost_source", "error")} if result["call"] else {}, indent=2))
    return 0 if result["json_ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
