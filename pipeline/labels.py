"""Shared label schema, classifier question definitions and record validation.

Topic, intent and severity definitions restate GRADING_CONTRACT.md ("Common labels for fair
comparison"). Any change to the questions, mapping or consistency rules must bump
PROMPT_VERSION or SCHEMA_VERSION so that label_config changes and cached results are not reused.
"""

import json
import math

from pipeline.common import INTENTS, TOPICS

SCHEMA_VERSION = "s1"
PROMPT_VERSION = "p1"
SEVERITIES = (1, 2, 3, 4, 5)

TOPIC_CRITERIA = {
    "access": "Login, signup, password or account access",
    "usability": "Navigation, controls, layout, queue/playlist management, ad interruptions",
    "playback": "Playback failure, crashes, lag, connection failures, audio quality, resource use",
    "downloads": "Downloading, saved music, offline listening, disappearing downloads",
    "catalog": "Missing songs/artists, search/discovery, recommendations, lyrics availability",
    "billing": "Price, charges, subscriptions, paywalls, premium entitlement, premium-only controls",
    "support": "Contacting support and the support response",
    "other": "General praise or criticism, unrelated content, or no specific topic",
}
INTENT_CRITERIA = {
    "cancellation": "Explicitly leaving, uninstalling, cancelling, or threatening to",
    "complaint": "Reports a negative experience, including mixed praise and criticism, or generic 'bad app'",
    "request": "Asks for a change without reporting a failure",
    "praise": "Positive experience only",
    "unclear": "Unrelated or meaningless text, or a bare boycott slogan without a product complaint",
}
SEVERITY_CRITERIA = {
    "1": "No reported problem: praise, neutral or unclear content, or a pure feature request",
    "2": "Dislike, generic criticism, minor annoyance or cosmetic issue; no functional loss",
    "3": "A degraded or restricted function; some use or a workaround remains",
    "4": "A core task is clearly blocked, such as being unable to log in or play music",
    "5": "Explicit serious financial, privacy or data harm",
}
SENTIMENT_LEVELS = ["Very negative", "Negative", "Neutral or mixed", "Positive", "Very positive"]

# Classifier questions (TypeSafe/Jev System One shapes: choice and score).
QUESTIONS = {
    "topic": {"type": "choice",
              "instructions": "Which product area is the highest-severity problem in this app review? "
                              "On a tie, the first one mentioned. For praise, the first specific praised feature.",
              "criteria": TOPIC_CRITERIA},
    "intent": {"type": "choice", "instructions": "What is the reviewer's intent?", "criteria": INTENT_CRITERIA},
    "severity": {"type": "choice", "instructions": "How severe is the reported impact? Stars, angry language "
                                                   "or cancellation alone do not raise severity.",
                 "criteria": SEVERITY_CRITERIA},
    "sentiment": {"type": "score", "instructions": "What is the overall sentiment of the review?",
                  "criteria": SENTIMENT_LEVELS},
}

LABEL_FIELDS = ("topic", "intent", "sentiment", "severity", "entities", "evidence_quote", "needs_review")


def label_config(model, extraction_version, extra=None):
    """Identity of everything that determines a label. Results are reused only under an identical value."""
    parts = [model, PROMPT_VERSION, SCHEMA_VERSION, extraction_version]
    if extra:
        parts.append(extra)
    return "+".join(parts)


def apply_consistency(topic, intent, severity):
    """Deterministic rules taken directly from the shared definitions.

    - praise / request / unclear report no problem, so severity is 1.
    - a complaint reports a negative experience, so severity is at least 2.
    Returns (severity, adjustments) where adjustments lists any change made.
    """
    adjustments = []
    if intent in ("praise", "request", "unclear") and severity != 1:
        adjustments.append(f"severity {severity}->1 (intent {intent} reports no problem)")
        severity = 1
    elif intent == "complaint" and severity == 1:
        adjustments.append("severity 1->2 (a complaint reports a negative experience)")
        severity = 2
    return severity, adjustments


def validate_label(label, source_text):
    """Return a list of schema problems; mirrors the checks in check_submission.py."""
    problems = []
    if label.get("topic") not in TOPICS:
        problems.append("invalid topic")
    if label.get("intent") not in INTENTS:
        problems.append("invalid intent")
    if type(label.get("severity")) is not int or label["severity"] not in SEVERITIES:
        problems.append("invalid severity")
    sentiment = label.get("sentiment")
    if type(sentiment) not in (int, float) or not math.isfinite(sentiment) or not -1 <= sentiment <= 1:
        problems.append("invalid sentiment")
    if type(label.get("needs_review")) is not bool:
        problems.append("invalid needs_review")
    entities = label.get("entities")
    if not isinstance(entities, list) or not all(isinstance(x, str) and x.strip() for x in entities):
        problems.append("invalid entities")
    quote = label.get("evidence_quote")
    if not isinstance(quote, str) or not quote.strip():
        problems.append("missing evidence_quote")
    elif quote not in source_text:
        problems.append("evidence_quote is not an exact substring")
    return problems


def record_from_row(row):
    """Build a grading-contract record dict from a joined reviews/labels database row (sqlite3.Row)."""
    record = {"review_id": row["review_id"], "source_sha256": row["source_sha256"], "status": "completed",
              "topic": row["topic"], "intent": row["intent"], "sentiment": row["sentiment"],
              "severity": row["severity"], "entities": json.loads(row["entities"]),
              "evidence_quote": row["evidence_quote"], "needs_review": bool(row["needs_review"]),
              "label_config": row["label_config"]}
    if row["cache_source_id"]:
        record["cache_source_id"] = row["cache_source_id"]
    return record
