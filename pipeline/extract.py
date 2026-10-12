"""Deterministic evidence-quote and entity extraction (no model calls).

The quote is always an exact substring of the original text: the whole review when it is
short, otherwise the sentence with the most topic-keyword matches (first sentence on a tie or
no match). Entities are canonical feature names whose patterns explicitly occur in the text.
Whether a quote actually supports the label is evaluated separately on the golden set.
"""

import re

EXTRACTION_VERSION = "x1"
FULL_TEXT_MAX_CHARS = 200

# Canonical entity -> regex (case-insensitive, word-bounded). Only explicit mentions count.
ENTITY_PATTERNS = {
    "ads": r"\bads?\b|\badvert\w*|\bcommercials?\b",
    "premium": r"\bpremium\b",
    "subscription": r"\bsubscri\w*",
    "price": r"\bprice\w*|\bexpensive\b|\bcost\w*",
    "login": r"\blog ?in\b|\blogg(?:ed|ing) (?:in|out)\b|\bsign(?:ed|ing)? ?in\b|\bpassword\b",
    "account": r"\baccount\b",
    "shuffle": r"\bshuffl\w*",
    "skip": r"\bskip\w*",
    "queue": r"\bqueue\w*",
    "playlist": r"\bplaylists?\b",
    "offline mode": r"\boffline\b",
    "downloads": r"\bdownload\w*",
    "lyrics": r"\blyrics?\b",
    "search": r"\bsearch\w*",
    "recommendations": r"\brecommend\w*|\bdiscover weekly\b|\balgorithm\w*",
    "podcasts": r"\bpodcasts?\b",
    "crash": r"\bcrash\w*",
    "buffering": r"\bbuffer\w*|\blag\w*",
    "audio quality": r"\baudio quality\b|\bsound quality\b",
    "bluetooth": r"\bbluetooth\b",
    "car": r"\bcar\b|\bandroid auto\b",
    "widget": r"\bwidgets?\b",
    "update": r"\bupdat\w*",
    "customer support": r"\bcustomer (?:service|support)\b|\bsupport team\b",
    "battery": r"\bbattery\b",
    "storage": r"\bstorage\b",
}
_COMPILED = {name: re.compile(pattern, re.IGNORECASE) for name, pattern in ENTITY_PATTERNS.items()}

TOPIC_KEYWORDS = {
    "access": ("login", "account"),
    "usability": ("ads", "shuffle", "skip", "queue", "playlist", "widget", "update"),
    "playback": ("crash", "buffering", "audio quality", "bluetooth", "car", "battery"),
    "downloads": ("downloads", "offline mode", "storage"),
    "catalog": ("lyrics", "search", "recommendations", "podcasts"),
    "billing": ("premium", "subscription", "price"),
    "support": ("customer support",),
    "other": (),
}

_SENTENCE_END = re.compile(r"[.!?。！？]+[\"')\]]*\s+|\n+")


def find_entities(text):
    """Canonical entity names explicitly mentioned in the text, in order of first appearance."""
    found = []
    for name, pattern in _COMPILED.items():
        match = pattern.search(text)
        if match:
            found.append((match.start(), name))
    return [name for _, name in sorted(found)]


def sentences(text):
    """Split into sentence spans that are exact substrings, preserving original characters."""
    spans, start = [], 0
    for match in _SENTENCE_END.finditer(text):
        end = match.start() + len(match.group(0).rstrip())
        if text[start:end].strip():
            spans.append(text[start:end].strip())
        start = match.end()
    if text[start:].strip():
        spans.append(text[start:].strip())
    return spans


def evidence_quote(text, topic):
    """Return (quote, method). The quote is always an exact substring of text."""
    if len(text) <= FULL_TEXT_MAX_CHARS:
        return text, "full_text"
    candidates = sentences(text) or [text]
    keywords = TOPIC_KEYWORDS.get(topic, ())
    best, best_hits = candidates[0], 0
    for sentence in candidates:
        hits = sum(bool(_COMPILED[k].search(sentence)) for k in keywords)
        if hits > best_hits:
            best, best_hits = sentence, hits
    method = "topic_keyword_sentence" if best_hits else "first_sentence"
    assert best in text
    return best, method
