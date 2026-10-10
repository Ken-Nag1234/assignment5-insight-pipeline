"""Shared helpers for the Assignment 5 insight pipeline.

Source-row hashing, CSV parsing and the ingestion profile are delegated to the
instructor-provided check_submission.py so that our values match the grader's
exactly (no whitespace, accent or Unicode normalization).
"""

import hashlib
import json
import os
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import check_submission as provided  # noqa: E402  (instructor helper, kept unmodified)

FIELDS = provided.FIELDS
TOPICS = provided.TOPICS
INTENTS = provided.INTENTS
VALID_RATINGS = {"1", "2", "3", "4", "5"}
EMPTY_TEXT_REASON = "empty_review_text"

row_sha = provided.row_sha
csv_rows = provided.csv_rows
file_sha256 = provided.sha
grading_profile = provided.profile


def is_empty_text(text):
    # Same rule as the grader: whitespace-only text counts as empty.
    return not text.strip()


def text_sha256(text):
    # Exact-text identity for cache reuse; the text is hashed byte-for-byte.
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def atomic_write_text(path, text):
    """Write a file via a temp file + rename so readers never see partial output."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix="." + path.name + ".", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
        os.chmod(tmp, 0o644)  # mkstemp creates 0600; outputs are meant to be shared
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def atomic_write_json(path, value):
    atomic_write_text(path, json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n")


def atomic_write_jsonl(path, rows):
    atomic_write_text(path, "".join(json.dumps(r, ensure_ascii=False, sort_keys=True, allow_nan=False) + "\n" for r in rows))
