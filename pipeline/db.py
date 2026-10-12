"""SQLite working store for pipeline state.

One database holds one input file. Every source review_id gets exactly one row
with an explicit status:
  pending      - nonempty text not yet classified (NOT a quarantine)
  completed    - validated classification saved
  quarantined  - genuinely unprocessable, with a nonempty reason
"""

import sqlite3
from pathlib import Path

SCHEMA_VERSION = "2"

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reviews (
    review_id        TEXT PRIMARY KEY,
    source_row       INTEGER NOT NULL UNIQUE,  -- 1-based data row number in the input CSV
    review_text      TEXT NOT NULL,
    review_rating    TEXT NOT NULL,
    review_likes     TEXT NOT NULL,
    app_version      TEXT NOT NULL,
    review_timestamp TEXT NOT NULL,
    source_sha256    TEXT NOT NULL,            -- grading-contract row hash
    text_sha256      TEXT,                     -- exact-text hash; NULL for empty text
    text_rep_id      TEXT,                     -- first review_id with identical text
    status           TEXT NOT NULL CHECK (status IN ('pending', 'completed', 'quarantined')),
    reason           TEXT,
    attempts         INTEGER NOT NULL DEFAULT 0,
    CHECK (status != 'quarantined' OR (reason IS NOT NULL AND length(trim(reason)) > 0))
);

CREATE INDEX IF NOT EXISTS idx_reviews_text ON reviews (text_sha256);
CREATE INDEX IF NOT EXISTS idx_reviews_status ON reviews (status);

-- Validated classifications (one per completed review). cache_source_id points directly
-- to the original completed record with identical text and label_config.
CREATE TABLE IF NOT EXISTS labels (
    review_id       TEXT PRIMARY KEY REFERENCES reviews (review_id),
    label_config    TEXT NOT NULL,
    topic           TEXT NOT NULL,
    intent          TEXT NOT NULL,
    sentiment       REAL NOT NULL,
    severity        INTEGER NOT NULL,
    entities        TEXT NOT NULL,             -- JSON list of strings
    evidence_quote  TEXT NOT NULL,
    needs_review    INTEGER NOT NULL,
    cache_source_id TEXT REFERENCES labels (review_id),
    request_id      TEXT,                      -- enrichment call that produced the original
    details         TEXT NOT NULL DEFAULT '{}', -- JSON: probabilities, adjustments, quote method
    completed_at    TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_labels_config ON labels (label_config);

-- Every attempted model call, including failures and retries.
CREATE TABLE IF NOT EXISTS calls (
    request_id     TEXT PRIMARY KEY,
    run_id         TEXT NOT NULL,
    role           TEXT NOT NULL,
    phase          TEXT,
    provider       TEXT NOT NULL,
    model          TEXT NOT NULL,
    label_config   TEXT,
    review_ids     TEXT NOT NULL,              -- JSON list of review IDs actually sent
    outcome        TEXT NOT NULL CHECK (outcome IN ('succeeded', 'failed')),
    error          TEXT,
    input_tokens   INTEGER,
    output_tokens  INTEGER,
    cost_usd       TEXT,                       -- decimal string; NULL when unknown
    cost_source    TEXT NOT NULL,              -- provider_reported | estimated | unknown | none
    started_at     TEXT NOT NULL,
    elapsed_s      REAL,
    attempt        INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS sample_membership (
    sample      TEXT NOT NULL,
    review_id   TEXT NOT NULL REFERENCES reviews (review_id),
    sample_row  INTEGER NOT NULL,
    PRIMARY KEY (sample, review_id)
);
"""


def connect(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.executescript(SCHEMA)
    columns = {row[1] for row in conn.execute("PRAGMA table_info(reviews)")}
    if "invalid_outputs" not in columns:
        # Added in schema 2: count of invalid model outputs, for the retry-once rule.
        conn.execute("ALTER TABLE reviews ADD COLUMN invalid_outputs INTEGER NOT NULL DEFAULT 0")
        conn.commit()
    conn.row_factory = sqlite3.Row
    return conn


def get_meta(conn, key, default=None):
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row[0] if row else default


def set_meta(conn, key, value):
    conn.execute("INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                 (key, str(value)))


def status_counts(conn):
    counts = {"pending": 0, "completed": 0, "quarantined": 0}
    for status, n in conn.execute("SELECT status, COUNT(*) FROM reviews GROUP BY status"):
        counts[status] = n
    return counts
