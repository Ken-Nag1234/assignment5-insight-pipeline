# Spotify Insight Pipeline (MBA 290T, Assignment 5)

A staged, auditable pipeline that turns 660,622 public Spotify Google Play reviews
(May 2022 – November 2023) into a product-priority recommendation backed by traceable evidence.

> **Status: work in progress.** Phase 0 (project setup) and Phase 1 (offline ingestion and
> validation) are complete. No model calls have been made yet. Enrichment, verification,
> grouping, ranking, the memo, the cost calculator and the dashboard are not built yet.
> Every number below is measured from the local run, not estimated.

| Item | Location |
|---|---|
| Live dashboard | Not deployed yet |
| Grading export | [`grading/`](grading/) (only `ingestion.json` so far) |
| Ingestion report | [`outputs/ingestion/ingestion_report.json`](outputs/ingestion/ingestion_report.json) |
| Dataset documentation (original, unmodified) | [`docs/DATASET_README.md`](docs/DATASET_README.md) |
| Golden-set labeling guide | [`docs/GOLDEN_LABELING_GUIDE.md`](docs/GOLDEN_LABELING_GUIDE.md) |
| Grading contract / cost calculator spec | [`GRADING_CONTRACT.md`](GRADING_CONTRACT.md), [`COST_CALCULATOR.md`](COST_CALCULATOR.md) |

## Current results (Phase 1, measured)

Input: `spotify_reviews_18months.csv`, 97,400,616 bytes,
SHA-256 `1fc85de68a304dd8978b537cfa58793d5f41cbaf417fa32cb53899f83a2fcef6`.

| Metric | Value |
|---|---|
| Source rows / unique review IDs | 660,622 / 660,622 (0 duplicate IDs) |
| Nonempty reviews queued as `pending` | 660,609 |
| Quarantined (`empty_review_text`) | 13 |
| Completed classifications | 0 (enrichment not started) |
| Distinct nonempty texts (exact match) | 484,189 |
| Rows reusable via exact-text cache | 176,420 |
| Missing `app_version` | 159,701 |
| Invalid ratings / unparseable timestamps | 0 / 0 |
| Review length (characters) | mean 74.8, median 32, p90 207, max 1,693 |
| Ingestion wall-clock time | about 69 s on a laptop (single pass + grading profile pass) |

All 18 ingestion checks and 24 development-sample checks pass. `grading/ingestion.json` is
byte-identical to the output of the provided `check_submission.py profile` command.

**Status semantics.** Every source ID has exactly one status. `pending` means not yet
classified; it is *not* a quarantine and *not* a completed classification. Only empty or
whitespace-only texts are quarantined at ingestion. Missing app versions and star ratings never
affect classification eligibility.

## Setup

Requirements: Python 3.11 or newer. Phase 1 uses only the standard library
(see [`requirements.txt`](requirements.txt)).

1. Download the course dataset ZIP and place these files in the repository root.
   Raw review CSVs are **not** committed to this repository.

   | File | SHA-256 |
   |---|---|
   | `spotify_reviews_18months.csv` | `1fc85de68a304dd8978b537cfa58793d5f41cbaf417fa32cb53899f83a2fcef6` |
   | `analysis_10000.csv` | `eaa62ca6d44d717302904a9c922e99b0b4584d174309cb45295ecbc2ba2a91b5` |
   | `checkpoint_500.csv` | `a94e31663ee7b7eaa77e23b7a8425b530cc0c866ed5e714953172a0afef6a12f` |
   | `cost_100.csv` | `c884ac3b9be5066995d5063f96ad9af6e5e082975788c1684c4f6b6ea661dd0e` |
   | `golden_50_to_label.csv` | `1a125c3e509f58b0246ba16d0ea332675a53ffadb1928be4338a0bd7053a11c7` |

   Original source: BwandoWando, *3.4 Million Spotify Google Store Reviews*, version 2, Kaggle
   (CC0: Public Domain). See [`docs/DATASET_README.md`](docs/DATASET_README.md) and
   [`manifest.json`](manifest.json) for provenance and sampling rules.

2. API keys (needed only for later, paid stages; nothing in Phase 1 uses them):
   copy `.env.example` to `.env` and fill in only the keys you use. `.env` is git-ignored.
   Never put keys in code, prompts, logs or submitted artifacts.

## Commands (all offline, no API key, no model calls)

```sh
# Stage 1: ingest + profile the input CSV into the local SQLite working store (work/, git-ignored)
python3 -m pipeline.ingest --input spotify_reviews_18months.csv --manifest manifest.json

# Verify the fixed development samples and golden set against the ingested full file
python3 -m pipeline.samples --manifest manifest.json

# Golden 50: create the blank human-labeling file (refuses to overwrite), then validate labels
python3 -m pipeline.golden template
python3 -m pipeline.golden validate

# Unit tests (synthetic fixtures only)
python3 -m unittest discover -s tests -t . -v

# Provided zero-API grading profile (should match grading/ingestion.json exactly)
python3 check_submission.py profile --full spotify_reviews_18months.csv --out /tmp/ingestion_check.json
```

`pipeline.ingest` accepts any CSV with the six source columns. Re-running it on the same input
is idempotent and keeps saved statuses; pointing an existing database at a different input is refused.

## Repository layout

```text
pipeline/            Pipeline code
  common.py          Shared helpers; row hashing and profiling delegate to check_submission.py
  db.py              SQLite schema and status model (pending / completed / quarantined)
  ingest.py          Stage 1: ingest, profile, exact-text dedup, pending queue, quarantine log
  samples.py         Development-sample and golden-set verification
  golden.py          Golden-50 blank template and label validator (never fills in labels)
tests/               Offline unit tests with synthetic fixtures
outputs/ingestion/   ingestion_report.json, data_manifest.json, quarantine.jsonl, samples_report.json
grading/             Standardized grading export (GRADING_CONTRACT.md)
docs/                Original dataset README and the golden labeling guide
golden/              Golden-50 working file (git-ignored until labeling is complete)
work/                Local SQLite working store (git-ignored)
```

Instructor-provided files (`GRADING_CONTRACT.md`, `COST_CALCULATOR.md`, `manifest.json`,
`check_submission.py`, `prepare_dataset.py`) are kept unmodified.

## Planned stages

| Stage | Owner | Status |
|---|---|---|
| 1. Prepare: ingest, profile, dedup, queue | Code | Done |
| 2. Classify: topic, intent, severity, sentiment, quote, entities (at most 50 reviews per request) | Model + code validation | Not started |
| 3. Verify: blind re-labeling of a declared random sample; planted-error and injection tests | Separate model task + code | Not started |
| 4. Group: stable issue membership for complaint/cancellation records | Code (+ model for issue names) | Not started |
| 5. Rank: `priority = severity_sum`, deterministic tie-break | Code | Not started |
| 6. Recommend: memo from saved aggregates and a bounded evidence pack | Model + code claim checks | Not started |
| Cost/runtime calculator with real 100-review cold/warm pilot | Code | Not started |
| Database, backend and deployed dashboard | Code | Not started |

Coverage target: at least 100,000 completed classifications, with the full 660,609 nonempty
reviews as the stretch goal, within a $10 total budget for external APIs and hosting.
Any review not classified will remain visibly `pending` (or quarantined with a genuine reason)
and will be disclosed as incomplete.

## Rubric evidence map

To be completed as each stage produces evidence.

## Limitations (data)

Self-selected, historical public app reviews. No revenue, plan tier, confirmed churn or complete
customer population. Cancellation language is expressed intent, not observed churn. Timestamps
have no specified timezone; the first and last calendar months are partial.
