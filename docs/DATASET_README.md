# Assignment 5 — Spotify Insight Pipeline dataset

## Start here

1. Unzip `spotify-insight-dataset.zip`.
2. A convenient starting design is SQL/code for preparation and Jev for fixed labels; choose the tools that fit your pipeline. Build the required calculator in `COST_CALCULATOR.md`; measure a real cold/warm run on `cost_100.csv` before scaling to `checkpoint_500.csv`. Save outputs and an offline replay command so graders need no API key.
3. Hand-label `golden_50_to_label.csv`. These 50 reviews are separate from the development checkpoints. Keep their human answer labels out of prompts; their original texts are still part of the full final run.
4. In Class 7, ingest and profile `spotify_reviews_18months.csv` (660,622 rows; 97,400,616 bytes), then classify all 660,609 nonempty texts and explicitly quarantine the 13 empty texts. Use `analysis_10000.csv` as a development and budget checkpoint first.
5. Read `GRADING_CONTRACT.md` for common labels, bounded tasks, saved outputs, and the required grading export. Run `check_submission.py` to check your evidence with no model calls.
6. Follow the full assignment brief on the course site: `assignment5.html`.

The CSV size is 97.4 decimal MB (92.9 MiB). The ZIP download is smaller. Review text may contain embedded line breaks; physical line counts are not row counts.

## API keys: keep them local

For the suggested setup, you will need **an OpenAI or Claude (Anthropic) API key, plus a Jev (TypeSafe) API key**. You do not need both OpenAI and Claude. Other implementations need only their own provider credentials; local models may need none. Check API billing/credits before the pilot.

In your project root, commit a blank `.env.example`:

```dotenv
OPENAI_API_KEY=
ANTHROPIC_API_KEY=
TYPESAFE_API_KEY=
```

`TYPESAFE_API_KEY` is for Jev. Copy this example to a local `.env`, fill in only the keys you use, and load them as environment variables through your program (for example, with a dotenv loader). Add any other provider's variable name to the example with an empty value.

Before your first commit, add these lines to `.gitignore` and commit the ignore file:

```gitignore
.env
.env.*
!.env.example
```

Before pushing, `git check-ignore .env` should print `.env`; `git ls-files -- .env '.env.*'` should list only the blank `.env.example`, if tracked. Inspect staged changes for secrets. If `.env` was already tracked, use `git rm --cached .env`; ignore rules do not untrack it. Revoke/rotate any key that was committed or shared; deleting the file does not erase Git history.

Never put keys in code, browser code, prompts, notebook outputs, screenshots, logs, or submitted artifacts. Submit `.env.example`, `.gitignore`, and setup instructions, **never `.env` or actual keys**. Offline calculator replay and grading checks must work without credentials.

Setup references: [OpenAI key safety](https://help.openai.com/en/articles/5112595-best-practices-for-api-key-safety), [Claude authentication](https://platform.claude.com/docs/en/manage-claude/authentication), [Jev quick start](https://docs.typesafe.ai/introduction/quickstart), and [GitHub ignore rules](https://docs.github.com/en/get-started/git-basics/ignoring-files).

## Provenance

- Creator: BwandoWando.
- Source: https://www.kaggle.com/datasets/bwandowando/3-4-million-spotify-google-store-reviews
- Version: 2, published November 17, 2023.
- Publisher-listed license: CC0: Public Domain.
- Pinned source download: https://www.kaggle.com/api/v1/datasets/download/bwandowando/3-4-million-spotify-google-store-reviews?datasetVersionNumber=2
- Source archive: 273,598,020 bytes; original CSV: 655,836,189 bytes; 3,377,423 rows.

This course extract includes all source rows dated 2022-05-17 (inclusive) through 2023-11-17 (exclusive). The last observed review is dated 2023-11-15. The original row index, author names, and author IDs were removed. `author_app_version` was renamed to `app_version`; other retained values are unchanged. Review text itself has not been redacted or translated.

## Fields

- `review_id`: original stable review ID.
- `review_text`: original customer text.
- `review_rating`: original star rating, 1–5; not an assigned severity label.
- `review_likes`: helpful-vote count in the source snapshot.
- `app_version`: source app-version string; often missing.
- `review_timestamp`: original timestamp; timezone unspecified.

The full extract has 13 empty review texts, 159,701 missing app versions, and no repeated review IDs. All these rows remain in the full CSV. Samples use unique nonempty reviews with valid ratings. The 100-review cost pilot is the first 100 rows of the 500-review checkpoint, which is a subset of the 10,000-review development checkpoint; the golden 50 is disjoint from all three. Sample order is determined by SHA-256 with the fixed seed recorded in `manifest.json`.

## Limits

This is a historical snapshot of self-selected public app reviews. It contains no account revenue, plan tier, confirmed churn, or complete customer population. Missing versions and unclear or unsupported-language text need explicit handling. A review expressing cancellation intent does not prove that cancellation happened. The final analysis covers the full corpus; report incomplete classifications and their effect on conclusions. Do not include synthetic evaluation cases in business results.

## Reproduce and verify

Download the pinned source ZIP above, then run:

```sh
python prepare_dataset.py /path/to/spotify-source.zip --output data
```

The script uses only Python's standard library. `manifest.json` records source and output SHA-256 checksums, exact byte sizes, counts, filtering, and sampling rules. No model calls are made during preparation.

Full CSV SHA-256:

```text
1fc85de68a304dd8978b537cfa58793d5f41cbaf417fa32cb53899f83a2fcef6
```
