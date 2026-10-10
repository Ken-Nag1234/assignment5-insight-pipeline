# Final assignment: machine-checkable evidence, v1

## Scope and what constitutes a pipeline

The final run accounts for **all 660,622 original review IDs** in `spotify_reviews_18months.csv`.
Classify every nonempty review: **660,609 rows**. Record the 13 empty texts as `quarantined` with reason `empty_review_text`.
Other failures must be retained with reasons and reduce the completed fraction. Accounting for a failure does not make it a successful classification.

`cost_100.csv`, `checkpoint_500.csv` and `analysis_10000.csv` are development checkpoints. The required cost calculator starts with a real 100-review pilot; see `COST_CALCULATOR.md`.
The 50 golden examples are excluded from development prompts and examples. Their original review texts are included in the final full-corpus run; their human answer labels are never model inputs.

Your saved program must accept an input CSV path and run the stages without manual chat pasting: ingest, enrich, verify, group, rank, and recommend. Codex or another assistant may help write and run the program.
Use distinct model roles with saved inputs/outputs. One provider may serve several roles. A chat transcript or memo alone does not demonstrate the pipeline.

For the enrichment stage, send **at most 50 reviews per model request**, validate every returned ID, and save results after each batch. Smaller requests are acceptable.
Demonstrate interruption after saved progress, then resume with additional completed records and no new enrichment calls for already completed IDs under the same configuration.
Supply a short recording and the two checkpoint snapshots. The instructor may ask for a small fresh-input demonstration after submission.

## Common labels for fair comparison

Use these exact primary labels in the grading export. You may add custom subtopics and richer internal labels in other columns/files.
Classify the review text itself. Keep stars and other metadata for analysis; do not use them to substitute for interpreting the text.

| `topic` | Definition |
|---|---|
| `access` | Login, signup, password or account access |
| `usability` | Navigation, controls, layout, queue/playlist management, ad interruptions |
| `playback` | Playback failure, crashes, lag, connection failures, audio quality, resource use |
| `downloads` | Downloading, saved music, offline listening, disappearing downloads |
| `catalog` | Missing songs/artists, search/discovery, recommendations, lyrics availability |
| `billing` | Price, charges, subscriptions, paywalls, premium entitlement; explicitly premium-only controls go here |
| `support` | Contacting support and the support response |
| `other` | General praise/criticism, unrelated content, or no supported specific topic |

Choose the problem with the highest supported severity. On a tie, choose the first specific problem mentioned. For a positive review, choose the first specific praised feature; general praise is `other`.
Mentioning a paid plan alone does not make the topic `billing`. A subscription failing to activate does; music crashing for a paying customer is `playback`.

Intent precedence: `cancellation` (explicitly leaving, uninstalling, cancelling, or threatening to do so) → `complaint` (negative experience, including mixed praise/criticism) → `request` (desired change without a reported failure) → `praise` → `unclear`.
Bare boycott slogans and unrelated/meaningless text are `unclear`, unless there is a product complaint or explicit personal departure. General “bad app” is a complaint; do not infer a specific defect.

| `severity` | Shared meaning |
|---|---|
| 1 | No reported problem: praise, neutral/unclear content, or a pure feature request |
| 2 | Dislike, generic criticism, minor annoyance, or a cosmetic issue; no supported functional loss |
| 3 | A degraded or restricted function; some use or workaround remains |
| 4 | A clearly blocked core task, such as inability to log in or play music |
| 5 | Explicit serious financial, privacy, or data harm; an expensive plan, a crash, or angry language alone is insufficient |

Cancellation intent does not automatically raise severity. Missing context should trigger `needs_review`; do not invent impact. Write examples of how you applied these shared definitions.

## Export one grading folder

Keep your normal project organization. Add this compact adapter/export folder and link it from the README. Large `.jsonl` files may be gzip-compressed (`.jsonl.gz`). Submit the folder as a downloadable ZIP/release asset if needed.

| File | Required contents |
|---|---|
| `run.json` | Version, input identity and declared scope |
| `ingestion.json` | Full-file deterministic profile, produced by the provided helper |
| `records.jsonl` | Exactly one final record per source ID, completed or quarantined |
| `membership.csv` | `issue_id,review_id`, one pair per membership |
| `ranking.csv` | `rank,issue_id,complaint_count,severity_sum,mean_severity,priority_score` |
| `claims.csv` | `claim_id,issue_id,metric,value`, for each material issue-level number in the memo |
| `calls.jsonl` | Model-call log with bounded inputs, roles, usage and run phase |
| `checkpoint_before.json` | `completed_ids` saved before interruption |
| `checkpoint_after.json` | `completed_ids` after resuming and completing additional work |

`run.json`:

```json
{"version":"a5-audit-v1","analysis_count":660622,"analysis_sha256":"1fc85de68a304dd8978b537cfa58793d5f41cbaf417fa32cb53899f83a2fcef6","classification_input_fields":["review_text"],"allow_multi_issue":false}
```

A completed `records.jsonl` entry contains:

```json
{"review_id":"ORIGINAL-ID","source_sha256":"HASH-OF-EXACT-SOURCE-ROW","status":"completed","topic":"playback","intent":"complaint","sentiment":-0.6,"severity":4,"entities":[],"evidence_quote":"EXACT SOURCE SUBSTRING","needs_review":false,"label_config":"model+prompt+schema-version"}
```

A quarantined entry contains `review_id`, `source_sha256`, `status:"quarantined"`, and a nonempty `reason`. Keep retry details in the run logs.
The source row hash is SHA-256 of UTF-8 compact JSON of the six original field strings in this order: `review_id, review_text, review_rating, review_likes, app_version, review_timestamp`. Use the provided `row_sha` helper to avoid serialization differences. Do not normalize whitespace or accents.

Exact-duplicate text may reuse one saved, validated model result. Add `cache_source_id` pointing directly to a completed original record with identical text, identical classification fields, and identical `label_config`. Keep every original ID as a separate output row and count it separately in aggregates. The source record must have model-call evidence. Similar text, different context, or changed model/prompt/schema settings require new work. Reference only direct originals, not chains of cache aliases.

Each `calls.jsonl` entry contains:

```json
{"request_id":"provider-request-or-local-call-id","role":"enrich","review_ids":["ORIGINAL-ID"],"model":"EXACT-MODEL-ID","phase":"initial","outcome":"succeeded","label_config":"model+prompt+schema-version","input_tokens":200,"output_tokens":120}
```

Use `role` values `enrich`, `verify`, `group`, `memo`; enrichment `phase` is `initial` or `resume`. IDs are the reviews actually sent. Group/memo may have an empty `review_ids` list when consuming saved aggregate artifacts; identify those artifacts in your full run log. Do not report cached reuse as a new API call. Log failed attempts with `outcome:"failed"`; successful calls use `outcome:"succeeded"`. Enrichment calls must name the same `label_config` as their completed records. Usage/cost claims must be real or clearly marked unavailable in the README; the checker reports missing usage rather than inventing it.

## Reproducible baseline ranking

The required baseline uses completed `complaint` and `cancellation` records. Each must belong to at least one issue. No duplicate `(issue_id, review_id)` pairs; praise/requests/unclear records are excluded.
Normally use one issue per complaint. If you justify multiple issues, set `allow_multi_issue:true` and explain that totals across issues overlap.

`complaint_count` is the membership count; `severity_sum` is the sum of member severity; `mean_severity = severity_sum / complaint_count`; `priority_score = severity_sum`.
This is exactly count × mean severity, calculated without floating-point rounding loss. Order by descending score, then ascending issue ID for ties. Number ranks from 1. Export means to six decimal places using decimal half-up rounding. All other numbers are integer strings.

You may add another business ranking, but also submit this baseline so we can compare calculations consistently. Cite `claims.csv` claim IDs in the memo. Supported automated metrics are `complaint_count`, `severity_sum`, `mean_severity`, `priority_score`; explain other quantities separately. Human review checks whether the prose represents all numerical claims and supports the recommendation.

## Run the zero-API self-check

With `check_submission.py`, your input CSV and your grading folder in the same working directory:

```sh
python3 check_submission.py profile --full spotify_reviews_18months.csv --out grading/ingestion.json
python3 check_submission.py reference --full spotify_reviews_18months.csv --analysis spotify_reviews_18months.csv --out local-reference.json
python3 check_submission.py check --reference local-reference.json --submission grading --out self-check.json
```

The reference reads the CSV and creates no classifications. Keep the report outside the submitted grading folder. `review_required` identifies missing or inconsistent evidence; it does not itself set a grade. A passing mechanical check does not prove that the labels are correct.

The instructor compares your existing classifications against a private, instructor-reviewed sample under the same definitions. No paid model rerun is needed for this comparison. Accuracy, macro F1 and severity error are diagnostics, not automatic substitutions for the 4/3/3 rubric. Ambiguous benchmark cases can have multiple accepted labels. Missing/quarantined benchmark predictions remain in the agreement denominator.

## Spend and time

**Build the required 100-review cost/runtime calculator described in [COST_CALCULATOR.md](COST_CALCULATOR.md).** Submit its `cost/` folder alongside `grading/`, including real cold/warm pilot outputs, usage, dated editable rates, measured time, full-run estimates, and an offline replay command. Instructors check this small artifact separately; the main checker does not execute student code or certify costs.

A suggested route is SQL/code for preparation, cache lookup, counts and sorting, then Jev for fixed classifications. This order makes inexpensive processing practical before later language tasks. SQL, Jev and particular model families are optional; explain your chosen tools using measured quality, cost and runtime. Consider small models for bounded language tasks and capped stronger-model fallbacks. Measure 100, then 500, then 10,000 reviews before committing to the full run. Include retries and verification. Apply batching, exact-text caching, saved-state recovery, supported low/no-thinking settings, and cautious concurrency with shared rate/spend limits. Preserve all source IDs; SQL filters and star ratings must not bypass required semantic classification.

As an illustration only, 660,609 reviews averaging 100 billed input and 150 billed output tokens would cost about **$56.15** on GPT-6 Luna at $0.10/$0.50 per million tokens, before retries and other stages. Reusing exact texts would reduce this example to about **$41.16** for 484,189 distinct nonempty texts. These token counts are assumptions, not measured run costs. Batch pricing can reduce eligible API costs further. Measure your own prompts and output lengths.
At Sonnet 5's $2/$10 standard rates the same uncached token assumptions would be about **$1,123.04**, so reserve it for measured high-value tasks rather than every review by default.

Official rates checked for this revision: [GPT-6 Luna](https://developers.openai.com/api/docs/models/gpt-6-luna), [Claude Sonnet 5](https://www.anthropic.com/news/claude-sonnet-5). Current account limits and actual usage govern your run.
