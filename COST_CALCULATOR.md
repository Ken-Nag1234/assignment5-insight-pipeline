# Required deliverable: a 100-review cost and runtime calculator

Build this calculator as part of your pipeline. A command, notebook, or small local interface is enough. The instructor must be able to recompute its results from saved measurements **without an API key or new model calls**. This file specifies the work; it is not a finished calculator.

Before any paid pilot, follow the API-key setup in `README.md`: the suggested setup uses an OpenAI or Claude key plus a Jev/TypeSafe key. Keep real credentials in a local `.env` excluded by `.gitignore`; submit only a blank `.env.example`. Load keys from environment variables and never record them in usage logs. Offline replay must not require keys or make API calls.

## 1. A suggested inexpensive route: SQL/code, then Jev

The stages are ordered to make inexpensive tools practical: preparation exposes missing values and duplicate texts before classification, fixed labels suit a classifier, and aggregation reduces what the memo model needs to read. **SQL and Jev are recommendations, not required technologies.** Choose your implementation and explain its measured quality, cost and runtime.

SQL or ordinary code can parse the CSV, preserve IDs, check missing values, find exact duplicate texts, look up valid cached results, and create the pending-work queue. SQL/code also handles date/rating filters, counts, grouping, sorting, validation, and priority arithmetic. These operations need no language model. Filters may organize work, but must not remove required reviews from the final run.

**Jev is one suitable option for fixed topic, intent, and severity choices** under the shared rubric. Combine questions about one review when supported. Map results into the common schema and validate in code. Other classifiers or models are equally acceptable; evaluate your choice and explain the tradeoffs. Stars stay metadata: they must not substitute for text-based intent or severity.

Use deterministic extraction where it works. For example, a short review's complete original text can be its evidence quote; explicitly matched feature terms can form an entity list. Evaluate whether this actually supports the label. Use a small model for extraction or ambiguity that needs language judgment. Route only a declared, capped fraction of difficult cases to a stronger model. Never automatically switch every failed request to an expensive model.

After enrichment, SQL/code can filter complaint/cancellation records, save issue membership, and compute the baseline ranking. A small model can name issues from a bounded evidence pack and write the memo from saved aggregates. The full raw CSV never needs to become one large-model prompt.

## 2. Measure a real run on the supplied 100 reviews

Use `cost_100.csv` unchanged. It is the first 100 rows of `checkpoint_500.csv`, nested inside the 10,000-review development set and separate from the golden 50. Verify its checksum in `manifest.json`.

Run the actual configured pipeline on these 100 reviews with an empty **result cache for this experiment**, starting with one worker. Include enrichment, a declared verification sample, grouping, ranking, and memo generation. Save actual outputs and all attempted calls, including failures, retries, and fallbacks. Report completed and failed records separately; fix failures before scaling. Do not present invented usage or timing as a measured pilot. Save any provider prompt-cache savings that actually occurred; they are separate from your result cache.

Repeat the same run with the saved result cache. Demonstrate **zero new enrichment calls** under unchanged settings and show the warm-run time and total incremental cost. Cache downstream artifacts by their inputs and configuration too, or disclose any downstream calls that still occurred. Budget the pilot and any additional experiments before making calls.

## 3. What the calculator must display

| Measured on the 100-review pilot | Estimated before the full run |
|---|---|
| Input checksum, 100 IDs, completed/failed counts, unique texts, result-cache hits | All 660,622 rows accounted for; 660,609 nonempty outputs; 13 empty-text quarantines |
| Per-stage provider, exact model ID, effort setting, prompt/schema version, batch size and worker limit | 484,189 distinct nonempty texts if valid exact-text reuse is enabled; also show the no-reuse comparison |
| Requests and attempts, including failed calls, retries, fallbacks and verification | Stage-specific verification/fallback rates, retry assumptions, fixed overhead and variable work |
| Actual usage, rate units, currency, dated price-source links, and calculated cost by stage | Total cost, elapsed time and throughput assumptions; a base case and a conservative case with higher retries/fallbacks |
| End-to-end wall-clock seconds, stage times, throughput, cost per 1,000 input rows and per completed record | Editable budget, output-token cap, maximum concurrency and a declared maximum fallback fraction |
| Separate cold-run and warm-run results | A clear warning when a scenario exceeds the declared budget |

Show API spend separately from infrastructure/local-compute estimates. Local inference can have zero API charges while still using hardware, time, memory and electricity. Mark unmeasured costs as unknown; do not silently turn them into zero. Subscription access is not evidence that programmatic calls are free.

Use editable rates in their actual billing units: tokens, requests, classifications, compute time, or another documented unit. For each mutually exclusive billing item:

```text
item_cost = billed_units × price_per_unit
total_cost = sum(item_cost)
```

For a price quoted per million tokens, divide that price by 1,000,000 first. If total input already includes cached input, subtract cached input before applying the uncached rate. Do not add reasoning tokens again if the provider already includes them in billed output. Use the processing tier actually used. A hypothetical batch discount belongs in an estimated scenario, not the measured bill. Record raw provider usage and any reconciliation with the account's usage dashboard. Missing usage must remain visibly unresolved or a labeled estimate.

Measure end-to-end duration using a clock. Summed request durations can exceed wall-clock time when calls overlap. Extrapolate each stage using its own work count and observed throughput; add one-time overhead once. Do not multiply the cost of one memo by 6,606.22, or assume every review needs verification/fallback. The first 100 reviews provide an initial estimate, not a guarantee about rare failures or sustained throughput. Refresh the estimate after 500 and 10,000 reviews, then explicitly start the full run within your chosen budget.

## 4. Scaling options to consider

- **Batching:** distinguish reading CSV chunks, putting multiple reviews in a request, and a provider's asynchronous Batch API. Enrichment requests have at most 50 reviews and must fit token/output limits. Validate every returned ID. Start with small requests; a single bad response must not silently lose the whole batch. Provider Batch discounts depend on eligibility and may trade speed for price.
- **Result caching:** key by exact text plus all relevant model, effort, prompt and schema settings. Keep every source ID and direct cache provenance. Cache reuse must pass the grading contract. Similar text is not an exact match. Provider prompt caching is a different mechanism; apply savings only when measured or clearly assumed.
- **Saved state:** persist validated results and status after each completed batch. Use atomic writes/upserts so two workers cannot overwrite or double-count a result. On restart, load completed work and dispatch pending work. Reconcile uncertain in-flight requests where possible; retrying an unknown outcome may create another charge. Demonstrate interruption and resume.
- **Cheap model settings:** Jev is worth considering for fixed choices. For individual language tasks, candidates include GPT-6 Luna, Google's Gemini Flash-Lite, Claude Haiku, and suitable DeepSeek/Qwen or local models. Select low/minimal/no thinking only where supported, cap output length, and request concise structured results. Sonnet 5 can be a bounded fallback or memo option if its measured benefit justifies the cost. You do not need to pay to test every provider.
- **Careful parallelism:** begin at one worker, then try two and only increase further if measurements and limits allow. Share a global requests/tokens limiter and a single spend ledger across workers. Use bounded retries with backoff and jitter. Reserve estimated worst-case spend for queued/in-flight calls before dispatch; stop admitting work when spent plus reserved plus the next reservation exceeds the cap. Timeouts can have unknown charges: flag and reconcile them. Use provider spending controls when available. Your local estimate is not a guarantee about delayed billing.
- **Compare honestly:** a worker-count comparison must use the same reviews and cache conditions. Additional cold runs cost money; label modeled speedups if you do not run them. Parallelism may shorten elapsed time; it does not inherently reduce the cost per classification. Rate limits, retries, local GPU memory and contention can make more workers slower or more expensive. Record the chosen concurrency and justify it.

Current provider references (checked September 29, 2026): [Jev Choice](https://docs.typesafe.ai/primitives/choice), [GPT-6 Luna](https://developers.openai.com/api/docs/models/gpt-6-luna), [Gemini thinking](https://ai.google.dev/gemini-api/docs/thinking), [Claude model overview](https://platform.claude.com/docs/en/models/overview), and [Ollama thinking](https://docs.ollama.com/capabilities/thinking). GPT-6 Luna supports `reasoning.effort: "none"` or `"low"`; do not assume its default is the cheapest. Flash-Lite is a Gemini model family. Thinking controls differ by model/version; record the exact supported setting you used and recheck current prices.

## 5. Submit and make it inexpensive to grade

Include a `cost/` folder in your repository containing:

- The runnable calculator and a README with separate **offline replay** and **explicit pilot execution** commands. Offline replay is the default; importing/opening the calculator must not start paid calls.
- `pilot_records.jsonl`: one result/status per pilot ID, with the grading contract's original row hash and common labels. Keep this pilot evidence separate from the full `grading/records.jsonl` export.
- `pilot_calls.jsonl`: real per-attempt stage/model/settings/usage/timing evidence for cold and warm runs, with run IDs and request IDs. Do not include API keys. Use the contract's role names; any extra provider billing fields may be added.
- `rates.csv` and `usage.csv`, or equivalent readable structured files, with units, dated source links, and a clear mapping from every charged item to the call log. Include local-compute assumptions separately.
- `report.md` or `report.html`: the measured-100 dashboard/table and full-run estimates above, plus your chosen budget, concurrency, fallback limit, and scaling decision. Link the source measurements and formulas.

**T3 grading evidence:** (a) a real 100-review cold/warm pilot with all costs/failures accounted for; (b) an offline calculator whose arithmetic and editable assumptions work; (c) demonstrated retry, spending and recovery controls. T3 earns 1 when all three are demonstrated, 0.5 for materially incomplete evidence, and 0 for absent or contradictory evidence. The overall 4/3/3 rubric is unchanged. There is no reward for spending more or using more providers.

The instructor checks the saved 100 IDs against `cost_100.csv`, recomputes cost from the usage/rate files, and runs offline replay. With usage fixed, doubling all API rates must double the API subtotal while leaving local-compute costs and measured time unchanged. Changing projected record count must not change the recorded 100-review results. The main submission checker audits full-corpus coverage and ranking; it does not certify provider bills or the calculator. Instructors inspect the calculator separately without paid calls and may ask for provider receipts to resolve discrepancies.
