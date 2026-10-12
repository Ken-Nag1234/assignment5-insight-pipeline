# Submission checklist and grading-contract audit

Status as of 2026-10-12 (classification run finished at the $4.00 cap: 284,709 completed, 375,900 pending). "Offline" items can be finished
without paid calls; "approval" items need an explicit decision first. Tick each item only after the
check has actually been run on the final outputs.

## 1. Grading-contract audit

| Requirement (source) | Implementation | Status |
|---|---|---|
| Account for all 660,622 source IDs; quarantine the 13 empty texts with `empty_review_text` (contract, scope) | `pipeline/ingest.py`; `outputs/ingestion/` | Done |
| Classify every nonempty review (660,609) | `pipeline/enrich.py`; cumulative classification cap $4.00 approved | **Partial by budget**: all 192,342 duplicated-text reviews plus 92,367 seeded singletons; pending reviews are reported, never relabeled; shares stratum-weighted |
| At most 50 reviews per enrichment request; validate returned IDs; save after each batch | 1 review per request; ID validation; one transaction per response | Done |
| Interruption and resume with no re-sent completed IDs; recording + two snapshots | `outputs/checkpoints/`, `evals/system/resume_evidence.json` (checker rules pass) | Done; **recording link: approval** (where to host the video) |
| Program accepts an input CSV and runs ingest → enrich → verify → group → rank → recommend | `pipeline/run.py` (mock-tested end to end) | Done; real run is the pilot |
| Distinct model roles with saved inputs/outputs | enrich (Jev), verify (Jev, separate instructions), group naming and memo (GPT-6 Luna) | Done: verify 500; naming + memo with `openai/gpt-6-luna` (2026-10-12, $0.0037) |
| Common labels, intent precedence, severity scale, `needs_review`; written examples of applying them | `pipeline/labels.py`, `docs/GOLDEN_LABELING_GUIDE.md` | Done |
| Golden 50 excluded from prompts/examples; human labels never model inputs | Checked: no golden text or ID appears in prompts, docs, tests or outputs | Done |
| `grading/` export: run.json, ingestion.json, records.jsonl(.gz), membership.csv, ranking.csv, claims.csv, calls.jsonl(.gz), both checkpoints | `pipeline/finalize.py export` (stages into `work/final/<stamp>/grading`, then `promote`) | Done 2026-10-12; checker issues only `missing_records`/`unfinished_classification` (coverage) |
| calls.jsonl roles enrich/verify/group/memo; phases initial/resume; usage integers; failed attempts logged | `pipeline/export.py` (+ verify and text-model call logs; rejects mock, foreign IDs and other roles) | Done |
| Exact-text cache reuse with direct `cache_source_id` | enrichment cache | Done |
| Baseline ranking: severity_sum, half-up six-decimal means, tie-break by issue ID; regenerable without a model | `pipeline/rank.py`; checker recomputes identical ranking in tests | Done |
| Memo cites `claims.csv` claim IDs; numbers trace to saved calculations | `pipeline/insights.py` (code inserts numbers; claims.csv from the memo's claims) | Done: `outputs/analysis/memo.md`, `grading/claims.csv` C001–C008 |
| Self-check report kept outside `grading/` | `work/final/<stamp>/check_report.json` | Done |
| Stable run ID and a manifest tying checksum, code version, prompts, models, settings, outputs | `run_manifest.json` written by `finalize export` | Done (offline-tested) |
| Cost calculator: real cold/warm 100-review pilot, dated rates, offline replay, scenarios | `cost/`, `pipeline/pilot.py` | Done: measured pilot 2026-10-12 (cold 76.1 s, warm 0 new calls, $0.0059) |
| Independent verification, planted-error and injection tests | `pipeline/verify.py`, `evals/injection_cases.csv` | Done: agreement topic 95.2% / intent 96.4% / severity 91.8%; planted 10/10; injection 8/8 schema-valid, 2/8 topic steered |
| Deployed dashboard + backend + database; overall metrics, ranking, AI recommendations with evidence | `dashboard/`, `pipeline/serving.py` (SQLite locally, Postgres when deployed) | Built and tested locally; **deployment: approval** (accounts, package install, push) |
| README: results summary, architecture with DB/backend/dashboard, one real review traced, a failed/ambiguous case, golden comparison, trend with denominators, memo, limitations | README sections; `pipeline/trace.py`, `pipeline/trends.py` | Done except dashboard URL and recording link |
| Complete golden comparison "expected labels, model predictions, error analysis" (README evidence list) | Aggregates and disagreement patterns published; per-case file only with `--publish-golden` | **Approval**: publishing per-review human labels is currently not allowed |
| `.env` absent from tracked files and history; blank `.env.example` | Audit 2026-10-11: no key in tracked/untracked publishable files or git history | Done; re-run before push |
| Raw dataset not committed; source link and checksums in README | `.gitignore`, README Setup | Done |

## 2. Final-run sequence (after the enrichment run stops)

1. `python3 -m pipeline.status` — confirm the run stopped with `budget_cap` (exit 4) and no process holds the database.
2. Done 2026-10-12 — approved paid steps, each with its own incremental cap: text-model smoke test (≤ $0.001), verifier 500 + injection 8 (≤ $0.05 combined; injection with `--out evals/injection`), 100-review cold/warm pilot (≤ $0.05).
3. `python3 -m pipeline.finalize analysis` → note the stamp.
4. Issue naming and memo with GPT-6 Luna on `work/final/<stamp>/analysis` (needs a cap decision; estimated < $0.01).
5. `python3 -m pipeline.finalize export --stamp <stamp>` → read `check_report.json`.
6. `python3 -m pipeline.finalize serving --stamp <stamp>` and inspect the dashboard locally.
7. `python3 -m pipeline.finalize promote --stamp <stamp>`; `python3 -m pipeline.evals_export`.
8. `python3 -m pipeline.trace --analysis outputs/analysis --pick` and `--pick-failed` → paste into the README.
9. Update README numbers from saved files only; mark estimates as estimates.

## 3. Validation checklist

### grading/
- [x] `run.json` has `analysis_count` 660622 and the source SHA-256 `1fc85de6…2fcef6`.
- [x] `records.jsonl.gz`: one line per completed or quarantined ID; pending IDs are absent and counted in the README.
- [x] `calls.jsonl.gz` contains roles enrich, verify, group, memo; no `mock/` models, no `smoke` or `injection_test` roles.
- [x] `membership.csv`, `ranking.csv`, `claims.csv` present; `checkpoint_before.json` ⊂ `checkpoint_after.json`.
- [x] Provided checker: `check_report.json` lists no issues other than those explained in the README (e.g. coverage).
- [x] `python3 -m pipeline.rank --records grading/records.jsonl.gz --membership grading/membership.csv --out /tmp/rank` reproduces `ranking.csv` byte for byte.

### evals/
- [x] `golden/metrics.json`, `golden/disagreement_patterns.json` (aggregates only unless publication is approved).
- [x] `verification/report.json`, `report.md`, `disagreements.csv`, `planted_report.json`, `sample.json`.
- [x] `injection/injection_report.json` (synthetic cases) and `system/resume_evidence.json` with `checks_pass: true`.

### cost/
- [x] `pilot_calls.jsonl`, `pilot_runs.json`, `pilot_records.jsonl` from the real pilot; `rates.csv` dated rows for every model used.
- [x] `python3 cost/calculator.py replay` works without a key; warm run shows zero new enrichment calls.

### Run logs and checkpoints
- [x] `outputs/runs/` contains every enrichment run (`run_summary.json`, `progress.jsonl`, `run_log.jsonl`) and supervisor logs.
- [x] `outputs/checkpoints/` unchanged since the recording (compare with `work/backups/`).
- [x] `outputs/analysis/run_manifest.json` present; hashes match the promoted files.

### README
- [ ] Measured vs. estimated separated; completed vs. accounted-for rows separated.
- [x] Coverage: completed / pending / quarantined counts and the stratum-weighted sensitivity result.
- [x] One traced review and one failed/ambiguous case; trend with denominators.
- [ ] Live dashboard URL and access instructions (no login); recording link; evidence links open signed out.

### Dashboard
- [ ] URL loads signed out; `/api/health` reports `postgres` and nonzero rows.
- [ ] Ranking on the page equals `outputs/analysis/ranking.csv`; recommendations show claim values and cited reviews.
- [ ] No API key in the deployment; the database role used by the app can only read (role `dashboard_reader` created and verified 2026-10-12; check that Vercel uses it).

### Publishing safety (re-run immediately before any push)
- [ ] `git status` / `git ls-files -co --exclude-standard` reviewed; no `.env`, databases, raw CSVs, `golden/` files, `work/`.
- [ ] Secret scan of publishable files and `git log -p` for the gateway key prefix and connection strings.
- [ ] No file > 50 MB (GitHub warns at 50 MB and rejects files over 100 MB); large exports gzip-compressed.
- [ ] No golden review IDs with human labels in any publishable file (unless publication is approved).
