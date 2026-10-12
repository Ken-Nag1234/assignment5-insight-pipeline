# Dashboard and backend

A read-only web dashboard over the processed results. The backend (`app.py`, Python standard library
WSGI) reads the serving tables from a database and returns JSON. The page (`static/index.html`, plain
JavaScript, no build step) shows only what the backend returns. Nothing in the dashboard calls a model:
the AI-generated recommendations were written once by the memo stage and stored in the database.

```
pipeline outputs (ranking.csv, membership.csv, memo, recommendations.json, weighted_comparison.json)
        │  python3 -m pipeline.serving   (read-only on the pipeline database)
        ▼
serving database (SQLite locally, Postgres when deployed) ──► app.py /api/* ──► static/index.html
```

## Run locally (no account, no packages)

```bash
python3 -m pipeline.serving --db work/pipeline.sqlite --sample analysis_10000 \
    --analysis outputs/analysis_preview_10000 --out work/serving_preview.sqlite
SERVING_DB=work/serving_preview.sqlite python3 dashboard/app.py --port 8000
# open http://127.0.0.1:8000/
```

## API

| Endpoint | Returns |
|---|---|
| `GET /api/health` | database kind and row counts |
| `GET /api/overview` | provenance (`meta`), overall metrics with denominators, label distributions |
| `GET /api/issues?limit=N` | baseline ranking with issue names |
| `GET /api/issues/<issue_id>?limit=&offset=` | one issue and its member reviews (most severe first) |
| `GET /api/recommendations` | AI recommendations with resolved claim values and cited reviews |
| `GET /api/reviews/<review_id>` | one processed review and its issue |
| `GET /api/weighted` | stratum-weighted sensitivity ranking |

Every value comes from a stored table; `tests/test_dashboard.py` checks that the served ranking equals
the saved `ranking.csv` and that the API cannot write.

## Data to store and size

| Table | Contents | Needed for |
|---|---|---|
| `meta`, `metrics`, `breakdown`, `trends` | provenance, overall counts with denominators, label distributions, monthly complaint share | overall metrics |
| `issues`, `weighted_issues` | baseline ranking with names; stratum-weighted sensitivity ranking | issue ranking |
| `memberships`, `reviews` | issue membership and processed records (labels, exact evidence quote, text) | drill-down evidence |
| `claims`, `recommendations` | memo claim values; AI recommendations with cited claim and review IDs | grounded recommendations |

Minimum for grading: every table above with `reviews` limited to issue members and cited reviews.
`--all-records` also stores every completed record (all processed review data); recommended if it fits.
`python3 -m pipeline.serving ... --estimate` prints the size before anything is loaded. Measured on the
10,000-review preview: 2.7 MB (members only) and 4.3 MB (all records), so a run with about 285,000
completed reviews needs roughly 80-130 MB including indexes, well inside the Neon free plan
(1 GB per project, checked 2026-10-11 at https://neon.com/pricing).

## Deployment (Vercel + Neon Postgres)

Status 2026-10-12: all steps done. **Live dashboard: <https://assignment5-insight-pipeline.vercel.app>** (no login, no API key).
The final serving tables are loaded in a dedicated Neon project (PostgreSQL 18, 117 MB; every table
verified against the local serving database and `grading/`). The Vercel deployment (Root Directory
`dashboard`) was checked signed out: the page and every `/api/*` endpoint return 200, `/api/health`
reports `postgres` with 34 issues, 284,709 reviews, 92,035 memberships and 4 recommendations, the
34-row ranking equals `outputs/analysis/ranking.csv`, and every recommendation's claim values equal
`grading/claims.csv` with all cited reviews resolved.

1. Create a Neon Postgres project (free plan) and copy two connection strings: the owner role
   (for loading) and, after step 3, a read-only role (for the app).
2. Load the serving tables from the final analysis, in one verified transaction:
   `python3 -m pipeline.serving --db work/pipeline.sqlite --analysis outputs/analysis --claims grading/claims.csv --all-records --postgres`
   (`DATABASE_URL` is read from the environment or the git-ignored `.env`; it is never printed.)
   The load drops and recreates only the serving tables, copies the rows, re-counts every table and
   re-reads the ranking, and commits only if everything matches (otherwise the previous tables stay).
   Loading needs the `psycopg` package on this machine (one package in a virtual environment).
3. Create a read-only login role for the app (done 2026-10-12 from the owner connection, password
   generated locally and stored only in the git-ignored `.env` as `DATABASE_URL_READONLY`):
   ```sql
   CREATE ROLE dashboard_reader LOGIN PASSWORD '<generated>' NOINHERIT CONNECTION LIMIT 20;
   ALTER ROLE dashboard_reader SET default_transaction_read_only = on;
   ALTER ROLE dashboard_reader SET statement_timeout = '10s';
   GRANT CONNECT ON DATABASE neondb TO dashboard_reader;
   GRANT USAGE ON SCHEMA public TO dashboard_reader;
   GRANT SELECT ON meta, metrics, breakdown, issues, memberships, reviews, claims, recommendations,
                   weighted_issues, trends TO dashboard_reader;
   ```
   Verified with rollback-only probes: it reads every serving table, and INSERT, UPDATE, DELETE,
   TRUNCATE, CREATE/ALTER/DROP TABLE, CREATE SCHEMA and CREATE ROLE are refused (also after an
   explicit `SET TRANSACTION READ WRITE`). Reloads re-grant SELECT to this role in the same
   transaction (`pipeline.serving.write_postgres`), so no manual GRANT is needed afterwards.
4. Import the GitHub repository in Vercel with **Root Directory = `dashboard`** and set the environment
   variable `DATABASE_URL` to the read-only connection string (`DATABASE_URL_READONLY` in `.env`). Vercel installs `requirements.txt`
   (`psycopg[binary]`) and serves `app.py` (the WSGI `app`) for both the page and `/api/*`.
5. Check signed out: the page loads, `/api/health` reports `postgres`, and the ranking matches
   `outputs/analysis/ranking.csv`.

The dashboard needs no API key, and neither does a grader. Neon suspends idle compute after five
minutes; the first request after a pause can take a moment longer.
