# Cost and runtime calculator

Implements the required 100-review calculator described in [`COST_CALCULATOR.md`](../COST_CALCULATOR.md).

> **Status:** the calculator and its offline replay work, but **no paid pilot has been measured yet**.
> `report.md` therefore shows empty measured tables and projections based on the clearly labeled
> pre-pilot assumptions in `assumptions.json`. These are estimates, not measurements.

## Offline replay (default; no API key, no model calls)

```sh
python3 cost/calculator.py            # same as: python3 cost/calculator.py replay
```

Recomputes `usage.csv`, `report.json` and `report.md` from the saved files below. Opening or
importing the calculator never starts paid calls.

## Files

| File | Contents |
|---|---|
| `rates.csv` | Editable prices in their billing units, with checked date and source link |
| `assumptions.json` | Budget, output-token cap, max workers, max fallback fraction, projection scenarios, local-compute assumptions |
| `pilot_calls.jsonl` | One line per attempted pilot call (cold and warm), with real usage only *(created by the pilot)* |
| `pilot_runs.json` | Measured wall-clock and stage times, counts and checksum per pilot run *(created by the pilot)* |
| `pilot_records.jsonl` | One result/status per pilot ID *(created by the pilot)* |
| `usage.csv` | Every billed item mapped to its call: `billed_units x price_per_unit` |
| `report.md`, `report.json` | Measured cold/warm tables and full-run projections |

## Formulas

```text
item_cost  = billed_units x (price_usd / per_units)      # per-million prices: per_units = 1,000,000
total_cost = sum(item_cost)
```

- Input, cached input and output are mutually exclusive items; cached input is subtracted from
  input before the uncached rate is applied.
- Missing usage stays `unknown`; it is never converted to zero.
- API spend is reported separately from local compute (unknown, not measured).
- Projections scale each stage by its own work count (distinct texts for enrichment, a sampled
  fraction for verification); group and memo are fixed once-per-run costs.
- Base and conservative scenarios differ in retry, verification and token multipliers.
- A scenario is flagged over budget when its known parts already exceed `budget_usd`.

## Explicit pilot execution (paid; requires approval)

```sh
python3 -m pipeline.pilot --execute --budget-usd <cumulative cap> --workdir work/pilot \
    --text-model-id <provider/model> --input-price-per-m <p> --output-price-per-m <q> --verify-size 20
```

The pilot runs the whole staged pipeline (`pipeline.run`: ingest, enrich, a declared verification
sample, grouping with issue naming, ranking, memo, export) on `cost_100.csv` unchanged (checksum
checked against `manifest.json`), cold with an empty result cache and one worker, then warm on the
same work directory (expected: zero new enrichment, verification, naming and memo calls). It writes
`pilot_calls.jsonl`, `pilot_runs.json` and `pilot_records.jsonl` here and runs the offline replay.
It refuses to overwrite an earlier pilot and refuses a text model that has no dated row in
`rates.csv`. The text model and its prices must be approved before this command is run.
