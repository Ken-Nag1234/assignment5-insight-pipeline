# Cost and runtime calculator report

Rates checked: 2026-10-09, 2026-10-11. Recompute with `python3 cost/calculator.py replay`.

## Measured cold run

- Input checksum: `c884ac3b9be5066995d5063f96ad9af6e5e082975788c1684c4f6b6ea661dd0e`; IDs: 100; completed: 100; failed/quarantined: 0; unique texts: 100; result-cache hits: 0
- Batch size: 1; workers: 1; wall clock: 76.131 s
- API cost: $0.00591814; per 1,000 inputs: $0.059181; per completed record: $0.00005918; throughput: 1.314 rows/s

| Role | Model / settings | Requests | Failed | Retries | Fallbacks | Input tok | Output tok | Cost (USD) | Seconds |
|---|---|---|---|---|---|---|---|---|---|
| enrich | typesafe-ai/jev prompt p1, schema s1, extraction x1, 1 review/request | 100 | 0 | 0 | 0 | 87787.000000 | 19649.000000 | 0.00368705 | 50.823 |
| verify | typesafe-ai/jev prompt verify-v1, sample 20 | 20 | 0 | 0 | 0 | 18895.000000 | 3927.000000 | 0.00079359 | 10.067 |
| group | openai/gpt-6-luna prompt n1, reasoning_effort=none, json_object, max_tokens 1500 | 1 | 0 | 0 | 0 | 2195.000000 | 150.000000 | 0.00029450 | 2.495 |
| memo | openai/gpt-6-luna prompt m1, reasoning_effort=none, json_object, max_tokens 2500 | 1 | 0 | 0 | 0 | 3555.000000 | 1575.000000 | 0.00114300 | 12.597 |

## Measured warm run

- Input checksum: `c884ac3b9be5066995d5063f96ad9af6e5e082975788c1684c4f6b6ea661dd0e`; IDs: 100; completed: 100; failed/quarantined: 0; unique texts: 0; result-cache hits: 0
- Batch size: 1; workers: 1; wall clock: 0.111 s
- API cost: $0.00000000; per 1,000 inputs: $0.000000; per completed record: $0.00000000; throughput: 900.901 rows/s

| Role | Model / settings | Requests | Failed | Retries | Fallbacks | Input tok | Output tok | Cost (USD) | Seconds |
|---|---|---|---|---|---|---|---|---|---|
| enrich | -  | 0 | 0 | 0 | 0 | 0.000000 | 0.000000 | 0.00000000 | 0.005 |
| verify | -  | 0 | 0 | 0 | 0 | 0.000000 | 0.000000 | 0.00000000 | 0.005 |
| group | -  | 0 | 0 | 0 | 0 | 0.000000 | 0.000000 | 0.00000000 | 0.011 |
| memo | -  | 0 | 0 | 0 | 0 | 0.000000 | 0.000000 | 0.00000000 | 0.007 |

Warm run new enrichment calls: 0 (zero, as required).

## Controls

Budget $10; output-token cap None; max workers 1; max fallback fraction 0.0.

## Projections (estimates)

| Target rows | Scenario | Unit cost source | Reuse | Texts | Requests | Enrich USD | Verify USD | Fallback USD | Fixed USD | Total USD | Enrich hours | Over budget |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 100,000 | base | measured_cold_pilot | with_exact_text_reuse | 73,294 | 73,294 | 2.7564 | 0.0405 | 0.0000 | 0.0014 | 2.7983 | 10.55 | no |
| 100,000 | base | measured_cold_pilot | no_reuse | 100,000 | 100,000 | 3.7608 | 0.0405 | 0.0000 | 0.0014 | 3.8027 | 14.40 | no |
| 100,000 | conservative | measured_cold_pilot | with_exact_text_reuse | 73,294 | 73,294 | 3.7158 | 0.1091 | 0.0000 | 0.0014 | 3.8263 | 11.38 | no |
| 100,000 | conservative | measured_cold_pilot | no_reuse | 100,000 | 100,000 | 5.0697 | 0.1091 | 0.0000 | 0.0014 | 5.1803 | 15.53 | no |
| 660,609 | base | measured_cold_pilot | with_exact_text_reuse | 484,189 | 484,189 | 18.2094 | 0.2674 | 0.0000 | 0.0014 | 18.4782 | 69.72 | **YES** |
| 660,609 | base | measured_cold_pilot | no_reuse | 660,609 | 660,609 | 24.8442 | 0.2674 | 0.0000 | 0.0014 | 25.1130 | 95.13 | **YES** |
| 660,609 | conservative | measured_cold_pilot | with_exact_text_reuse | 484,189 | 484,189 | 24.5469 | 0.7209 | 0.0000 | 0.0014 | 25.2693 | 75.19 | **YES** |
| 660,609 | conservative | measured_cold_pilot | no_reuse | 660,609 | 660,609 | 33.4909 | 0.7209 | 0.0000 | 0.0014 | 34.2132 | 102.59 | **YES** |

## Local compute (separate from API spend)

Ingestion, validation, grouping and ranking run on the author's personal laptop (macOS); no paid infrastructure. Cost: unknown (not measured).

Projections scale each stage by its own work count; group and memo are fixed once-per-run costs.
Partial targets assume the full-corpus exact-text reuse ratio; the seeded queue can differ.
