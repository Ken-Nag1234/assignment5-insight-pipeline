# Independent verification report


Declared sample: 500 records, seed `berkeley-fall-2026-assignment-5-verify-v1`, scope analysis_10000 (completed original records (cache copies excluded, one per distinct text)).
Compared 500; missing 0; unresolved 0.

| Field | Agreement | Cohen's kappa |
|---|---|---|
| topic | 0.952 | 0.9291 |
| intent | 0.964 | 0.9452 |
| severity | 0.918 | 0.8735 |

Severity within +/-1: 0.992; mean absolute gap 0.09. All three fields agree: 0.864.

Disagreements: 68 (see `disagreements.csv`). The verifier never overwrites enrichment labels; disagreements are inputs for human inspection.
