"""Runnable entry point for the cost/runtime calculator. Offline replay is the default.

    python3 cost/calculator.py              # same as: replay
    python3 cost/calculator.py replay
    python3 cost/calculator.py export-calls --db work/pilot_cold.sqlite --kind cold

Opening or importing this file never makes API calls. See cost/README.md.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.costcalc import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:] or ["replay"]))
