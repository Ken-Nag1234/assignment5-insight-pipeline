"""Append-only spend ledger shared by every pipeline database and run.

Before dispatch, the orchestrator reserves the estimated worst-case cost of a request and
admits it only if spent + reserved + reservation <= cap. After the response, the reservation
is replaced by the actual cost (provider-reported when available, otherwise usage x rate).
A call whose outcome is unknown (for example, a timeout) keeps its full reservation as spent,
because it may still be billed.

This is a local control; provider-side billing may be delayed, so it is not a guarantee.
"""

import json
import threading
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from pipeline.common import REPO_ROOT

DEFAULT_LEDGER = REPO_ROOT / "work" / "spend_ledger.jsonl"


class BudgetExceeded(Exception):
    pass


class SpendLedger:
    def __init__(self, cap_usd, path=DEFAULT_LEDGER):
        self.cap = Decimal(str(cap_usd))
        self.path = Path(path)
        self._reserved = {}
        self._next_token = 0
        self._lock = threading.Lock()
        self._spent = self._load()

    def _load(self):
        total = Decimal("0")
        if self.path.exists():
            for line in self.path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    total += Decimal(json.loads(line)["charged_usd"])
        return total

    @property
    def spent(self):
        return self._spent

    @property
    def reserved(self):
        return sum(self._reserved.values(), Decimal("0"))

    def remaining(self):
        return self.cap - self._spent - self.reserved

    def reserve(self, amount):
        """Return a reservation token, or None if admitting this request could exceed the cap."""
        amount = Decimal(str(amount))
        with self._lock:
            if self._spent + self.reserved + amount > self.cap:
                return None
            self._next_token += 1
            self._reserved[self._next_token] = amount
            return self._next_token

    def settle(self, token, entry):
        """Replace a reservation with the charged amount and append one ledger line.

        entry must contain request_id, cost_source and cost_usd (None when unknown); an unknown
        cost is charged at the reserved amount.
        """
        with self._lock:
            reserved = self._reserved.pop(token)
            cost = entry.get("cost_usd")
            charged = reserved if cost is None else Decimal(str(cost))
            line = dict(entry, reserved_usd=format(reserved, "f"), charged_usd=format(charged, "f"),
                        recorded_at=datetime.now(timezone.utc).isoformat())
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(line, sort_keys=True) + "\n")
                f.flush()
            self._spent += charged
            return charged
