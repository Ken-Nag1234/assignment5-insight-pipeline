"""Bounded automatic restart for unattended enrichment.

Usage (paid; run it yourself in a terminal or a `screen` session, never from a chat tool):
    python3 -m pipeline.supervise --log-dir outputs/runs/supervisor-dup --caffeinate -- \
        --db work/pipeline.sqlite --provider jev --stratum duplicates --budget-usd 1.05 \
        --workers 2 --rps 4 --execute --out outputs/runs

Everything after "--" is passed unchanged to `python3 -m pipeline.enrich`. The supervisor reads
only the child's exit code (pipeline.enrich.EXIT_CODES):

    restart after a delay   1 (unexpected error/crash), 6 network_unavailable, 7 circuit_breaker,
                            or the child killed by a signal the supervisor did not send
    stop, never restart     0 finished (queue empty or the run's request limit reached),
                            2 refused (lock held / missing --execute), 3 interrupted (Ctrl+C),
                            4 budget_cap, 5 fatal_provider_error (auth/billing), 8 unknown_outcome_limit

Restarts are bounded by --max-restarts. Spending stays bounded by the cumulative --budget-usd cap
in the shared ledger, which every restarted run re-reads; a restart can never raise the cap.
Ctrl+C, SIGTERM or SIGHUP to the supervisor forwards a graceful SIGINT to the child and stops
without restarting. One supervisor per database ("<db>.supervisor.lock").
"""

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from pipeline.runlock import LockHeld, hold_lock

RESTART_CODES = {1, 6, 7}
STOP_REASONS = {0: "finished", 2: "refused", 3: "interrupted", 4: "budget_cap", 5: "fatal_provider_error",
                8: "unknown_outcome_limit", 1: "error", 6: "network_unavailable", 7: "circuit_breaker"}
SUPERVISOR_LOCK = ".supervisor.lock"


def _now():
    return datetime.now(timezone.utc).isoformat()


def _db_from(enrich_args):
    for i, arg in enumerate(enrich_args):
        if arg == "--db" and i + 1 < len(enrich_args):
            return Path(enrich_args[i + 1])
        if arg.startswith("--db="):
            return Path(arg.split("=", 1)[1])
    raise SystemExit("ERROR: pass --db to the enrichment command after '--'")


class Supervisor:
    def __init__(self, command, log_dir, max_restarts=10, restart_delay_s=600.0, sleep=time.sleep,
                 caffeinate=False, echo=True):
        self.command = list(command)
        self.log_dir = Path(log_dir)
        self.max_restarts = max_restarts
        self.restart_delay_s = restart_delay_s
        self.sleep = sleep
        self.caffeinate = caffeinate
        self.echo = echo
        self.stop_requested = False
        self.child = None
        self.log_dir.mkdir(parents=True, exist_ok=True)

    def log(self, event, **data):
        record = dict(data, event=event, at=_now(), supervisor_pid=os.getpid())
        with (self.log_dir / "supervisor.jsonl").open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, sort_keys=True) + "\n")
            f.flush()
            os.fsync(f.fileno())
        if self.echo:
            print(f"[supervisor {record['at'][:19]}] {event} {json.dumps(data, sort_keys=True)}", file=sys.stderr,
                  flush=True)

    def request_stop(self, signum=None, _frame=None):
        self.stop_requested = True
        if self.child is not None and self.child.poll() is None:
            self.child.send_signal(signal.SIGINT)  # graceful: the child finishes in-flight work and saves

    def _wait(self, seconds):
        remaining = seconds
        while remaining > 0 and not self.stop_requested:
            step = min(1.0, remaining)
            self.sleep(step)
            remaining -= step

    def run_child(self):
        with (self.log_dir / "enrich_output.log").open("a", encoding="utf-8") as out:
            out.write(f"\n===== {_now()} start: {' '.join(self.command)}\n")
            out.flush()
            self.child = subprocess.Popen(self.command, stdout=out, stderr=subprocess.STDOUT)
            code = self.child.wait()
            self.child = None
        return code

    def run(self):
        keep_awake = None
        if self.caffeinate and shutil.which("caffeinate"):
            # Prevent idle and (on AC power) system sleep for as long as this supervisor lives.
            keep_awake = subprocess.Popen(["caffeinate", "-ims", "-w", str(os.getpid())])
        restarts = 0
        self.log("supervisor_started", command=self.command, max_restarts=self.max_restarts,
                 restart_delay_s=self.restart_delay_s, caffeinate=keep_awake is not None)
        try:
            while True:
                started = time.monotonic()
                code = self.run_child()
                reason = STOP_REASONS.get(code, "killed_by_signal" if code < 0 else f"exit_{code}")
                self.log("child_exited", exit_code=code, reason=reason, seconds=round(time.monotonic() - started, 1))
                restartable = code in RESTART_CODES or (code < 0 and code not in (-signal.SIGINT, -signal.SIGTERM))
                if self.stop_requested:
                    final = "stopped_by_user"
                    break
                if not restartable:
                    final = reason
                    break
                if restarts >= self.max_restarts:
                    final = "restart_limit_reached"
                    break
                restarts += 1
                self.log("restart_scheduled", restart=restarts, delay_s=self.restart_delay_s, after=reason)
                self._wait(self.restart_delay_s)
                if self.stop_requested:
                    final = "stopped_by_user"
                    break
            self.log("supervisor_finished", result=final, restarts=restarts, last_exit_code=code)
            return final, code, restarts
        finally:
            if keep_awake is not None and keep_awake.poll() is None:
                keep_awake.terminate()


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if "--" not in argv:
        raise SystemExit("usage: python3 -m pipeline.supervise [options] -- <pipeline.enrich arguments>")
    split = argv.index("--")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--log-dir", type=Path, required=True, help="supervisor.jsonl and enrich_output.log go here")
    parser.add_argument("--max-restarts", type=int, default=10)
    parser.add_argument("--restart-delay-min", type=float, default=10.0)
    parser.add_argument("--caffeinate", action="store_true", help="Keep the Mac awake while supervising (macOS)")
    args = parser.parse_args(argv[:split])
    enrich_args = argv[split + 1:]
    db_path = _db_from(enrich_args)
    supervisor = Supervisor([sys.executable, "-m", "pipeline.enrich", *enrich_args], args.log_dir,
                            args.max_restarts, args.restart_delay_min * 60, caffeinate=args.caffeinate)
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        if sig == signal.SIGHUP and signal.getsignal(sig) == signal.SIG_IGN:
            continue  # started under nohup: a closed terminal must not stop the run
        signal.signal(sig, supervisor.request_stop)
    try:
        with hold_lock(db_path, "pipeline.supervise " + " ".join(argv), suffix=SUPERVISOR_LOCK):
            final, code, _ = supervisor.run()
    except LockHeld as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return 2
    return 0 if final == "finished" else (code if code and code > 0 else 1)


if __name__ == "__main__":
    sys.exit(main())
