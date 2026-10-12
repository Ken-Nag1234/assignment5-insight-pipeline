"""Local credential loading and a read-only presence check. No network calls.

Usage:
    python3 -m pipeline.config check     # reports presence only; never prints any part of a key

Keys are read from the process environment first, then from the git-ignored .env file
in the repository root. Values are never printed, logged or written anywhere.
"""

import argparse
import os
import stat
import subprocess
import sys

from pipeline.common import REPO_ROOT

ENV_FILE = REPO_ROOT / ".env"
KNOWN_KEYS = ("AI_GATEWAY_API_KEY", "TYPESAFE_API_KEY", "OPENAI_API_KEY", "ANTHROPIC_API_KEY")


def read_env_file(path=None):
    """Minimal KEY=VALUE parser (comments and blank lines ignored; optional surrounding quotes removed)."""
    path = path or ENV_FILE
    values = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip().removeprefix("export ").strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key] = value
    return values


def get_key(name):
    """Return the secret value for use by API clients, or None. Callers must never print it."""
    return os.environ.get(name) or read_env_file().get(name) or None


def _key_status(name, file_values):
    value = os.environ.get(name) or file_values.get(name, "")
    source = "environment" if os.environ.get(name) else (".env" if file_values.get(name) else None)
    problems = []
    if value:
        if value != value.strip() or any(c.isspace() for c in value):
            problems.append("contains whitespace")
        if value.startswith(("<", "your", "YOUR")) or value in {"...", "xxx", "changeme"}:
            problems.append("looks like a placeholder")
    return {"present": bool(value), "source": source, "problems": problems}


def _git(*args):
    try:
        return subprocess.run(["git", *args], cwd=REPO_ROOT, capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.TimeoutExpired):
        return None


def check(required=("AI_GATEWAY_API_KEY",)):
    file_values = read_env_file()
    lines, ok = [], True

    exists = ENV_FILE.exists()
    lines.append(f".env file exists: {'yes' if exists else 'no'}")
    if exists:
        mode = stat.S_IMODE(ENV_FILE.stat().st_mode)
        private = not mode & (stat.S_IRWXG | stat.S_IRWXO)
        lines.append(f".env readable only by you: {'yes' if private else 'no (run: chmod 600 .env)'}")
        ok &= private

    ignored = _git("check-ignore", "-q", ".env")
    is_ignored = ignored is not None and ignored.returncode == 0
    lines.append(f".env ignored by Git: {'yes' if is_ignored else 'NO'}")
    tracked = _git("ls-files", "--", ".env")
    is_tracked = tracked is not None and bool(tracked.stdout.strip())
    lines.append(f".env tracked by Git: {'YES (run: git rm --cached .env)' if is_tracked else 'no'}")
    ok &= is_ignored and not is_tracked

    for name in KNOWN_KEYS:
        status = _key_status(name, file_values)
        label = "required" if name in required else "optional"
        state = f"present (from {status['source']})" if status["present"] else "missing"
        if status["problems"]:
            state += " - check value: " + ", ".join(status["problems"])
        lines.append(f"{name} [{label}]: {state}")
        if name in required:
            ok &= status["present"] and not status["problems"]
    return ok, lines


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["check"])
    parser.add_argument("--require", nargs="*", default=["AI_GATEWAY_API_KEY"],
                        help="Keys that must be present (default: AI_GATEWAY_API_KEY)")
    args = parser.parse_args(argv)
    ok, lines = check(tuple(args.require))
    print("\n".join(lines))
    print("RESULT:", "ready" if ok else "not ready")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
