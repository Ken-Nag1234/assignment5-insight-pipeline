"""One writer process per database.

A run holds an exclusive, non-blocking fcntl lock on "<database>.lock" for its whole life. The
operating system releases the lock when the process exits for any reason (normal exit, crash,
kill), so a stale lock file never blocks a restart. The file's text records who holds the lock,
for pipeline.status; that text is informational only and is never used to decide ownership.
"""

import fcntl
import json
import os
import socket
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path


class LockHeld(Exception):
    pass


def lock_path(db_path, suffix=".lock"):
    db_path = Path(db_path)
    return db_path.with_name(db_path.name + suffix)


@contextmanager
def hold_lock(db_path, command, suffix=".lock"):
    path = lock_path(db_path, suffix)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            holder = read_holder(db_path, suffix)
            raise LockHeld(f"{path} is held by another process: {holder or 'unknown'}") from None
        info = {"pid": os.getpid(), "host": socket.gethostname(), "command": command[:500],
                "started_at": datetime.now(timezone.utc).isoformat()}
        os.ftruncate(fd, 0)
        os.write(fd, json.dumps(info).encode() + b"\n")
        os.fsync(fd)
        yield info
    finally:
        os.close(fd)  # closing the descriptor releases the lock


def is_locked(db_path, suffix=".lock"):
    """True when a live process holds the lock. Never creates the lock file."""
    path = lock_path(db_path, suffix)
    if not path.exists():
        return False
    fd = os.open(path, os.O_RDONLY)
    try:
        fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
    except BlockingIOError:
        return True
    finally:
        os.close(fd)
    return False


def read_holder(db_path, suffix=".lock"):
    try:
        return json.loads(lock_path(db_path, suffix).read_text() or "null")
    except (OSError, ValueError):
        return None
