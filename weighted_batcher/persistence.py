"""Durable append-only metric log with crash recovery.

Each metric occupies exactly one newline-terminated JSON line.  A record is
appended with :func:`os.write` against a single ``O_APPEND`` file
descriptor, so concurrent appending processes never interleave one line
with another.  Recovery acknowledges only newline-terminated lines: a
record torn by a mid-write crash survives as an unterminated tail and is
silently discarded.
"""

from __future__ import annotations

import os

from . import parse_metrics, render_metrics

try:  # POSIX-only; the lock is a belt-and-braces guard around O_APPEND.
    import fcntl
except ImportError:  # pragma: no cover - exercised on non-POSIX platforms
    fcntl = None

__all__ = ["append_metrics", "recover_metrics"]


def append_metrics(path, line):
    """Validate one metric line and append it to the log at ``path``.

    The line must be a string holding a JSON object whose values are ints
    or floats (booleans are rejected).  It is rendered canonically before
    writing, so every call adds exactly one compact newline-terminated
    record regardless of the input's spacing.

    Raises:
        OSError: ``line`` is not a string, or the target cannot be written.
        ValueError: the line is not valid JSON, its top level is not an
            object, or a value is NaN or Infinity.
        TypeError: a metric value is neither an int nor a float.
    """
    if not isinstance(line, str):
        raise OSError(
            f"metrics line must be a string, got {type(line).__name__}"
        )
    metrics = parse_metrics(line)
    payload = render_metrics(metrics).encode("utf-8")

    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o666)
    try:
        if fcntl is not None:
            # Serialise whole appends between processes; harmless on
            # filesystems whose O_APPEND writes are already atomic.
            try:
                fcntl.flock(fd, fcntl.LOCK_EX)
            except OSError:
                pass
        # One line per write call on an O_APPEND descriptor: the kernel
        # appends each write atomically, so concurrent processes do not
        # tear or interleave records.
        view = memoryview(payload)
        while view:
            written = os.write(fd, view)
            view = view[written:]
    finally:
        os.close(fd)


def recover_metrics(path):
    """Read metric records back from the append log at ``path``.

    Only newline-terminated lines are acknowledged, so a torn write left
    at the tail is discarded without error.  Blank lines are ignored.
    Any other line that is not a metrics JSON object raises
    :class:`ValueError` naming its 1-based line number.

    Returns an empty list for an empty file or a file containing only
    newlines.  Integer values come back as exact arbitrary-precision ints,
    ``-0.0`` is preserved, and key order follows the file.

    Raises:
        FileNotFoundError: ``path`` does not exist.
        IsADirectoryError: ``path`` is a directory.
        ValueError: a non-empty complete line is not a metrics object.
    """
    with open(path, "rb") as stream:
        data = stream.read()

    records = []
    # Splitting on the newline makes every segment but the last
    # newline-terminated (hence complete); the final segment is a possible
    # torn tail from a crashed writer and is dropped without inspecting it.
    complete_lines = data.split(b"\n")[:-1]
    for lineno, raw in enumerate(complete_lines, start=1):
        if not raw:
            # A blank separator line carries no record.
            continue
        try:
            text = raw.decode("utf-8")
            records.append(parse_metrics(text))
        except ValueError as exc:
            raise ValueError(f"invalid metrics at line {lineno}: {exc}") from exc
    return records
