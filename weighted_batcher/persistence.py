"""Durable append and crash recovery for metric lines.

A metric record is one JSON object on a single line terminated by ``"\\n"``.
:func:`record_metrics` appends such a line to a plain text file and
:func:`recover_metrics` reads the file back into a list of dicts.

Concurrency
-----------
Concurrent appends from multiple processes never interleave: each record is
issued as a single :meth:`os.write` against a file opened with
:data:`os.O_APPEND`.  The kernel applies one ``write(2)`` to a regular file
at the current end of file as one indivisible step, so whole records land
next to each other instead of being spliced together.

Crash recovery
--------------
A crash mid-record leaves a final line without a trailing newline.  Recovery
acknowledges only complete (newline-terminated) lines, so that trailing
fragment is silently dropped.  A non-JSON line *among* the complete lines is
not a crash artefact: recovery raises :class:`ValueError` naming its line
number.
"""

from __future__ import annotations

import os

from . import parse_metrics

__all__ = ["record_metrics", "recover_metrics"]


def record_metrics(line, path) -> None:
    """Validate one metric line and append it to the file at ``path``.

    ``line`` must already be a string: passing any other type to be written
    raises :class:`OSError`, as does an unwritable ``path``.  The string must
    be a JSON object at the top level (otherwise :class:`ValueError`) whose
    keys are strings and whose values are ints or floats but not booleans
    (otherwise :class:`TypeError`).  The line is appended verbatim, with a
    newline appended if it does not already end in one.
    """
    # The thing handed to the writer must be textual; a non-string payload is
    # reported at the I/O boundary, alongside unwritable paths.
    if not isinstance(line, str):
        raise OSError(
            f"metric line to append must be a string, got {type(line).__name__}"
        )

    # Parse and fully validate before touching the file, so a rejected record
    # never leaves bytes behind.  parse_metrics raises ValueError for malformed
    # JSON, non-finite constants, and a non-object top level.
    metrics = parse_metrics(line)
    for key, value in metrics.items():
        if not isinstance(key, str):
            raise TypeError(
                f"metric name must be a string, got {type(key).__name__}"
            )
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError(
                f"metric value must be an int or float, got {type(value).__name__}"
            )

    # Keep the line verbatim ("-0.0", key order, spacing) but guarantee the
    # record is newline-terminated, without which recovery could not see it.
    payload = line if line.endswith("\n") else line + "\n"
    data = payload.encode("utf-8")

    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o666)
    try:
        # Exactly one write syscall: splitting this into a short-write loop
        # would reopen the interleave window, and regular-file writes of these
        # small records do not short-write.  A directory/unwritable path makes
        # os.open raise OSError (e.g. IsADirectoryError, PermissionError).
        written = os.write(fd, data)
    finally:
        os.close(fd)
    if written != len(data):
        # Defensive: a partial write would be indistinguishable from a crash
        # fragment, so report it rather than silently truncating the record.
        raise OSError(
            f"short write appending metric line: {written}/{len(data)} bytes"
        )


def recover_metrics(path) -> list:
    """Read records appended by :func:`record_metrics` back into a list.

    Only complete lines (terminated by ``"\\n"``) are acknowledged: a trailing
    fragment left by a crashed writer is dropped without error.  A line that is
    empty apart from its line ending (``"\\n"`` or ``"\\r\\n"``) carries no
    record and is skipped; any other non-JSON complete line raises
    :class:`ValueError` with its 1-based line number.

    A missing path raises :class:`FileNotFoundError`; a directory raises
    :class:`IsADirectoryError`.
    """
    records = []
    # Binary iteration lets a truncated, undecodable tail be dropped just like
    # any other incomplete trailing line.
    with open(path, "rb") as handle:
        line_no = 0
        while True:
            raw = handle.readline()
            if raw == b"":
                break  # End of file.
            line_no += 1
            if not raw.endswith(b"\n"):
                # Incomplete trailing record: the writer crashed mid-line.
                break
            # Only a bare newline (LF, or CRLF) is an empty, record-less line.
            # A line carrying spaces is still a complete non-JSON line and is
            # therefore reported rather than skipped.
            body = raw[:-1]
            if body.endswith(b"\r"):
                body = body[:-1]
            if body == b"":
                continue
            try:
                records.append(parse_metrics(raw.decode("utf-8")))
            except ValueError as exc:
                raise ValueError(
                    f"invalid metrics on line {line_no}: {exc}"
                ) from exc
    return records
