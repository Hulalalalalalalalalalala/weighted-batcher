"""Durable append-only metric log with rotation and crash recovery.

Each metric occupies exactly one newline-terminated JSON line.  A record is
appended with :func:`os.write` against a single ``O_APPEND`` file
descriptor, so concurrent appending processes never interleave one line
with another.  Recovery acknowledges only newline-terminated lines: a
record torn by a mid-write crash survives as an unterminated tail and is
silently discarded.

:func:`rotate_metrics` seals the current log into a numbered segment
(``path.1``, ``path.2``, ...) and leaves an empty log behind.  Recovery
walks the segments in ascending order followed by the current log, so a
record accepted before a rotation never crosses a segment boundary.
:func:`iter_metrics` performs the same walk as a line-by-line generator,
never loading the segment set into memory.

:func:`compact_metrics` merges every segment and the current log into a
single ``path.1`` segment, copying each complete record line verbatim.
The merge is published under the staging name ``path.compact`` only once
it is fully on disk; while that staging segment exists it is the only
file recovery reads, and a crash-interrupted compaction is finished by
the next append, rotation, or compaction before they proceed.
:func:`resume_metrics` re-reads the log set starting at a record
position that stays valid across appends, rotations, and compactions.
"""

from __future__ import annotations

import os

from . import parse_metrics, render_metrics

try:  # POSIX-only; the lock guards O_APPEND and serialises rotation.
    import fcntl
except ImportError:  # pragma: no cover - exercised on non-POSIX platforms
    fcntl = None

__all__ = [
    "append_metrics",
    "recover_metrics",
    "rotate_metrics",
    "iter_metrics",
    "compact_metrics",
    "resume_metrics",
]

_READ_CHUNK = 1 << 20


def _segment_numbers(path):
    """Return the already-used segment numbers for ``path`` as a set."""
    directory = os.path.dirname(path)
    prefix = os.path.basename(path) + "."
    numbers = set()
    try:
        names = os.listdir(directory or ".")
    except FileNotFoundError:
        return numbers
    for name in names:
        if name.startswith(prefix):
            suffix = name[len(prefix):]
            if suffix.isascii() and suffix.isdigit():
                numbers.add(int(suffix))
    return numbers


def _staging_path(path):
    """Name a compaction publishes its merged segment under."""
    return path + ".compact"


def _members(path):
    """Return the files holding the readable record sequence, in order.

    A staging segment left behind by an interrupted compaction is the
    only authoritative source while it exists: it already holds every
    record from the segments and the current log it replaces, so those
    are not read again.  Otherwise the segments in ascending order are
    followed by the current log.
    """
    staging = _staging_path(path)
    if os.path.exists(staging):
        return [staging]
    members = [
        f"{path}.{number}" for number in sorted(_segment_numbers(path))
    ]
    if os.path.exists(path):
        members.append(path)
    return members


def _finalize_compact(path, fd):
    """Finish a compaction that staged its merge but crashed before cleanup.

    The caller must hold the current log's lock through ``fd``.  The
    staging segment already holds every record from the old segments and
    the current log, so the old segments are removed and the current log
    is truncated in place (keeping the inode the lock is held on, so
    appenders serialised on it are not split away).  The staging segment
    is renamed to ``path.1`` last: until that rename a crash still leaves
    the staging segment as the authoritative source, and after it the
    compacted layout is complete.
    """
    staging = _staging_path(path)
    if not os.path.exists(staging):
        return
    for number in _segment_numbers(path):
        try:
            os.remove(f"{path}.{number}")
        except FileNotFoundError:
            # A segment removed externally is already gone.
            pass
    os.ftruncate(fd, 0)
    os.rename(staging, f"{path}.1")


def _same_inode(fd, path):
    """True when ``fd`` still names the file currently found at ``path``.

    A rotation can rename the file while we wait for its lock, leaving the
    descriptor aimed at a sealed segment.  A momentarily missing ``path``
    (rotation crashed before recreating it) also reports False, so an
    appender reopens and recreates the live log.
    """
    try:
        current = os.stat(path)
    except FileNotFoundError:
        return False
    held = os.fstat(fd)
    return (current.st_dev, current.st_ino) == (held.st_dev, held.st_ino)


def _open_locked(path, create):
    """Open the current log, take the exclusive lock, and follow rotation.

    Waits for the inter-process lock and then re-checks that the
    descriptor still names the live inode: a rotation may have swapped the
    path to a fresh file while we waited.  When that happened the sealed
    descriptor is dropped and the call re-opens the new current log and
    takes its lock, so a record is never written into a segment and two
    rotations never pick the same segment number.

    ``create`` determines whether a missing log is created (append) or
    reported (rotation).  A lock failure closes the descriptor and leaves
    nothing written.
    """
    def _open():
        if create:
            return os.open(
                path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o666
            )
        return os.open(path, os.O_RDWR)

    fd = _open()
    if fcntl is None:
        return fd
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
    except OSError:
        os.close(fd)
        raise
    while not _same_inode(fd, path):
        os.close(fd)
        fd = _open()
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
        except OSError:
            os.close(fd)
            raise
    return fd


def rotate_metrics(path):
    """Seal the current log into a numbered segment and start a new one.

    The segment is named ``path.N`` with ``N`` one greater than the
    highest segment number already present (``path.1`` when there are
    none), so a number is never reused after a segment is removed
    externally; an empty log seals an empty segment.  Afterwards ``path``
    exists again as an empty file ready for appending.  Every record
    acknowledged by a write ends up either wholly in a segment or wholly
    in the current log, never split across two.

    A crash after the rename but before the live file is recreated still
    leaves the sealed segment readable; the next appender recreates the
    live log.  A staging segment left by an interrupted compaction is
    retired first (see :func:`compact_metrics`).

    Raises:
        FileNotFoundError: ``path`` does not exist.
        IsADirectoryError: ``path`` is a directory.
        OSError: ``path`` is not a string, locking fails, or another OS
            error occurs while sealing.  Nothing is written when locking
            fails.
    """
    if not isinstance(path, str):
        raise OSError(f"log path must be a string, got {type(path).__name__}")
    if os.path.isdir(path):
        raise IsADirectoryError(f"log path is a directory: {path!r}")

    fd = _open_locked(path, create=False)
    try:
        _finalize_compact(path, fd)
        used = _segment_numbers(path)
        number = max(used, default=0) + 1
        segment = f"{path}.{number}"
        # The rename atomically publishes the sealed inode under its
        # segment name; the lock we hold makes concurrent appenders reopen
        # the path before writing.
        os.rename(path, segment)
        # Re-create an empty current log.  An appender racing the rename
        # window may have created the path and landed a record there first;
        # opening without O_TRUNC keeps that record in the current log,
        # where it belongs.
        new_fd = os.open(
            path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o666
        )
        os.close(new_fd)
    finally:
        os.close(fd)


def append_metrics(path, line):
    """Validate one metric line and append it to the log at ``path``.

    The line must be a string holding a JSON object whose values are ints
    or floats (booleans are rejected).  It is rendered canonically before
    writing, so every call adds exactly one compact newline-terminated
    record regardless of the input's spacing.

    If another process rotates the log while this call waits for the lock,
    the descriptor is re-opened against the freshly re-created file before
    writing, so no record is directed into a sealed segment.  A staging
    segment left by an interrupted compaction is likewise retired before
    the record is written (see :func:`compact_metrics`).

    Raises:
        OSError: ``line`` or ``path`` is not a string, the target cannot
            be written, or locking the log fails.  Nothing is written when
            locking fails.
        ValueError: the line is not valid JSON, its top level is not an
            object, or a value is NaN or Infinity.
        TypeError: a metric value is neither an int nor a float.
    """
    if not isinstance(line, str):
        raise OSError(
            f"metrics line must be a string, got {type(line).__name__}"
        )
    if not isinstance(path, str):
        raise OSError(f"log path must be a string, got {type(path).__name__}")
    metrics = parse_metrics(line)
    payload = render_metrics(metrics).encode("utf-8")

    fd = _open_locked(path, create=True)
    try:
        _finalize_compact(path, fd)
        # One line per write loop on an O_APPEND descriptor: the kernel
        # appends each write atomically, so concurrent processes do not
        # tear or interleave records.
        view = memoryview(payload)
        while view:
            written = os.write(fd, view)
            view = view[written:]
    finally:
        os.close(fd)


def _iter_lines(path, chunksize=_READ_CHUNK):
    """Yield ``(line_number, raw_bytes)`` for complete lines in one file.

    Reading happens one fixed-size chunk at a time, so an arbitrarily long
    single line is streamed rather than requiring the whole file in
    memory (only that one line is ever held at once).  The final
    unterminated tail is a possible torn write and is never produced, so
    callers cannot mistake it for a complete line.
    """
    fd = os.open(path, os.O_RDONLY)
    pending = b""
    lineno = 0
    try:
        while True:
            chunk = os.read(fd, chunksize)
            if not chunk:
                break
            pending += chunk
            *complete, pending = pending.split(b"\n")
            for raw in complete:
                lineno += 1
                yield lineno, raw
    finally:
        os.close(fd)


def _parse_file(path, label):
    """Stream one segment or the current log through the metrics parser."""
    for lineno, raw in _iter_lines(path):
        if not raw or raw == b"\r":
            # A bare newline or a CRLF blank line carries no record.
            continue
        try:
            text = raw.decode("utf-8")
            yield parse_metrics(text)
        except ValueError as exc:
            raise ValueError(
                f"invalid metrics in {label} at line {lineno}: {exc}"
            ) from exc


def iter_metrics(path):
    """Yield metric records across the segments and the current log.

    Segments ``path.1``, ``path.2``, ... are streamed first in ascending
    segment order, followed by the current log at ``path``.  While a
    staging segment ``path.compact`` from an interrupted compaction
    exists, it is the only file streamed: it already holds every record
    of the set it replaces.  Files are read line by line, so the segment
    set is never loaded into memory and a single line may be arbitrarily
    long.

    A torn unterminated tail at the end of any file is silently discarded;
    a segment holding only a torn tail yields nothing.  Blank empty lines
    (``\\n``) and CRLF blank lines (``\\r\\n``) are skipped.  Any other
    complete line that is not a metrics JSON object raises
    :class:`ValueError` naming the file and its 1-based line number.

    Path-level problems are reported when this function is called; line
    content problems are reported lazily as the offending line is reached.

    Raises:
        FileNotFoundError: neither ``path`` nor any segment exists.
        IsADirectoryError: ``path`` is a directory.
        OSError: ``path`` is not a string.
        ValueError: a non-empty complete line is not a metrics JSON
            object (raised while iterating).
    """
    if not isinstance(path, str):
        raise OSError(f"log path must be a string, got {type(path).__name__}")
    if os.path.isdir(path):
        raise IsADirectoryError(f"log path is a directory: {path!r}")

    members = _members(path)
    if not members:
        raise FileNotFoundError(f"no metrics log or segments at {path!r}")

    def _generate():
        for member in members:
            yield from _parse_file(member, member)

    return _generate()


def recover_metrics(path):
    """Read metric records back from the rotated log set at ``path``.

    Segments are read in ascending order followed by the current log; see
    :func:`iter_metrics` for the line-level rules.  This materialises the
    records into a list, while :func:`iter_metrics` streams them.

    Raises:
        FileNotFoundError: neither ``path`` nor any segment exists.
        IsADirectoryError: ``path`` is a directory.
        ValueError: a non-empty complete line is not a metrics object.
    """
    return list(iter_metrics(path))


def compact_metrics(path):
    """Merge all segments and the current log into a single segment.

    Every complete record line is copied verbatim, newline included, in
    write order: nothing is re-rendered, de-duplicated, or reordered, so
    exact counters, ``-0.0``, and key order survive untouched, and a
    single line may be arbitrarily long.  A torn unterminated tail is not
    a record and is dropped; blank lines are skipped.  The merged result
    becomes ``path.1`` and the current log is left empty; compacting an
    empty log set still yields an empty ``path.1``.

    The merge is written to a temporary file and flushed to disk before
    it is published under the staging name ``path.compact`` with an
    atomic rename, so a crash mid-merge leaves only a partial temporary
    file that recovery never reads.  While the staging segment exists it
    is the only file recovery and streaming read, and its record sequence
    equals the pre-compaction sequence item for item.  Cleanup then
    removes the old segments, truncates the current log, and renames the
    staging segment to ``path.1`` last.  Appends, rotations, and further
    compactions serialise on the same lock, so every record accepted
    during a compaction lands in the segment set exactly once, in write
    order.

    Raises:
        FileNotFoundError: neither ``path`` nor any segment exists.
        IsADirectoryError: ``path`` is a directory.
        OSError: ``path`` is not a string, locking fails, or writing the
            merged segment fails.
    """
    if not isinstance(path, str):
        raise OSError(f"log path must be a string, got {type(path).__name__}")
    if os.path.isdir(path):
        raise IsADirectoryError(f"log path is a directory: {path!r}")
    staging = _staging_path(path)
    if not os.path.exists(staging) and not _members(path):
        raise FileNotFoundError(f"no metrics log or segments at {path!r}")

    fd = _open_locked(path, create=True)
    try:
        # A crashed predecessor's staging segment is retired first.
        _finalize_compact(path, fd)
        temp = staging + ".tmp"
        out = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o666)
        try:
            for member in _members(path):
                for _lineno, raw in _iter_lines(member):
                    if not raw or raw == b"\r":
                        # Blank lines carry no record; the torn tail is
                        # never produced by _iter_lines at all.
                        continue
                    view = memoryview(raw + b"\n")
                    while view:
                        written = os.write(out, view)
                        view = view[written:]
            # The staging name is published only once the merged segment
            # is complete on disk.
            os.fsync(out)
        finally:
            os.close(out)
        os.rename(temp, staging)
        _finalize_compact(path, fd)
    finally:
        os.close(fd)


def resume_metrics(path, position=0):
    """Read records back starting ``position`` records into the log set.

    ``position`` counts complete records from the start of the segment
    set in write order — the number of records already read.  It defaults
    to 0 (read from the start) and needs no adjustment across appends,
    rotations, compactions, or crash clean-ups, since those never reorder
    the record sequence.  Only a position beyond the total record count
    is out of bounds; a position equal to the count yields ``[]``.

    The log set is streamed, never loaded whole: records before
    ``position`` are counted and skipped, and only the remainder is
    parsed, so the result equals ``recover_metrics(path)[position:]``
    item for item and is produced in linear time.

    Raises:
        FileNotFoundError: neither ``path`` nor any segment exists.
        IsADirectoryError: ``path`` is a directory.
        OSError: ``path`` is not a string.
        TypeError: ``position`` is not an integer (booleans excluded).
        ValueError: ``position`` is negative or beyond the record count,
            or a non-empty complete line at or past ``position`` is not a
            metrics JSON object (named with its file and line number).
    """
    if not isinstance(path, str):
        raise OSError(f"log path must be a string, got {type(path).__name__}")
    if isinstance(position, bool) or not isinstance(position, int):
        raise TypeError(
            f"read position must be an integer, got {type(position).__name__}"
        )
    if os.path.isdir(path):
        raise IsADirectoryError(f"log path is a directory: {path!r}")
    if position < 0:
        raise ValueError("read position must not be negative")

    members = _members(path)
    if not members:
        raise FileNotFoundError(f"no metrics log or segments at {path!r}")

    records = []
    skipped = 0
    for member in members:
        for lineno, raw in _iter_lines(member):
            if not raw or raw == b"\r":
                continue
            if skipped < position:
                skipped += 1
                continue
            try:
                text = raw.decode("utf-8")
                records.append(parse_metrics(text))
            except ValueError as exc:
                raise ValueError(
                    f"invalid metrics in {member} at line {lineno}: {exc}"
                ) from exc
    if skipped < position:
        raise ValueError(
            f"read position {position} is beyond the {skipped} records present"
        )
    return records
