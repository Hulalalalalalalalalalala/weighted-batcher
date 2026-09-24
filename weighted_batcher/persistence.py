"""Durable append-only metric log with rotation, compaction and resume.

Each metric occupies exactly one newline-terminated JSON line.  A record is
appended with :func:`os.write` against a single ``O_APPEND`` file
descriptor, so concurrent appending processes never interleave one line
with another.  Recovery acknowledges only newline-terminated lines: a
record torn by a mid-write crash survives as an unterminated tail and is
silently discarded.

:func:`rotate_metrics` seals the current log into a numbered segment
(``path.1``, ``path.2``, ...) and leaves an empty log behind.  Segment
numbers are one past the highest number ever used, so deleting a segment
from the outside never makes its number come back.  Recovery walks the
segments in ascending order followed by the current log, so a record
accepted before a rotation never crosses a segment boundary.
:func:`iter_metrics` performs the same walk as a line-by-line generator,
never loading the segment set into memory.

:func:`compact_metrics` merges every segment and the current log into the
single fixed segment ``path.1`` and leaves an empty current log.  Records
are copied byte for byte, still newline terminated, without re-rendering.
The merge lands first in a staging file at ``path.compact``; only after it
is fully on disk does cleanup begin.

:func:`prune_metrics` enforces a record-count retention quota by evicting
whole records from the oldest end.  The segment set remembers how many
records were evicted in a ``path.pruned`` counter sidecar, so read
positions -- ordinal numbers in write order -- never shift: evicted
records keep occupying their original positions.

:func:`resume_metrics` resumes the same write-order walk at an integer
read position counting records already read from the start, evicted
records included; the position needs no translation across appends,
rotations, compactions, pruning or crash cleanup.
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
    "prune_metrics",
]

_READ_CHUNK = 1 << 20
# Suffix of the staging file compaction writes before cleanup begins; it
# only ever names a whole, fully-fsynced segment that has been published.
_COMPACT_SUFFIX = ".compact"
# Suffix of the sidecar remembering how many records have been pruned.
_PRUNED_SUFFIX = ".pruned"
# Suffix pattern of a published prune marker: ``path.prune.<t|e>.<n>.<c>``.
_PRUNE_MARKER_SUFFIX = ".prune."
# Marker kinds: ``t`` truncates one segment at record index ``n`` (the
# marker carries the new cumulative pruned count); ``e`` evicts every
# segment through ``n``.
_MARKER_TRUNCATE = "t"
_MARKER_EVICT = "e"


def _segment_numbers(path):
    """Return the already-used segment numbers for ``path`` as a set.

    The ``path.compact`` staging file is never a numbered segment and is
    deliberately invisible here.
    """
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
            if suffix == "compact":
                continue
            if suffix.isascii() and suffix.isdigit():
                numbers.add(int(suffix))
    return numbers


def _staging_path(path):
    """The compaction staging file path (``path.compact``)."""
    return path + _COMPACT_SUFFIX


def _pruned_path(path):
    """The sidecar path remembering the cumulative pruned-record count."""
    return path + _PRUNED_SUFFIX


def _read_pruned(path):
    """Return the cumulative number of pruned records recorded on disk.

    The sidecar holds one base-ten integer written atomically by
    :func:`prune_metrics`; an absent sidecar -- every segment set that was
    never pruned, including all pre-pruning sets -- reads back as zero.
    """
    sidecar = _pruned_path(path)
    try:
        fd = os.open(sidecar, os.O_RDONLY)
    except FileNotFoundError:
        return 0
    try:
        data = b""
        while True:
            chunk = os.read(fd, _READ_CHUNK)
            if not chunk:
                break
            data += chunk
    finally:
        os.close(fd)
    return int(data.decode("utf-8").strip())


def _write_pruned(path, count):
    """Atomically replace the pruned-record sidecar with ``count``."""
    sidecar = _pruned_path(path)
    tmp = sidecar + ".tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o666)
    try:
        payload = str(count).encode("ascii")
        view = memoryview(payload)
        while view:
            written = os.write(fd, view)
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp, sidecar)


def _find_prune_marker(path):
    """Find a published prune marker left by an interrupted prune.

    Returns ``(kind, segment_number, count, marker_path)`` or ``None``.
    Segment number ``0`` names the current log; the sidecar and private
    temporary files never match the marker pattern.
    """
    directory = os.path.dirname(path)
    prefix = os.path.basename(path) + _PRUNE_MARKER_SUFFIX
    try:
        names = os.listdir(directory or ".")
    except FileNotFoundError:
        return None
    matches = []
    for name in names:
        if not name.startswith(prefix):
            continue
        parts = name[len(prefix):].split(".")
        if len(parts) != 3:
            continue
        kind, number, count = parts
        if kind not in (_MARKER_TRUNCATE, _MARKER_EVICT):
            continue
        if not (number.isascii() and number.isdigit()):
            continue
        if not (count.isascii() and count.isdigit()):
            continue
        matches.append((name, kind, int(number), int(count)))
    if not matches:
        return None
    # A prune publishes exactly one marker; several names can only be a
    # leftover of an impossible state, so resolve deterministically.
    name, kind, number, count = sorted(matches)[0]
    if directory:
        marker_path = os.path.join(directory, name)
    else:
        marker_path = name
    return kind, number, count, marker_path


def _finish_prune_marker(path):
    """Finish an interrupted prune whose marker is still published.

    Called with the live-log lock held by any mutating operation, mirroring
    :func:`_finish_staged_compact`.  The marker alone names the new world
    until cleanup completes:

    * a truncate marker's own bytes are the surviving tail of the cut
      segment; lower numbered segments are removed, the new pruned count
      is persisted, and the marker is atomically renamed onto the cut
      segment (or onto the live log for segment number zero);
    * an evict marker removes every numbered segment through its number,
      persists the new count, and removes itself; the live log survives.

    The sidecar is written before the marker disappears, so a crash at any
    point leaves readers resolving either through the marker or through an
    already-updated sidecar -- never with the old count over the new files.
    """
    marker = _find_prune_marker(path)
    if marker is None:
        return
    kind, number, count, marker_path = marker
    if kind == _MARKER_TRUNCATE:
        for other in sorted(_segment_numbers(path)):
            if number == 0 or other < number:
                # Number zero names the live log, the newest member, so
                # every numbered segment is older and goes away.
                os.unlink(f"{path}.{other}")
        _write_pruned(path, count)
        target = path if number == 0 else f"{path}.{number}"
        os.replace(marker_path, target)
    else:
        for other in sorted(_segment_numbers(path)):
            if other <= number:
                os.unlink(f"{path}.{other}")
        if not _segment_members(path):
            # Every record is gone (a rotation-crashed world may have no
            # live log): keep an empty file at the path so the committed
            # empty set reads back as empty rather than as a missing log.
            _truncate_live_log(path)
        _write_pruned(path, count)
        os.unlink(marker_path)


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
    greatest number already present (``path.1`` for the first rotation),
    so a segment that was deleted from the outside never hands its number
    back out; an empty log seals an empty segment.  Afterwards ``path``
    exists again as an empty file ready for appending.  Every record
    acknowledged by a write ends up either wholly in a segment or wholly
    in the current log, never split across two.

    A crash after the rename but before the live file is recreated still
    leaves the sealed segment readable; the next appender recreates the
    live log.

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
        if fcntl is None:
            # Windows refuses to rename onto a path while one of our
            # descriptors is still open against it; this descriptor only
            # carries the POSIX lock, which does not exist here, so drop
            # it before any finish-rename or the sealing rename below.
            os.close(fd)
            fd = None
        _finish_staged_compact(path)
        _finish_prune_marker(path)
        used = _segment_numbers(path)
        number = (max(used) + 1) if used else 1
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
        if fd is not None:
            os.close(fd)


def append_metrics(path, line):
    """Validate one metric line and append it to the log at ``path``.

    The line must be a string holding a JSON object whose values are ints
    or floats (booleans are rejected).  It is rendered canonically before
    writing, so every call adds exactly one compact newline-terminated
    record regardless of the input's spacing.

    If another process rotates the log while this call waits for the lock,
    the descriptor is re-opened against the freshly re-created file before
    writing, so no record is directed into a sealed segment.

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
        stale = False
        if fcntl is None:
            # No lock is carried on this platform; drop the descriptor
            # before pending cleanup, which may rename onto the live path
            # (Windows rejects replacing an open path), then reopen.
            os.close(fd)
            fd = None
        _finish_staged_compact(path)
        marker = _find_prune_marker(path)
        if marker is not None:
            stale = marker[0] == _MARKER_TRUNCATE and marker[1] == 0
        _finish_prune_marker(path)
        if fd is None or stale:
            # Either Windows cleanup above or a truncate marker finishing
            # onto the live log left this descriptor pointing at a stale
            # (now unlinked) inode; reopen so the record reaches the live
            # log rather than an orphaned file.
            if fd is not None:
                os.close(fd)
            fd = _open_locked(path, create=True)
        # One line per write loop on an O_APPEND descriptor: the kernel
        # appends each write atomically, so concurrent processes do not
        # tear or interleave records.
        view = memoryview(payload)
        while view:
            written = os.write(fd, view)
            view = view[written:]
    finally:
        if fd is not None:
            os.close(fd)


def _segment_members(path):
    """Return the segment-set member file paths in read/write order.

    Numbered segments come first in ascending order, followed by the
    current log when it exists.  The ``path.compact`` staging file is
    never part of this list: while present it replaces the whole list
    via :func:`_resolve_members`.
    """
    members = [
        f"{path}.{number}" for number in sorted(_segment_numbers(path))
    ]
    if os.path.exists(path):
        members.append(path)
    return members


def _resolve_members(path):
    """Pick the member files to read, honouring published staging state.

    A compaction publishes ``path.compact`` with a single atomic rename
    of a fully written and flushed file, so the name only ever holds a
    complete whole segment.  While it exists -- during cleanup or after a
    crash interrupted cleanup -- it alone is the segment set, and the
    record sequence it yields equals the pre-compaction one item by item.
    A crash earlier, while the merge is still being written, leaves the
    half-written private temp file instead; that never carries this name
    and so neither joins recovery nor affects reads.

    A prune likewise publishes a marker naming the world after eviction:
    a ``t`` marker's own bytes replace the cut segment (lower numbered
    segments are already excluded) and an ``e`` marker excludes every
    numbered segment through its number.  Either way the files still
    awaiting cleanup are hidden, so a reader landing mid-cleanup or after
    a crash already sees exactly the surviving records.
    """
    staging = _staging_path(path)
    if os.path.exists(staging):
        return [staging]
    marker = _find_prune_marker(path)
    if marker is not None:
        kind, number, _count, marker_path = marker
        numbers = sorted(_segment_numbers(path))
        if kind == _MARKER_TRUNCATE:
            if number == 0:
                # The marker replaces the live log; every numbered
                # segment is older and awaits unlinking.
                return [marker_path]
            members = [marker_path]
            members += [f"{path}.{n}" for n in numbers if n > number]
            if os.path.exists(path):
                members.append(path)
            return members
        members = [f"{path}.{n}" for n in numbers if n > number]
        if number != 0 and os.path.exists(path):
            # A marker cutting at the live log excludes that log until
            # cleanup has emptied it; its old bytes are still evicted.
            members.append(path)
        return members
    return _segment_members(path)


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
    segment order, followed by the current log at ``path``.  Files are
    read line by line, so the segment set is never loaded into memory and
    a single line may be arbitrarily long.

    When a compaction has staged its merged segment at ``path.compact``
    but has not finished cleanup, that staging file alone is read; a
    staging file left behind by a crashed compaction is ignored until
    another append, rotation or compaction finishes it.

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

    members = _resolve_members(path)
    if not members:
        if _find_prune_marker(path) is not None:
            # A total eviction is published while its cleanup has not yet
            # recreated an empty live log: the committed set reads empty.
            def _generate():
                yield from ()

            return _generate()
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


def _iter_raw_records(path, chunksize=_READ_CHUNK):
    """Yield each complete record line's raw bytes with its newline.

    The chunked walk mirrors :func:`_iter_lines`, but lines are never
    parsed or re-rendered: blank lines are skipped exactly as on read,
    the torn unterminated tail is dropped, and every other complete line
    comes back verbatim with ``\\n`` reattached, so byte-level content
    such as ``-0.0``, huge counters and key ordering survives compaction
    untouched.
    """
    fd = os.open(path, os.O_RDONLY)
    pending = b""
    try:
        while True:
            chunk = os.read(fd, chunksize)
            if not chunk:
                break
            pending += chunk
            *complete, pending = pending.split(b"\n")
            for raw in complete:
                if raw and raw != b"\r":
                    yield raw + b"\n"
    finally:
        os.close(fd)


def _truncate_live_log(path):
    """Shrink the current log to zero bytes without unlinking its path.

    Keeping the inode and path in place means a process that waits on the
    lock during compaction never observes -- and so never recreates -- a
    missing live log: once it takes the lock the path is still the same
    empty file.
    """
    fd = os.open(
        path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_APPEND, 0o666
    )
    os.close(fd)


def _finish_staged_compact(path):
    """Finish compaction cleanup when a staging file is present.

    Called with the live-log lock held, both at the end of
    :func:`compact_metrics` and by any append, rotation or compaction
    that finds a staging file left by a crashed cleanup.  The staging
    file only ever holds a whole fully landed segment, so finishing is
    pure filesystem cleanup.

    The staging file stays the sole authoritative member (see
    :func:`_resolve_members`) until the final step: old numbered
    segments are removed and the current log is emptied while reads
    still resolve to staging, and only then is staging renamed onto
    ``path.1``.  The rename is atomic, so a crash at any point leaves
    either staging alone or the finished set, and the readable record
    sequence equals the pre-compaction one item by item throughout.
    """
    staging = _staging_path(path)
    if not os.path.exists(staging):
        return
    for number in _segment_numbers(path):
        os.unlink(f"{path}.{number}")
    _truncate_live_log(path)
    os.replace(staging, f"{path}.1")


def compact_metrics(path):
    """Merge every segment and the current log into one segment.

    All records are copied into the fixed segment ``path.1`` in write
    order; the current log is left empty afterwards.  Records move byte
    for byte with their original newlines -- nothing is re-rendered,
    de-duplicated or reordered, so ``-0.0``, oversized counters and key
    order are all preserved.  Residual torn lines are not records and
    are dropped; blank lines are skipped as on read; overlong single
    lines move as-is.  A completely empty set still produces an empty
    ``path.1``.

    The merge is first written to a private temporary file and only
    renamed onto ``path.compact`` after the whole file has landed and
    been flushed, so that name always holds a complete whole segment:
    a half-written file left by a crash never joins recovery and never
    affects reads.  Once staged, recovery and streaming read only that
    segment; cleanup publishes it as ``path.1``, removes the other
    numbered segments and empties the current log.  An interrupted
    cleanup is finished transparently by the next append, rotation or
    compaction.

    Raises:
        FileNotFoundError: ``path`` does not exist.
        IsADirectoryError: ``path`` is a directory.
        OSError: ``path`` is not a string, or locking or writing the
            merged segment fails.  A failed write removes the private
            temporary file and leaves the segment set untouched.
    """
    if not isinstance(path, str):
        raise OSError(f"log path must be a string, got {type(path).__name__}")
    if os.path.isdir(path):
        raise IsADirectoryError(f"log path is a directory: {path!r}")

    fd = _open_locked(path, create=False)
    try:
        if fcntl is None:
            # The descriptor only carries the POSIX lock; drop it before
            # the cleanup renames replace segment paths (Windows rejects
            # replacing an open path) and reopen nothing here -- the merge
            # opens its own descriptors.
            os.close(fd)
            fd = None
        # A previous compaction may have staged its segment and died
        # before cleanup; settle that world before merging again.
        _finish_staged_compact(path)
        _finish_prune_marker(path)

        staging = _staging_path(path)
        tmp = staging + ".tmp"
        try:
            out = os.open(
                tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o666
            )
            try:
                for member in _segment_members(path):
                    for raw in _iter_raw_records(member):
                        view = memoryview(raw)
                        while view:
                            written = os.write(out, view)
                            view = view[written:]
                os.fsync(out)
            finally:
                os.close(out)

            # Atomic publication: from this instant the whole merged
            # segment is the only member reads may use during cleanup.
            os.rename(tmp, staging)
        except BaseException:
            # Nothing reached the staging name: the private temp is the
            # only file touched, so removing it leaves the old segments
            # and current log exactly as they were.
            try:
                os.unlink(tmp)
            except FileNotFoundError:
                pass
            raise
        _finish_staged_compact(path)
    finally:
        if fd is not None:
            os.close(fd)


def prune_metrics(path, quota):
    """Enforce a retention ``quota`` by evicting records oldest first.

    ``quota`` is the greatest number of records the segment set keeps;
    records are dropped whole, in write order, and a record is never split
    in two.  A quota of zero evicts every record; a quota at or above the
    current record total evicts nothing.  Empty segments and segments
    holding only a torn tail consume no quota.  Only the one segment the
    cut lands inside is rewritten -- with its surviving complete records
    copied byte for byte, blank lines and torn tails dropped -- while
    every wholly evicted segment is simply deleted, so the work grows with
    the amount evicted, not with the segment set.

    Evicted records keep occupying their original write-order positions:
    the cumulative number of evicted records is remembered in a
    ``path.pruned`` sidecar and a read position is always an ordinal in
    write order.  See :func:`resume_metrics`.

    The eviction first lands in a private temporary file and is then
    published under a single marker name (``path.prune.<kind>.<n>.<c>``)
    with one atomic rename, so the name only ever names a complete
    boundary: a crash before publication leaves the segment set exactly as
    it was, and a crash afterwards leaves a fully evicted-to-boundary set
    whose cleanup the next append, rotation, compaction or prune finishes
    transparently.  An unfinished compaction is settled first.

    Raises:
        FileNotFoundError: ``path`` does not exist.
        IsADirectoryError: ``path`` is a directory.
        OSError: ``path`` is not a string, or locking or writing fails.
        TypeError: ``quota`` is not an integer (booleans do not count).
        ValueError: ``quota`` is negative.
    """
    if not isinstance(path, str):
        raise OSError(f"log path must be a string, got {type(path).__name__}")
    if isinstance(quota, bool) or not isinstance(quota, int):
        raise TypeError(
            f"retention quota must be an integer, got {type(quota).__name__}"
        )
    if quota < 0:
        raise ValueError("retention quota must not be negative")
    if os.path.isdir(path):
        raise IsADirectoryError(f"log path is a directory: {path!r}")

    fd = _open_locked(path, create=False)
    try:
        if fcntl is None:
            # No lock is carried on this platform; release the live-log
            # descriptor before cleanup replaces or truncates named paths
            # (Windows rejects replacing an open file).
            os.close(fd)
            fd = None
        _finish_staged_compact(path)
        _finish_prune_marker(path)

        members = _segment_members(path)
        if not members:
            raise FileNotFoundError(f"no metrics log or segments at {path!r}")
        pruned = _read_pruned(path)

        # Locate the cut from the newest end by reserving the newest
        # ``quota`` records as survivors: wholly evicted older segments are
        # never opened, and only the one segment the cut lands in is ever
        # rewritten.  ``cut`` is the oldest member that still keeps a
        # surviving record; ``survivors`` is how many of its records keep.
        need = quota
        cut = None
        survivors = 0
        boundary = False
        for index in range(len(members) - 1, -1, -1):
            count = sum(1 for _ in _iter_raw_records(members[index]))
            if count == need:
                if need == 0:
                    # An empty member at the surviving end carries no
                    # record; step past it toward the records below.
                    continue
                if index == 0:
                    # The quota equals the whole record total: nothing to
                    # evict.  (Older empty members above were skipped via
                    # the ``evicted == 0`` check below regardless.)
                    cut = None
                    break
                # The cut lands on a member boundary: this member wholly
                # survives and every older member is evicted.
                cut = index
                survivors = count
                boundary = True
                break
            if count > need:
                cut = index
                survivors = need
                break
            need -= count
        if cut is None:
            return

        # Records older than the cut segment are evicted whole; count them
        # only to keep the cumulative sidecar exact -- that walk scales
        # with the evicted amount, never with the survivors.
        cut_member = members[cut]
        cut_count = sum(1 for _ in _iter_raw_records(cut_member))
        evicted = 0
        for index in range(cut):
            evicted += sum(1 for _ in _iter_raw_records(members[index]))
        evicted += cut_count - survivors
        if evicted == 0:
            # Quota already met (e.g. equals the total with empty members
            # between records); no file changes at all.
            return
        new_pruned = pruned + evicted

        if boundary:
            # members[cut] wholly survives; the member just older than it
            # is the newest fully evicted one (always a numbered segment).
            evicted_member = members[cut - 1]
            cut_number = int(evicted_member[len(path) + 1:])
            kind = _MARKER_EVICT
        elif survivors == 0:
            # The cut member itself loses every record.  The live log is
            # emptied via a truncate marker; a numbered segment and all
            # older ones are deleted wholesale, never rewritten empty.
            if cut_member == path:
                cut_number = 0
                kind = _MARKER_TRUNCATE
            else:
                cut_number = int(cut_member[len(path) + 1:])
                kind = _MARKER_EVICT
        elif cut_member == path:
            # A partial cut in the live log replaces it with the marker's
            # own bytes, so the path never disappears.
            cut_number = 0
            kind = _MARKER_TRUNCATE
        else:
            cut_number = int(cut_member[len(path) + 1:])
            kind = _MARKER_TRUNCATE
        marker = (
            f"{path}{_PRUNE_MARKER_SUFFIX}{kind}.{cut_number}.{new_pruned}"
        )
        tmp = marker + ".tmp"
        out = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o666)
        try:
            if kind == _MARKER_TRUNCATE:
                # Copy only the newest ``survivors`` complete records of
                # the cut segment; torn tails and blank lines never move.
                drop_within = cut_count - survivors
                record_index = 0
                for raw in _iter_raw_records(cut_member):
                    record_index += 1
                    if record_index <= drop_within:
                        continue
                    view = memoryview(raw)
                    while view:
                        written = os.write(out, view)
                        view = view[written:]
            os.fsync(out)
        finally:
            os.close(out)
        try:
            # Atomic publication: from this instant reads resolve through
            # the marker to exactly the surviving record sequence.
            os.replace(tmp, marker)
        except BaseException:
            try:
                os.unlink(tmp)
            except FileNotFoundError:
                pass
            raise
        _finish_prune_marker(path)
    finally:
        if fd is not None:
            os.close(fd)


def resume_metrics(path, position=0):
    """Stream records starting at integer read position ``position``.

    ``position`` is the record ordinal in write order, counting records
    already read from the start of the segment set -- records since pruned
    away included, since pruned records keep their original ordinals and
    the segment set remembers how many were evicted.  Omitting it (or
    passing 0) reads from the oldest surviving record.  A position inside
    the pruned prefix, or exactly at the prune boundary, likewise starts
    at the oldest surviving record.  The position needs no conversion
    across appends, rotations, compactions, pruning or crash cleanup.  It
    is only out of range when it exceeds the record total *including*
    evicted records; a position equal to the total yields an empty stream.

    Exactly like :func:`iter_metrics`, reading is line by line and takes
    time linear in the skipped-and-yielded prefix without materialising
    the segment set; an overrun is reported once the end is reached.  At
    any instant -- before or after pruning -- the result equals the
    matching slice of a fresh recovery from the start.

    Path-level problems are reported when this function is called; line
    content problems and an out-of-range position are reported lazily
    while iterating.

    Raises:
        FileNotFoundError: neither ``path`` nor any segment exists.
        IsADirectoryError: ``path`` is a directory.
        OSError: ``path`` is not a string.
        TypeError: ``position`` is not an integer (booleans do not count).
        ValueError: ``position`` is negative or past the record total
            (the latter raised while iterating), or a complete line is
            not a metrics JSON object.
    """
    # Eager path validation first, matching iter_metrics/recover_metrics.
    records = iter_metrics(path)
    if isinstance(position, bool) or not isinstance(position, int):
        raise TypeError(
            f"read position must be an integer, got {type(position).__name__}"
        )
    if position < 0:
        raise ValueError("read position must not be negative")

    def _generate():
        # The sidecar only reaches its new value during marker cleanup, so
        # while a marker is published its own carried count is authoritative.
        marker = _find_prune_marker(path)
        pruned = marker[2] if marker is not None else _read_pruned(path)
        # Translate the write-order ordinal into a physical offset among
        # the surviving records; a position in the pruned prefix starts at
        # the oldest survivor rather than overshooting it.
        skip = max(position - pruned, 0)
        physical = 0
        for record in records:
            physical += 1
            if skip:
                skip -= 1
                continue
            yield record
        if position > pruned + physical:
            raise ValueError(
                f"read position {position} exceeds the record total of "
                f"{pruned + physical}"
            )

    return _generate()
