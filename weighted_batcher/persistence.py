"""Durable append-only metric log with rotation, compaction, pruning and resume.

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

:func:`prune_metrics` enforces a retention quota by dropping whole records
from the oldest end.  Evicted records keep their write-order ordinal: the
segment set remembers how many records have been pruned in ``path.prune``,
and no operation renumbers surviving records.  :func:`resume_metrics`
resumes the same write-order walk at an integer read position; the
position needs no translation across appends, rotations, compactions,
pruning or crash cleanup.

:func:`snapshot_metrics` pins the complete record sequence at creation
time into a snapshot-private copy (``path.snap.<handle>``) and records the
handle in the snapshot registry (``path.snapshots``), so later appends,
rotations, compactions and prunes never change what the snapshot reads.
:func:`resume_snapshot_metrics` streams a snapshot from a write-order
read position with the same ordinal rules as :func:`resume_metrics`, and
:func:`release_metrics` retires a handle and its private copy.  A copy
left behind by a crash before its handle was registered is not readable
and is removed transparently by the next write.

:func:`snapshot_diff_metrics` reconciles two snapshots as the old and new
sides of one write-order sequence: records are aligned by their write
order ordinal (the pruned prefix each snapshot was taken after, plus its
index inside the snapshot), and each aligned pair whose bytes differ is a
``changed`` difference, an old-only ordinal is ``missing`` and a
new-only ordinal is ``added``.  Differences are produced one JSON line at
a time straight from the two private copies, which are read
interleavingly without being loaded whole and without taking the writer
lock.  :func:`resume_snapshot_delta_metrics` is the cursor form of
:func:`resume_snapshot_metrics`: it streams the pinned records a cursor
position has not consumed yet, from the private copy alone.

Every data-file descriptor is released before a rename on platforms that
cannot rename open files (Windows), so sealing, compaction and pruning no
longer raise ``PermissionError`` there.
"""

from __future__ import annotations

import json
import os

from . import parse_metrics, render_metrics

try:  # POSIX-only; the lock guards O_APPEND and serialises every mutation.
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
    "snapshot_metrics",
    "release_metrics",
    "resume_snapshot_metrics",
    "snapshot_diff_metrics",
    "resume_snapshot_delta_metrics",
]

_READ_CHUNK = 1 << 20
# Suffix of the staging file compaction writes before cleanup begins; it
# only ever names a whole, fully-fsynced segment that has been published.
_COMPACT_SUFFIX = ".compact"
# Prune state.  Finalised content is the decimal count of pruned records;
# mid-cleanup content is a JSON plan (see _finish_staged_prune).
_PRUNE_SUFFIX = ".prune"
# Published content of the one segment prune truncates, used while the
# JSON prune plan is still finishing cleanup.
_TRIM_SUFFIX = ".trim"
_PRUNE_TMP_SUFFIX = ".prune.tmp"
_TRIM_TMP_SUFFIX = ".trim.tmp"
# Snapshot state.  ``path.snapshots`` is the registry of live handles, a
# JSON object mapping each handle to ``{"pruned": ..., "total": ...}`` as
# of creation; ``path.snap.<handle>`` is that snapshot's private copy of
# the complete record sequence, so pinned records survive any later
# compaction or prune of the segment set itself.
_REGISTRY_SUFFIX = ".snapshots"
_REGISTRY_TMP_SUFFIX = ".snapshots.tmp"
_SNAP_COPY_SUFFIX = ".snap."
# Label of the live log inside a prune plan; numbered segments use their
# segment number.  Labels keep the plan independent of how ``path`` was
# spelled when the plan was written.
_LIVE_LABEL = -1


def _segment_numbers(path):
    """Return the already-used segment numbers for ``path`` as a set.

    Every non-numeric sidecar (``path.compact``, ``path.prune``,
    ``path.trim`` and their temporaries) is deliberately invisible here.
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
            if suffix.isascii() and suffix.isdigit():
                numbers.add(int(suffix))
    return numbers


def _staging_path(path):
    """The compaction staging file path (``path.compact``)."""
    return path + _COMPACT_SUFFIX


def _write_bytes(fd, payload):
    """Write all of ``payload`` to ``fd``, retrying short writes."""
    view = memoryview(payload)
    while view:
        written = os.write(fd, view)
        view = view[written:]


def _unlink_if_exists(path):
    """Remove ``path``; a missing file is already the desired state."""
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


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
    reported (rotation, compaction, pruning).  A lock failure closes the
    descriptor and leaves nothing written.

    On platforms without :mod:`fcntl` (Windows) there is no process lock;
    the descriptor is returned unlocked, and callers that rename the
    live path release every handle first -- Windows cannot rename an open
    file.
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


def _mutation_lock(path, create):
    """Lock the segment set for a mutation, or just prove the log exists.

    Returns the live-log lock fd on POSIX (caller closes it) or ``None``
    on platforms without :mod:`fcntl`, where the existence check has
    already closed its descriptor and no rename may happen with a handle
    still open.
    """
    fd = _open_locked(path, create)
    if fcntl is None:
        os.close(fd)
        return None
    return fd


def _relock_after_settle(fd, path, create):
    """Re-lock the live log when settling swapped its inode.

    Finishing a crashed quota-zero prune replaces the live file, so the
    lock acquired before settling dangles on the replaced inode and the
    new live log would otherwise be unlocked for the rest of the
    operation.  Re-open and re-lock the settled path (the same
    follow-rotation loop :func:`_open_locked` uses), returning the new
    fd; ``None`` (a lock-less platform) is returned unchanged.
    """
    if fd is None:
        return None
    if _same_inode(fd, path):
        return fd
    os.close(fd)
    return _open_locked(path, create)


def _member_label(name, path):
    """Map a member path to its prune-plan label (number or live marker)."""
    return _LIVE_LABEL if name == path else int(name[len(path) + 1:])


def _segment_members(path):
    """Return the segment-set member file paths in read/write order.

    Numbered segments come first in ascending order, followed by the
    current log (which is always assumed to exist when this is called
    inside a writer that has just opened it).  Sidecars never appear
    here; the ``path.compact`` staging file replaces the whole list in
    :func:`_resolve_members` while present.
    """
    members = [
        f"{path}.{number}" for number in sorted(_segment_numbers(path))
    ]
    members.append(path)
    return members


def _label_member(label, path):
    """Map a prune-plan label back to its member file path."""
    return path if label == _LIVE_LABEL else f"{path}.{label}"


def _read_prune_state(path):
    """Return ``(pruned_count, plan)`` from the prune marker.

    A finalised marker holds the decimal pruned-record count; a pending
    one holds the JSON plan dict written by :func:`prune_metrics`, in
    which case the count it carries is returned alongside the plan.  With
    no marker the result is ``(0, None)``.
    """
    try:
        fd = os.open(path + _PRUNE_SUFFIX, os.O_RDONLY)
    except FileNotFoundError:
        return 0, None
    try:
        raw = b""
        while True:
            chunk = os.read(fd, _READ_CHUNK)
            if not chunk:
                break
            raw += chunk
    finally:
        os.close(fd)
    data = json.loads(raw.decode("utf-8"))
    if isinstance(data, dict):
        return int(data["pruned"]), data
    return int(data), None


def _replace_marker(path, text):
    """Atomically publish ``text`` as the prune marker and fsync it."""
    tmp = path + _PRUNE_TMP_SUFFIX
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o666)
    try:
        _write_bytes(fd, text.encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp, path + _PRUNE_SUFFIX)


def _read_snapshot_registry(path):
    """Return the live snapshot handles mapped to their metadata.

    The registry is a JSON object ``{handle: {"pruned": n, "total": n}}``
    published atomically, so a read never observes a half-written one; a
    missing registry simply means no snapshots exist.
    """
    try:
        fd = os.open(path + _REGISTRY_SUFFIX, os.O_RDONLY)
    except FileNotFoundError:
        return {}
    try:
        raw = b""
        while True:
            chunk = os.read(fd, _READ_CHUNK)
            if not chunk:
                break
            raw += chunk
    finally:
        os.close(fd)
    return json.loads(raw.decode("utf-8"))


def _write_snapshot_registry(path, registry):
    """Atomically publish the snapshot registry and fsync it."""
    tmp = path + _REGISTRY_TMP_SUFFIX
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o666)
    try:
        payload = json.dumps(registry, separators=(",", ":"))
        _write_bytes(fd, payload.encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp, path + _REGISTRY_SUFFIX)


def _finish_snapshot_orphans(path):
    """Remove snapshot copies no registered handle refers to.

    A crash between staging a snapshot's private copy and publishing its
    handle leaves the copy unnamed by the registry; such a half-finished
    handle never participates in reads, and its copy is deleted here by
    the next write.  Copies of live handles are always kept, so a prune
    (even with quota zero) or a compaction never makes a pinned record
    silently disappear.
    """
    directory = os.path.dirname(path) or "."
    prefix = os.path.basename(path) + _SNAP_COPY_SUFFIX
    try:
        names = os.listdir(directory)
    except FileNotFoundError:
        return
    live = {prefix + handle for handle in _read_snapshot_registry(path)}
    for name in names:
        if name.startswith(prefix) and name not in live:
            _unlink_if_exists(os.path.join(directory, name))
    _unlink_if_exists(path + _REGISTRY_TMP_SUFFIX)


def _finish_staged_prune(path):
    """Finish prune cleanup when a prune plan is present.

    Called with the writer lock held, by :func:`prune_metrics` itself and
    by any append, rotation or compaction that finds a plan a crashed
    prune left behind.  The plan is the commit point: the trimmed boundary
    segment has already landed whole at ``path.trim``, so finishing is
    pure filesystem cleanup -- delete the evicted members, publish the
    trimmed boundary atomically, then finalise the marker to the pruned
    count.  Lock-free readers reconcile a half-finished plan themselves in
    :func:`_resolve_members`, so a crash at any point exposes either the
    old segment set or the fully pruned one, never a mixture.
    """
    marker = path + _PRUNE_SUFFIX
    if not os.path.exists(marker):
        # A trim left by a crash before the plan was published names no
        # committed state and is discarded.
        _unlink_if_exists(path + _TRIM_SUFFIX)
        _unlink_if_exists(path + _TRIM_TMP_SUFFIX)
        return
    _pruned, plan = _read_prune_state(path)
    if plan is None:
        # Finalised marker: only an unpublished trim could linger.
        _unlink_if_exists(path + _TRIM_SUFFIX)
        _unlink_if_exists(path + _TRIM_TMP_SUFFIX)
        return
    for label in plan.get("evict", ()):
        _unlink_if_exists(_label_member(label, path))
    target = plan.get("trim")
    trim = path + _TRIM_SUFFIX
    if target is not None and os.path.exists(trim):
        # The plan is only published once trim holds a complete whole
        # segment.  Replace (not unlink-then-rename) swaps the boundary
        # atomically and creates the live log in the quota-zero case.
        # No data-file handle is held open here on Windows; on POSIX the
        # caller's lock fd is allowed to dangle on the replaced inode.
        os.replace(trim, _label_member(target, path))
    _replace_marker(path, str(int(plan["pruned"])))


def _marker_observation(path):
    """One immutable description of the prune marker.

    Returns the marker text as bytes (``b""`` when absent).  Two equal
    observations bracket a window in which no prune committed, so the
    directory listing taken between them belongs to that marker's world.
    """
    try:
        fd = os.open(path + _PRUNE_SUFFIX, os.O_RDONLY)
    except FileNotFoundError:
        return b""
    try:
        raw = b""
        while True:
            chunk = os.read(fd, _READ_CHUNK)
            if not chunk:
                break
            raw += chunk
    finally:
        os.close(fd)
    return raw


def _resolve_members(path):
    """Pick the member files and pruned count to read.

    Returns ``(members, pruned)``.  Numbered segments come first in
    ascending order, followed by the current log when it exists; the
    pruned count is the number of records already gone from the oldest
    end.

    Resolution is lock-free but crash- and commit-consistent: the prune
    marker is read before and after the directory is listed and the pair
    must match, otherwise a commit straddled the listing and the read
    resolves again.  Pruning commits in the order plan publication,
    filesystem cleanup, marker finalisation, so an unchanged marker
    brackets a directory state that marker reconciles to exactly one
    settled record sequence:

    * A compaction's fully landed ``path.compact`` staging file, while
      present, alone is the segment set.
    * A pending prune plan: evicted members still on disk are ignored,
      and while the trimmed boundary waits at ``path.trim`` it stands in
      for the boundary member; once the boundary is published its staging
      name is gone and the finished member is read directly.
    """
    members = pruned = None
    before = _marker_observation(path)
    for _ in range(1000):
        members, pruned, after = _resolve_once(path, before)
        if after == before:
            return members, pruned
        before = after
    # Extremely contended: return the last self-consistent pair rather
    # than loop forever; the data it reads is itself whole.
    return members, pruned


def _resolve_once(path, marker_raw):
    """Resolve one directory state, returning it with the post-listing marker."""
    if marker_raw:
        data = json.loads(marker_raw.decode("utf-8"))
        pruned, plan = (
            (int(data["pruned"]), data) if isinstance(data, dict)
            else (int(data), None)
        )
    else:
        pruned, plan = 0, None

    staging = _staging_path(path)
    if os.path.exists(staging):
        members = [staging]
    elif plan is None:
        members = [
            f"{path}.{number}" for number in sorted(_segment_numbers(path))
        ]
        if os.path.exists(path):
            members.append(path)
    else:
        evict = set(plan.get("evict", ()))
        target = plan.get("trim")
        trim = path + _TRIM_SUFFIX
        use_trim = target is not None and os.path.exists(trim)
        labels = sorted(_segment_numbers(path))
        # Include the live log when it exists, and also when a quota-zero
        # cleanup is an instant away from replacing it from its published
        # trim, so the committed (empty) set never momentarily vanishes.
        if os.path.exists(path) or (use_trim and target == _LIVE_LABEL):
            labels.append(_LIVE_LABEL)
        members = []
        for label in labels:
            if label in evict:
                continue
            if use_trim and label == target:
                members.append(trim)
            else:
                members.append(_label_member(label, path))

    return members, pruned, _marker_observation(path)


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
    another append, rotation or compaction finishes it.  A pending prune
    is reconciled the same way: only surviving records are yielded.

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

    members, _pruned = _resolve_members(path)
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


def _iter_raw_records(path, chunksize=_READ_CHUNK):
    """Yield each complete record line's raw bytes with its newline.

    The chunked walk mirrors :func:`_iter_lines`, but lines are never
    parsed or re-rendered: blank lines are skipped exactly as on read,
    the torn unterminated tail is dropped, and every other complete line
    comes back verbatim with ``\\n`` reattached, so byte-level content
    such as ``-0.0``, huge counters and key ordering survives compaction
    and pruning untouched.
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

    Called with the writer lock held, both at the end of
    :func:`compact_metrics` and by any append, rotation, compaction or
    prune that finds a staging file left by a crashed cleanup.  The
    staging file only ever holds a whole fully landed segment, so
    finishing is pure filesystem cleanup.

    The staging file stays the sole authoritative member (see
    :func:`_resolve_members`) until the final step: old numbered
    segments are removed and the current log is emptied while reads
    still resolve to staging, and only then is staging renamed onto
    ``path.1``.  Every involved descriptor is closed first, so no handle
    is held across the rename, which therefore also succeeds on Windows.
    The rename is atomic, so a crash at any point leaves either staging
    alone or the finished set, and the readable record sequence equals
    the pre-compaction one item by item.
    """
    staging = _staging_path(path)
    if not os.path.exists(staging):
        return
    for number in _segment_numbers(path):
        os.unlink(f"{path}.{number}")
    _truncate_live_log(path)
    os.replace(staging, f"{path}.1")


def _finish_pending(path):
    """Settle any half-finished compaction, prune or snapshot before a mutation."""
    _finish_staged_compact(path)
    _finish_staged_prune(path)
    _finish_snapshot_orphans(path)


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

    fd = _mutation_lock(path, create=False)
    try:
        _finish_pending(path)
        fd = _relock_after_settle(fd, path, create=False)
        used = _segment_numbers(path)
        number = (max(used) + 1) if used else 1
        segment = f"{path}.{number}"
        # POSIX: the rename happens while the lock fd is held (an open
        # file may be renamed); the lock makes appenders reopen the path
        # before writing.  Windows has no fd in hand at all, so the
        # PermissionError an open live log used to cause cannot happen.
        os.rename(path, segment)
        # Re-create an empty current log without O_TRUNC: an appender that
        # won the unlocked rename window on a lock-less platform may have
        # landed a record there first, and it belongs in the current log.
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

    If another process rotates, compacts or prunes the log while this
    call waits for the lock, the descriptor is re-opened against the
    settled live file before writing, so no record is misdirected.

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

    fd = _mutation_lock(path, create=True)
    try:
        _finish_pending(path)
        if fd is None:
            # Lock-less platform (Windows): the existence-proving handle is
            # already closed and settling ran first, so open a fresh
            # O_APPEND descriptor for the write and close it again, leaving
            # no handle open across a later rename.
            out = os.open(
                path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o666
            )
            try:
                _write_bytes(out, payload)
            finally:
                os.close(out)
        else:
            # Settling may have replaced the live inode (a crashed
            # quota-zero prune); lock the settled file before writing.
            fd = _relock_after_settle(fd, path, create=True)
            # One line per write loop on the locked O_APPEND descriptor:
            # the kernel appends each write atomically, so concurrent
            # processes do not tear or interleave records.
            _write_bytes(fd, payload)
    finally:
        if fd is not None:
            os.close(fd)


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
    been flushed (its descriptor already closed, so the rename also
    works on Windows), so that name always holds a complete whole
    segment: a half-written file left by a crash never joins recovery
    and never affects reads.  Once staged, recovery and streaming read
    only that segment; cleanup publishes it as ``path.1``, removes the
    other numbered segments and empties the current log.  An interrupted
    cleanup is finished transparently by the next append, rotation,
    compaction or prune.  The pruned-record count stored by earlier
    prunes is left untouched.

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

    fd = _mutation_lock(path, create=False)
    try:
        # A previous compaction may have staged its segment and died
        # before cleanup; settle that world before merging again.  So may
        # a previous prune.
        _finish_pending(path)
        fd = _relock_after_settle(fd, path, create=False)

        staging = _staging_path(path)
        tmp = staging + ".tmp"
        try:
            out = os.open(
                tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o666
            )
            try:
                for member in _segment_members(path):
                    for raw in _iter_raw_records(member):
                        _write_bytes(out, raw)
                os.fsync(out)
            finally:
                # Closed before the rename: on Windows an open file cannot
                # be renamed.
                os.close(out)

            # Atomic publication: from this instant the whole merged
            # segment is the only member reads may use during cleanup.
            os.rename(tmp, staging)
        except BaseException:
            # Nothing reached the staging name: the private temp is the
            # only file touched, so removing it leaves the old segments
            # and current log exactly as they were.
            _unlink_if_exists(tmp)
            raise
        _finish_staged_compact(path)
    finally:
        if fd is not None:
            os.close(fd)


def _stage_trim(path, source, skip):
    """Publish the surviving tail of one truncated member at ``path.trim``.

    The first ``skip`` complete records of ``source`` are dropped; every
    later complete record is copied byte for byte with its original
    newline.  Blank lines and the torn unterminated tail are discarded,
    matching a read of the truncated segment.  The private temp is fully
    written and flushed and its descriptor closed before the atomic
    rename, so ``path.trim`` only ever holds a complete whole segment and
    the rename works on Windows.
    """
    tmp = path + _TRIM_TMP_SUFFIX
    out = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o666)
    try:
        seen = 0
        for raw in _iter_raw_records(source):
            if seen < skip:
                seen += 1
                continue
            _write_bytes(out, raw)
        os.fsync(out)
    finally:
        os.close(out)
    os.rename(tmp, path + _TRIM_SUFFIX)


def prune_metrics(path, quota):
    """Retain at most ``quota`` records, dropping the oldest whole records.

    ``quota`` is the maximum number of records kept in the segment set;
    when it already holds no more than ``quota`` records nothing is
    removed, and ``quota`` 0 drops every record.  Records are dropped in
    write order from the oldest end, one complete line at a time -- a
    record is never split.  Empty segments and segments holding only a
    torn tail spend no quota and are removed with the prefix ahead of
    them.  Surviving records move byte for byte: order, original
    newlines, ``-0.0``, oversized counters and key order are unchanged,
    and an overlong single line still counts as exactly one record.

    Evicted records keep their write-order ordinals: the set remembers
    the total number pruned, so read positions never shift (see
    :func:`resume_metrics`).  Whole evicted segments are deleted; only
    the single segment the cut lands inside is rewritten, via a
    ``path.trim`` staging file and a JSON plan in ``path.prune``, so a
    crash leaves either the old set or a set pruned to a complete record
    boundary -- a half-finished trim never joins recovery.  Pruning is
    serialised with appends, rotations and compactions by the live-log
    lock, and an unfinished compaction is settled first; every accepted
    record is therefore either kept or dropped, never lost or duplicated.

    Raises:
        FileNotFoundError: ``path`` does not exist.
        IsADirectoryError: ``path`` is a directory.
        TypeError: ``quota`` is not an integer (booleans do not count).
        ValueError: ``quota`` is negative.
        OSError: ``path`` is not a string, or locking or writing the
            truncated segment fails.
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

    fd = _mutation_lock(path, create=False)
    try:
        _finish_pending(path)
        fd = _relock_after_settle(fd, path, create=False)

        members = _segment_members(path)
        # One streaming count pass (the same cheap line walk reads use)
        # locates the cut; only the single boundary member is rewritten.
        counts = [sum(1 for _ in _iter_raw_records(member))
                  for member in members]
        total = sum(counts)
        if total <= quota:
            return
        drop = total - quota

        index = 0
        cumulative = 0
        while index < len(members) and drop >= cumulative + counts[index]:
            cumulative += counts[index]
            index += 1
        if index < len(members):
            boundary_index = index
            skip = drop - cumulative
        else:
            # Every record lies inside the dropped prefix.  The live log
            # is the last member and always exists, so it is the boundary
            # and is rewritten empty; everything before it is deleted.
            boundary_index = len(members) - 1
            skip = counts[-1]
        boundary = members[boundary_index]
        evict = [
            _member_label(name, path) for name in members[:boundary_index]
        ]

        pruned = _read_prune_state(path)[0] + drop
        if skip:
            _stage_trim(path, boundary, skip)
            target = _member_label(boundary, path)
        else:
            # The cut lands exactly on a member boundary: that member is
            # already the oldest survivor and needs no rewrite.
            target = None
        # Publishing the plan is the commit point; from here on readers
        # reconcile to the committed set even if cleanup is interrupted.
        plan = json.dumps(
            {"pruned": pruned, "evict": evict, "trim": target},
            separators=(",", ":"),
        )
        _replace_marker(path, plan)
        _finish_staged_prune(path)
    finally:
        if fd is not None:
            os.close(fd)


def resume_metrics(path, position=0):
    """Stream records starting at integer read position ``position``.

    ``position`` counts records from the start of write order, exactly
    the ordinals :func:`iter_metrics` walks with pruning applied:
    records pruned from the oldest end keep their ordinals and the set
    remembers how many are gone.  Streaming therefore starts at the
    first surviving record whose ordinal is not less than ``position``;
    a position inside the pruned prefix or equal to the prune point
    starts at the oldest surviving record.  The position needs no
    conversion across appends, rotations, compactions, prunes or crash
    cleanup.

    It is out of range only when ``position`` exceeds the record total,
    which includes pruned records; equal to the total the stream is
    simply empty.  At every moment the stream equals the corresponding
    ordinal slice of a fresh from-the-start recovery (the member set and
    the pruned count are resolved together in one call).  Exactly like
    :func:`iter_metrics`, reading is line by line and linear without
    materialising the segment set; an overrun is reported once the end
    is reached.

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
    if not isinstance(path, str):
        raise OSError(f"log path must be a string, got {type(path).__name__}")
    if os.path.isdir(path):
        raise IsADirectoryError(f"log path is a directory: {path!r}")
    # One resolution call supplies both the walk and the slice boundary,
    # resolved in commit order, so they describe the same settled state.
    members, pruned = _resolve_members(path)
    if not members:
        raise FileNotFoundError(f"no metrics log or segments at {path!r}")
    if isinstance(position, bool) or not isinstance(position, int):
        raise TypeError(
            f"read position must be an integer, got {type(position).__name__}"
        )
    if position < 0:
        raise ValueError("read position must not be negative")

    def _generate():
        seen = 0
        for member in members:
            for record in _parse_file(member, member):
                if pruned + seen >= position:
                    yield record
                seen += 1
        if position > pruned + seen:
            raise ValueError(
                f"read position {position} exceeds the record total of "
                f"{pruned + seen}"
            )

    return _generate()


def snapshot_metrics(path):
    """Pin the current record sequence and return a persistable handle.

    The complete record sequence as of this call -- every surviving
    record in write order -- is copied byte for byte into a
    snapshot-private file ``path.snap.<handle>``, and the handle is
    registered in ``path.snapshots`` together with the pruned-record
    count and the record total (pruned records included) at creation.
    From then on no append, rotation, compaction or prune changes what
    the snapshot reads: the pinned records live in the private copy, so
    they survive even a quota-zero prune of the segment set, and a
    compaction that rewrites members leaves the creation-time bytes
    untouched.  Records move with their original newlines and are never
    re-rendered, so ``-0.0``, oversized counters and key order are
    preserved exactly.

    The copy is staged under a temporary name, fsynced and atomically
    renamed before the handle is registered, and registering the handle
    is the commit point: a crash earlier leaves an unnamed copy that no
    read can reach and that the next write removes transparently, so a
    half-finished handle never participates in reads.  Snapshot creation
    is serialised with appends, rotations, compactions and prunes by the
    live-log lock, and an unfinished compaction or prune is settled
    first, so every record belongs to exactly one side of the snapshot
    point.  Snapshots are independent of each other, and old segment
    sets with no registry need no migration.

    The returned handle is a plain string: persist it and pass it back
    to :func:`resume_snapshot_metrics` and :func:`release_metrics`.

    Raises:
        FileNotFoundError: ``path`` does not exist.
        IsADirectoryError: ``path`` is a directory.
        OSError: ``path`` is not a string, or locking or writing the
            snapshot copy fails.  A failed write removes the private
            temporary file and leaves the segment set and registry
            untouched.
    """
    if not isinstance(path, str):
        raise OSError(f"log path must be a string, got {type(path).__name__}")
    if os.path.isdir(path):
        raise IsADirectoryError(f"log path is a directory: {path!r}")

    fd = _mutation_lock(path, create=False)
    try:
        _finish_pending(path)
        fd = _relock_after_settle(fd, path, create=False)

        registry = _read_snapshot_registry(path)
        while True:
            handle = os.urandom(8).hex()
            if handle not in registry:
                break
        copy_path = path + _SNAP_COPY_SUFFIX + handle
        tmp = copy_path + ".tmp"
        count = 0
        try:
            out = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o666)
            try:
                for member in _segment_members(path):
                    for raw in _iter_raw_records(member):
                        _write_bytes(out, raw)
                        count += 1
                os.fsync(out)
            finally:
                # Closed before the rename: on Windows an open file
                # cannot be renamed.
                os.close(out)
            os.rename(tmp, copy_path)
        except BaseException:
            # Nothing reached the copy name: the private temp is the
            # only file touched and the registry is not yet updated.
            _unlink_if_exists(tmp)
            raise
        pruned = _read_prune_state(path)[0]
        # Publishing the handle is the commit point; the copy it names
        # is already whole on disk.
        registry[handle] = {"pruned": pruned, "total": pruned + count}
        _write_snapshot_registry(path, registry)
        return handle
    finally:
        if fd is not None:
            os.close(fd)


def release_metrics(path, handle):
    """Release a snapshot handle and delete its private record copy.

    Afterwards the handle is unknown to the segment set: reading it
    through :func:`resume_snapshot_metrics` or releasing it again raises
    :class:`ValueError`.  The registry update is the commit point and
    the copy is unlinked only after it, so a crash never leaves a
    registered handle without its copy; an orphaned copy left by a
    crash is removed by the next write.

    Raises:
        FileNotFoundError: ``path`` does not exist.
        IsADirectoryError: ``path`` is a directory.
        TypeError: ``handle`` is not a string.
        ValueError: ``handle`` is forged, belongs to another segment
            set, or was already released.
        OSError: ``path`` is not a string, or locking or writing the
            registry fails.
    """
    if not isinstance(path, str):
        raise OSError(f"log path must be a string, got {type(path).__name__}")
    if not isinstance(handle, str):
        raise TypeError(
            f"snapshot handle must be a string, got {type(handle).__name__}"
        )
    if os.path.isdir(path):
        raise IsADirectoryError(f"log path is a directory: {path!r}")

    fd = _mutation_lock(path, create=False)
    try:
        _finish_pending(path)
        registry = _read_snapshot_registry(path)
        if handle not in registry:
            raise ValueError(
                f"unknown or already released snapshot handle: {handle!r}"
            )
        del registry[handle]
        _write_snapshot_registry(path, registry)
        _unlink_if_exists(path + _SNAP_COPY_SUFFIX + handle)
    finally:
        if fd is not None:
            os.close(fd)


def resume_snapshot_metrics(path, handle, position=0):
    """Stream a snapshot's records starting at read position ``position``.

    The stream is exactly the record sequence :func:`snapshot_metrics`
    pinned for ``handle``, however the segment set has changed since:
    appends, rotations, compactions and prunes after creation are
    invisible to it, and repeated reads of the same handle always yield
    the creation-time slice, byte for byte.

    ``position`` uses the same write-order ordinals as
    :func:`resume_metrics`: records pruned before the snapshot was taken
    still occupy their ordinals, so a position inside that pruned region
    starts at the oldest record the snapshot holds.  The position is out
    of range only when it exceeds the record total at creation (pruned
    records included); equal to the total the stream is empty.

    Reading is line by line from the snapshot's private copy and never
    touches the segment-set members, so a member pruned or compacted
    away concurrently cannot raise :class:`FileNotFoundError` here.

    Path-level and handle problems are reported when this function is
    called; line content problems and an out-of-range position are
    reported lazily while iterating.

    Raises:
        FileNotFoundError: neither ``path`` nor any segment exists.
        IsADirectoryError: ``path`` is a directory.
        OSError: ``path`` is not a string.
        TypeError: ``handle`` is not a string, or ``position`` is not an
            integer (booleans do not count).
        ValueError: ``handle`` is forged, belongs to another segment set
            or was released; ``position`` is negative or past the
            snapshot's record total (the latter raised while iterating);
            or a complete line is not a metrics JSON object.
    """
    if not isinstance(path, str):
        raise OSError(f"log path must be a string, got {type(path).__name__}")
    if not isinstance(handle, str):
        raise TypeError(
            f"snapshot handle must be a string, got {type(handle).__name__}"
        )
    if isinstance(position, bool) or not isinstance(position, int):
        raise TypeError(
            f"read position must be an integer, got {type(position).__name__}"
        )
    if position < 0:
        raise ValueError("read position must not be negative")
    if os.path.isdir(path):
        raise IsADirectoryError(f"log path is a directory: {path!r}")
    members, _pruned = _resolve_members(path)
    if not members:
        raise FileNotFoundError(f"no metrics log or segments at {path!r}")

    entry = _read_snapshot_registry(path).get(handle)
    if entry is None:
        raise ValueError(
            f"unknown or already released snapshot handle: {handle!r}"
        )
    pruned = int(entry["pruned"])
    copy_path = path + _SNAP_COPY_SUFFIX + handle

    def _generate():
        seen = 0
        for record in _parse_file(copy_path, copy_path):
            if pruned + seen >= position:
                yield record
            seen += 1
        if position > pruned + seen:
            raise ValueError(
                f"read position {position} exceeds the snapshotted record "
                f"total of {pruned + seen}"
            )

    return _generate()


def _require_position(position):
    """Validate a write-order read position the way every resume API does."""
    if isinstance(position, bool) or not isinstance(position, int):
        raise TypeError(
            f"read position must be an integer, got {type(position).__name__}"
        )
    if position < 0:
        raise ValueError("read position must not be negative")


def _require_handle(handle, order):
    """Validate one snapshot argument of a two-handle entry point."""
    if not isinstance(handle, str):
        raise TypeError(
            f"{order} snapshot handle must be a string, got "
            f"{type(handle).__name__}"
        )


def _resolve_snapshot_handle(path, handle, members_exist):
    """Pin a validated handle to its private copy and creation metadata.

    Returns ``(copy_path, pruned, total)``.  The registry is the sole
    authority on whether a handle is live for this segment set, so a
    forged handle, one minted for another ``path`` or one already
    released is all the same :class:`ValueError`.  The copy itself is
    not opened here: registered copies are whole by construction, and
    opening them only when streaming starts means neither reconciling
    nor delta reads ever take -- or fail to take -- the writer lock.
    """
    if not members_exist:
        raise FileNotFoundError(f"no metrics log or segments at {path!r}")
    entry = _read_snapshot_registry(path).get(handle)
    if entry is None:
        raise ValueError(
            f"unknown or already released snapshot handle: {handle!r}"
        )
    return (
        path + _SNAP_COPY_SUFFIX + handle,
        int(entry["pruned"]),
        int(entry["total"]),
    )


def _iter_pinned_raw(copy_path, pruned):
    """Yield ``(ordinal, lineno, raw)`` pairs from one snapshot copy.

    Ordinal is the record's write-order position: the pruned prefix the
    snapshot was taken after plus its zero-based index inside the copy.
    The copy is streamed one line at a time exactly like a segment read,
    so neither snapshot slice is loaded whole during reconciliation.
    """
    index = 0
    for lineno, raw in _iter_lines(copy_path):
        if not raw or raw == b"\r":
            continue
        yield pruned + index, lineno, raw
        index += 1


def _decode_pinned(raw, copy_path, lineno):
    """Parse one pinned record line, naming the copy on bad content."""
    try:
        return parse_metrics(raw.decode("utf-8"))
    except ValueError as exc:
        raise ValueError(
            f"invalid metrics in {copy_path} at line {lineno}: {exc}"
        ) from exc


def snapshot_diff_metrics(path, old_handle, new_handle):
    """Reconcile two snapshots record by record in write order.

    ``old_handle`` is the older side (the snapshot created first) and
    ``new_handle`` the newer side; either ordering works mechanically,
    but which side is which decides whether a record is reported as
    ``added`` or ``missing``.  Both handles must belong to the segment
    set at ``path``.

    Records align by their write-order ordinal -- the pruned prefix the
    snapshot was taken after plus the record's index in the pinned
    sequence -- so ordinals stay comparable across snapshots however the
    segment set was appended to, rotated, compacted or pruned between
    the two creation points.  Each aligned pair whose pinned bytes
    differ is one ``changed`` difference, an ordinal only the old side
    holds is ``missing`` and one only the new side holds is ``added``;
    a record identical on both sides produces nothing.  Post-creation
    appends, rotations, compactions and prunes change neither snapshot,
    so the result is stable however the segment set mutates afterwards.

    Differences are produced lazily as one JSON string per difference,
    each a compact object on a single newline-terminated line with keys
    in the order ``kind``, ``position`` and then ``old`` and/or
    ``new``: ``added`` carries ``new`` only, ``missing`` carries
    ``old`` only, ``changed`` carries both, and ``position`` is the
    aligned write-order ordinal.  The two private copies are streamed
    interleavingly, line by line, without being read whole and without
    taking the writer lock, so upstream writes proceed while
    reconciliation runs and a locking problem can never be reported
    here.  Re-evaluating the same pair of handles yields the same
    lines, and separate reconciliations are independent.

    Path and handle problems are reported when this function is called;
    line content problems are reported lazily while iterating.

    Raises:
        FileNotFoundError: neither ``path`` nor any segment exists.
        IsADirectoryError: ``path`` is a directory.
        OSError: ``path`` is not a string.
        TypeError: ``old_handle`` or ``new_handle`` is not a string.
        ValueError: either handle is forged, belongs to another segment
            set or was released (raised on call), or a pinned complete
            line is not a metrics JSON object (raised while iterating).
    """
    if not isinstance(path, str):
        raise OSError(f"log path must be a string, got {type(path).__name__}")
    _require_handle(old_handle, "old")
    _require_handle(new_handle, "new")
    if os.path.isdir(path):
        raise IsADirectoryError(f"log path is a directory: {path!r}")

    members, _pruned = _resolve_members(path)
    old_copy, old_pruned, _old_total = _resolve_snapshot_handle(
        path, old_handle, bool(members)
    )
    new_copy, new_pruned, _new_total = _resolve_snapshot_handle(
        path, new_handle, bool(members)
    )

    def _generate():
        old_iter = iter(_iter_pinned_raw(old_copy, old_pruned))
        new_iter = iter(_iter_pinned_raw(new_copy, new_pruned))
        old = next(old_iter, None)
        new = next(new_iter, None)
        while old is not None or new is not None:
            if new is None or (old is not None and old[0] < new[0]):
                ordinal, lineno, raw = old
                entry = {
                    "kind": "missing",
                    "position": ordinal,
                    "old": _decode_pinned(raw, old_copy, lineno),
                }
                old = next(old_iter, None)
            elif old is None or new[0] < old[0]:
                ordinal, lineno, raw = new
                entry = {
                    "kind": "added",
                    "position": ordinal,
                    "new": _decode_pinned(raw, new_copy, lineno),
                }
                new = next(new_iter, None)
            else:
                ordinal = old[0]
                if old[2] != new[2]:
                    entry = {
                        "kind": "changed",
                        "position": ordinal,
                        "old": _decode_pinned(old[2], old_copy, old[1]),
                        "new": _decode_pinned(new[2], new_copy, new[1]),
                    }
                else:
                    entry = None
                old = next(old_iter, None)
                new = next(new_iter, None)
            if entry is not None:
                yield json.dumps(
                    entry, separators=(",", ":"), ensure_ascii=False
                ) + "\n"

    return _generate()


def resume_snapshot_delta_metrics(path, handle, position=0):
    """Stream a snapshot's not-yet-read records from a delta cursor.

    This is the cursor counterpart of
    :func:`resume_snapshot_metrics`: it pins the snapshot's current
    record boundary -- the one fixed at creation -- and yields the
    pinned records at write-order ordinals at or after ``position``
    that the cursor has not consumed yet.  Everything is read from the
    handle's private copy, so appends, rotations, compactions and
    prunes happening upstream change neither the boundary nor the
    sequence, reading never waits on or contends for the writer lock,
    and a locking failure can never be reported.  The same handle
    resumed from the same position always yields the same records, even
    while the segment set is being written.

    ``position`` uses write-order ordinals exactly like
    :func:`resume_metrics`: a position inside the pruned prefix the
    snapshot was taken after starts at the oldest pinned record, equal
    to the creation-time record total the stream is empty, and only a
    position past that total (pruned records included) is out of range
    and reported while iterating.

    Path, handle and cursor problems are reported when this function is
    called; line content problems and an out-of-range cursor are
    reported lazily while iterating.

    Raises:
        FileNotFoundError: neither ``path`` nor any segment exists.
        IsADirectoryError: ``path`` is a directory.
        OSError: ``path`` is not a string.
        TypeError: ``handle`` is not a string, or ``position`` is not an
            integer (booleans do not count).
        ValueError: ``handle`` is forged, belongs to another segment set
            or was released; ``position`` is negative or past the
            snapshot's record total (the latter raised while iterating);
            or a pinned complete line is not a metrics JSON object.
    """
    if not isinstance(path, str):
        raise OSError(f"log path must be a string, got {type(path).__name__}")
    _require_handle(handle, "snapshot")
    _require_position(position)
    if os.path.isdir(path):
        raise IsADirectoryError(f"log path is a directory: {path!r}")

    members, _pruned = _resolve_members(path)
    copy_path, pruned, total = _resolve_snapshot_handle(
        path, handle, bool(members)
    )

    def _generate():
        seen = 0
        for _ordinal, lineno, raw in _iter_pinned_raw(copy_path, pruned):
            if pruned + seen >= position:
                yield _decode_pinned(raw, copy_path, lineno)
            seen += 1
        if position > total:
            raise ValueError(
                f"read position {position} exceeds the snapshotted record "
                f"total of {total}"
            )

    return _generate()
