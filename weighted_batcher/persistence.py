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

:func:`snapshot_diff_metrics` reconciles two snapshots of one segment set
in write order -- the handle created first is the old side -- reporting
each extra record as ``added``, each vanished one as ``missing`` and each
byte-for-byte changed one as ``changed``, one JSON line per difference.
:func:`resume_snapshot_delta_metrics` pins the live record boundary the
way :func:`snapshot_metrics` pins a snapshot (a brief lock to settle and
capture the members, then a lock-free copy) and streams the records past
a read position that the holder of an older snapshot handle has not read
yet.  A reconciliation is fully lock free, and an incremental pull holds
the write lock only for the constant-sized boundary pin -- never while
its records are produced -- so appends, rotations, compactions and
prunes proceed normally while either read runs.

:func:`checkpoint_metrics` persists a reader's place in a caller-named
cursor file.  The cursor binds the write-order read position, the
snapshot handle the reader pulls through and that snapshot's record
boundary (its pruned count and record total); every advance replaces the
whole cursor atomically, so a crash between consuming and advancing
leaves either the old cursor or the new one, never a half-written file.
:func:`resume_checkpoint_metrics` reads that one file and streams the
pinned snapshot from the stored position with exactly the ordinal rules
of :func:`resume_snapshot_metrics`.  A cursor takes no log lock to write
and reads only the snapshot's private copy, so concurrent readers with
separate cursor files and same-process writers neither block nor lose
updates.

Every data-file descriptor is released before a rename on platforms that
cannot rename open files (Windows), so sealing, compaction and pruning no
longer raise ``PermissionError`` there.
"""

from __future__ import annotations

import json
import os
import tempfile

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
    "checkpoint_metrics",
    "resume_checkpoint_metrics",
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
# Cursor state.  The caller names the final cursor file; each write stages
# its replacement at a unique ``cursor_path.cursor.tmp.<random>`` name,
# fsyncs it and atomically renames it over the cursor, so the cursor
# itself is never observed half written and concurrent writers (serialised
# by a lock anchored on the cursor file, for the unusual case of two
# processes sharing one cursor file) never clobber one another's staging.
_CURSOR_TMP_SUFFIX = ".cursor.tmp."
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
    # An incremental-pull copy is anonymous from birth on Linux; only the
    # named-temp fallback on an O_TMPFILE-less filesystem can leave a
    # file behind if the process died in the create-to-unlink window.
    cursor_prefix = os.path.basename(path) + ".cursor."
    for name in names:
        if name.startswith(cursor_prefix):
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


def _parse_lined(lines, label):
    """Parse ``(line_number, raw_bytes)`` complete lines into metrics."""
    for lineno, raw in lines:
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


def _parse_file(path, label):
    """Stream one segment or the current log through the metrics parser."""
    yield from _parse_lined(_iter_lines(path), label)


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


def _iter_raw_file_records(copy_path, chunksize=_READ_CHUNK):
    """Yield each complete record line's raw bytes with its newline.

    The chunked walk mirrors :func:`_iter_lines`, but lines are never
    parsed or re-rendered: blank lines are skipped exactly as on read,
    the torn unterminated tail is dropped, and every other complete line
    comes back verbatim with ``\\n`` reattached, so byte-level content
    such as ``-0.0``, huge counters and key ordering survives untouched.
    """
    fd = os.open(copy_path, os.O_RDONLY)
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


def _iter_raw_records(path, chunksize=_READ_CHUNK):
    """Yield each complete record line's raw bytes with its newline.

    Delegates to :func:`_iter_raw_file_records`; records are copied byte
    for byte exactly as on a fresh read, so compaction and pruning move
    original newlines, ``-0.0``, huge counters and key ordering untouched.
    """
    yield from _iter_raw_file_records(path, chunksize)


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


def _snapshot_entry(path, handle):
    """Validate ``handle`` against the registry and return its metadata.

    Mirrors the handle checks of :func:`resume_snapshot_metrics`: a
    non-string handle is a :class:`TypeError`; a forged handle, one from
    another segment set, or one already released is a
    :class:`ValueError`.
    """
    if not isinstance(handle, str):
        raise TypeError(
            f"snapshot handle must be a string, got {type(handle).__name__}"
        )
    entry = _read_snapshot_registry(path).get(handle)
    if entry is None:
        raise ValueError(
            f"unknown or already released snapshot handle: {handle!r}"
        )
    return entry


class _LiveTruncated(RuntimeError):
    """Internal signal: the live inode shrank while its bytes were copied."""


def _capture_delta_members(path):
    """Take the write lock, settle pending work, and open every member.

    Returns ``(lock_fd, pruned, captured)``; each captured item is
    ``(descriptor, size, is_live)`` in member order.  Ownership of the
    lock and every descriptor passes to the caller; on failure every
    descriptor opened here is closed and the lock released.
    """
    lock_fd = _open_locked(path, create=False)
    captured = []
    try:
        _finish_pending(path)
        lock_fd = _relock_after_settle(lock_fd, path, create=False)
        members = _segment_members(path)
        pruned = _read_prune_state(path)[0]
        for member in members:
            descriptor = os.open(member, os.O_RDONLY)
            captured.append(
                (descriptor, os.fstat(descriptor).st_size,
                 member == path, member)
            )
        return lock_fd, pruned, captured
    except BaseException:
        for descriptor, _size, _is_live, _member in captured:
            try:
                os.close(descriptor)
            except OSError:
                pass
        try:
            os.close(lock_fd)
        except OSError:
            # _relock_after_settle may already have closed this fd
            # before failing to re-open the settled live log.
            pass
        raise


def _pin_once(path, copy_under_lock):
    """Build one boundary pin; raise :class:`_LiveTruncated` on a live race.

    Numbered members are never truncated, so their open descriptors and
    pinned sizes already pin the bytes.  The live member (always last)
    alone is copied into an anonymous inode: unless
    ``copy_under_lock`` is set, the write lock is released immediately
    before that copy -- keeping writer blocking constant in the data
    size -- and a compaction truncating the live inode during the copy
    surfaces as a short read and asks for a locked retry.
    """
    lock_fd, pruned, captured = _capture_delta_members(path)
    pins = []
    try:
        for descriptor, size, is_live, member in captured:
            if is_live:
                if not copy_under_lock:
                    os.close(lock_fd)
                    lock_fd = None
                # _anonymous_copy always closes the live source
                # descriptor, returning the anonymous one (or raising);
                # the live member is last, so every numbered pin is
                # already in ``pins`` and released there on failure.
                descriptor = _anonymous_copy(
                    descriptor, size, path,
                    allow_short=not copy_under_lock,
                )
            pins.append((descriptor, size, member))
        return pruned, pins
    except BaseException:
        _close_pins(pins)
        # On a failure while the live copy is under way, the numbered
        # descriptors already moved into ``pins`` (closed just above);
        # the live descriptor was closed by _anonymous_copy.  Nothing
        # else captured remains open.
        raise
    finally:
        if lock_fd is not None:
            os.close(lock_fd)


def _pin_numbered_only(members, pruned):
    """Pin members when no live log exists (a rotation crashed mid-seal).

    Numbered files are never truncated -- only renamed, unlinked or
    replaced -- so an open descriptor keeps its pinned bytes without
    any lock, matching the settled slice a lock-free recovery reads in
    this state.
    """
    pins = []
    for member in members:
        descriptor = os.open(member, os.O_RDONLY)
        pins.append((descriptor, os.fstat(descriptor).st_size, member))
    return pruned, pins


def _pin_live_delta(path):
    """Fix this instant's record boundary for an incremental pull.

    Returns ``(pruned, pins)``; each pin is ``(descriptor, size)`` with
    ``descriptor`` a read-only cursor on the member's pinned bytes
    (``None`` on a lock-less platform, where members are read by path
    instead) and ``size`` the pinned byte length.

    The write lock is held only while any half-finished mutation is
    settled and each member is opened and sized -- constant time in the
    data -- and the live log's bytes are copied after the lock is
    released, so an upstream append, rotation, compaction or prune is
    never blocked for long and proceeds normally for the whole
    duration of the stream:

    * a numbered segment is never truncated -- only renamed away by
      rotation, unlinked by compaction or replaced by a prune trim -- so
      an open descriptor keeps its pinned bytes readable through every
      later mutation;
    * the live log alone can be truncated in place (compaction empties
      it), so its pinned bytes move into an anonymous descriptor,
      unnamed from birth, that is immune to truncation and dies with the
      generator; a crash leaves nothing behind for a later write to
      clean up.  The copy normally runs lock free; in the rare event a
      compaction truncates the inode during it, the pin is retried once
      with the copy under the lock;
    * every descriptor is bounded by the pinned byte length, so bytes
      appended after the pin never enter the stream.

    Settling runs while the lock is held, so a half-finished
    compaction, prune or snapshot left by a crash is cleaned up
    transparently exactly as by a write.  On platforms without
    :mod:`fcntl` (Windows) no descriptor is held, matching the
    lock-less tradeoffs the other readers make there.
    """
    members, pruned = _resolve_members(path)
    if not members:
        raise FileNotFoundError(f"no metrics log or segments at {path!r}")
    if fcntl is None:
        return pruned, [(None, None, member) for member in members]
    if not os.path.exists(path):
        # A rotation crashed after sealing and before recreating the
        # live log: only numbered members exist, none truncatable.
        return _pin_numbered_only(members, pruned)
    try:
        return _pin_once(path, copy_under_lock=False)
    except _LiveTruncated:
        # A compaction raced the lock-free live copy; re-capture and
        # copy with the lock held, which no compaction can interrupt.
        return _pin_once(path, copy_under_lock=True)


def _anonymous_copy(source, size, path, allow_short):
    """Copy ``size`` bytes of ``source`` into a seeked anonymous file.

    Always closes ``source``.  With ``allow_short``, a truncation of the
    live inode during the copy -- a compaction racing the lock-free pin
    -- makes the read end early and raises :class:`_LiveTruncated`;
    otherwise (the locked retry) the copy is uninterrupted.
    """
    directory = os.path.dirname(path) or "."
    tmp_fd = None
    o_tmpfile = getattr(os, "O_TMPFILE", 0)
    if o_tmpfile:
        try:
            tmp_fd = os.open(directory, os.O_RDWR | o_tmpfile, 0o666)
        except OSError:
            # Some filesystems reject O_TMPFILE; fall through to a named
            # temp that is unlinked on the next line.
            tmp_fd = None
    if tmp_fd is None:
        tmp_fd, tmp_name = tempfile.mkstemp(
            dir=directory, prefix=os.path.basename(path) + ".cursor."
        )
        os.unlink(tmp_name)
    try:
        remaining = size
        while remaining:
            chunk = os.read(source, min(_READ_CHUNK, remaining))
            if not chunk:
                if allow_short:
                    raise _LiveTruncated("live log truncated during pin")
                break
            _write_bytes(tmp_fd, chunk)
            remaining -= len(chunk)
        os.lseek(tmp_fd, 0, os.SEEK_SET)
    except BaseException:
        os.close(tmp_fd)
        raise
    finally:
        os.close(source)
    return tmp_fd


def _close_pins(pins):
    """Release every descriptor a delta boundary pin holds."""
    for descriptor, _size, _member in pins:
        if descriptor is not None:
            os.close(descriptor)


def _iter_fd_lines(descriptor, cap, chunksize=_READ_CHUNK):
    """Yield ``(line_number, raw_bytes)`` from ``descriptor`` up to ``cap``.

    The bounded counterpart of :func:`_iter_lines`: at most the pinned
    byte length is read, so a record appended after the pin is never
    produced, and the final unterminated tail at the pinned end is a
    possible torn write and is never produced, exactly as on a normal
    read.
    """
    remaining = cap
    pending = b""
    lineno = 0
    while remaining > 0:
        chunk = os.read(descriptor, min(chunksize, remaining))
        if not chunk:
            break
        remaining -= len(chunk)
        pending += chunk
        *complete, pending = pending.split(b"\n")
        for raw in complete:
            lineno += 1
            yield lineno, raw


def snapshot_diff_metrics(path, old_handle, new_handle):
    """Reconcile two snapshots in write order and yield the differences.

    ``old_handle`` is the snapshot created first -- the old side -- and
    ``new_handle`` the new side.  Both private copies are walked in
    write-order ordinals side by side, one record at a time, so neither
    snapshot is materialised: records only on the new side are
    ``added``, records only on the old side ``missing``, and records
    present on both but differing in their stored bytes ``changed``.
    Equal ordinals stop once both sides are exhausted, so records
    appended after either snapshot was taken never appear.

    Each difference is one JSON object rendered on its own
    newline-terminated line.  The keys come in the order ``kind``,
    ``position``, ``old``, ``new``, with ``old``/``new`` present exactly
    on the side the difference has (``added`` carries ``new``,
    ``missing`` carries ``old``, ``changed`` both), and their content is
    the record exactly as pinned: a snapshot stores every record in its
    compact appended form, so parsing a pinned line and re-rendering it
    reproduces its bytes -- original newlines aside, ``-0.0``, oversized
    counters and key ordering are untouched.  Whether a shared ordinal is
    ``changed`` is decided on the stored bytes themselves, so two lines
    that differ only in key order are still a change.

    The walk is fully lock free: snapshot copies are never members of the
    live segment set, and each side is streamed line by line rather than
    read whole, so reads neither take nor wait on the write lock and
    never block an upstream append, rotation, compaction or prune.
    Repeated reconciliation of the same handle pair always yields the
    same lines, and different pairs never interfere.

    Path- and handle-level problems are reported when this function is
    called; a line that is not valid metrics JSON is reported lazily
    while iterating.

    Raises:
        FileNotFoundError: neither ``path`` nor any segment exists.
        IsADirectoryError: ``path`` is a directory.
        OSError: ``path`` is not a string.
        TypeError: either handle is not a string.
        ValueError: either handle is forged, belongs to another segment
            set or was released; or a complete line is not a metrics
            JSON object (raised while iterating).
    """
    if not isinstance(path, str):
        raise OSError(f"log path must be a string, got {type(path).__name__}")
    if os.path.isdir(path):
        raise IsADirectoryError(f"log path is a directory: {path!r}")
    members, _pruned = _resolve_members(path)
    if not members:
        raise FileNotFoundError(f"no metrics log or segments at {path!r}")
    old_entry = _snapshot_entry(path, old_handle)
    new_entry = _snapshot_entry(path, new_handle)

    old_pruned = int(old_entry["pruned"])
    new_pruned = int(new_entry["pruned"])
    old_copy = path + _SNAP_COPY_SUFFIX + old_handle
    new_copy = path + _SNAP_COPY_SUFFIX + new_handle

    def _records(copy_path):
        # The private copy is streamed line by line and each stored line
        # is carried both as its raw bytes (which decide ``changed``) and
        # as the parsed record (which fills ``old``/``new``), so no
        # snapshot is ever read whole and no write lock is ever held.
        for raw in _iter_raw_file_records(copy_path):
            text = raw[:-1].decode("utf-8")
            yield raw, parse_metrics(text)

    def _line(kind, position, old, new):
        difference = {"kind": kind, "position": position}
        if old is not None:
            difference["old"] = old[1]
        if new is not None:
            difference["new"] = new[1]
        return json.dumps(
            difference, ensure_ascii=False, separators=(",", ":")
        ) + "\n"

    def _generate():
        # Keep one generator per side in lock step.  Both always point at
        # the record with the next ordinal; advancing a side aligns it to
        # the ordinal the other side is already sitting on.
        old_records = _records(old_copy)
        new_records = _records(new_copy)
        old_ordinal = old_pruned
        new_ordinal = new_pruned
        old_record = next(old_records, None)
        new_record = next(new_records, None)
        while old_record is not None or new_record is not None:
            if old_record is not None and (
                new_record is None or old_ordinal < new_ordinal
            ):
                yield _line("missing", old_ordinal, old_record, None)
                old_ordinal += 1
                old_record = next(old_records, None)
            elif new_record is not None and (
                old_record is None or new_ordinal < old_ordinal
            ):
                yield _line("added", new_ordinal, None, new_record)
                new_ordinal += 1
                new_record = next(new_records, None)
            else:
                # Byte comparison of the pinned lines: key order, -0.0
                # and oversized counters are part of the stored bytes.
                if old_record[0] != new_record[0]:
                    yield _line(
                        "changed", old_ordinal, old_record, new_record
                    )
                old_ordinal += 1
                new_ordinal += 1
                old_record = next(old_records, None)
                new_record = next(new_records, None)

    return _generate()


def resume_snapshot_delta_metrics(path, old_handle, position=0):
    """Stream not-yet-read live records using an older snapshot handle.

    The call fixes the present record boundary: under one short-lived
    write lock it settles any half-finished mutation, captures every
    member's bytes as independent read-only descriptors bounded by their
    pinned lengths, and copies the live log into an anonymous descriptor
    immune to the in-place truncation a later compaction performs.  The
    lock is released again before this returns, so while records are
    produced upstream appends, rotations, compactions and prunes proceed
    normally: appended bytes sit past a pinned length and never leak in,
    rotated or unlinked members stay readable through their captured
    descriptors, the compacted live log survives in its anonymous copy,
    and a prune leaves every pinned inode -- replaced or unlinked --
    readable.  ``old_handle`` only identifies the reader (its forged,
    foreign or released forms are rejected); the records come from the
    pinned live boundary, not from that snapshot's private copy.

    ``position`` uses the same write-order ordinals as
    :func:`resume_metrics`.  Records at ordinals below it are skipped; a
    position inside the pruned region starts at the oldest surviving
    record; equal to the current total (pruned records included) the
    stream is empty.  A position past that total is out of range and,
    like :func:`resume_metrics`, raises :class:`ValueError` only once the
    end is reached while iterating.

    The pulled sequence matches a fresh from-the-start recovery of the
    pinned instant, and neither append, rotation, compaction nor prune
    after the pin changes it; repeated calls with the same arguments
    yield the same records.  Streaming is line by line and never takes
    the write lock, so a concurrent writer neither deadlocks against
    the pull nor reports a spurious locking failure.

    Path-, handle- and position-level problems are reported when this
    function is called; line content problems and an out-of-range
    position are reported lazily while iterating.

    Raises:
        FileNotFoundError: neither ``path`` nor any segment exists.
        IsADirectoryError: ``path`` is a directory.
        OSError: ``path`` is not a string.
        TypeError: ``old_handle`` is not a string, or ``position`` is
            not an integer (booleans do not count).
        ValueError: ``old_handle`` is forged, belongs to another segment
            set or was released; ``position`` is negative or past the
            record total (the latter raised while iterating); or a
            complete line is not a metrics JSON object.
    """
    if not isinstance(path, str):
        raise OSError(f"log path must be a string, got {type(path).__name__}")
    if not isinstance(old_handle, str):
        raise TypeError(
            f"snapshot handle must be a string, got {type(old_handle).__name__}"
        )
    if isinstance(position, bool) or not isinstance(position, int):
        raise TypeError(
            f"read position must be an integer, got {type(position).__name__}"
        )
    if position < 0:
        raise ValueError("read position must not be negative")
    if os.path.isdir(path):
        raise IsADirectoryError(f"log path is a directory: {path!r}")
    # The boundary is fixed first, exactly as in resume_snapshot_metrics,
    # so a missing log reports FileNotFoundError before the handle is
    # consulted; the registry then rejects a forged, foreign or released
    # handle without streaming.
    pruned, pins = _pin_live_delta(path)
    try:
        _snapshot_entry(path, old_handle)
    except BaseException:
        # A forged, foreign or released handle rejects the pull without
        # ever handing back a generator, so the boundary descriptors must
        # be released here rather than waiting for iteration cleanup.
        _close_pins(pins)
        raise

    def _generate():
        seen = 0
        try:
            for descriptor, size, member in pins:
                if descriptor is None:
                    # Lock-less platform: members are read by path.
                    records = _parse_file(member, member)
                else:
                    records = _parse_lined(
                        _iter_fd_lines(descriptor, size), member
                    )
                for record in records:
                    if pruned + seen >= position:
                        yield record
                    seen += 1
            if position > pruned + seen:
                raise ValueError(
                    f"read position {position} exceeds the record total of "
                    f"{pruned + seen}"
                )
        finally:
            # Descriptors -- the anonymous live copy included -- die
            # with the generator, whether it ran out or was closed early.
            _close_pins(pins)

    return _generate()


def _load_cursor(cursor_path):
    """Read and structurally validate a cursor file, returning its state.

    The cursor is one JSON object binding the read position, the snapshot
    handle and the snapshot's record boundary (``pruned`` and ``total``).
    A missing file is a missing checkpoint; anything present but not
    exactly such a document is a corrupt cursor, including a cursor an
    earlier version could have written with different fields.
    """
    fd = os.open(cursor_path, os.O_RDONLY)
    try:
        raw = b""
        while True:
            chunk = os.read(fd, _READ_CHUNK)
            if not chunk:
                break
            raw += chunk
    finally:
        os.close(fd)
    try:
        state = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError(f"corrupt cursor file {cursor_path!r}: {exc}") from exc
    if not isinstance(state, dict):
        raise ValueError(
            f"corrupt cursor file {cursor_path!r}: state must be a JSON object"
        )
    keys = ("version", "position", "handle", "pruned", "total")
    if set(state) != set(keys):
        raise ValueError(
            f"corrupt cursor file {cursor_path!r}: unexpected cursor fields"
        )
    if state["version"] != 1 or not isinstance(state["handle"], str):
        raise ValueError(f"corrupt cursor file {cursor_path!r}: bad cursor state")
    for key in ("position", "pruned", "total"):
        # Booleans are not acceptable cursor numbers.
        if isinstance(state[key], bool) or not isinstance(state[key], int):
            raise ValueError(
                f"corrupt cursor file {cursor_path!r}: {key} must be an integer"
            )
    if state["position"] < 0 or state["pruned"] < 0 or state["total"] < 0:
        raise ValueError(
            f"corrupt cursor file {cursor_path!r}: negative cursor value"
        )
    if state["total"] < state["pruned"]:
        raise ValueError(
            f"corrupt cursor file {cursor_path!r}: pruned count past the total"
        )
    return state


def checkpoint_metrics(path, handle, cursor_path, position=0):
    """Write the reader's incremental-pull cursor to ``cursor_path``.

    The cursor binds three things together: the integer write-order read
    ``position`` (the next ordinal to consume), the snapshot ``handle``
    the pull streams through, and that snapshot's record boundary -- its
    pruned-record count and its record total including pruned records --
    captured from the snapshot registry at write time.  Repeated calls
    advance the cursor by replacing the whole file, so a crash between
    consuming records and advancing the checkpoint leaves either the
    previous cursor or this one on disk, never a half-written file.

    The replacement is staged at a unique
    ``cursor_path.cursor.tmp.<random>`` name, fsynced and atomically
    renamed over the cursor; on platforms with :mod:`fcntl` the cursor
    file itself is locked for the write, so even two processes that
    mistakenly share one cursor file replace it serially without ever
    interleaving payloads or losing one another's update.  Distinct
    readers that name distinct cursor files never touch each other's
    state and never block one another; no log lock is taken here, so an
    upstream append, rotation, compaction or prune in the same process
    neither deadlocks against the checkpoint write nor fails to acquire
    its lock.

    Raises:
        TypeError: ``handle`` is not a string, or ``position`` is not an
            integer (booleans do not count).
        ValueError: ``position`` is negative, or ``handle`` is forged,
            belongs to another segment set or was released.
            A position past the snapshot total (pruned records included)
            is not rejected here; :func:`resume_checkpoint_metrics`
            reports it lazily while iterating, like
            :func:`resume_snapshot_metrics`.
        FileNotFoundError: neither ``path`` nor any segment exists.
        IsADirectoryError: ``path`` is a directory.
        OSError: ``path`` is not a string, or locking or writing the
            cursor file fails.  A failed write leaves the previously
            published cursor (if any) untouched.
    """
    if not isinstance(path, str):
        raise OSError(f"log path must be a string, got {type(path).__name__}")
    if not isinstance(handle, str):
        raise TypeError(
            f"snapshot handle must be a string, got {type(handle).__name__}"
        )
    if not isinstance(cursor_path, str):
        raise OSError(
            f"cursor path must be a string, got {type(cursor_path).__name__}"
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
    entry = _snapshot_entry(path, handle)
    pruned = int(entry["pruned"])
    total = int(entry["total"])
    state = {
        "version": 1,
        "position": position,
        "handle": handle,
        "pruned": pruned,
        "total": total,
    }
    payload = json.dumps(state, separators=(",", ":")).encode("utf-8")

    tmp = cursor_path + _CURSOR_TMP_SUFFIX + os.urandom(8).hex()
    existed = os.path.exists(cursor_path)
    lock_fd = None
    out = None
    try:
        if fcntl is not None:
            # The lock is anchored on the cursor file itself, never on
            # the log: it serialises writes that share a cursor file
            # without ever blocking an upstream append, rotation,
            # compaction or prune or a reader using another cursor file.
            # Follow the same inode-revalidation rule as _open_locked: a
            # failed predecessor may have unlinked an empty anchor while
            # this open waited, so re-lock until the descriptor names the
            # file currently at the cursor path.
            while True:
                lock_fd = os.open(cursor_path, os.O_RDWR | os.O_CREAT, 0o666)
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_EX)
                except OSError:
                    os.close(lock_fd)
                    raise
                held = os.fstat(lock_fd)
                try:
                    current = os.stat(cursor_path)
                except FileNotFoundError:
                    current = None
                if current is not None and (
                    current.st_dev, current.st_ino
                ) == (held.st_dev, held.st_ino):
                    break
                os.close(lock_fd)
                lock_fd = None
        # A per-process unique staging name means two waiters never
        # clobber one another's staging file while queued on the lock.
        out = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o666)
        _write_bytes(out, payload)
        os.fsync(out)
        os.close(out)
        out = None
        os.replace(tmp, cursor_path)
    except BaseException:
        if out is not None:
            try:
                os.close(out)
            except OSError:
                pass
        _unlink_if_exists(tmp)
        # An empty file created only to anchor the lock is no cursor at
        # all; remove it when no published cursor predated this call, so
        # a failed write leaves no half cursor behind.
        if not existed:
            try:
                if os.path.exists(cursor_path) and os.path.getsize(
                    cursor_path
                ) == 0:
                    os.unlink(cursor_path)
            except OSError:
                pass
        raise
    finally:
        if lock_fd is not None:
            os.close(lock_fd)


def resume_checkpoint_metrics(path, cursor_path):
    """Stream an incremental pull onward from the cursor in ``cursor_path``.

    The cursor file is read first and its bound state validated eagerly:
    the snapshot handle is checked against the current registry exactly as
    in :func:`resume_snapshot_metrics` -- a forged handle, one from another
    segment set or a released one is rejected -- and the stored boundary
    is compared with the registry's, so a cursor whose state was tampered
    with is reported rather than silently trusted.  Streaming then runs
    through :func:`resume_snapshot_metrics` from the stored position:

    * records already consumed (ordinals below the position) are not
      produced again, and no unconsumed record is skipped;
    * a position inside the snapshot's pruned region starts at the oldest
      pinned record, because pruned records keep their ordinals;
    * a position equal to the snapshot total (pruned included) yields an
      empty stream; a larger one raises :class:`ValueError` only once the
      end is reached while iterating.

    Because the snapshot is a creation-time private copy, records appended,
    rotated, compacted or pruned after the cursor was written are invisible
    to this pull -- callers pin a fresh snapshot to see them -- and the
    read never blocks a same-process or upstream writer.

    Raises:
        FileNotFoundError: ``cursor_path`` does not exist, or neither
            ``path`` nor any segment exists.
        IsADirectoryError: ``path`` is a directory.
        OSError: ``path`` or ``cursor_path`` is not a string, or reading
            the cursor file fails.
        ValueError: the cursor file is corrupt (including a non-string
            handle or a non-integer position), or its handle is forged,
            belongs to another segment set, was released, or records a
            boundary that no longer matches the snapshot registry; a
            stored position past the snapshot total raises lazily while
            iterating, as do non-metrics lines in the snapshot copy.
    """
    if not isinstance(path, str):
        raise OSError(f"log path must be a string, got {type(path).__name__}")
    if not isinstance(cursor_path, str):
        raise OSError(
            f"cursor path must be a string, got {type(cursor_path).__name__}"
        )
    if os.path.isdir(path):
        raise IsADirectoryError(f"log path is a directory: {path!r}")
    state = _load_cursor(cursor_path)
    members, _pruned = _resolve_members(path)
    if not members:
        raise FileNotFoundError(f"no metrics log or segments at {path!r}")
    entry = _snapshot_entry(path, state["handle"])
    if (
        int(entry["pruned"]) != state["pruned"]
        or int(entry["total"]) != state["total"]
    ):
        raise ValueError(
            f"cursor {cursor_path!r} does not match the snapshot record "
            f"boundary of handle {state['handle']!r}"
        )
    return resume_snapshot_metrics(path, state["handle"], state["position"])
