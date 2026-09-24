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

:func:`snapshot_metrics` fixes the complete record sequence of one
instant and returns an opaque persistable handle.  The snapshot never
changes afterwards: while it is alive, a prune that evicts one of its
records first copies the evicted bytes into a private sidecar, so a
fixed record is either still in the segment set or already in that
copy.  :func:`resume_snapshot_metrics` re-reads the fixed slice any
number of times with the same write-order positions, and
:func:`release_metrics` drops the handle and its copy.

Every data-file descriptor is released before a rename on platforms that
cannot rename open files (Windows), so sealing, compaction and pruning no
longer raise ``PermissionError`` there.
"""

from __future__ import annotations

import base64
import binascii
import hmac
import json
import os
import secrets

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
    "resume_snapshot_metrics",
    "release_metrics",
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
# Directory holding one private sidecar pair per snapshot: ``<id>.json``
# is the published (or half-published) snapshot metadata and ``<id>.log``
# its private copy of records evicted while the snapshot is alive.
_SNAP_DIR_SUFFIX = ".snapd"
_SNAP_META_SUFFIX = ".json"
_SNAP_COPY_SUFFIX = ".log"
_SNAP_TMP_SUFFIX = ".tmp"
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


def _iter_raw_fd(fd):
    """Yield each complete record line's raw bytes with newline, from an fd.

    The fd-based twin of :func:`_iter_raw_records`: snapshot reads hold the
    member descriptor open themselves so a member a concurrent prune
    evicts can be detected by inode before a single byte is consumed.
    """
    pending = b""
    while True:
        chunk = os.read(fd, _READ_CHUNK)
        if not chunk:
            break
        pending += chunk
        *complete, pending = pending.split(b"\n")
        for raw in complete:
            if raw and raw != b"\r":
                yield raw + b"\n"


def _parse_raw_fd(fd, label):
    """Parse complete raw lines from an open fd through the metrics parser."""
    lineno = 0
    for raw in _iter_raw_fd(fd):
        lineno += 1
        try:
            yield parse_metrics(raw.decode("utf-8"))
        except ValueError as exc:
            raise ValueError(
                f"invalid metrics in {label} at line {lineno}: {exc}"
            ) from exc


def _resolve_member_ids(path):
    """Resolve members with identity captured in one marker-consistent world.

    Returns ``(ids, pruned, marker)``.  Each id is
    ``(member, dev, inode, size)``.  The member list and pruned count
    come from one marker-consistent resolution; the identity stats are
    then bracketed by another pair of marker observations, so a prune
    (the only operation that rewrites the marker) committing between
    the listing and a stat invalidates the attempt.  Callers open every
    member, compare descriptor identities and sizes against the captured
    ones, and re-observe the marker: a commit landing in any of those
    gaps is caught before the first record is yielded.
    """
    before = _marker_observation(path)
    for _ in range(1000):
        members, pruned, after_listing = _resolve_once(path, before)
        if after_listing != before:
            before = after_listing
            continue
        ids = []
        try:
            for member in members:
                stat = os.stat(member)
                ids.append((member, stat.st_dev, stat.st_ino, stat.st_size))
        except FileNotFoundError:
            before = _marker_observation(path)
            continue
        after_stats = _marker_observation(path)
        if after_stats == before:
            return ids, pruned, before
        before = after_stats
    members, pruned = _resolve_members(path)
    return (
        [(member, None, None, None) for member in members],
        pruned,
        before,
    )


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
    """Settle any half-finished compaction or prune before a new mutation."""
    _finish_staged_compact(path)
    _finish_staged_prune(path)
    _cleanup_snapshot_sidecars(path)


# ---------------------------------------------------------------------------
# Consistency snapshots
#
# A snapshot fixes the complete record sequence of one creation instant:
# records with write-order ordinals in ``[start, end)`` exactly as the bytes
# were then on disk.  Nothing written afterwards ever enters that window.
#
# While a snapshot is alive every prune that evicts one of its records first
# rebuilds a private copy of the evicted bytes at
# ``path.snapd/<id>.log``; the private copy is staged at
# ``<id>.log.tmp`` and atomically replaced before the prune plan is
# published, so a reader that observes the committed prune always finds a
# copy that is complete up to the committed pruned ordinal.  Records not
# yet evicted stay in the segment set itself.  Reads are lock-free: one
# :func:`_resolve_members` observation supplies both the surviving member
# list and the pruned ordinal the private copy must cover.
#
# Metadata at ``path.snapd/<id>.json`` appears atomically, so a crashed
# creation (only a ``.tmp`` left) names no readable handle; later writes
# sweep such orphans transparently.
# ---------------------------------------------------------------------------

_SNAP_ID_LEN = 32
_SNAP_HEX = frozenset("0123456789abcdef")


def _snapshot_dir(path):
    """The directory holding private snapshot sidecars for ``path``."""
    return path + _SNAP_DIR_SUFFIX


def _valid_snapshot_id(sid):
    """Snapshot ids are 32 lowercase hex characters, so never escape the
    sidecar directory via crafted handles."""
    return (
        isinstance(sid, str)
        and len(sid) == _SNAP_ID_LEN
        and all(ch in _SNAP_HEX for ch in sid)
    )


def _snapshot_meta_path(path, sid):
    return os.path.join(_snapshot_dir(path), sid + _SNAP_META_SUFFIX)


def _snapshot_copy_path(path, sid):
    return os.path.join(_snapshot_dir(path), sid + _SNAP_COPY_SUFFIX)


def _read_whole_file(path):
    """Read a small sidecar file completely."""
    fd = os.open(path, os.O_RDONLY)
    raw = b""
    try:
        while True:
            chunk = os.read(fd, _READ_CHUNK)
            if not chunk:
                break
            raw += chunk
    finally:
        os.close(fd)
    return raw


def _load_snapshot_meta(path, sid):
    """Return the published snapshot metadata dict, or ``None`` if absent.

    A published metadata file is always complete (it is renamed into place
    atomically); an externally damaged file reads like no snapshot -- the
    handle it named can no longer be authenticated.
    """
    try:
        raw = _read_whole_file(_snapshot_meta_path(path, sid))
    except FileNotFoundError:
        return None
    try:
        meta = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(meta, dict):
        return None
    if meta.get("id") != sid or not isinstance(meta.get("key"), str):
        return None
    start = meta.get("start")
    end = meta.get("end")
    if (isinstance(start, bool) or isinstance(end, bool)
            or not isinstance(start, int) or not isinstance(end, int)
            or start < 0 or end < start):
        return None
    if not isinstance(meta.get("path"), str):
        return None
    return meta


def _cleanup_snapshot_sidecars(path):
    """Reclaim snapshot files no published metadata names.

    Runs under the writer lock as part of :func:`_finish_pending`, so the
    private copy a released snapshot left behind and every ``.tmp`` a
    crashed create or copy rebuild dropped are swept by the next append,
    rotation, compaction, prune, snapshot or release.  A live snapshot
    keeps exactly its metadata and one private copy; the empty sidecar
    directory is removed as well.
    """
    snapd = _snapshot_dir(path)
    try:
        names = os.listdir(snapd)
    except FileNotFoundError:
        return
    live = set()
    for name in names:
        stem = name[:-len(_SNAP_META_SUFFIX)]
        if (len(name) == _SNAP_ID_LEN + len(_SNAP_META_SUFFIX)
                and name.endswith(_SNAP_META_SUFFIX)
                and _valid_snapshot_id(stem)):
            live.add(stem)
    suffixes = (
        _SNAP_COPY_SUFFIX + _SNAP_TMP_SUFFIX,
        _SNAP_META_SUFFIX + _SNAP_TMP_SUFFIX,
        _SNAP_COPY_SUFFIX,
    )
    for name in names:
        # Touch only files of our own ``<32-hex-id><suffix>`` shape;
        # anything else a user dropped in the directory is left alone.
        stem = None
        for suffix in suffixes:
            if (len(name) == _SNAP_ID_LEN + len(suffix)
                    and name.endswith(suffix)):
                stem = name[:_SNAP_ID_LEN]
                break
        if stem is None or not _valid_snapshot_id(stem):
            continue
        if name.endswith(_SNAP_COPY_SUFFIX) and stem in live:
            # A live snapshot's complete copy stays.
            continue
        _unlink_if_exists(os.path.join(snapd, name))
    try:
        os.rmdir(snapd)
    except OSError:
        pass


def _encode_snapshot_handle(path, sid, key):
    """Build the opaque, persistable handle string for a snapshot."""
    payload = json.dumps(
        {"v": 1, "p": os.path.abspath(path), "id": sid, "k": key},
        separators=(",", ":"),
    ).encode("utf-8")
    token = base64.urlsafe_b64encode(payload).decode("ascii")
    return token.rstrip("=")


def _decode_snapshot_handle(handle):
    """Decode an opaque handle into its payload dict.

    Raises :class:`TypeError` for a non-string and :class:`ValueError`
    for anything that is not a handle this module could have produced.
    """
    if not isinstance(handle, str):
        raise TypeError(
            f"snapshot handle must be a string, got {type(handle).__name__}"
        )
    try:
        padded = handle.encode("ascii")
        padded += b"=" * ((4 - len(padded) % 4) % 4)
        raw = base64.b64decode(
            padded, altchars=b"-_", validate=True
        )
        data = json.loads(raw.decode("utf-8"))
    except (binascii.Error, ValueError, UnicodeDecodeError) as exc:
        raise ValueError("invalid snapshot handle") from exc
    if not isinstance(data, dict):
        raise ValueError("invalid snapshot handle")
    sid = data.get("id")
    key = data.get("k")
    bound = data.get("p")
    if not _valid_snapshot_id(sid) or not isinstance(key, str) \
            or not isinstance(bound, str):
        raise ValueError("invalid snapshot handle")
    return data


def _authenticate_snapshot(path, payload):
    """Resolve and authenticate a decoded handle against ``path``.

    Returns the snapshot metadata.  Raises :class:`ValueError` when the
    handle belongs to another log, is forged, names a crashed/never
    committed creation, or has already been released.
    """
    bound = payload["p"]
    if bound != os.path.abspath(path):
        raise ValueError("snapshot handle belongs to a different metrics log")
    meta = _load_snapshot_meta(path, payload["id"])
    if meta is None:
        raise ValueError("snapshot handle is unknown or already released")
    if not hmac.compare_digest(meta["key"], payload["k"]):
        raise ValueError("snapshot handle is invalid")
    if meta["path"] != bound:
        raise ValueError("snapshot handle belongs to a different metrics log")
    return meta


def _stage_snapshot_copies(path, members, old_pruned, new_pruned):
    """Stage private copies for records a prune is about to evict.

    A single raw walk over the pre-prune ``members`` fans every evicted
    record out to each still-alive snapshot whose fixed window contains
    it.  A copy is rebuilt at ``<id>.log.tmp`` -- the marker-consistent
    prefix of the previous copy first (a crash after an earlier staging
    rename may have left that copy slightly AHEAD of the still-old
    marker, so only the first ``old_pruned - start`` records carry over),
    then the newly evicted range -- flushed, closed and atomically
    renamed over ``<id>.log``.  This happens before the prune plan is
    published, which is the invariant snapshot reads rely on: a reader
    that can observe the new pruned ordinal always finds a copy that
    covers it.  A crash during staging leaves only a ``.tmp`` beside the
    previous complete copy; it joins no read and the next write sweeps
    it and the rebuild simply repeats.
    """
    snapd = _snapshot_dir(path)
    try:
        names = os.listdir(snapd)
    except FileNotFoundError:
        return
    pinning = []
    tmp_paths = []
    try:
        for name in names:
            if not (len(name) == _SNAP_ID_LEN + len(_SNAP_META_SUFFIX)
                    and name.endswith(_SNAP_META_SUFFIX)):
                continue
            sid = name[:-len(_SNAP_META_SUFFIX)]
            if not _valid_snapshot_id(sid):
                continue
            meta = _load_snapshot_meta(path, sid)
            if meta is None:
                continue
            start, end = meta["start"], meta["end"]
            need = max(start, old_pruned)
            upto = min(end, new_pruned)
            if upto <= need:
                continue
            copy_path = _snapshot_copy_path(path, sid)
            tmp_path = copy_path + _SNAP_TMP_SUFFIX
            tmp_paths.append(tmp_path)
            out = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o666)
            prefix_limit = max(0, min(old_pruned, end) - start)
            if prefix_limit and os.path.exists(copy_path):
                # Carry only the marker-consistent prefix of the previous
                # copy (a crash may have left it one prune ahead); the
                # walk below appends exactly the newly evicted range.
                carried = 0
                for raw in _iter_raw_records(copy_path):
                    if carried >= prefix_limit:
                        break
                    _write_bytes(out, raw)
                    carried += 1
            pinning.append((need, upto, out))
        if not pinning:
            return
        ordinal = old_pruned
        for member in members:
            for raw in _iter_raw_records(member):
                for need, upto, out in pinning:
                    if need <= ordinal < upto:
                        _write_bytes(out, raw)
                ordinal += 1
        for _need, _upto, out in pinning:
            os.fsync(out)
            os.close(out)
    except BaseException:
        for _need, _upto, out in pinning:
            try:
                os.close(out)
            except OSError:
                pass
        for tmp_path in tmp_paths:
            _unlink_if_exists(tmp_path)
        raise
    # Published only after every descriptor is closed, so the rename
    # also succeeds on Windows.
    for tmp_path in tmp_paths:
        os.replace(tmp_path, tmp_path[:-len(_SNAP_TMP_SUFFIX)])


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

        old_pruned = _read_prune_state(path)[0]
        pruned = old_pruned + drop
        # Before the prune commits, pin every evicted record that a live
        # snapshot still fixes into that snapshot's private copy.  Once
        # the plan below is published the members may vanish, but the
        # copy already covers every ordinal up to the new pruned count.
        _stage_snapshot_copies(path, members, old_pruned, pruned)
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


def _validate_log_path(path):
    """The shared path type/shape check every public entry uses."""
    if not isinstance(path, str):
        raise OSError(f"log path must be a string, got {type(path).__name__}")
    if os.path.isdir(path):
        raise IsADirectoryError(f"log path is a directory: {path!r}")


def snapshot_metrics(path):
    """Fix the current complete record sequence and return a handle.

    The snapshot captures the write-order ordinals
    ``[pruned, pruned + surviving)`` of exactly this instant: every
    complete record the segment set holds now, in its current order and
    bytes (original newlines, ``-0.0``, oversized counters and key order
    all untouched).  Appends, rotations, compactions and later prunes
    never alter that sequence, and several snapshots are mutually
    independent.

    The returned string is an opaque, persistable handle: it binds the
    snapshot to this log (an absolute path) and carries a random key, so
    a forged handle or one produced for another segment set is rejected.
    Pass it to :func:`resume_snapshot_metrics` any number of times -- the
    result is always the creation-time slice -- and to
    :func:`release_metrics` when done.

    While the snapshot is alive, a prune that evicts one of its records
    copies the evicted bytes into the snapshot's private sidecar first,
    so a fixed record is either still in the segment set or already in
    the private copy, never silently dropped.  A crash mid-creation
    leaves only an unpublished staging file; it names no readable handle
    and the next write sweeps it.

    Raises:
        FileNotFoundError: ``path`` does not exist.
        IsADirectoryError: ``path`` is a directory.
        OSError: ``path`` is not a string, or locking or writing the
            snapshot metadata fails; a failed attempt leaves no partial
            metadata behind.
    """
    _validate_log_path(path)

    fd = _mutation_lock(path, create=False)
    try:
        _finish_pending(path)
        fd = _relock_after_settle(fd, path, create=False)

        members, pruned = _resolve_members(path)
        if not members:
            raise FileNotFoundError(f"no metrics log or segments at {path!r}")
        count = 0
        for member in members:
            count += sum(1 for _ in _iter_raw_records(member))

        sid = secrets.token_hex(_SNAP_ID_LEN // 2)
        key = secrets.token_urlsafe(24)
        meta = {
            "id": sid,
            "key": key,
            "path": os.path.abspath(path),
            "start": pruned,
            "end": pruned + count,
        }
        snapd = _snapshot_dir(path)
        os.makedirs(snapd, exist_ok=True)
        meta_path = _snapshot_meta_path(path, sid)
        tmp_path = meta_path + _SNAP_TMP_SUFFIX
        try:
            out = os.open(
                tmp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o666
            )
            try:
                _write_bytes(
                    out,
                    json.dumps(meta, separators=(",", ":")).encode("utf-8"),
                )
                os.fsync(out)
            finally:
                os.close(out)
            os.replace(tmp_path, meta_path)
        except BaseException:
            _unlink_if_exists(tmp_path)
            raise
        return _encode_snapshot_handle(path, sid, key)
    finally:
        if fd is not None:
            os.close(fd)


def release_metrics(path, handle):
    """Release a snapshot handle and drop its private copy.

    Afterwards the handle reads no longer: reading it or releasing it
    again raises :class:`ValueError`.  Other snapshots and the segment
    set itself are untouched; any staging file a crashed creation or
    prune left behind is swept as part of the call.

    Raises:
        FileNotFoundError: ``path`` does not exist.
        IsADirectoryError: ``path`` is a directory.
        OSError: ``path`` is not a string or locking fails.
        TypeError: ``handle`` is not a string.
        ValueError: the handle is forged, belongs to another log, was
            never committed or has already been released.
    """
    _validate_log_path(path)
    members, _pruned = _resolve_members(path)
    if not members:
        raise FileNotFoundError(f"no metrics log or segments at {path!r}")
    payload = _decode_snapshot_handle(handle)
    if payload["p"] != os.path.abspath(path):
        raise ValueError("snapshot handle belongs to a different metrics log")

    fd = _mutation_lock(path, create=False)
    try:
        _finish_pending(path)
        fd = _relock_after_settle(fd, path, create=False)
        meta = _authenticate_snapshot(path, payload)
        _unlink_if_exists(_snapshot_meta_path(path, meta["id"]))
        _unlink_if_exists(_snapshot_copy_path(path, meta["id"]))
        _cleanup_snapshot_sidecars(path)
    finally:
        if fd is not None:
            os.close(fd)


def _acquire_shared_lock(path):
    """Take the readers' shared side of the mutation lock, or fall back.

    Snapshot reads hold :data:`fcntl.LOCK_SH` on the live log for the
    whole walk: appends, rotations, compactions, prunes, snapshot
    creation and release all hold ``LOCK_EX`` on that same file, so once
    the shared lock is granted none of them can rename a segment out
    from under the walk or truncate the live log in place (compaction's
    same-inode truncate is the one change marker gating cannot see).  A
    rotation recreates the live log while the writer's lock is held, so
    after waiting the descriptor is re-checked against the path exactly
    like :func:`_open_locked`.

    Returns ``None`` on a platform without :mod:`fcntl`, or when the
    live log is momentarily missing (a crashed rotation's narrow
    window): then no same-inode mutator can run either -- every writer
    requires the live log to take its lock -- so the lock-free identity
    gating in :func:`_iter_snapshot` is sufficient.
    """
    if fcntl is None:
        return None
    for _ in range(1000):
        try:
            fd = os.open(path, os.O_RDONLY)
        except FileNotFoundError:
            return None
        try:
            fcntl.flock(fd, fcntl.LOCK_SH)
        except OSError:
            os.close(fd)
            raise
        if _same_inode(fd, path):
            return fd
        os.close(fd)
    return None


def _iter_snapshot(path, meta, position):
    """Yield the fixed snapshot records at or after write ordinal ``position``.

    Lock-free and crash-consistent the same way reads of the live set
    are.  The private copy holds a prefix of the snapshot's own
    ordinals, rebuilt and atomically renamed *before* the prune plan it
    belongs to is published; the resolved surviving members hold the
    rest, anchored by the pruned ordinal observed in the same
    marker-consistent resolution.  The two ranges therefore meet at the
    observed pruned ordinal; they can only overlap when a crash left a
    rebuilt copy one prune ahead of a still-old marker, in which case the
    overlap is read from the copy exactly once.

    Every descriptor -- the copy and each resolved member -- is opened
    and (for members) identity-checked against the resolution *before*
    the first record leaves this generator, so an eviction or replace a
    concurrent prune causes a silent re-resolve rather than a
    :class:`FileNotFoundError`, and a re-resolution can never duplicate a
    record the caller has already received.  A descriptor held open
    across the prune that unlinks it still reads the same whole bytes.
    """
    start = meta["start"]
    end = meta["end"]
    copy_path = _snapshot_copy_path(path, meta["id"])
    cutoff = max(start, position)

    # Held for the whole walk (see _acquire_shared_lock): while it is
    # held every mutator blocks, so the marker/identity gates never go
    # stale on a locked platform.  It may be absent only where locking
    # is unavailable or the live log is in rotation's missing window.
    lock_fd = _acquire_shared_lock(path)
    try:
        for _ in range(1000):
            # One marker-consistent observation anchors the member ordinals.
            ids, pruned_now, marker = _resolve_member_ids(path)

            copy_fd = None
            member_fds = []
            try:
                if os.path.exists(copy_path):
                    try:
                        copy_fd = os.open(copy_path, os.O_RDONLY)
                    except FileNotFoundError:
                        continue
                stale = False
                for member, dev, ino, size in ids:
                    try:
                        member_fd = os.open(member, os.O_RDONLY)
                    except FileNotFoundError:
                        stale = True
                        break
                    stat = os.fstat(member_fd)
                    if dev is not None and (
                        stat.st_dev,
                        stat.st_ino,
                        stat.st_size,
                    ) != (dev, ino, size):
                        os.close(member_fd)
                        stale = True
                        break
                    member_fds.append((member, member_fd))
                if stale:
                    continue
                # Belt and braces for the lock-less paths: a prune
                # committing between the identity stats and the last
                # open changed the marker; retry before yielding.
                if _marker_observation(path) != marker:
                    continue

                # All inputs are pinned open and identity-consistent; a
                # later commit only unlinks inodes we already hold.
                ordinal = start
                if copy_fd is not None:
                    for record in _parse_raw_fd(copy_fd, copy_path):
                        if ordinal >= end:
                            break
                        if ordinal >= cutoff:
                            yield record
                        ordinal += 1
                copy_upto = ordinal
                if copy_upto < min(pruned_now, end):
                    # Defensive: the staged-before-published ordering
                    # makes a short copy unobservable; retry rather than
                    # invent bytes.
                    continue

                seen = 0
                for member, member_fd in member_fds:
                    for record in _parse_raw_fd(member_fd, member):
                        ordinal = pruned_now + seen
                        seen += 1
                        if ordinal < copy_upto or ordinal < cutoff:
                            continue
                        if ordinal >= end:
                            break
                        yield record
            finally:
                if copy_fd is not None:
                    try:
                        os.close(copy_fd)
                    except OSError:
                        pass
                for _member, member_fd in member_fds:
                    try:
                        os.close(member_fd)
                    except OSError:
                        pass
            return
    finally:
        if lock_fd is not None:
            os.close(lock_fd)


def resume_snapshot_metrics(path, handle, position=0):
    """Stream records of a snapshot from write-order position ``position``.

    Positions are the same write-order ordinals :func:`resume_metrics`
    uses, counted across the whole log's history: records pruned before
    or after the snapshot was taken keep theirs.  A position that lands
    in a region the snapshot's pruned prefix or the set's own pruned
    prefix once occupied starts at the oldest record the snapshot still
    offers, and a position equal to the snapshot's creation-time record
    total yields nothing.  Only a position past that total is out of
    range.  Repeated calls -- including after appends, rotations,
    compactions, quota-zero prunes and other snapshots' release -- yield
    exactly the creation-time slice, byte for byte.

    Raises:
        FileNotFoundError: the log no longer exists.
        IsADirectoryError: ``path`` is a directory.
        OSError: ``path`` is not a string.
        TypeError: ``handle`` is not a string, or ``position`` is not an
            integer (booleans do not count).
        ValueError: ``position`` is negative or past the creation-time
            total, or the handle is forged, belongs to another log, was
            never committed or has already been released.
    """
    _validate_log_path(path)
    # Eager path and existence checks first, matching iter_metrics and
    # resume_metrics; argument/handle errors are reported afterwards.
    members, _pruned = _resolve_members(path)
    if not members:
        raise FileNotFoundError(f"no metrics log or segments at {path!r}")
    if isinstance(position, bool) or not isinstance(position, int):
        raise TypeError(
            f"read position must be an integer, got {type(position).__name__}"
        )
    if position < 0:
        raise ValueError("read position must not be negative")
    payload = _decode_snapshot_handle(handle)
    meta = _authenticate_snapshot(path, payload)
    if position > meta["end"]:
        raise ValueError(
            f"read position {position} exceeds the snapshot record total "
            f"of {meta['end']}"
        )

    return _iter_snapshot(path, meta, position)
