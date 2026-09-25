"""Integrity and tamper-evidence audit chain for a metric segment set.

The audit chain registers every record of a segment set -- the exact
bytes it was written with and its write-order ordinal -- beside the set
itself, so a record that was modified, truncated, dropped or mixed in
afterwards is found by re-checking the set against the chain.

The chain state lives in one sidecar file, ``path.audit``: a single JSON
object line

    {"version": 1, "pruned": P, "base": B, "tail": T, "entries": [...]}

serialised with the established exact rules -- counts and checksums are
written as exact decimal integers and never pass through floating point,
and keys keep their written order.  ``entries`` holds one 256-bit byte
fingerprint per surviving record in write order; the fingerprint covers
the record's exact stored bytes, newline included, so ``-0.0``,
oversized counters and key order are all part of it.  ``base`` and
``tail`` fold the fingerprints into a chain: the tail is the chain value
after the last registered record, and the base is the chain value at the
prune point, so records pruned from the oldest end keep their
write-order ordinals (carried as the pruned count ``P``) without the
chain storing their fingerprints.  The state is staged at
``path.audit.tmp``, fsynced and atomically renamed into place, so a
crash never leaves half a state file behind -- at worst the chain
momentarily lags the record set, and the next append, rotation,
compaction, prune, snapshot or chain build catches it up under the
write lock before continuing.  Old segment sets need no migration: the
chain simply does not exist until it is built.

:func:`audit_metrics` builds the chain the first time and refreshes it
incrementally afterwards, returning the number of registered records and
the chain-tail checksum.  :func:`verify_metrics` re-checks the segment
set against the chain record by record: an empty set or one holding only
a torn tail matches an empty chain, a record changed by as little as one
byte or mixed into the set is reported with its file and line number,
and a record truncated away or a replaced segment is reported by the
write-order ordinal that no longer lines up.  Verification reads without
taking the write lock, so it neither blocks nor is blocked by writers,
and same-process reads and writes interleaved with it neither deadlock
nor report spurious locking failures.
"""

from __future__ import annotations

import hashlib
import json
import os

from .persistence import (
    _READ_CHUNK,
    _finish_pending,
    _iter_lines,
    _iter_raw_file_records,
    _mutation_lock,
    _read_prune_state,
    _relock_after_settle,
    _resolve_members,
    _segment_members,
    _unlink_if_exists,
    _write_bytes,
)

__all__ = [
    "audit_metrics",
    "verify_metrics",
]

# The chain state is ``path.audit``, one JSON object line; its staging
# name is ``path.audit.tmp``.  Both are non-numeric sidecars, invisible
# to ``_segment_numbers``.
_AUDIT_SUFFIX = ".audit"
_AUDIT_TMP_SUFFIX = ".audit.tmp"
_STATE_VERSION = 1
# Fingerprints and chain values are 256-bit numbers, written as exact
# decimal integers.
_HASH_SIZE = 32
_CHAIN_LIMIT = 1 << 256


def _record_digest(raw):
    """Fingerprint one complete record's stored bytes (newline included)."""
    return int.from_bytes(hashlib.sha256(b"\x00" + raw).digest(), "big")


def _chain_link(previous, digest):
    """Fold one record fingerprint into the running chain value."""
    payload = (
        b"\x01"
        + previous.to_bytes(_HASH_SIZE, "big")
        + digest.to_bytes(_HASH_SIZE, "big")
    )
    return int.from_bytes(hashlib.sha256(payload).digest(), "big")


def _audit_path(path):
    """The audit state file path (``path.audit``)."""
    return path + _AUDIT_SUFFIX


def _load_audit_state(path):
    """Read and structurally validate the audit state file.

    A missing state file raises :class:`FileNotFoundError`; anything
    present but not exactly one whole, self-consistent chain document is
    a corrupt state file and raises :class:`ValueError`.
    """
    audit_path = _audit_path(path)
    try:
        fd = os.open(audit_path, os.O_RDONLY)
    except FileNotFoundError:
        raise FileNotFoundError(f"no audit chain at {audit_path!r}") from None
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
        raise ValueError(f"corrupt audit state {audit_path!r}: {exc}") from exc
    if not isinstance(state, dict) or set(state) != {
        "version", "pruned", "base", "tail", "entries"
    }:
        raise ValueError(
            f"corrupt audit state {audit_path!r}: unexpected audit fields"
        )
    if state["version"] != _STATE_VERSION:
        raise ValueError(f"corrupt audit state {audit_path!r}: bad audit state")
    for key in ("pruned", "base", "tail"):
        # Booleans are not acceptable chain numbers.
        if isinstance(state[key], bool) or not isinstance(state[key], int):
            raise ValueError(
                f"corrupt audit state {audit_path!r}: {key} must be an integer"
            )
        if state[key] < 0 or state[key] >= _CHAIN_LIMIT:
            raise ValueError(
                f"corrupt audit state {audit_path!r}: {key} is out of range"
            )
    entries = state["entries"]
    if not isinstance(entries, list):
        raise ValueError(
            f"corrupt audit state {audit_path!r}: entries must be a list"
        )
    for entry in entries:
        if isinstance(entry, bool) or not isinstance(entry, int):
            raise ValueError(
                f"corrupt audit state {audit_path!r}: fingerprint must be "
                f"an integer"
            )
        if entry < 0 or entry >= _CHAIN_LIMIT:
            raise ValueError(
                f"corrupt audit state {audit_path!r}: fingerprint is out "
                f"of range"
            )
    # The tail must be exactly the chain value the fingerprints fold to;
    # a state file edited after the fact does not line up.
    link = state["base"]
    for entry in entries:
        link = _chain_link(link, entry)
    if link != state["tail"]:
        raise ValueError(
            f"corrupt audit state {audit_path!r}: chain tail does not "
            f"match the registered fingerprints"
        )
    return state


def _write_audit_state(path, state):
    """Atomically publish ``state`` as the audit state file and fsync it.

    The replacement is staged at ``path.audit.tmp``, fsynced and
    atomically renamed over the state file, so the state is never
    observed half written and a crash leaves either the previous state
    or this one.  A failed write removes the staging file and leaves the
    previously published state untouched.
    """
    payload = json.dumps(state, separators=(",", ":")) + "\n"
    tmp = path + _AUDIT_TMP_SUFFIX
    try:
        out = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o666)
        try:
            _write_bytes(out, payload.encode("utf-8"))
            os.fsync(out)
        finally:
            # Closed before the rename: on Windows an open file cannot
            # be renamed.
            os.close(out)
        os.replace(tmp, _audit_path(path))
    except BaseException:
        _unlink_if_exists(tmp)
        raise


def _build_audit_state(members, pruned):
    """Compute a fresh chain over every record of the segment set.

    Records pruned before the chain exists have no fingerprints to
    register; they survive only as the pruned count, and the chain
    starts at the oldest surviving record with a zero base.
    """
    entries = []
    link = 0
    for member in members:
        for raw in _iter_raw_file_records(member):
            digest = _record_digest(raw)
            link = _chain_link(link, digest)
            entries.append(digest)
    return {
        "version": _STATE_VERSION,
        "pruned": pruned,
        "base": 0,
        "tail": link,
        "entries": entries,
    }


def _sync_audit_locked(path, build=False):
    """Catch the audit chain up with the settled segment set.

    Called with the write lock held, after any half-finished compaction,
    prune or snapshot has been settled.  With no chain present nothing
    happens unless ``build`` is set -- writes keep an existing chain
    consistent, while :func:`audit_metrics` also creates it.  A chain
    that merely lags (records appended since the last catch-up) is
    extended over the new records; a chain whose prune point moved is
    folded forward, the pruned prefixes dropped from ``entries`` and
    carried only by the base.  A chain that names records the set no
    longer holds -- modified, truncated or replaced records -- cannot be
    caught up and raises :class:`ValueError`.
    """
    # A staging file a crashed write never published is not a state; the
    # atomically renamed state file alone is authoritative.
    _unlink_if_exists(path + _AUDIT_TMP_SUFFIX)
    members = _segment_members(path)
    pruned = _read_prune_state(path)[0]
    try:
        state = _load_audit_state(path)
    except FileNotFoundError:
        if not build:
            return
        _write_audit_state(path, _build_audit_state(members, pruned))
        return
    skip = pruned - state["pruned"]
    if skip < 0:
        raise ValueError(
            f"audit chain pruned count {state['pruned']} exceeds the "
            f"segment set's {pruned}"
        )
    entries = state["entries"]
    if skip > len(entries):
        # The prune point lies past every registered record (the pruned
        # records were written before the chain existed): the chain
        # shares no ordinal with the set, so it starts over.
        _write_audit_state(path, _build_audit_state(members, pruned))
        return
    # Fold the pruned prefix into the base; the tail is unchanged
    # because folding is sequential.
    base = state["base"]
    for digest in entries[:skip]:
        base = _chain_link(base, digest)
    registered = entries[skip:]
    link = state["tail"]
    kept = list(registered)
    count = 0
    for member in members:
        for raw in _iter_raw_file_records(member):
            if count < len(registered):
                # Already registered; verify_metrics re-checks these.
                pass
            else:
                digest = _record_digest(raw)
                link = _chain_link(link, digest)
                kept.append(digest)
            count += 1
    if count < len(registered):
        raise ValueError(
            f"audit chain names write ordinal {pruned + count} but the "
            f"record set holds no such record"
        )
    if skip == 0 and count == len(registered):
        # Already up to date; leave the published state untouched.
        return
    _write_audit_state(
        path,
        {
            "version": _STATE_VERSION,
            "pruned": pruned,
            "base": base,
            "tail": link,
            "entries": kept,
        },
    )


def audit_metrics(path):
    """Build or incrementally refresh the audit chain of the segment set.

    The first call registers every record of the segment set -- its
    exact stored bytes and its write-order ordinal -- in the chain state
    file ``path.audit``; later calls extend the chain over records
    appended since and fold pruned records into the chain base, so old
    segment sets need no migration and a chain left behind by a crash is
    caught up before anything else happens.  Returns ``(registered,
    tail)``: the number of records the chain covers and the chain-tail
    checksum, both exact integers.

    Building and refreshing take the same write lock as appends,
    rotations, compactions, prunes and snapshots, and any half-finished
    mutation left by a crash is settled first.  The state file is
    staged, fsynced and atomically renamed into place, so a crash never
    leaves half a state file behind.

    Raises:
        FileNotFoundError: ``path`` does not exist.
        IsADirectoryError: ``path`` is a directory.
        ValueError: the state file is corrupt, or the chain names
            records the segment set no longer holds.
        OSError: ``path`` is not a string, or locking or writing the
            state file fails.
    """
    if not isinstance(path, str):
        raise OSError(f"log path must be a string, got {type(path).__name__}")
    if os.path.isdir(path):
        raise IsADirectoryError(f"log path is a directory: {path!r}")

    fd = _mutation_lock(path, create=False)
    try:
        _finish_pending(path)
        fd = _relock_after_settle(fd, path, create=False)
        _sync_audit_locked(path, build=True)
    finally:
        if fd is not None:
            os.close(fd)
    state = _load_audit_state(path)
    return len(state["entries"]), state["tail"]


def _iter_numbered_records(members):
    """Yield ``(member, line_number, raw_record)`` across the segment set.

    The walk matches a fresh recovery exactly: complete
    newline-terminated records come back verbatim with ``\\n``
    reattached, blank lines are skipped and a torn unterminated tail is
    dropped, so an empty set or one holding only a torn tail yields
    nothing.
    """
    for member in members:
        for lineno, raw in _iter_lines(member):
            if raw and raw != b"\r":
                yield member, lineno, raw + b"\n"


def verify_metrics(path):
    """Re-check every record of the segment set against the audit chain.

    Each surviving record's stored bytes are fingerprinted and compared
    with the fingerprint the chain registered for its write-order
    ordinal.  When every record lines up the call returns normally; an
    empty segment set or one holding only a torn tail matches an empty
    chain.  A record modified by as little as one byte, or one mixed
    into the set, raises :class:`ValueError` naming its file and line
    number; a record truncated away or a segment replaced wholesale
    raises :class:`ValueError` naming the write-order ordinal that no
    longer lines up.

    Verification resolves the segment set the same lock-free way a
    recovery does and never takes the write lock, so a concurrent
    append, rotation, compaction or prune proceeds normally and a
    same-process writer neither deadlocks against the check nor reports
    a spurious locking failure.

    Raises:
        FileNotFoundError: neither ``path`` nor any segment exists, or
            the audit chain state file does not exist.
        IsADirectoryError: ``path`` is a directory.
        OSError: ``path`` is not a string.
        ValueError: the state file is corrupt, or a record does not
            match the audit chain.
    """
    if not isinstance(path, str):
        raise OSError(f"log path must be a string, got {type(path).__name__}")
    if os.path.isdir(path):
        raise IsADirectoryError(f"log path is a directory: {path!r}")

    members, pruned = _resolve_members(path)
    if not members:
        raise FileNotFoundError(f"no metrics log or segments at {path!r}")
    state = _load_audit_state(path)
    skip = pruned - state["pruned"]
    if skip < 0:
        raise ValueError(
            f"audit mismatch: audit chain pruned count {state['pruned']} "
            f"exceeds the segment set's {pruned}"
        )
    entries = state["entries"]
    registered = entries[skip:] if skip <= len(entries) else []

    # Walk the records one at a time beside the registered fingerprints,
    # keeping one record of look-ahead so a vanished record (the chain
    # entry lines up with the next record) is told apart from a modified
    # or mixed-in one (the record at this position lines up with
    # nothing).
    records = _iter_numbered_records(members)
    current = next(records, None)
    index = 0
    while current is not None:
        member, lineno, raw = current
        digest = _record_digest(raw)
        following = next(records, None)
        if index < len(registered) and digest == registered[index]:
            index += 1
        elif index >= len(registered):
            raise ValueError(
                f"audit mismatch in {member} at line {lineno}: record is "
                f"not registered in the audit chain"
            )
        elif index + 1 < len(registered) and digest == registered[index + 1]:
            # The registered record vanished from the set.
            raise ValueError(
                f"audit mismatch: write ordinal {pruned + index} is "
                f"registered in the audit chain but missing from the "
                f"record set"
            )
        elif following is not None and _record_digest(following[2]) == (
            registered[index]
        ):
            # This record is unknown to the chain; the next one lines
            # up with the fingerprint at this position.
            raise ValueError(
                f"audit mismatch in {member} at line {lineno}: record is "
                f"not registered in the audit chain"
            )
        else:
            raise ValueError(
                f"audit mismatch in {member} at line {lineno}: record "
                f"content does not match the audit chain"
            )
        current = following
    if index < len(registered):
        raise ValueError(
            f"audit mismatch: write ordinal {pruned + index} is "
            f"registered in the audit chain but missing from the record "
            f"set"
        )
