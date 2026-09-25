"""Durable weighted sampling batches bound to an append-only metric log.

A sampling batch carries the progress of one without-replacement weighted
draw next to its metric log in a caller-named batch file.
:func:`sample_batch_metrics` starts that state from zero on first use and,
on every later call with the same batch file, continues the exact same
draw; :func:`resume_batch_metrics` reads the already drawn indices back
from a write-order position.

The draw uses the established :class:`~weighted_batcher.Sampler`
semantics exactly: weights of ``0`` and ``-0.0`` have effective weight
zero and can never be drawn, a non-real weight is a :class:`TypeError`
and a negative, NaN or infinite weight is a :class:`ValueError`.
Without replacement, indices already drawn -- including draws made by
earlier processes before a restart -- never come back, un-drawn indices
are never skipped, and asking for more draws than the remaining
positive-weight items raises :class:`ValueError`.  A batch draws from
one fixed seeded stream, so rebuilding the sampler and fast-forwarding
past the stored draws reproduces every PRNG step an uninterrupted run
would have made: after a restart the continued index sequence is, item
for item, the sequence one uninterrupted draw produces.

Every advance publishes the whole state by staging it at
``batch_path.tmp``, fsyncing it and atomically renaming it over the
batch file, so a crash leaves either the pre-advance file or the
post-advance one, never a half-written state.  The private staging file
a crash leaves unnamed is removed transparently by the next write.
Distinct batch files use distinct locks anchored on their own
``batch_path.lock`` -- never the log and never the state file itself --
so two processes advancing one file serialise without losing an update
and never self-deadlock or report a spurious locking failure, while
different batch files and every other log operation proceed
independently.

The state is one JSON object serialised with the established exact
rules: oversized integer weights and counters never pass through
floating point, ``-0.0`` is preserved, and keys keep their insertion
order.  Old data needs no migration: a missing batch file is simply a
batch that has never been started.
"""

from __future__ import annotations

import json
import os

from . import Sampler
from .persistence import _write_bytes

try:  # POSIX-only; the lock serialises every advance of one batch file.
    import fcntl
except ImportError:  # pragma: no cover - exercised on non-POSIX platforms
    fcntl = None

__all__ = [
    "sample_batch_metrics",
    "resume_batch_metrics",
]

# A batch draws from one fixed seeded stream, so the same weights always
# reproduce the same sequence item by item -- including after a restart
# in a fresh process, where an OS-seeded stream could not be rebuilt.
_BATCH_SEED = 0
_READ_CHUNK = 1 << 20
_STATE_VERSION = 1
# Sidecar names beside the caller-named batch file.  The batch file
# itself is the one atomically renamed JSON object; it is never replaced
# in place.  Its lock file is never renamed or replaced either, so the
# held lock stays valid while the state changes inode underneath it.
_TMP_SUFFIX = ".tmp"
_LOCK_SUFFIX = ".lock"


def _validate_weights(weights):
    """Validate weights with the Sampler's own rules and normalise for JSON.

    Returns the weights exactly as they must be stored and later drawn
    with: plain integers (booleans already rejected by the
    :class:`~weighted_batcher.Sampler` constructor) keep their exact
    magnitude, so an oversized counter never passes through a float,
    while every other real number collapses to the float the sampler
    would draw with -- negative zero staying ``-0.0``.  A non-real
    weight is a :class:`TypeError`; a negative, NaN or infinite one is a
    :class:`ValueError`, exactly as in :class:`~weighted_batcher.Sampler`.
    """
    # Constructing a Sampler performs the public weight validation, so a
    # batch can never accept a weight the sampler itself would reject.
    sampler = Sampler(weights, replacement=False, seed=_BATCH_SEED)
    normalised = []
    for weight in sampler.weights:
        if isinstance(weight, int) and not isinstance(weight, bool):
            normalised.append(weight)
        else:
            normalised.append(float(weight))
    return normalised


def _validate_count(count):
    """Type- and range-check one per-call draw count (booleans do not count)."""
    if isinstance(count, bool) or not isinstance(count, int):
        raise TypeError(
            f"sample count must be an integer, got {type(count).__name__}"
        )
    if count < 0:
        raise ValueError("sample count must not be negative")


def _batch_lock(batch_path):
    """Take the exclusive lock for one batch file and return its fd.

    The lock is anchored on the dedicated ``batch_path.lock`` file,
    never on the log and never on the batch state file, so an advance
    interleaved with a log append, rotation, compaction, prune or an
    advance of another batch neither deadlocks nor reports a spurious
    locking failure.  The blocking lock waits its turn instead of
    failing, so two processes sharing one file never report a false
    locking failure.  Returns ``None`` on platforms without :mod:`fcntl`.
    """
    if fcntl is None:
        return None
    fd = os.open(batch_path + _LOCK_SUFFIX, os.O_RDWR | os.O_CREAT, 0o666)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
    except OSError:
        os.close(fd)
        raise
    return fd


def _reject_constant(name):
    """Turn a NaN/Infinity JSON token into a parse failure."""
    raise ValueError(f"invalid numeric constant {name}")


def _read_state(batch_path):
    """Read and structurally validate the published batch state.

    A missing batch file propagates :class:`FileNotFoundError`; a path
    naming a directory propagates :class:`IsADirectoryError`; anything
    present but undecodable or not exactly one whole batch-state
    document is a corrupt batch file and raises :class:`ValueError`.
    """
    fd = os.open(batch_path, os.O_RDONLY)
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
        state = json.loads(raw.decode("utf-8"), parse_constant=_reject_constant)
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError(f"corrupt batch file {batch_path!r}: {exc}") from exc
    if not isinstance(state, dict) or set(state) != {
        "version", "weights", "drawn"
    }:
        raise ValueError(
            f"corrupt batch file {batch_path!r}: unexpected batch fields"
        )
    if state["version"] != _STATE_VERSION or not isinstance(
        state["weights"], list
    ):
        raise ValueError(f"corrupt batch file {batch_path!r}: bad batch state")
    weights = state["weights"]
    positive = 0
    for weight in weights:
        if isinstance(weight, bool) or not isinstance(weight, (int, float)):
            raise ValueError(
                f"corrupt batch file {batch_path!r}: weight must be a number"
            )
        if weight != weight or weight in (float("inf"), float("-inf")):
            raise ValueError(
                f"corrupt batch file {batch_path!r}: invalid stored weight"
            )
        if weight < 0:
            raise ValueError(
                f"corrupt batch file {batch_path!r}: negative stored weight"
            )
        if weight > 0:
            positive += 1
    drawn = state["drawn"]
    if not isinstance(drawn, list):
        raise ValueError(
            f"corrupt batch file {batch_path!r}: drawn indices must be a list"
        )
    seen = set()
    for index in drawn:
        if isinstance(index, bool) or not isinstance(index, int):
            raise ValueError(
                f"corrupt batch file {batch_path!r}: drawn index must be an "
                f"integer"
            )
        if index < 0 or index >= len(weights) or not weights[index] > 0:
            raise ValueError(
                f"corrupt batch file {batch_path!r}: drawn index {index} is "
                f"not a positive-weight item"
            )
        if index in seen:
            raise ValueError(
                f"corrupt batch file {batch_path!r}: drawn index {index} "
                f"appears more than once"
            )
        seen.add(index)
    if len(drawn) > positive:
        raise ValueError(
            f"corrupt batch file {batch_path!r}: more draws than "
            f"positive-weight items"
        )
    return state


def _write_state(batch_path, state):
    """Atomically publish ``state`` as the batch file.

    The state is staged complete at ``batch_path.tmp``, fsynced and
    atomically renamed over the batch file, so the published file is
    never observed half written and a crash leaves either the previous
    state or this one.  Serialisation is the caller's concern (the batch
    lock); a failed write removes the staging file and leaves the
    previously published state untouched.
    """
    tmp = batch_path + _TMP_SUFFIX
    payload = json.dumps(state, separators=(",", ":")).encode("utf-8")
    try:
        out = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o666)
        try:
            _write_bytes(out, payload)
            os.fsync(out)
        finally:
            # Closed before the rename: on Windows an open file cannot
            # be renamed.
            os.close(out)
        os.replace(tmp, batch_path)
    except BaseException:
        # Nothing reached the published name; the private staging file is
        # the only path touched, so removing it leaves the previously
        # published batch file exactly as it was.
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise


def _drop_staging_leftover(batch_path):
    """Remove the private staging file a crashed advance never published."""
    try:
        os.unlink(batch_path + _TMP_SUFFIX)
    except FileNotFoundError:
        pass


def _positive_count(weights):
    """The number of weights with positive effective weight."""
    return sum(1 for weight in weights if weight > 0)


def sample_batch_metrics(path, weights, count, batch_path):
    """Draw ``count`` items of one durable without-replacement weighted batch.

    ``path`` is the metric log the batch belongs to (a batch only reads
    and writes its own batch file; the log is never modified),
    ``weights`` is the fixed list of item weights, ``count`` is how many
    items this call draws, and ``batch_path`` names the file that carries
    the batch's progress.  The first call for a missing batch file
    starts the sampling state from zero; every later call with the same
    file continues it.  The indices drawn by this call are returned in
    draw order; drawing zero items returns an empty list -- it still
    establishes a brand-new batch as a from-zero state, while a zero
    draw on an existing batch neither advances nor rewrites anything.

    The draw follows :class:`~weighted_batcher.Sampler` without-replacement
    semantics: an index already drawn by any earlier call -- including
    calls from earlier processes after a restart -- is never drawn
    again, no still-available index is skipped, and a zero or
    negative-zero weight can never be drawn.  Requesting more draws than
    the remaining positive-weight items raises :class:`ValueError` and
    leaves the state untouched.  Because a batch rebuilds its one fixed
    seeded stream and fast-forwards past every stored draw, the index
    sequence continued after a restart equals the sequence one
    uninterrupted draw produces, item by item.

    The state is written after every advance, staged and atomically
    renamed, so a crash leaves either the pre-advance state or the
    post-advance one and the next write clears any half-finished staging
    file transparently.  Advances sharing one batch file are serialised
    by a lock anchored on ``batch_path.lock`` -- never on the log -- so
    they lose no update and neither deadlock nor report a spurious lock
    failure; different batch files never interfere.

    Raises:
        TypeError: a weight is not a real number, or ``count`` is not an
            integer (booleans do not count).
        ValueError: a weight is negative, NaN or Infinity, ``count`` is
            negative, more than the remaining positive-weight items are
            requested, the stored weights differ from the weights
            passed, or the batch file is corrupt.
        FileNotFoundError: the parent directory of the batch file does
            not exist.
        IsADirectoryError: ``path`` or ``batch_path`` is a directory.
        OSError: ``path`` or ``batch_path`` is not a string, or locking
            or writing the batch state fails.  A failed write leaves the
            previously published state (if any) untouched.
    """
    if not isinstance(path, str):
        raise OSError(f"log path must be a string, got {type(path).__name__}")
    if not isinstance(batch_path, str):
        raise OSError(
            f"batch path must be a string, got {type(batch_path).__name__}"
        )
    # Argument validation matches the established entry-point order: the
    # inputs are checked before any state file is addressed, so a bad
    # weight or count is reported even for a missing batch file.
    normalised = _validate_weights(weights)
    _validate_count(count)
    if os.path.isdir(path):
        raise IsADirectoryError(f"log path is a directory: {path!r}")
    if os.path.isdir(batch_path):
        raise IsADirectoryError(
            f"batch path is a directory: {batch_path!r}"
        )
    # Zero items is no advance; it still establishes a brand-new batch
    # as a from-zero state below, but on an existing batch it writes
    # nothing.
    lock_fd = _batch_lock(batch_path)
    try:
        # A crashed predecessor's partial staging file is not a state;
        # the atomically published batch file alone is authoritative.
        _drop_staging_leftover(batch_path)
        try:
            state = _read_state(batch_path)
        except FileNotFoundError:
            state = None
        if state is None:
            weights = normalised
            drawn = []
        else:
            weights = state["weights"]
            # The batch plan is fixed: continuing with different weights
            # would silently change what the stored indices mean.
            if weights != normalised:
                raise ValueError(
                    f"existing batch file {batch_path!r} was started with "
                    f"different weights"
                )
            drawn = list(state["drawn"])

        # The entry establishes the batch on first use, so a zero draw
        # on a fresh file still publishes the from-zero state; on an
        # existing batch a zero draw advances nothing and rewrites
        # nothing.
        if count == 0:
            if state is None:
                _write_state(
                    batch_path,
                    {
                        "version": _STATE_VERSION,
                        "weights": weights,
                        "drawn": [],
                    },
                )
            return []

        remaining = _positive_count(weights) - len(drawn)
        if count > remaining:
            raise ValueError(
                f"cannot draw {count} items without replacement: only "
                f"{remaining} positive-weight items remain"
            )

        # Rebuild the fixed seeded stream at its exact midpoint.  The
        # Sampler's without-replacement pool depletes deterministically,
        # so consuming the stored draws reproduces every PRNG step an
        # uninterrupted run would have made, and the next ``count``
        # draws are exactly the ones that run would produce now.
        sampler = Sampler(weights, replacement=False, seed=_BATCH_SEED)
        if drawn:
            sampler.sample(len(drawn))
        indices = sampler.sample(count)

        published = {
            "version": _STATE_VERSION,
            "weights": weights,
            "drawn": drawn + indices,
        }
        _write_state(batch_path, published)
        return indices
    finally:
        if lock_fd is not None:
            os.close(lock_fd)


def resume_batch_metrics(batch_path, position=0):
    """Stream the batch's drawn indices from a write-order ``position``.

    ``position`` counts already produced draws from the start of the
    sequence, exactly the length of the drawn-index list stored in the
    batch file: a position inside the drawn prefix starts at that drawn
    index, equal to the current drawn count the stream is simply empty,
    and past that count an error is raised once iteration reaches the
    end, mirroring :func:`~weighted_batcher.persistence.resume_metrics`.
    The read takes no lock and only ever opens the atomically published
    batch file, so a concurrent advance neither blocks nor disturbs it
    and a same-process advance interleaved with a read neither
    deadlocks nor reports a spurious locking failure.

    The indices come straight from the stored state, so the exact
    serialisation round-trips: oversized integer weights and counters
    stay integers, ``-0.0`` is preserved and key order is unchanged.

    Path- and position-level problems are reported when this function
    is called; a past-end position is reported lazily while iterating.

    Raises:
        TypeError: ``position`` is not an integer (booleans do not
            count).
        ValueError: ``position`` is negative, the batch file is corrupt,
            or ``position`` is past the drawn count (the latter raised
            while iterating).
        FileNotFoundError: the batch file does not exist.
        IsADirectoryError: ``batch_path`` is a directory.
        OSError: ``batch_path`` is not a string.
    """
    if not isinstance(batch_path, str):
        raise OSError(
            f"batch path must be a string, got {type(batch_path).__name__}"
        )
    if os.path.isdir(batch_path):
        raise IsADirectoryError(
            f"batch path is a directory: {batch_path!r}"
        )
    # Resolve the batch file first, exactly as resume_metrics resolves
    # the segment set before consulting the position, so a missing or
    # corrupt file reports ahead of a bad position.
    state = _read_state(batch_path)
    if isinstance(position, bool) or not isinstance(position, int):
        raise TypeError(
            f"read position must be an integer, got {type(position).__name__}"
        )
    if position < 0:
        raise ValueError("read position must not be negative")
    drawn = state["drawn"]

    def _generate():
        for index in drawn[position:]:
            yield index
        if position > len(drawn):
            raise ValueError(
                f"read position {position} exceeds the drawn count of "
                f"{len(drawn)}"
            )

    return _generate()
