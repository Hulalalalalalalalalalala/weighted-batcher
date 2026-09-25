"""Durable weighted batch sampling over an append-only metric log.

A batch is a named without-replacement sampling plan over a fixed list of
weights, persisted beside the metric log in a sidecar directory named
``path + ".batch"``.  :func:`start_batch_metrics` opens (or rejoins) a
batch; :func:`draw_batch_metrics` draws the requested number of items from
the batch and, for every drawn item, appends one metrics record to the
same log the other entry points append to -- keys ``ordinal`` then
``index``, rendered with the established :func:`render_metrics` format.

Persistence follows the rest of the package: a draw is staged in the
batch's pending file first, under an exclusive batch lock; only then are
its records appended to the log (each one a single locked O_APPEND
write), and the batch cursor advances by re-staging and atomically
renaming its state.  A crash at any point leaves either the previous
cursor or the new one plus every record up to it -- never a half record
or a double draw -- and the next operation settles the staged plan
transparently, so the on-disk sequence is item-for-item identical to one
uninterrupted run, with ordinals counting continuously from zero.

Concurrency is anchored on files that are never the log itself: the
batch lock ``path.batch/<name>.lock`` serialises same-name batches while
different batches use different locks, and neither takes the log lock
for longer than a plain append does, so appends, rotations,
compactions and prunes on the same log -- and draws on other batches --
proceed independently and never deadlock a batch.
"""

from __future__ import annotations

import json
import math
import numbers
import os
from collections.abc import Mapping

from . import Sampler, render_metrics
from .persistence import (
    _append_payload,
    _iter_raw_file_records,
    _resolve_members,
    _write_bytes,
)

try:  # POSIX-only; the lock serialises every draw of one named batch.
    import fcntl
except ImportError:  # pragma: no cover - exercised on non-POSIX platforms
    fcntl = None

__all__ = [
    "start_batch_metrics",
    "draw_batch_metrics",
]

# Sidecar layout, all beneath the batch directory ``path + ".batch"`` so
# none of these names can be mistaken for a numbered segment or for any
# other sidecar (``.snapshots``, ``.group.<name>`` and the rest live as
# direct siblings of the log, never inside this directory):
#   <name>        the published batch state, one atomically renamed JSON
#                 object, never replaced in place
#   <name>.tmp    staging name of the next published state
#   <name>.lock   the inter-process lock that serialises same-name
#                 batches; it is never renamed or replaced
#   <name>.pending staging name of a committed draw plan: its appearance
#                 is the commit point; a crash leaves it behind and the
#                 next operation finishes it exactly once
_BATCH_DIR_SUFFIX = ".batch"
_STATE_TMP_SUFFIX = ".tmp"
_STATE_LOCK_SUFFIX = ".lock"
_PENDING_SUFFIX = ".pending"
_PENDING_TMP_SUFFIX = ".pending.tmp"


def _batch_dir(path):
    """The directory holding every batch sidecar of one log."""
    return path + _BATCH_DIR_SUFFIX


def _state_path(path, name):
    """The published state file for batch ``name``."""
    return os.path.join(_batch_dir(path), name)


def _unpack_plan(plan):
    """Validate one sampling plan and return ``(weights, total, seed)``.

    The plan is a mapping ``{"weights": [...], "total": n, "seed": s}``
    with ``seed`` optional.  Mirrors the :class:`Sampler` rules at the
    public boundary: every weight must be a real number that is neither
    NaN nor Infinity, with negative weights rejected (negative zero is
    allowed and simply carries no effective weight); ``total`` and
    ``seed`` must be plain integers -- booleans do not count.  A plan of
    the wrong shape or with a missing field is a :class:`ValueError`.
    """
    if not isinstance(plan, Mapping):
        raise TypeError(
            f"sampling plan must be a mapping, got {type(plan).__name__}"
        )
    if "weights" not in plan or "total" not in plan:
        raise ValueError("sampling plan must contain 'weights' and 'total'")
    weights = plan["weights"]
    total = plan["total"]
    seed = plan.get("seed")
    checked = []
    for weight in weights:
        if isinstance(weight, bool) or not isinstance(weight, numbers.Real):
            raise TypeError(
                f"weight must be a real number, got {type(weight).__name__}"
            )
        if math.isnan(weight) or math.isinf(weight):
            raise ValueError("weight must not be NaN or Infinity")
        if weight < 0:
            raise ValueError("weight must not be negative")
        # JSON is the plan's storage format: ints keep their exact
        # magnitude (huge ones included, matching the Sampler, which
        # converts to float itself), while other reals (fractions,
        # decimals) collapse to the float they would be drawn with;
        # negative zero stays stored as -0.0.
        checked.append(weight if isinstance(weight, int) else float(weight))
    if isinstance(total, bool) or not isinstance(total, int):
        raise TypeError(
            f"total draw count must be an integer, got {type(total).__name__}"
        )
    if total < 0:
        raise ValueError("total draw count must not be negative")
    if seed is not None and (
        isinstance(seed, bool) or not isinstance(seed, int)
    ):
        raise TypeError(
            f"sampling seed must be an integer, got {type(seed).__name__}"
        )
    return checked, total, seed


def _validate_count(count):
    """Type-check a per-call draw count (booleans do not count)."""
    if isinstance(count, bool) or not isinstance(count, int):
        raise TypeError(
            f"draw count must be an integer, got {type(count).__name__}"
        )


def _batch_lock(path, name):
    """Take the exclusive lock for one named batch and return its fd.

    The lock is anchored on a dedicated file inside the batch directory,
    never on the log and never on the batch state file, so a draw
    interleaved with an append, rotation, compaction, prune, snapshot or
    another batch's draw neither deadlocks nor reports a spurious locking
    failure.  Returns ``None`` on platforms without :mod:`fcntl`.
    """
    if fcntl is None:
        return None
    lock_path = _state_path(path, name) + _STATE_LOCK_SUFFIX
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o666)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
    except OSError:
        os.close(fd)
        raise
    return fd


def _write_json_atomic(target, tmp, payload):
    """Stage ``payload`` at ``tmp``, fsync it, then atomically publish it."""
    out = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o666)
    try:
        _write_bytes(out, payload)
        os.fsync(out)
    finally:
        # Closed before the rename: on Windows an open file cannot be
        # renamed.
        os.close(out)
    os.replace(tmp, target)


def _read_state(path, name):
    """Read and structurally validate a published batch state.

    A missing state file propagates :class:`FileNotFoundError`; any
    present but unreadable, undecodable or structurally wrong document is
    a corrupt batch file and raises :class:`ValueError`.
    """
    state_path = _state_path(path, name)
    try:
        fd = os.open(state_path, os.O_RDONLY)
    except FileNotFoundError:
        raise
    try:
        raw = b""
        while True:
            chunk = os.read(fd, 1 << 20)
            if not chunk:
                break
            raw += chunk
    finally:
        os.close(fd)
    try:
        state = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError(f"corrupt batch file {state_path!r}: {exc}") from exc
    if not isinstance(state, dict) or set(state) != {
        "version", "weights", "total", "seed", "drawn",
    }:
        raise ValueError(
            f"corrupt batch file {state_path!r}: unexpected batch fields"
        )
    if state["version"] != 1 or not isinstance(state["weights"], list):
        raise ValueError(f"corrupt batch file {state_path!r}: bad batch state")
    for key in ("total", "seed", "drawn"):
        value = state[key]
        if key == "seed":
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int)
            ):
                raise ValueError(
                    f"corrupt batch file {state_path!r}: seed must be an "
                    f"integer or null"
                )
        elif isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(
                f"corrupt batch file {state_path!r}: {key} must be an integer"
            )
    if state["total"] < 0 or state["drawn"] < 0:
        raise ValueError(
            f"corrupt batch file {state_path!r}: negative batch value"
        )
    if state["drawn"] > state["total"]:
        raise ValueError(
            f"corrupt batch file {state_path!r}: drawn count past the total"
        )
    for weight in state["weights"]:
        if isinstance(weight, bool) or not isinstance(weight, numbers.Real):
            raise ValueError(
                f"corrupt batch file {state_path!r}: weight must be a number"
            )
        if math.isnan(weight) or math.isinf(weight) or weight < 0:
            raise ValueError(
                f"corrupt batch file {state_path!r}: invalid stored weight"
            )
    return state


def _write_state(path, name, state):
    """Atomically publish the batch state, staged at its ``.tmp`` name."""
    target = _state_path(path, name)
    payload = json.dumps(state, separators=(",", ":")).encode("utf-8")
    _write_json_atomic(target, target + _STATE_TMP_SUFFIX, payload)


def _read_pending(path, name):
    """Read a staged draw plan, or ``None`` when no plan is present."""
    pending_path = _state_path(path, name) + _PENDING_SUFFIX
    try:
        fd = os.open(pending_path, os.O_RDONLY)
    except FileNotFoundError:
        return None
    try:
        raw = b""
        while True:
            chunk = os.read(fd, 1 << 20)
            if not chunk:
                break
            raw += chunk
    finally:
        os.close(fd)
    try:
        plan = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError(
            f"corrupt pending batch file {pending_path!r}: {exc}"
        ) from exc
    if (
        not isinstance(plan, dict)
        or set(plan) != {"drawn", "indices"}
        or isinstance(plan["drawn"], bool)
        or not isinstance(plan["drawn"], int)
        or not isinstance(plan["indices"], list)
        or any(
            isinstance(index, bool) or not isinstance(index, int)
            for index in plan["indices"]
        )
    ):
        raise ValueError(
            f"corrupt pending batch file {pending_path!r}: bad draw plan"
        )
    return plan


def _stage_pending(path, name, plan):
    """Publish a draw plan as the commit point of a draw.

    The pending file names the cursor the state must reach and the
    indices, in draw order, of every record the plan appends.  It is
    staged, fsynced and atomically renamed into place before a single log
    byte is written, so crashing during the log appends still leaves
    enough on disk to finish deterministically.
    """
    target = _state_path(path, name) + _PENDING_SUFFIX
    payload = json.dumps(plan, separators=(",", ":")).encode("utf-8")
    _write_json_atomic(
        target, _state_path(path, name) + _PENDING_TMP_SUFFIX, payload
    )


def _drop_pending(path, name):
    """Retire a fully applied draw plan; a missing one is the goal."""
    try:
        os.unlink(_state_path(path, name) + _PENDING_SUFFIX)
    except FileNotFoundError:
        pass


def _present_lines(path, wanted):
    """Return the subset of ``wanted`` surviving verbatim in the log set.

    Members are streamed byte for byte -- the same walk compaction uses --
    rather than materialised, so a large log costs only the size of the
    pending block.  Rotation and compaction preserve every surviving line
    exactly and a prune only removes lines, so a wanted line found in any
    member is the record an uninterrupted write landed, byte for byte.  A
    torn unterminated tail is never a complete line and matches nothing.
    """
    found = set()
    remaining = set(wanted)
    members, _pruned = _resolve_members(path)
    for member in members:
        if not remaining:
            break
        for raw in _iter_raw_file_records(member):
            if raw in remaining:
                found.add(raw)
                remaining.discard(raw)
    return found


def _render_record(ordinal, index):
    """Render one draw record with keys ``ordinal`` then ``index``."""
    return render_metrics({"ordinal": ordinal, "index": index})


def _finish_pending(path, name, state):
    """Apply a staged draw plan left behind by a crash.

    Called with the batch lock held, by both :func:`start_batch_metrics`
    and :func:`draw_batch_metrics`, before any new draw.  The plan is the
    commit point: it names the cursor the state must reach and, in draw
    order, the index of every record that draw appends.  The state cursor
    only advances after the appends, so on recovery exactly one of two
    settled worlds is possible:

    * the cursor already equals the plan's cursor -- the plan's records
      were appended and the state published before the crash, so only the
      plan file remains to retire;
    * the cursor still sits at the plan's start -- every plan record
      whose verbatim line no longer survives is appended, and the cursor
      then advances exactly once.

    Records carry only ``ordinal`` and ``index`` by design, rendered
    canonically, so a line that landed -- whole, after a rotation or
    compaction, or interleaved with other writers' records -- is
    byte-identical to the one a retry would append and is never written
    twice; checking content presence rather than position also makes a
    prune that dropped part of a crashed block safe: surviving lines are
    kept (never duplicated) and only genuinely missing lines are
    re-appended with their original ordinals, so the batch's surviving
    records stay intact with unique continuous ordinals.  The block
    itself goes out in one locked O_APPEND write, so an uninterrupted run
    lands strictly in order; this presence repair only runs after a
    crash.
    """
    plan = _read_pending(path, name)
    if plan is None:
        return
    target_drawn = int(plan["drawn"])
    indices = plan["indices"]
    drawn = int(state["drawn"])
    if drawn == target_drawn:
        # The cursor reached the plan: the appends had necessarily
        # happened (the cursor never advances ahead of them), so the
        # crash could only have left the plan file behind.
        _drop_pending(path, name)
        return
    if target_drawn < drawn or target_drawn - drawn != len(indices):
        raise ValueError(
            f"corrupt pending batch file for {name!r}: plan does not "
            f"match the batch state"
        )
    weight_count = len(state["weights"])
    for index in indices:
        if index < 0 or index >= weight_count:
            raise ValueError(
                f"corrupt pending batch file for {name!r}: drawn index "
                f"{index} is outside the plan's weights"
            )
    wanted = [
        _render_record(drawn + offset, index).encode("utf-8")
        for offset, index in enumerate(indices)
    ]
    present = _present_lines(path, wanted)
    missing = b"".join(line for line in wanted if line not in present)
    if missing:
        # One locked O_APPEND write for every still-missing record, so
        # the recovery block lands whole relative to concurrent appends.
        _append_payload(path, missing)
    state["drawn"] = target_drawn
    _write_state(path, name, state)
    _drop_pending(path, name)


def _drop_staging_leftovers(path, name):
    """Remove private staging files a crashed operation never published."""
    state_path = _state_path(path, name)
    for leftover in (
        state_path + _STATE_TMP_SUFFIX,
        state_path + _PENDING_TMP_SUFFIX,
    ):
        try:
            os.unlink(leftover)
        except FileNotFoundError:
            pass


def start_batch_metrics(path, name, plan):
    """Open (or rejoin) the named weighted batch against log ``path``.

    ``plan`` is the sampling plan, a mapping with ``weights`` (the fixed
    list of item weights), ``total`` (how many items the batch draws in
    all across every call) and an optional integer ``seed`` making the
    whole without-replacement sequence reproducible item by item.  The
    batch's state lives beside the log under ``path + ".batch"`` and the
    log itself is created empty on first use, so no record is needed
    before drawing.

    Reopening an existing batch with exactly the same weights, total and
    seed reuses it and draws continue where they stopped.  A plan that
    differs in any way raises :class:`ValueError`; no state changes.

    Raises:
        TypeError: ``name`` is not a string, ``plan`` is not a mapping,
            a weight is not a real number, or ``total`` or ``seed`` is
            not an integer (booleans do not count).
        ValueError: a weight is negative, NaN or Infinity, ``total`` is
            negative, the plan is missing a field, or an existing
            batch's plan differs.
        IsADirectoryError: ``path`` is a directory.
        OSError: ``path`` is not a string, or locking or writing the
            batch state fails.  A failed write leaves no half state.
    """
    if not isinstance(path, str):
        raise OSError(f"log path must be a string, got {type(path).__name__}")
    if not isinstance(name, str):
        raise TypeError(
            f"batch name must be a string, got {type(name).__name__}"
        )
    weights, total, seed = _unpack_plan(plan)
    if os.path.isdir(path):
        raise IsADirectoryError(f"log path is a directory: {path!r}")

    # The sidecar directory and lock anchor are created before the lock
    # is taken, so the lock file itself is never renamed or replaced.
    os.makedirs(_batch_dir(path), exist_ok=True)
    lock_fd = _batch_lock(path, name)
    try:
        _drop_staging_leftovers(path, name)
        try:
            state = _read_state(path, name)
        except FileNotFoundError:
            state = None
        if state is not None:
            if (
                state["weights"] != weights
                or state["total"] != total
                or state["seed"] != seed
            ):
                raise ValueError(
                    f"existing batch {name!r} was started with a different "
                    f"sampling plan"
                )
            # A crash between publishing a plan and advancing the cursor
            # is settled before the batch is handed back, so rejoiners
            # always observe a settled state.
            _finish_pending(path, name, state)
            return
        # Publishing the state is the commit point of the start.
        state = {
            "version": 1,
            "weights": weights,
            "total": total,
            "seed": seed,
            "drawn": 0,
        }
        _write_state(path, name, state)
    finally:
        if lock_fd is not None:
            os.close(lock_fd)


def draw_batch_metrics(path, name, count):
    """Draw ``count`` items from batch ``name`` and record each one.

    Sampling is without replacement over the fixed weights: an index
    already drawn by any earlier call of the same batch -- including
    calls from earlier processes after a crash -- is never drawn again,
    weights of zero and negative zero can never be drawn, and the same
    seed reproduces the same sequence item by item however the requested
    counts are split across calls.

    Each drawn item appends exactly one record to the log at ``path``,
    keys ``ordinal`` then ``index``, rendered with
    :func:`render_metrics`; ordinals count continuously from zero across
    every call and every crash recovery.  The indices drawn by this call
    are returned in draw order.  Drawing zero items returns an empty list
    and appends nothing.

    Raises:
        TypeError: ``count`` is not an integer (booleans do not count).
        ValueError: ``count`` is negative, more than the number of
            not-yet-drawn positive-weight items remain, the batch total
            would be exceeded, or the batch state is corrupt.
        FileNotFoundError: the batch has not been started.
        IsADirectoryError: ``path`` is a directory.
        OSError: ``path`` is not a string, or locking or writing fails.
        Nothing is appended when the call itself is rejected.
    """
    if not isinstance(path, str):
        raise OSError(f"log path must be a string, got {type(path).__name__}")
    if not isinstance(name, str):
        raise TypeError(
            f"batch name must be a string, got {type(name).__name__}"
        )
    _validate_count(count)
    if count < 0:
        raise ValueError("draw count must not be negative")
    if os.path.isdir(path):
        raise IsADirectoryError(f"log path is a directory: {path!r}")

    # The lock anchor lives inside the sidecar directory; opening it also
    # proves the batch was started, and the state read below turns a
    # missing batch into FileNotFoundError.  A zero draw still addresses a
    # batch, so that check precedes the empty-list shortcut -- but a zero
    # draw on an existing batch appends nothing and creates no log.
    lock_fd = _batch_lock(path, name)
    try:
        _drop_staging_leftovers(path, name)
        state = _read_state(path, name)
        _finish_pending(path, name, state)
        if count == 0:
            return []

        weights = state["weights"]
        total = int(state["total"])
        seed = state["seed"]
        drawn = int(state["drawn"])
        remaining = total - drawn
        if count > remaining:
            raise ValueError(
                f"cannot draw {count} items: batch has {remaining} of "
                f"{total} draws remaining"
            )

        # Rebuild the stream at its exact midpoint: the Sampler's own
        # without-replacement machinery depletes the pool deterministically,
        # so consuming ``drawn`` items first reproduces every PRNG step an
        # uninterrupted run would have made.  A batch that omitted a seed
        # still draws from one fixed internal stream, so after a crash the
        # rebuilt midpoint in a new process removes exactly the indices
        # already drawn -- an OS-seeded stream could not -- and the
        # on-disk sequence matches one uninterrupted run.  Zero and
        # negative-zero weights never enter the pool, so they can never
        # be drawn.
        effective_seed = 0 if seed is None else seed
        sampler = Sampler(weights, replacement=False, seed=effective_seed)
        if drawn:
            sampler.sample(drawn)
        indices = sampler.sample(count)

        # Commit point: once the plan is published every later caller --
        # including this one after a crash -- finishes it to the same
        # records, so the on-disk ordinal/index sequence is identical to
        # one uninterrupted run.
        _stage_pending(path, name, {"drawn": drawn + count, "indices": indices})
        payload = b"".join(
            _render_record(drawn + offset, index).encode("utf-8")
            for offset, index in enumerate(indices)
        )
        _append_payload(path, payload)
        state["drawn"] = drawn + count
        _write_state(path, name, state)
        _drop_pending(path, name)
        return indices
    finally:
        if lock_fd is not None:
            os.close(lock_fd)
