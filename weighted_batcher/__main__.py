"""Module entry point.

    python3 -m weighted_batcher --selftest
    python3 -m weighted_batcher record FILE METRICS_JSON
    python3 -m weighted_batcher recover FILE
    python3 -m weighted_batcher rotate FILE
    python3 -m weighted_batcher stream FILE
    python3 -m weighted_batcher compact FILE
    python3 -m weighted_batcher resume FILE [POSITION]
    python3 -m weighted_batcher prune FILE QUOTA
    python3 -m weighted_batcher snapshot FILE
    python3 -m weighted_batcher release FILE HANDLE
    python3 -m weighted_batcher snap-resume FILE HANDLE [POSITION]
    python3 -m weighted_batcher snap-diff FILE OLD_HANDLE NEW_HANDLE
    python3 -m weighted_batcher snap-cursor FILE HANDLE [POSITION]
    python3 -m weighted_batcher checkpoint FILE HANDLE CURSOR_FILE [POSITION]
    python3 -m weighted_batcher cursor-resume FILE CURSOR_FILE
    python3 -m weighted_batcher group-checkpoint FILE CURSOR_FILE POSITION [CURSOR_FILE POSITION ...]
    python3 -m weighted_batcher replay FILE CURSOR_FILE START [END]
    python3 -m weighted_batcher group-join FILE GROUP MEMBER LEASE_SECONDS
    python3 -m weighted_batcher group-read FILE GROUP
    python3 -m weighted_batcher group-advance FILE GROUP TOKEN POSITION
    python3 -m weighted_batcher group-takeover FILE GROUP MEMBER [LEASE_SECONDS]
"""

from __future__ import annotations

import json
import sys

from . import (
    Sampler,
    append_metrics,
    advance_group_metrics,
    checkpoint_group_metrics,
    checkpoint_metrics,
    compact_metrics,
    group_resume_metrics,
    iter_metrics,
    join_group_metrics,
    parse_metrics,
    prune_metrics,
    recover_metrics,
    release_metrics,
    render_metrics,
    replay_metrics,
    resume_checkpoint_metrics,
    resume_metrics,
    resume_snapshot_delta_metrics,
    resume_snapshot_metrics,
    rotate_metrics,
    snapshot_diff_metrics,
    snapshot_metrics,
    takeover_group_metrics,
)

_USAGE = (
    "usage:\n"
    "  python3 -m weighted_batcher --selftest\n"
    "  python3 -m weighted_batcher record FILE METRICS_JSON\n"
    "  python3 -m weighted_batcher recover FILE\n"
    "  python3 -m weighted_batcher rotate FILE\n"
    "  python3 -m weighted_batcher stream FILE\n"
    "  python3 -m weighted_batcher compact FILE\n"
    "  python3 -m weighted_batcher resume FILE [POSITION]\n"
    "  python3 -m weighted_batcher prune FILE QUOTA\n"
    "  python3 -m weighted_batcher snapshot FILE\n"
    "  python3 -m weighted_batcher release FILE HANDLE\n"
    "  python3 -m weighted_batcher snap-resume FILE HANDLE [POSITION]\n"
    "  python3 -m weighted_batcher snap-diff FILE OLD_HANDLE NEW_HANDLE\n"
    "  python3 -m weighted_batcher snap-cursor FILE HANDLE [POSITION]\n"
    "  python3 -m weighted_batcher checkpoint FILE HANDLE CURSOR_FILE [POSITION]\n"
    "  python3 -m weighted_batcher cursor-resume FILE CURSOR_FILE\n"
    "  python3 -m weighted_batcher group-checkpoint FILE CURSOR_FILE POSITION [CURSOR_FILE POSITION ...]\n"
    "  python3 -m weighted_batcher replay FILE CURSOR_FILE START [END]\n"
    "  python3 -m weighted_batcher group-join FILE GROUP MEMBER LEASE_SECONDS\n"
    "  python3 -m weighted_batcher group-read FILE GROUP\n"
    "  python3 -m weighted_batcher group-advance FILE GROUP TOKEN POSITION\n"
    "  python3 -m weighted_batcher group-takeover FILE GROUP MEMBER [LEASE_SECONDS]"
)


def _selftest():
    # Same seed reproduces the same sequence, item by item.
    a = Sampler([1.0, 2.0, 0.0, 3.0], seed=42)
    b = Sampler([1.0, 2.0, 0.0, 3.0], seed=42)
    assert a.sample(8) == b.sample(8)
    assert a.weights == [1.0, 2.0, 0.0, 3.0]

    # Zero-weight items are never drawn, with or without replacement.
    c = Sampler([0.0, -0.0, 5.0], seed=7)
    assert c.sample(20) == [2] * 20

    # Without replacement: draws accumulate on one instance, so indices
    # drawn by earlier calls never reappear, and asking for more than the
    # remaining pool raises.
    d = Sampler([1.0, 1.0, 1.0, 1.0], replacement=False, seed=1)
    first = d.sample(2)
    try:
        d.sample(3)
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError when over-drawing")
    second = d.sample(2)
    assert sorted(first + second) == [0, 1, 2, 3]
    try:
        d.sample(1)
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError when pool is exhausted")

    # The same seed reproduces the same draws however the requested
    # counts are split across calls.
    f = Sampler([1.0, 2.0, 3.0, 4.0], replacement=False, seed=11)
    g = Sampler([1.0, 2.0, 3.0, 4.0], replacement=False, seed=11)
    assert f.sample(4) == g.sample(1) + g.sample(2) + g.sample(1)

    # No positive weights: any draw fails, zero draws return empty.
    e = Sampler([0.0, -0.0], seed=0)
    assert e.sample(0) == []
    try:
        e.sample(1)
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError with no effective samples")

    # Metrics round-trip: exact big integers, -0.0 preserved, key order kept.
    metrics = {"count": 10**40, "ratio": 0.25, "neg_zero": -0.0}
    line = render_metrics(metrics)
    assert line.endswith("\n") and line.count("\n") == 1
    parsed = parse_metrics(line)
    assert list(parsed) == list(metrics)
    assert parsed["count"] == 10**40 and isinstance(parsed["count"], int)
    assert parsed["neg_zero"] == 0.0 and str(parsed["neg_zero"]) == "-0.0"

    # Invalid input is rejected.
    for bad in (Sampler,):
        try:
            bad([1.0, "x"])
        except TypeError:
            pass
        else:
            raise AssertionError("expected TypeError for non-real weight")
    try:
        render_metrics({"bad": float("nan")})
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError for NaN metric")
    try:
        parse_metrics('{"x": Infinity}')
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError for Infinity literal")


def _record_line(metrics):
    """Serialise one record as a compact JSON line for CLI output.

    Unlike :func:`render_metrics` this imposes no numeric-value
    restriction, so records carrying non-numeric values (string tags and
    the like) print exactly as stored; key order, oversized counters and
    ``-0.0`` survive the round trip unchanged.
    """
    return json.dumps(metrics, ensure_ascii=False, separators=(",", ":")) + "\n"


def _record(path, line):
    # Append one metrics JSON object line, with validation aligned with
    # the append entry point (append_metrics): values must be ints or
    # floats (booleans rejected), and the record is stored in its
    # canonical compact rendering.  A non-numeric value raises TypeError,
    # reported as a failure with status 1.
    append_metrics(path, line)
    return 0


def _recover(path):
    # One canonical JSON line per recovered record, exit 0 afterwards.
    # Records print through _record_line rather than render_metrics so
    # records carrying non-numeric values print exactly as stored.
    for metrics in recover_metrics(path):
        sys.stdout.write(_record_line(metrics))
    return 0


def _rotate(path):
    rotate_metrics(path)
    return 0


def _stream(path):
    # Records stream straight to stdout one JSON object per line; the
    # segment set is never materialised in memory.
    for metrics in iter_metrics(path):
        sys.stdout.write(render_metrics(metrics))
    return 0


def _compact(path):
    compact_metrics(path)
    return 0


def _prune(path, quota):
    prune_metrics(path, quota)
    return 0


def _resume(path, position):
    # Like stream, starting at the record position; a position that does
    # not parse as an integer is a usage error handled in main().
    for metrics in resume_metrics(path, position):
        sys.stdout.write(render_metrics(metrics))
    return 0


def _snapshot(path):
    # Pin the current record sequence and print the persistable handle.
    sys.stdout.write(snapshot_metrics(path) + "\n")
    return 0


def _release(path, handle):
    release_metrics(path, handle)
    return 0


def _snap_resume(path, handle, position):
    # Stream the snapshot pinned by handle from the record position; a
    # position that does not parse as an integer is a usage error.
    for metrics in resume_snapshot_metrics(path, handle, position):
        sys.stdout.write(render_metrics(metrics))
    return 0


def _snap_diff(path, old_handle, new_handle):
    # Each difference is already a complete newline-terminated JSON
    # line with keys in the fixed kind/position/old/new order; write it
    # verbatim so pinned bytes and key order are not re-rendered.
    for line in snapshot_diff_metrics(path, old_handle, new_handle):
        sys.stdout.write(line)
    return 0


def _snap_cursor(path, handle, position):
    # Stream not-yet-read live records from the record position; a
    # position that does not parse as an integer is a usage error.
    for metrics in resume_snapshot_delta_metrics(path, handle, position):
        sys.stdout.write(render_metrics(metrics))
    return 0


def _checkpoint(path, handle, cursor_path, position):
    # Persist the pull cursor (position, handle and record boundary) to
    # the caller-named cursor file; a position that does not parse as an
    # integer is a usage error handled in main().
    checkpoint_metrics(path, handle, cursor_path, position)
    return 0


def _cursor_resume(path, cursor_path):
    # Stream the pinned snapshot onward from the stored cursor position,
    # one canonical JSON object per record; like _recover, records print
    # through _record_line so non-numeric values are not rejected.
    for metrics in resume_checkpoint_metrics(path, cursor_path):
        sys.stdout.write(_record_line(metrics))
    return 0


def _group_checkpoint(path, cursor_paths, positions):
    # Advance the whole group of cursors in one atomic commit; positions
    # that do not parse as integers are usage errors handled in main().
    checkpoint_group_metrics(path, cursor_paths, positions)
    return 0


def _group_join(path, group, member, lease_seconds):
    # Join (or create) the consumer group and print the lease's takeover
    # token, the way snapshot prints its handle; a lease-seconds value
    # that does not parse as an integer is a usage error in main().
    lease = join_group_metrics(path, group, member, lease_seconds)
    sys.stdout.write(lease["token"] + "\n")
    return 0


def _group_read(path, group):
    # Stream records from the group's current position, one canonical
    # JSON object per line.  Like stream/resume, records print through
    # render_metrics, so a stored record whose value is not numeric (a
    # string tag appended through record) is rejected with a TypeError,
    # matching the append entry's validation.
    for metrics in group_resume_metrics(path, group):
        sys.stdout.write(render_metrics(metrics))
    return 0


def _group_advance(path, group, token, position):
    # Move the group's read position under the lease token; a position
    # that does not parse as an integer is a usage error in main().
    advance_group_metrics(path, group, token, position)
    return 0


def _group_takeover(path, group, member, lease_seconds):
    # Take over the group's expired lease and print the fresh takeover
    # token; an omitted lease-seconds reuses the group's own seconds.
    lease = takeover_group_metrics(path, group, member, lease_seconds)
    sys.stdout.write(lease["token"] + "\n")
    return 0


def _replay(path, cursor_path, start, end):
    # Stream the window one compact JSON object per line: metric records
    # and gap markers (a gap marker is not a metric, so items are
    # rendered directly rather than through render_metrics).
    for item in replay_metrics(path, cursor_path, start, end):
        sys.stdout.write(
            json.dumps(item, ensure_ascii=False, separators=(",", ":")) + "\n"
        )
    return 0


def _group_pairs(rest):
    """Parse the group-checkpoint cursor/position arguments into pairs.

    Positions are the arguments that parse as integers.  Alternating
    ``CURSOR POSITION`` pairs, alternating ``POSITION CURSOR`` pairs and
    all cursor files followed by all positions are all accepted; a
    position that does not parse as an integer or a leftover argument
    count makes the whole form a usage error (``None``).
    """
    def _is_int(text):
        try:
            int(text)
        except ValueError:
            return False
        return True

    if not rest or len(rest) % 2 != 0:
        return None
    if all(
        not _is_int(rest[i]) and _is_int(rest[i + 1])
        for i in range(0, len(rest), 2)
    ):
        return [(rest[i], int(rest[i + 1])) for i in range(0, len(rest), 2)]
    if all(
        _is_int(rest[i]) and not _is_int(rest[i + 1])
        for i in range(0, len(rest), 2)
    ):
        return [(rest[i + 1], int(rest[i])) for i in range(0, len(rest), 2)]
    half = len(rest) // 2
    cursors, numbers = rest[:half], rest[half:]
    if all(not _is_int(text) for text in cursors) and all(
        _is_int(text) for text in numbers
    ):
        return list(zip(cursors, [int(text) for text in numbers]))
    return None


def _dispatch(handler, path):
    # Runtime failures (bad types or values discovered inside an entry
    # point, OS errors) are reported cleanly and end with status 1; only
    # argument counts and unparseable integer arguments are usage errors
    # (status 2), handled by main() before this is reached.
    try:
        return handler(path)
    except (OSError, TypeError, ValueError) as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


def main(argv=None):
    args = list(sys.argv[1:] if argv is None else argv)
    if args == ["--selftest"]:
        _selftest()
        print("selftest ok")
        return 0
    if args and args[0] == "record":
        if len(args) != 3:
            print(_USAGE, file=sys.stderr)
            return 2
        return _dispatch(lambda path: _record(path, args[2]), args[1])
    if args and args[0] == "recover":
        if len(args) != 2:
            print(_USAGE, file=sys.stderr)
            return 2
        return _dispatch(_recover, args[1])
    if args and args[0] == "rotate":
        if len(args) != 2:
            print(_USAGE, file=sys.stderr)
            return 2
        return _dispatch(_rotate, args[1])
    if args and args[0] == "stream":
        if len(args) != 2:
            print(_USAGE, file=sys.stderr)
            return 2
        return _dispatch(_stream, args[1])
    if args and args[0] == "compact":
        if len(args) != 2:
            print(_USAGE, file=sys.stderr)
            return 2
        return _dispatch(_compact, args[1])
    if args and args[0] == "resume":
        if len(args) not in (2, 3):
            print(_USAGE, file=sys.stderr)
            return 2
        position = 0
        if len(args) == 3:
            try:
                position = int(args[2])
            except ValueError:
                print(_USAGE, file=sys.stderr)
                return 2
        return _dispatch(lambda path: _resume(path, position), args[1])
    if args and args[0] == "prune":
        if len(args) != 3:
            print(_USAGE, file=sys.stderr)
            return 2
        try:
            quota = int(args[2])
        except ValueError:
            print(_USAGE, file=sys.stderr)
            return 2
        return _dispatch(lambda path: _prune(path, quota), args[1])
    if args and args[0] == "snapshot":
        if len(args) != 2:
            print(_USAGE, file=sys.stderr)
            return 2
        return _dispatch(_snapshot, args[1])
    if args and args[0] == "release":
        if len(args) != 3:
            print(_USAGE, file=sys.stderr)
            return 2
        return _dispatch(lambda path: _release(path, args[2]), args[1])
    if args and args[0] == "snap-resume":
        if len(args) not in (3, 4):
            print(_USAGE, file=sys.stderr)
            return 2
        position = 0
        if len(args) == 4:
            try:
                position = int(args[3])
            except ValueError:
                print(_USAGE, file=sys.stderr)
                return 2
        return _dispatch(
            lambda path: _snap_resume(path, args[2], position), args[1]
        )
    if args and args[0] == "snap-diff":
        if len(args) != 4:
            print(_USAGE, file=sys.stderr)
            return 2
        return _dispatch(
            lambda path: _snap_diff(path, args[2], args[3]), args[1]
        )
    if args and args[0] == "snap-cursor":
        if len(args) not in (3, 4):
            print(_USAGE, file=sys.stderr)
            return 2
        position = 0
        if len(args) == 4:
            try:
                position = int(args[3])
            except ValueError:
                print(_USAGE, file=sys.stderr)
                return 2
        return _dispatch(
            lambda path: _snap_cursor(path, args[2], position), args[1]
        )
    if args and args[0] == "checkpoint":
        if len(args) not in (4, 5):
            print(_USAGE, file=sys.stderr)
            return 2
        position = 0
        if len(args) == 5:
            try:
                position = int(args[4])
            except ValueError:
                print(_USAGE, file=sys.stderr)
                return 2
        return _dispatch(
            lambda path: _checkpoint(path, args[2], args[3], position),
            args[1],
        )
    if args and args[0] == "cursor-resume":
        if len(args) != 3:
            print(_USAGE, file=sys.stderr)
            return 2
        return _dispatch(
            lambda path: _cursor_resume(path, args[2]), args[1]
        )
    if args and args[0] == "group-checkpoint":
        if len(args) < 4 or len(args) % 2 != 0:
            print(_USAGE, file=sys.stderr)
            return 2
        pairs = _group_pairs(args[2:])
        if pairs is None:
            print(_USAGE, file=sys.stderr)
            return 2
        cursor_paths = [cursor for cursor, _position in pairs]
        positions = [position for _cursor, position in pairs]
        return _dispatch(
            lambda path: _group_checkpoint(path, cursor_paths, positions),
            args[1],
        )
    if args and args[0] == "replay":
        if len(args) not in (4, 5):
            print(_USAGE, file=sys.stderr)
            return 2
        try:
            start = int(args[3])
        except ValueError:
            print(_USAGE, file=sys.stderr)
            return 2
        end = None
        if len(args) == 5:
            try:
                end = int(args[4])
            except ValueError:
                print(_USAGE, file=sys.stderr)
                return 2
        return _dispatch(
            lambda path: _replay(path, args[2], start, end), args[1]
        )
    if args and args[0] == "group-join":
        if len(args) != 5:
            print(_USAGE, file=sys.stderr)
            return 2
        try:
            lease_seconds = int(args[4])
        except ValueError:
            print(_USAGE, file=sys.stderr)
            return 2
        return _dispatch(
            lambda path: _group_join(path, args[2], args[3], lease_seconds),
            args[1],
        )
    if args and args[0] == "group-read":
        if len(args) != 3:
            print(_USAGE, file=sys.stderr)
            return 2
        return _dispatch(lambda path: _group_read(path, args[2]), args[1])
    if args and args[0] == "group-advance":
        if len(args) != 5:
            print(_USAGE, file=sys.stderr)
            return 2
        try:
            position = int(args[4])
        except ValueError:
            print(_USAGE, file=sys.stderr)
            return 2
        return _dispatch(
            lambda path: _group_advance(path, args[2], args[3], position),
            args[1],
        )
    if args and args[0] == "group-takeover":
        if len(args) not in (4, 5):
            print(_USAGE, file=sys.stderr)
            return 2
        lease_seconds = None
        if len(args) == 5:
            try:
                lease_seconds = int(args[4])
            except ValueError:
                print(_USAGE, file=sys.stderr)
                return 2
        return _dispatch(
            lambda path: _group_takeover(
                path, args[2], args[3], lease_seconds
            ),
            args[1],
        )
    print(_USAGE, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
