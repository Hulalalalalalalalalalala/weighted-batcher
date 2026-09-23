"""Module entry point: ``python3 -m weighted_batcher --selftest``.

Also exposes two subcommands:

    python3 -m weighted_batcher record PATH
    python3 -m weighted_batcher recover PATH

``record`` reads one metric line from standard input and appends it to the
file at PATH.  ``recover`` prints each recovered metric line to standard
output, one JSON object per line.
"""

from __future__ import annotations

import sys

from . import Sampler, parse_metrics, record_metrics, recover_metrics, render_metrics

_USAGE = "usage: python3 -m weighted_batcher --selftest"
_RECORD_USAGE = "usage: python3 -m weighted_batcher record PATH"
_RECOVER_USAGE = "usage: python3 -m weighted_batcher recover PATH"


def _selftest():
    # Same seed reproduces the same sequence, item by item.
    a = Sampler([1.0, 2.0, 0.0, 3.0], seed=42)
    b = Sampler([1.0, 2.0, 0.0, 3.0], seed=42)
    assert a.sample(8) == b.sample(8)
    assert a.weights == [1.0, 2.0, 0.0, 3.0]

    # Zero-weight items are never drawn, with or without replacement.
    c = Sampler([0.0, -0.0, 5.0], seed=7)
    assert c.sample(20) == [2] * 20

    # Without replacement: indices are unique and bounded by positive weights.
    d = Sampler([1.0, 1.0, 1.0], replacement=False, seed=1)
    assert sorted(d.sample(3)) == [0, 1, 2]
    try:
        d.sample(4)
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError when over-drawing")

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


def _record(path):
    # One metric line from standard input.  sys.stdin.readline keeps the
    # newline if present; record_metrics tolerates either form.
    line = sys.stdin.readline()
    record_metrics(line, path)
    return 0


def _recover(path):
    # One recovered JSON object per line on standard output, exit code 0.
    for metrics in recover_metrics(path):
        sys.stdout.write(render_metrics(metrics))
    return 0


def main(argv=None):
    args = list(sys.argv[1:] if argv is None else argv)
    if args == ["--selftest"]:
        _selftest()
        print("selftest ok")
        return 0
    if args[:1] == ["record"]:
        if len(args) == 2:
            return _record(args[1])
        print(_RECORD_USAGE, file=sys.stderr)
        return 2
    if args[:1] == ["recover"]:
        if len(args) == 2:
            return _recover(args[1])
        print(_RECOVER_USAGE, file=sys.stderr)
        return 2
    print(_USAGE, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
