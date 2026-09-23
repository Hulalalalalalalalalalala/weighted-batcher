"""Module entry point: ``python3 -m weighted_batcher --selftest``."""

from __future__ import annotations

import math
import sys

from . import Sampler, parse_metrics, render_metrics

_USAGE = "usage: python3 -m weighted_batcher --selftest"


def _expect(exc_type, fn, *args, **kwargs):
    try:
        fn(*args, **kwargs)
    except exc_type:
        return
    except AssertionError:
        raise
    raise AssertionError(f"{getattr(fn, '__name__', fn)!r} did not raise {exc_type.__name__}")


def selftest() -> None:
    # Reproducible draw sequences for the same seed, with and without replacement.
    s1 = Sampler([1, 2, 3], replacement=True, seed=7).sample(10)
    s2 = Sampler([1, 2, 3], replacement=True, seed=7).sample(10)
    assert s1 == s2

    without = Sampler([1, 2, 3], replacement=False, seed=7).sample(3)
    assert len(without) == 3 and len(set(without)) == 3

    # Zero and negative zero never draw; all-zero weights leave no valid sample.
    assert set(Sampler([0, 5, -0.0], replacement=True, seed=1).sample(20)) == {1}
    _expect(ValueError, Sampler([0, -0.0], seed=1).sample, 1)

    # Invalid weights.
    _expect(TypeError, Sampler, ["1", 2.0])
    _expect(TypeError, Sampler, [True, 1.0])
    _expect(ValueError, Sampler, [-1, 1.0])
    _expect(ValueError, Sampler, [float("nan"), 1.0])
    _expect(ValueError, Sampler, [float("inf"), 1.0])

    # sample() argument validation.
    sampler = Sampler([1, 1], replacement=True, seed=0)
    assert sampler.sample(0) == []
    _expect(ValueError, sampler.sample, -1)
    _expect(TypeError, sampler.sample, 1.0)
    _expect(TypeError, sampler.sample, True)
    _expect(ValueError, Sampler([1, 1, 0], replacement=False, seed=0).sample, 3)

    # Metric rendering: exact integer text, insertion order, trailing newline.
    huge = 10**100
    line = render_metrics({"b": 2, "a": huge, "z": -0.0})
    assert line.endswith("\n") and line.count("\n") == 1
    assert str(huge) in line and "." not in str(huge)
    parsed = parse_metrics(line)
    assert list(parsed) == ["b", "a", "z"]
    assert parsed["a"] == huge and isinstance(parsed["a"], int)
    assert math.copysign(1.0, parsed["z"]) < 0 and parsed["z"] == 0.0

    # Round trips for ordinary numbers.
    for metrics in ({}, {"x": 0, "y": -7}, {"f": 1.5, "g": -0.0, "h": 2.0}):
        assert parse_metrics(render_metrics(metrics)) == metrics

    # Rendering validation.
    _expect(TypeError, render_metrics, {1: 1})  # non-string key
    for bad in ({"k": True}, {"k": "1"}, {"k": None}, {"k": [1]}):
        _expect(TypeError, render_metrics, bad)
    _expect(ValueError, render_metrics, {"k": float("nan")})
    _expect(ValueError, render_metrics, {"k": float("inf")})

    # Parsing validation.
    for bad in (b"{}", 1, None):
        _expect(TypeError, parse_metrics, bad)
    for bad in (
        "", "{", "not json", "[]", "1", '"x"', "null",
        '{"k": NaN}', '{"k": Infinity}', '{"k": -Infinity}',
    ):
        _expect(ValueError, parse_metrics, bad)
    assert parse_metrics("{}") == {}
    assert parse_metrics('  {"x": 1}  \n') == {"x": 1}


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else list(argv)
    if argv != ["--selftest"]:
        print(_USAGE, file=sys.stderr)
        return 2
    selftest()
    print("selftest ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
