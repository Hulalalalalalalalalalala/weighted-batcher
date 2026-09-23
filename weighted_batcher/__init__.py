"""Weighted sampling with seed-reproducible draws and exact metric serialisation."""

from __future__ import annotations

import json
import math
import numbers
import random

__all__ = ["Sampler", "render_metrics", "parse_metrics"]


def _validate_weight(weight):
    # bool is a Real/int subclass in Python but is not an acceptable weight.
    if isinstance(weight, bool) or not isinstance(weight, numbers.Real):
        raise TypeError("weights must be real numbers")
    if not math.isfinite(weight):
        raise ValueError("weights must not be NaN or infinite")
    if weight < 0:  # -0.0 compares equal to 0 and stays valid (with zero effective weight)
        raise ValueError("weights must not be negative")
    return weight


def _draw_index(rng, active):
    """Draw one index from ``[[index, weight], ...]`` with probability ~ weight/total."""
    total = 0
    for _, weight in active:
        total += weight
    point = rng.random() * total
    upto = 0
    for index, weight in active:
        upto += weight
        if point < upto:
            return index
    return active[-1][0]  # floating-point rounding safety net


class Sampler:
    """Weighted index sampler; the draw sequence is reproducible from ``seed``."""

    def __init__(self, weights, replacement=True, seed=None):
        self._weights = [_validate_weight(weight) for weight in weights]
        self._replacement = replacement
        self._rng = random.Random(seed)

    @property
    def weights(self):
        return list(self._weights)

    def sample(self, n):
        if isinstance(n, bool) or not isinstance(n, int):
            raise TypeError("sample count must be an integer")
        if n < 0:
            raise ValueError("sample count must not be negative")
        if n == 0:
            return []

        # 0 and -0.0 both compare non-positive, so they never become candidates.
        active = [[index, weight] for index, weight in enumerate(self._weights) if weight > 0]
        if not active:
            raise ValueError("no items with positive weight")
        if not self._replacement and n > len(active):
            raise ValueError(
                "cannot draw more items than the number of positive-weight items"
            )

        drawn = []
        for _ in range(n):
            index = _draw_index(self._rng, active)
            drawn.append(index)
            if not self._replacement:
                active = [pair for pair in active if pair[0] != index]
        return drawn


def render_metrics(metrics):
    """Render a name-to-number mapping to one compact JSON line ending in ``\\n``."""
    for key, value in metrics.items():
        if not isinstance(key, str):
            raise TypeError("metric names must be strings")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError("metric values must be integers or floats, not booleans")
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError("metric values must not be NaN or infinite")

    # json encodes ints with str(int) (arbitrary precision, never via float),
    # preserves dict insertion order, and allow_nan rejects NaN/Infinity.
    return (
        json.dumps(metrics, separators=(",", ":"), allow_nan=False, ensure_ascii=False)
        + "\n"
    )


def _reject_constant(value):
    raise ValueError(f"invalid JSON literal: {value}")


def parse_metrics(line):
    """Parse a line produced by :func:`render_metrics` back into a ``dict``."""
    if not isinstance(line, str):
        raise TypeError("metrics line must be a string")
    try:
        result = json.loads(line, parse_constant=_reject_constant)
    except ValueError as exc:  # JSONDecodeError and rejected NaN/Infinity constants
        raise ValueError("invalid metrics JSON") from exc
    if not isinstance(result, dict):
        raise ValueError("metrics JSON must have an object at the top level")
    return result
