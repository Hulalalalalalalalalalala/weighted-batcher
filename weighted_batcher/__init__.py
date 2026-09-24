"""Weighted sampling helpers and exact metric serialisation."""

from __future__ import annotations

import bisect
import json
import math
import numbers
import random

__all__ = [
    "Sampler",
    "render_metrics",
    "parse_metrics",
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
]


class Sampler:
    """Draw indices with probability proportional to their effective weight.

    The effective weight of ``0`` and ``-0.0`` is zero: such items can never
    be drawn.  All draws from one instance share a single PRNG stream, so the
    sequence produced from a given ``seed`` is reproducible item by item.

    Without replacement, draws accumulate across calls: an index drawn by
    one :meth:`sample` call is never drawn again by a later call on the same
    instance, and requesting more draws than the remaining positive-weight
    items raises :class:`ValueError`.
    """

    def __init__(self, weights, replacement=True, seed=None):
        checked = []
        for w in weights:
            if isinstance(w, bool) or not isinstance(w, numbers.Real):
                raise TypeError(
                    f"weight must be a real number, got {type(w).__name__}"
                )
            if math.isnan(w) or math.isinf(w):
                raise ValueError("weight must not be NaN or Infinity")
            if w < 0:
                raise ValueError("weight must not be negative")
            checked.append(w)
        self._weights = checked
        self._replacement = bool(replacement)
        self._rng = random.Random(seed)
        # Effective weights as floats; 0 and -0.0 both collapse to zero.
        self._effective = [float(w) if w > 0 else 0.0 for w in checked]
        self._positive = sum(1 for e in self._effective if e > 0.0)
        cumulative = []
        total = 0.0
        for e in self._effective:
            total += e
            cumulative.append(total)
        self._cumulative = cumulative
        # Without-replacement pool, depleted across repeated sample() calls.
        self._remaining = [i for i, e in enumerate(self._effective) if e > 0.0]

    @property
    def weights(self):
        """The weights as supplied to the constructor."""
        return list(self._weights)

    def sample(self, n):
        """Return a list of ``n`` drawn indices."""
        if isinstance(n, bool) or not isinstance(n, int):
            raise TypeError(f"sample count must be an integer, got {type(n).__name__}")
        if n < 0:
            raise ValueError("sample count must not be negative")
        if n == 0:
            return []
        if self._replacement:
            if self._positive == 0:
                raise ValueError("no items with positive weight to draw from")
            return self._sample_with_replacement(n)
        remaining = len(self._remaining)
        if n > remaining:
            raise ValueError(
                f"cannot draw {n} items without replacement: only "
                f"{remaining} positive-weight items remain"
            )
        return self._sample_without_replacement(n)

    def _draw_one(self, cumulative, total):
        r = self._rng.random() * total
        i = bisect.bisect_right(cumulative, r)
        # Guard against r rounding up to exactly total.
        return min(i, len(cumulative) - 1)

    def _sample_with_replacement(self, n):
        total = self._cumulative[-1] if self._cumulative else 0.0
        return [self._draw_one(self._cumulative, total) for _ in range(n)]

    def _sample_without_replacement(self, n):
        # The pool persists on the instance, so draws stay distinct across
        # repeated sample() calls.
        drawn = []
        for _ in range(n):
            total = 0.0
            cumulative = []
            for i in self._remaining:
                total += self._effective[i]
                cumulative.append(total)
            pos = self._draw_one(cumulative, total)
            drawn.append(self._remaining.pop(pos))
        return drawn


def render_metrics(metrics):
    """Render a name-to-number mapping as one compact JSON line.

    Keys keep their insertion order.  Integers are written as exact decimal
    integers regardless of magnitude; the line ends with a single newline.
    """
    for key, value in metrics.items():
        if not isinstance(key, str):
            raise TypeError(f"metric name must be a string, got {type(key).__name__}")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError(
                f"metric value must be an int or float, got {type(value).__name__}"
            )
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            raise ValueError("metric value must not be NaN or Infinity")
    return json.dumps(metrics, ensure_ascii=False, separators=(",", ":")) + "\n"


def parse_metrics(line):
    """Parse a line produced by :func:`render_metrics` back into a dict."""
    if not isinstance(line, str):
        raise TypeError(f"expected a string, got {type(line).__name__}")

    def _reject_constant(name):
        raise ValueError(f"invalid numeric constant {name}")

    try:
        data = json.loads(line, parse_constant=_reject_constant)
    except ValueError as exc:
        raise ValueError(f"invalid metrics line: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("metrics line must be a JSON object at the top level")
    return data


from .persistence import (  # noqa: E402
    append_metrics,
    compact_metrics,
    iter_metrics,
    prune_metrics,
    recover_metrics,
    release_metrics,
    resume_metrics,
    resume_snapshot_metrics,
    rotate_metrics,
    snapshot_metrics,
)
