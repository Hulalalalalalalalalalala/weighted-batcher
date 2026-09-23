# weighted-batcher

Weighted sampling helpers whose selection sequence is reproducible from a seed, plus metric rendering that keeps very large counters exact.

## Requirements

Python 3.11 or newer. Standard library only.

## Install

    python3 -m pip install -e .

## Run

    python3 -m weighted_batcher --selftest
    python3 -m weighted_batcher record PATH
    python3 -m weighted_batcher recover PATH

`record` reads one metric line from standard input and appends it to the file
at PATH; `recover` prints the metrics stored in PATH, one JSON object per
line, and exits 0.

## Public interface

`weighted_batcher.Sampler(weights, replacement=True, seed=None)`.
- `Sampler.sample(n) -> list[int]` returns drawn indices.
- `Sampler.weights -> list[float]` as supplied.
- `weighted_batcher.render_metrics(metrics) -> str` returns one line of JSON.
- `weighted_batcher.parse_metrics(line) -> dict` accepts what `render_metrics` produced.
- `weighted_batcher.record_metrics(line, path) -> None` validates one metric line and atomically appends it to a file.
- `weighted_batcher.recover_metrics(path) -> list[dict]` reads appended metrics back, dropping any incomplete trailing line and raising `ValueError` for a non-JSON complete line.

## Tests

    python3 -m unittest discover -s tests -t .

## Limits

The sampler is single-threaded and deterministic per seed.
Counters are integers; no floating point rounding is applied to them.
No dataset loading and no training loop.
