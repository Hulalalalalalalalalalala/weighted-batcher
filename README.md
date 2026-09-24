# weighted-batcher

Weighted sampling helpers whose selection sequence is reproducible from a seed, plus metric rendering that keeps very large counters exact.

## Requirements

Python 3.11 or newer. Standard library only.

## Install

    python3 -m pip install -e .

## Run

    python3 -m weighted_batcher --selftest

Append one metric line to a durable log, recover the log back as JSON, seal it into a numbered segment, or stream it back:

    python3 -m weighted_batcher record FILE METRICS_JSON
    python3 -m weighted_batcher recover FILE
    python3 -m weighted_batcher rotate FILE
    python3 -m weighted_batcher stream FILE

`recover` and `stream` print one JSON object per line on stdout and exit 0.
`rotate` seals `FILE` into a numbered segment (`FILE.1`, then `FILE.2`, ...),
leaving an empty `FILE` behind, and exits 0. `rotate` and `stream` exit 1 on
failure; an empty log rotates into an empty segment.

## Public interface

`weighted_batcher.Sampler(weights, replacement=True, seed=None)`.
- `Sampler.sample(n) -> list[int]` returns drawn indices.
- In without-replacement mode draws accumulate on the instance: repeated
  `sample` calls never return an index already drawn, and requesting more
  draws than the remaining positive-weight items raises `ValueError`.
- `Sampler.weights -> list[float]` as supplied.
- `weighted_batcher.render_metrics(metrics) -> str` returns one line of JSON.
- `weighted_batcher.parse_metrics(line) -> dict` accepts what `render_metrics` produced.
- `weighted_batcher.append_metrics(path, line) -> None` validates one JSON object
  line and appends it to `path`. Concurrent processes appending to the same file
  never interleave records. Raises `OSError` if `line` is not a string or the
  path is not writable, `ValueError` if the top level is not a JSON object, and
  `TypeError` if a key is not a string or a value is not an int or float
  (booleans do not count).
- `weighted_batcher.recover_metrics(path) -> list[dict]` reads the log set
  back: segments `path.1`, `path.2`, ... in ascending order followed by the
  current log at `path`. Every complete newline-terminated line is parsed; a
  torn unterminated tail left by a crashed writer is silently discarded.
  Blank lines (`\n` and `\r\n`) are skipped. A non-JSON complete line raises
  `ValueError` naming its file and line number. An empty or newline-only file
  yields `[]`. Missing files raise `FileNotFoundError` (only when neither the
  current log nor any segment exists); directories raise
  `IsADirectoryError`. Large counters return as exact ints, `-0.0` is
  preserved, and key order follows the file.
- `weighted_batcher.rotate_metrics(path) -> None` seals the current log into
  the next numbered segment (`path.1`, `path.2`, ...) and leaves an empty
  file at `path`; an empty log seals an empty segment. Concurrent appenders
  are serialised with a file lock, so every accepted record lands in exactly
  one segment or in the current log. A crash after sealing but before
  re-creation still leaves the segment readable in write order. Raises
  `FileNotFoundError` if the log is missing, `IsADirectoryError` for a
  directory, and `OSError` for a non-string path or a locking failure.
- `weighted_batcher.iter_metrics(path)` is the streaming counterpart of
  `recover_metrics`: it yields records line by line across segments and the
  current log without loading them into memory, with the same parsing and
  error semantics.

## Tests

    python3 -m unittest discover -s tests -t .

## Limits

The sampler is single-threaded and deterministic per seed.
Counters are integers; no floating point rounding is applied to them.
No dataset loading and no training loop.
