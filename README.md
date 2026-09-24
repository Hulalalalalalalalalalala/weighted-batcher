# weighted-batcher

Weighted sampling helpers whose selection sequence is reproducible from a seed, plus metric rendering that keeps very large counters exact.

## Requirements

Python 3.11 or newer. Standard library only.

## Install

    python3 -m pip install -e .

## Run

    python3 -m weighted_batcher --selftest

Append one metric line to a durable log, recover the log back as JSON, seal it into a numbered segment, stream it back, merge it into one segment, or resume reading from a record position:

    python3 -m weighted_batcher record FILE METRICS_JSON
    python3 -m weighted_batcher recover FILE
    python3 -m weighted_batcher rotate FILE
    python3 -m weighted_batcher stream FILE
    python3 -m weighted_batcher compact FILE
    python3 -m weighted_batcher resume FILE [POSITION]

`recover` and `stream` print one JSON object per line on stdout and exit 0.
`rotate` seals `FILE` into a numbered segment (`FILE.1`, then `FILE.2`, ...),
leaving an empty `FILE` behind, and exits 0. `compact` merges every segment
and the current log into a single `FILE.1`, leaving `FILE` empty, and exits 0.
`resume` prints the records from `POSITION` onwards (a count of records
already read, default 0) and exits 0. `rotate`, `stream`, `compact`, and
`resume` exit 1 on failure; an empty log rotates or compacts into an empty
segment.

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
  the next numbered segment, one past the highest segment number present
  (`path.1`, `path.2`, ...; numbers of externally deleted segments are not
  reused), and leaves an empty file at `path`; an empty log seals an empty
  segment. Concurrent appenders are serialised with a file lock, so every
  accepted record lands in exactly one segment or in the current log. A crash
  after sealing but before re-creation still leaves the segment readable in
  write order. Raises `FileNotFoundError` if the log is missing,
  `IsADirectoryError` for a directory, and `OSError` for a non-string path or
  a locking failure.
- `weighted_batcher.iter_metrics(path)` is the streaming counterpart of
  `recover_metrics`: it yields records line by line across segments and the
  current log without loading them into memory, with the same parsing and
  error semantics.
- `weighted_batcher.compact_metrics(path) -> None` merges every segment and
  the current log into a single segment `path.1`, leaving `path` empty; an
  all-empty log set still yields an empty segment. Each complete record line
  is copied verbatim with its newline — never re-rendered, de-duplicated, or
  reordered — so exact counters, `-0.0`, and key order survive, and a single
  line may be arbitrarily long. Torn tails are dropped and blank lines are
  skipped. The merge is flushed to disk under the staging name `path.compact`
  before any cleanup begins; while that staging segment exists it is the only
  file recovery and streaming read, and an interrupted compaction is finished
  by the next append, rotation, or compaction. Raises `FileNotFoundError` if
  neither the log nor any segment exists, `IsADirectoryError` for a directory,
  and `OSError` for a non-string path, a locking failure, or a write failure.
- `weighted_batcher.resume_metrics(path, position=0) -> list[dict]` reads the
  log set back starting `position` records in — the count of records already
  read, in write order from the start of the segment set. The position stays
  valid across appends, rotations, compactions, and crash clean-ups; only a
  position beyond the total record count is out of bounds. The log set is
  streamed rather than loaded whole, and the result equals the corresponding
  slice of `recover_metrics(path)`. Raises the same path errors as
  `recover_metrics`, `TypeError` if `position` is not an integer (booleans do
  not count), and `ValueError` if it is negative, out of bounds, or a complete
  line at or past it is not a JSON object (named with its file and line
  number).

## Tests

    python3 -m unittest discover -s tests -t .

## Limits

The sampler is single-threaded and deterministic per seed.
Counters are integers; no floating point rounding is applied to them.
No dataset loading and no training loop.
