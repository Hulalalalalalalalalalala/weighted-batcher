# weighted-batcher

Weighted sampling helpers whose selection sequence is reproducible from a seed, plus metric rendering that keeps very large counters exact.

## Requirements

Python 3.11 or newer. Standard library only.

## Install

    python3 -m pip install -e .

## Run

    python3 -m weighted_batcher --selftest

Append one metric line to a durable log, recover the log back as JSON, seal it into a numbered segment, stream it back, compact every segment into one, or resume reading at a saved position:

    python3 -m weighted_batcher record FILE METRICS_JSON
    python3 -m weighted_batcher recover FILE
    python3 -m weighted_batcher rotate FILE
    python3 -m weighted_batcher stream FILE
    python3 -m weighted_batcher compact FILE
    python3 -m weighted_batcher resume FILE [POSITION]
    python3 -m weighted_batcher prune FILE QUOTA

`recover` and `stream` print one JSON object per line on stdout and exit 0.
`rotate` seals `FILE` into a numbered segment (`FILE.1`, then one past the
highest number already present), leaving an empty `FILE` behind, and exits 0.
`compact` merges every segment and the current log into `FILE.1`, leaving an
empty `FILE` behind; an empty log set still produces an empty `FILE.1`.
`resume` prints records from `POSITION` (the number of records already read
from the start in write order, default 0) onward; the position stays valid
across appends, rotations and compactions. `rotate`, `stream`, `compact` and
`resume` exit 1 on failure; wrong argument counts and a `POSITION` that is
not an integer print usage on stderr and exit 2.
`prune` enforces retention `QUOTA`, the greatest number of records kept:
whole records are evicted oldest first (a record is never split), `QUOTA` 0
removes every record, and a quota at or above the surviving record count
changes nothing. Evicted records keep their write-order positions, so a
saved resume position stays valid; a position inside the evicted prefix
starts at the oldest surviving record. `prune` exits 0 on success, 1 on
failure, and 2 with usage on stderr for a non-integer `QUOTA` or the wrong
argument count.

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
  the next numbered segment (`path.1`, then one past the greatest number
  already present, so a deleted number is never reused) and leaves an empty
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
- `weighted_batcher.compact_metrics(path) -> None` merges all segments and
  the current log into the single fixed segment `path.1` and leaves the
  current log empty; a completely empty set still yields an empty segment.
  Complete records are copied byte for byte with their original newlines —
  not re-rendered, de-duplicated or reordered, so `-0.0`, oversized counters
  and key order are preserved; torn tails are dropped and blank lines
  skipped just like on read. The merge is fully written and flushed to a
  temporary file before being atomically published as `path.compact`, and
  only then are old segments removed and the result renamed to `path.1`;
  a half-written crash leftover never joins recovery, and while staging
  exists recovery and streaming read only it. A later append, rotation or
  compaction transparently finishes an interrupted cleanup. Raises
  `FileNotFoundError` if the log is missing, `IsADirectoryError` for a
  directory, and `OSError` for a non-string path or a locking/write failure.
- `weighted_batcher.prune_metrics(path, quota) -> None` enforces a retention
  quota by evicting whole records from the oldest end; `quota` is the
  greatest number of records kept, `0` evicts every record, and a quota at
  or above the surviving record count evicts nothing. Empty segments and
  torn-tail-only segments consume no quota, the cut lands between records
  (a record is never split, however long its line), wholly evicted segments
  are deleted, and only the one segment the cut lands in is rewritten with
  the surviving complete records copied byte for byte, so original
  newlines, `-0.0`, oversized counters and key order are preserved. The
  cumulative number of evicted records is remembered in a `path.pruned`
  sidecar, so evicted records keep their original write-order positions.
  Eviction is atomically published via a prune marker: a crash before
  publication leaves the set untouched, a crash afterwards leaves it
  evicted to a complete boundary, and the next append, rotation, compaction
  or prune finishes the cleanup; an unfinished compaction is settled first.
  Raises `FileNotFoundError` if the log is missing, `IsADirectoryError` for
  a directory, `OSError` for a non-string path or a locking/write failure,
  `TypeError` if `quota` is not an integer (booleans do not count), and
  `ValueError` for a negative quota.
- `weighted_batcher.resume_metrics(path, position=0)` streams records from
  `position` onward, where `position` is the record ordinal in write order
  counting records from the start -- records since pruned away included.
  A position inside the pruned prefix (or exactly at the prune boundary)
  starts at the oldest surviving record; only a position past the record
  total including evicted records is out of range, and a position equal to
  the total yields an empty stream. The position needs no conversion across
  appends, rotations, compactions, pruning or crash cleanup. Reading stays
  line by line and linear, never materialising the segment set. Raises
  `TypeError` if `position` is not an integer (booleans do not count),
  `ValueError` for a negative or out-of-range position (the latter while
  iterating), with the same path and line-level errors as `iter_metrics`.

## Tests

    python3 -m unittest discover -s tests -t .

## Limits

The sampler is single-threaded and deterministic per seed.
Counters are integers; no floating point rounding is applied to them.
No dataset loading and no training loop.
