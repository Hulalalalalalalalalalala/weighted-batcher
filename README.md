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
    python3 -m weighted_batcher snapshot FILE
    python3 -m weighted_batcher release FILE HANDLE
    python3 -m weighted_batcher snap-resume FILE HANDLE [POSITION]

`recover` and `stream` print one JSON object per line on stdout and exit 0.
`rotate` seals `FILE` into a numbered segment (`FILE.1`, then one past the
highest number already present), leaving an empty `FILE` behind, and exits 0.
`compact` merges every segment and the current log into `FILE.1`, leaving an
empty `FILE` behind; an empty log set still produces an empty `FILE.1`.
`resume` prints records from `POSITION` (the number of records already read
from the start in write order, default 0) onward; the position stays valid
across appends, rotations, compactions and pruning. `prune` retains at most
`QUOTA` records, dropping the oldest whole records; `QUOTA` 0 drops every
record. Pruned records keep their write-order positions, so a `POSITION`
inside or at the prune point reads from the oldest surviving record, and
only a position past the total record count (pruned included) is out of
range. `snapshot` pins the complete record sequence at creation time and
prints a persistable handle; `snap-resume` streams the pinned sequence
from `POSITION` (same ordinal rules as `resume`, default 0), and
`release` retires the handle. `rotate`, `stream`, `compact`, `resume`,
`prune`, `snapshot`, `release` and `snap-resume` exit 1 on
failure; wrong argument counts and a `POSITION` or `QUOTA` that is not an
integer print usage on stderr and exit 2.

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
- `weighted_batcher.resume_metrics(path, position=0)` streams records from
  `position` onward, where `position` is the number of records already read
  from the start of the segment set in write order. The position needs no
  conversion across appends, rotations, compactions, prunes or crash
  cleanup, and is only out of range when it exceeds the record total
  (pruned records included); equal to the total the stream is empty.
  Reading stays line by line and linear, never materialising the segment
  set. Raises `TypeError` if `position` is not an integer (booleans do not
  count), `ValueError` for a negative or out-of-range position (the latter
  while iterating), with the same path and line-level errors as
  `iter_metrics`.
- `weighted_batcher.prune_metrics(path, quota)` keeps at most `quota`
  records, dropping whole records from the oldest end in write order;
  `quota` 0 drops every record, and a quota at or above the current count
  removes nothing. A record is never split, empty and torn-tail-only
  segments spend no quota, and survivors keep their bytes, order,
  newlines, `-0.0`, oversized counters and key order. The set remembers
  the number of records pruned (`path.prune`), so pruned records keep
  their write-order ordinals for `resume_metrics`. Whole evicted segments
  are deleted and only the single boundary segment is rewritten, staged
  atomically at `path.trim`; a crash leaves either the old set or a set
  pruned to a complete record boundary, and a half-finished trim never
  joins recovery. Pruning is serialised with appends, rotations and
  compactions, and an unfinished compaction is settled first. Raises
  `TypeError` if `quota` is not an integer (booleans do not count),
  `ValueError` for a negative quota, `FileNotFoundError` if the log is
  missing, `IsADirectoryError` for a directory, and `OSError` for a
  non-string path or a locking/write failure. Old segment sets with no
  prune marker need no migration; the first prune starts at zero pruned.
- `weighted_batcher.snapshot_metrics(path) -> str` pins the complete
  record sequence as of the call and returns a persistable snapshot
  handle. The pinned records are copied byte for byte into a
  snapshot-private file, so no later append, rotation, compaction or
  prune changes what the snapshot reads — the records survive even a
  quota-0 prune, and a compaction rewriting the segment members leaves
  the creation-time bytes (original newlines, `-0.0`, oversized
  counters, key order) untouched. Snapshots are independent of each
  other. A crash mid-creation leaves no usable handle behind, and the
  orphaned copy is removed transparently by the next write. Raises
  `FileNotFoundError` if the log is missing, `IsADirectoryError` for a
  directory, and `OSError` for a non-string path or a locking/write
  failure.
- `weighted_batcher.release_metrics(path, handle) -> None` releases a
  snapshot handle and deletes its private copy. Reading or releasing the
  handle afterwards raises `ValueError`, as does a forged handle or one
  belonging to another segment set. Raises `TypeError` if `handle` is
  not a string, `FileNotFoundError` if the log is missing,
  `IsADirectoryError` for a directory, and `OSError` for a non-string
  path or a locking/write failure.
- `weighted_batcher.resume_snapshot_metrics(path, handle, position=0)`
  streams the records pinned for `handle` from `position` onward, with
  the same write-order ordinals as `resume_metrics`: records pruned
  before the snapshot still occupy their ordinals, a position inside
  that pruned region starts at the oldest record the snapshot holds, and
  only a position past the creation-time record total (pruned included)
  is out of range — equal to the total the stream is empty. Repeated
  reads of the same handle always yield the creation-time slice, and
  reading never touches the segment-set members, so a member pruned or
  compacted away concurrently raises no `FileNotFoundError`. Raises
  `TypeError` if `handle` is not a string or `position` is not an
  integer (booleans do not count), `ValueError` for a forged, foreign or
  released handle and for a negative or out-of-range position (the
  latter while iterating), `FileNotFoundError` when neither the log nor
  any segment exists, `IsADirectoryError` for a directory, and `OSError`
  for a non-string path.

## Tests

    python3 -m unittest discover -s tests -t .

## Limits

The sampler is single-threaded and deterministic per seed.
Counters are integers; no floating point rounding is applied to them.
No dataset loading and no training loop.
