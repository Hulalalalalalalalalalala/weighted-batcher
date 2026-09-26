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
    python3 -m weighted_batcher group-join FILE GROUP MEMBER LEASE_SECONDS
    python3 -m weighted_batcher group-read FILE GROUP
    python3 -m weighted_batcher group-advance FILE GROUP TOKEN POSITION
    python3 -m weighted_batcher group-takeover FILE GROUP MEMBER [LEASE_SECONDS]
    python3 -m weighted_batcher audit FILE
    python3 -m weighted_batcher verify FILE
    python3 -m weighted_batcher tx-begin FILE RECORDS_JSON [FILE RECORDS_JSON ...]
    python3 -m weighted_batcher tx-commit TRANSACTION_ID
    python3 -m weighted_batcher tx-rollback TRANSACTION_ID
    python3 -m weighted_batcher tx-read FILE TRANSACTION_ID
    python3 -m weighted_batcher tx-adjudicate TRANSACTION_ID
    python3 -m weighted_batcher tx-conflicts FILE

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
`release` retires the handle. `group-join` creates (or rejoins) consumer
group `GROUP` and prints the lease's takeover token; `group-read`
streams records from the group's current position; `group-advance`
moves the group position to `POSITION` under the lease token;
`group-takeover` takes over the group's expired lease (reusing its lease
seconds unless `LEASE_SECONDS` is given) and prints the fresh token.
`rotate`, `stream`, `compact`, `resume`,
`prune`, `snapshot`, `release` and `snap-resume` exit 1 on
failure; wrong argument counts and a `POSITION` or `QUOTA` that is not an
integer print usage on stderr and exit 2. The four group subcommands
exit 0 on success and 1 on failure; wrong argument counts and a
`POSITION` or `LEASE_SECONDS` that is not an integer print usage on
stderr and exit 2.
`audit` builds or refreshes the integrity chain `FILE.audit` — one JSON
line registering every surviving record's byte fingerprint and write
ordinal together with the pruned-count basis, the registered record
total and the chain-tail checksum, all exact decimal integers — prints
`{"count":N,"checksum":M}` and exits 0. `verify` compares the segment
set record by record against the chain and exits 0 when everything
matches; a record modified by a single byte or mixed in fails with
`ValueError` naming its file and line number, records truncated
wholesale or a replaced segment fails naming the mismatched write
ordinal, and any failure exits 1. Both print usage and exit 2 on a
wrong argument count.

`tx-begin` starts one atomic write over several logs: it takes one
`FILE RECORDS_JSON` pair per participating log (`RECORDS_JSON` is a JSON
array of metric objects for that log, in write order), prepares the
whole transaction and prints its persistable integer transaction
identifier. Before commit none of the logs gains a record;
`tx-commit TRANSACTION_ID` makes every participating log gain its whole
batch at once and `tx-rollback TRANSACTION_ID` discards the preparation;
`tx-read FILE TRANSACTION_ID` streams `FILE`'s records from that
transaction's point of view, one JSON object per line -- its own batch
included while open or mid-finalise, excluded after a rollback, never
half present. An identifier already committed cannot commit twice, take
effect on only some logs, or commit after a rollback; those cases raise
`ValueError` and exit 1, as does a forged or foreign identifier. All
four subcommands print usage on stderr and exit 2 on a wrong argument
count (or a transaction id that is not an integer), and otherwise exit 1
on failure.

`tx-conflicts FILE` lists the write-write conflicts among undecided
transactions on one log — two still-undecided transactions whose
write-serial ranges overlap on the log conflict — printing one JSON
object per line, `{"txid":N,"serials":[...]}` with the transaction
identifier and its overlapping write serials, all exact decimal
integers, ordered by ascending identifier. `tx-adjudicate
TRANSACTION_ID` decides a prepared transaction once: conflicting
transactions are ordered by ascending identifier, the smaller identifier
wins first and the conflicting later one loses; the verdict prints as
one JSON line, `{"txid":N,"verdict":"winner"|"rejected","conflicts":M,"serials":[...]}`
whose counts and serials are exact decimal integers. A rejected
transaction's status becomes `rejected`; committing it, rolling it back,
re-adjudicating it or adjudicating any already-decided identifier
raises `ValueError` and exits 1. A winner stays prepared and commits
normally, its batch landing whole in write-serial order. Both
subcommands exit 0 on success and 1 on failure, and print usage on
stderr and exit 2 on a wrong argument count (or a transaction id that
is not an integer).

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
- `weighted_batcher.join_group_metrics(path, group, member, lease_seconds)`
  joins consumer group `group` on the log at `path` and returns the
  group's lease: a persistable mapping carrying the holder `member`, the
  takeover `token`, the shared write-order read `position`, the
  `lease_seconds` and the `expires_at` timestamp. The first join creates
  the group file `path.group.<group>` at position 0; later joins return
  the group's current lease, so every member apportions the same record
  sequence and at any moment exactly one lease can advance the position.
  The state is published atomically and serialised with advances and
  takeovers by a lock anchored on `path.group.<group>.lock` — never on
  the log — so concurrent joins, advances and takeovers neither clobber
  one another nor lose updates, and same-process readers and writers
  interleaved with them neither deadlock nor report spurious locking
  failures. Old segment sets need no migration. Raises `TypeError` if
  `group` or `member` is not a string or `lease_seconds` is not an
  integer (booleans do not count), `ValueError` for a negative
  `lease_seconds` or a corrupt group file, `IsADirectoryError` for a
  directory, and `OSError` for a non-string path or a locking/write
  failure.
- `weighted_batcher.group_resume_metrics(path, group)` streams records
  from the group's current position with exactly the ordinal rules of
  `resume_metrics`: pruned records keep their ordinals, a position
  inside the pruned region starts at the oldest surviving record, a
  position equal to the record total (pruned records included) yields an
  empty stream, and only a position past that total is out of range
  (raised while iterating). Reading takes no lock. Raises
  `FileNotFoundError` if the group file is missing (or neither the log
  nor any segment exists), `ValueError` for a corrupt group file,
  `TypeError` if `group` is not a string, `IsADirectoryError` for a
  directory, and `OSError` for a non-string path.
- `weighted_batcher.advance_group_metrics(path, group, token, position)`
  moves the group's read position to `position` under the current
  lease's takeover `token` (the lease mapping itself is accepted too).
  The advance commits only when the token matches, the lease has not
  expired and `position` moves the group forward: a forged or
  superseded token, an expired lease, or a position another member
  already committed raises `ValueError`. The new state is published
  atomically, so a crash leaves either the old position or the new one.
  A position past the record total is not rejected here;
  `group_resume_metrics` reports it lazily while iterating. Raises
  `TypeError` if `token` is not a string or `position` is not an
  integer (booleans do not count), `ValueError` for a negative
  `position` or a corrupt group file, `FileNotFoundError` if the group
  file is missing, `IsADirectoryError` for a directory, and `OSError`
  for a non-string path or a locking/write failure.
- `weighted_batcher.takeover_group_metrics(path, group, member, lease_seconds=None)`
  takes over the group's expired lease: the caller becomes the holder, a
  fresh takeover token is issued (an advance under the old token now
  raises `ValueError`), the group position is kept so apportioning
  resumes without loss or duplication, and the new lease runs for
  `lease_seconds` — the group's own lease seconds when omitted. A
  takeover while the current lease is unexpired raises `ValueError`.
  Returns the same lease mapping `join_group_metrics` returns. Raises
  `TypeError` if `group` or `member` is not a string or `lease_seconds`
  is not an integer (booleans do not count), `ValueError` for a negative
  `lease_seconds`, a corrupt group file or an unexpired lease,
  `FileNotFoundError` if the group file is missing,
  `IsADirectoryError` for a directory, and `OSError` for a non-string
  path or a locking/write failure.
- `weighted_batcher.audit_metrics(path) -> dict` builds or incrementally
  refreshes the integrity audit chain of the segment set, kept as one
  JSON line in the state file `path.audit`: every surviving record's
  byte fingerprint and write ordinal (pruned records keep their
  ordinals), the pruned-count basis, the registered record total and
  the chain-tail checksum, all exact decimal integers with keys in
  insertion order. Returns `{"count": ..., "checksum": ...}` with the
  registered record total (pruned records included) and the chain-tail
  checksum. Chain building shares the live-log lock with appends and
  the other mutations and settles any half-finished compaction or prune
  first; the state file is staged, fsynced and atomically renamed, so a
  crash never leaves half a state file behind — the chain may lag, and
  the next write or chain build catches it up. Appends, rotations,
  compactions, prunes and snapshots keep the chain consistent as part
  of their own locked section; old segment sets need no migration.
  Raises `FileNotFoundError` if the log is missing, `IsADirectoryError`
  for a directory, `OSError` for a non-string path or a locking/write
  failure, and `ValueError` if the existing state file is corrupt.
- `weighted_batcher.verify_metrics(path) -> None` compares the segment
  set record by record against the audit chain and returns normally
  when everything matches; an empty set or one holding only a torn tail
  matches an accordingly empty chain. A record modified by a single
  byte or mixed in raises `ValueError` naming its file and line number;
  records truncated wholesale or a replaced segment raises `ValueError`
  naming the mismatched write ordinal. Reading is lock free. Raises
  `FileNotFoundError` if the log or the audit chain is missing,
  `IsADirectoryError` for a directory, `OSError` for a non-string path,
  and `ValueError` for a corrupt state file or a mismatch.
- `weighted_batcher.tx_begin_metrics(logs, records) -> int` starts one
  atomic write over several independent logs and returns a persistable
  exact-decimal-integer transaction identifier. `logs` is a sequence of
  participating log paths and `records` the matching sequence of record
  batches, each a list of metric mappings in write order; the sequences
  have the same length and no path repeats. Every record follows the
  established metric-line rules (exact oversized integer counters,
  `-0.0`, insertion key order). The coordinator record persists the
  identifier, the integer-indexed participating log set and every
  record's exact decimal write serial (the record total, pruned records
  included, at which the batch joins the log). Preparation stages each
  batch in that log's private sidecar files only, so before commit no
  participating log gains a record and ordinary appends, rotations,
  compactions and prunes proceed while the transaction is open; logs are
  locked in canonical order one at a time. Raises `TypeError` for a
  non-sequence argument, a non-mapping record or a non-string/non-number
  metric key or value (booleans do not count), `ValueError` for unequal
  lengths, a repeated path or a NaN/Infinity value, `FileNotFoundError`
  for a missing log, `IsADirectoryError` for a directory, and `OSError`
  for a non-string path or a locking/write failure; a failed preparation
  leaves nothing readable behind.
- `weighted_batcher.tx_commit_metrics(txid) -> None` commits the
  prepared transaction: the coordinator record is atomically published as
  committed — the single commit point, recording every batch record's
  exact decimal write serial while each participating log is locked and
  settled — and each log then appends its staged bytes as one whole
  newline-terminated write, so every participating log gains the whole
  batch or, before the commit point, none does, and a read never meets a
  half-written transaction. A crash after the commit point leaves
  deterministic residue that the next write on each log (or a repeated
  commit) finishes from the staged bytes; old segment sets are not
  migrated and their format is unchanged. The same identifier cannot
  commit twice, take effect on only some logs, or commit after a
  rollback; a repeated commit or a rolled-back identifier raises
  `ValueError`. Raises `TypeError` if `txid` is not an integer (booleans
  do not count), `ValueError` if it is negative, forged, already
  committed, already rolled back or still preparing,
  `FileNotFoundError` if the record or a participating log is missing,
  `IsADirectoryError` for a directory, and `OSError` for a locking/write
  failure.
- `weighted_batcher.tx_rollback_metrics(txid) -> None` rolls the prepared
  transaction back: the coordinator record is published as rolled back
  and every participating log drops its staged preparation, having never
  gained a readable record. Repeating a finished rollback is a no-op
  (it still sweeps crash residue), while committing a rolled-back
  identifier raises `ValueError`. Raises `TypeError` if `txid` is not an
  integer (booleans do not count), `ValueError` if it is negative,
  forged, still preparing or already committed, `FileNotFoundError` if a
  participating log is missing, `IsADirectoryError` for a directory,
  and `OSError` for a locking/write failure.
- `weighted_batcher.tx_read_metrics(path, txid)` streams one log's
  records from the transaction's point of view: an open or mid-finalise
  transaction yields the current records followed by its own staged batch
  in write order (the batch produced whole from the sidecar, never a torn
  tail), a rolled-back transaction yields the records without the batch,
  and a finished commit reads through the ordinary segment walk with no
  duplication. Reading is lock free and changes no state, so a
  same-process prepare, commit or rollback interleaved with it neither
  deadlocks nor reports a spurious locking failure. Raises `TypeError` if
  `txid` is not an integer (booleans do not count), `ValueError` if it is
  negative, forged or belongs to a different transaction than this log
  (a bad record line is raised lazily while iterating),
  `FileNotFoundError` if neither the log nor any segment exists,
  `IsADirectoryError` for a directory, and `OSError` for a non-string
  path.
- `weighted_batcher.tx_adjudicate_metrics(txid) -> dict` adjudicates the
  prepared transaction `txid` against its write-write conflicts once:
  two still-undecided transactions whose write-serial ranges overlap on
  the same log conflict, conflicting transactions are ordered by
  ascending identifier, the smaller identifier wins first and the
  conflicting later one loses. A loser is atomically published with
  status `rejected` — committing it, rolling it back or adjudicating it
  again raises `ValueError`, as does adjudicating any already-decided
  identifier — while a winner stays prepared and commits normally, its
  batches landing whole in write-serial order. Returns the verdict
  mapping `{"txid": ..., "verdict": "winner"|"rejected", "conflicts":
  ..., "serials": [...]}` with the number of conflicting transactions
  and the transaction's own write serials, every count and serial an
  exact decimal integer. The verdict is one atomic coordinator-record
  publication, so a crash leaves the pre- or post-adjudication state,
  never half an adjudication state file, and a rejected transaction's
  residue is swept by the next write on each log. Participating logs are
  locked one at a time in lexicographic path order with never two log
  locks held at once. Raises `TypeError` if `txid` is not an integer
  (booleans do not count), `ValueError` if it is negative, forged,
  still being prepared or already decided, `FileNotFoundError` if the
  record or a participating log is missing, `IsADirectoryError` for a
  directory, and `OSError` for a locking/write failure.
- `weighted_batcher.tx_conflicts_metrics(path) -> list[dict]` lists the
  write-write conflicts among undecided transactions on one log: one
  `{"txid": ..., "serials": [...]}` mapping per undecided transaction
  whose write-serial range overlaps another undecided transaction's,
  ordered by ascending identifier, with the overlapping write serials in
  ascending order — every identifier and serial an exact decimal
  integer. Raises `FileNotFoundError` if neither the log nor any
  segment exists, `IsADirectoryError` for a directory, `OSError` for a
  non-string path or a locking failure, and `ValueError` for corrupt
  transaction state.

## Tests

    python3 -m unittest discover -s tests -t .

## Limits

The sampler is single-threaded and deterministic per seed.
Counters are integers; no floating point rounding is applied to them.
No dataset loading and no training loop.
