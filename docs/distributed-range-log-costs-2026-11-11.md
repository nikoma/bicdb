# Range-write catalog costs: source facts and performance hypotheses

Examined runtime: **1.0.461-beta**, frozen source
`b05385d4b033edf0db03f3eccf4bdd86f7c8eddb`. This is read-only source review,
not a measured component-level write-amplification result, diagnosed cause,
engine change or production qualification. The newer source version does not
retroactively change the binary used in the Azure experiment.

## Persisted representation and transition cost

[`distribution_range_consensus.rs`](../crates/bicdb-core/src/distribution_range_consensus.rs)
stores a node-wide `RangeWriteCatalog`: a map of range IDs to indexed commands,
states, epochs and watermarks, plus backup fences. Each command retains exact
mutation records, not merely a transaction ID. The record representation is
`CommitAdmissionMutation` in [`db.rs`](../crates/bicdb-core/src/db.rs).

For a normal new command that reaches full local application, the state path is
`prepare` → `commit` (provisional) → `certify` → `mark_applied`. These transitions
call `persist_or_restore`. Idempotent prepare/commit/certify paths can return early;
repair, abort, fences, retries and replay have their own paths. Therefore four is
a description of the ordinary complete transition path, not a measured universal
file-write or fsync count per SQL statement.

`persist_or_restore` retains a previous catalog clone for failure rollback.
`persist` clones the current catalog into `RangeWriteEnvelope`; `catalog_checksum`
serializes the complete catalog into compact JSON for SHA-256, then `persist`
serializes the complete envelope as pretty JSON and passes it to
[`storage.rs`](../crates/bicdb-core/src/storage.rs)::`write_atomic`.
`replace_durably_steps` writes a new temporary file and, with fsync enabled,
syncs the file, renames it and syncs the parent directory. This is full-catalog
replacement, not an append of only the changed command. The source review does
not justify deleting these durability barriers or bypassing quorum decisions.

Consequently one persistence transition has CPU, temporary-memory and file-byte
work proportional to the **whole retained catalog**, including other ranges.
The clone/checksum/pretty-output representations add work beyond logical payload
bytes. At fixed topology and payload size, catalog growth before retention kicks
in can raise per-command cost; total replacement bytes over that growth phase
can grow faster than total logical row bytes. These are algorithmic implications,
not an attribution of the observed Azure latency to one function.

## Bounds and what they do not bound

- `RETAIN_APPLIED_COMMANDS_PER_RANGE = 1,024`; `compact_resolved` removes older
  applied/aborted entries below a range's resolved watermark. Unresolved entries
  are preserved. This is not a 1,024-command global catalog or global byte limit.
- Catalog size aggregates retained payload/history across ranges. Movement and
  recovery also affect which histories a node retains; a fixed per-range count
  does not prove a small constant node-wide byte footprint.
- Repair defaults to 128 commands/4 MiB per batch and eight batches per admission,
  with validated configurable maxima. These constrain repair transfer/work, not
  the full-catalog rewrite size.
- The bounded `inspect(max_catalog_bytes)` API does not impose that byte bound
  on ordinary `open`, which reads/deserializes the catalog file. No independent
  global catalog-byte ceiling is established by that ordinary open path.

## Relation to the longer Azure observations

The reported longer four-node baseline ran approximately 907 seconds, with
7,592 successful operations, zero failed calls and 3,804 acknowledged writes.
Successful write p50/p95/p99 were approximately 3.097/8.004/11.136 seconds; reads
were approximately 96/596/904 milliseconds. Observed range-write catalog files
were about 6.34–6.54 MB per node, versus roughly 2 MB in the earlier smoke.
Those facts motivate investigating catalog-size-dependent cost, but do not
isolate it. The later growth trial failed and must remain failed independently
of any subsequently successful diagnosis reads.

The approximately **159 GB aggregate process `write_bytes` delta** is not isolated
range-log write amplification. It includes data/page/transaction WAL, metadata,
background movement/checkpoints and other process I/O; verification timing and
which processes/counter windows are included must also remain explicit.
Logical 1 KiB event payloads alone are not a denominator proving storage-device
or this component's write-amplification ratio. Sampled counters are neither exact
fsync latency nor physical cloud-device writes.

No D2s_v3 VM/disk I/O bottleneck is diagnosed here. Such a claim needs the exact
SKU/disk cache and limits from current official Azure specifications, observed
IOPS/bandwidth/latency/throttling, CPU/network/client saturation and matched
measurement windows. No infrastructure sizing recommendation follows from this
source review.

A focused future experiment should hold workload, fsync, RF, range placement and
client transport fixed while varying retained catalog size. Instrument per-stage
serialization/checksum/clone time, replacement bytes, sync waits, storage counters
and RPC phases separately. Preserve crash recovery and quorum/unknown-outcome
assertions while comparing any proposed representation or batching change. No
such instrumented paired experiment or performance repair was run by this note.
