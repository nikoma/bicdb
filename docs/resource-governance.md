# Node resource governance

BicDB's PB-scale background operations must be bounded together, not only one
operation at a time. `ResourceGovernor` is the fixed-cardinality admission
primitive for that shared envelope.

Every admission declares four nonzero values before work starts:

- peak resident memory bytes;
- in-flight I/O bytes;
- CPU slots; and
- bytes charged to the lane's I/O-rate bucket.

The governor enforces node totals, per-lane totals and concurrency, a combined
background ceiling, and token-bucket I/O rates. It also subtracts an explicit
critical reserve from the capacity available to every noncritical lane. Repair,
backup, compaction, indexing, analytics, and foreground reads therefore cannot
consume the memory, I/O, or CPU slots reserved for consensus and foreground
writes.

The seven lanes are fixed: consensus, foreground write, foreground read,
anti-entropy, backup/restore, compaction, and index build. This keeps metrics
cardinality bounded. A snapshot reports current node, noncritical, background,
and per-lane usage together with admission/rejection totals and remaining I/O
tokens. It never uses tenant, query, record, or path values as metric labels.

Admission is fail-fast. A saturated or oversized request returns a resource-
governance error before allocating its declared resources. A successful call
returns an RAII permit; dropping or explicitly releasing it returns memory,
I/O, CPU, and active-operation reservations deterministically, including on
early error unwinding. Rate tokens remain consumed because completed and failed
I/O both impose real device work.

Configuration validation requires all lanes exactly once, rejects zero or
inconsistent capacities, and proves that background capacity plus critical
reserve fits inside the node envelope. A regressing scheduler clock fails
closed instead of minting rate tokens.

This release provides the common admission primitive. Individual supervisors
must integrate their bounded steps with the corresponding lane before the node
can claim end-to-end governed maintenance; those integrations are tracked in
the scale roadmap.

## Automatic replica integrity

Every distributed server owns a durable anti-entropy sweep controller. It
selects one locally led, non-relocating range under an explicit QPS ceiling and
obtains a lane permit separately for fence installation and for each bounded
digest step. The production supervisor also supplies actual active and queued
query/read/write pressure, so foreground work defers both initial fencing and
subsequent scan work. The default ceiling is zero QPS, and only one range per
node may be active. Retry, range spacing, and full-sweep timing are persisted;
an idle not-due controller performs no disk write. The complete limits object
is available through `PgWireConfig`; changing it while a durable run is active
fails closed.

## Backup and restore

Cluster backup runs expose governed entrypoints for bounded range-fencing,
node-capture, certificate-publication, and range-fence-release work. Admission
occurs before invoking a transport/provider callback. The permit is scoped to
one existing journaled batch and is released on callback failure, journal
failure, and success.

Offline whole-certificate restore admission retains a governed compatibility
entrypoint. For PB-scale restores, `ClusterRestoreAdmissionRun` holds an OS
single-owner lock and verifies at most one exact certificate candidate per
admitted tick. Candidate descriptors, the common restored topology, evidence,
cursor, limits, and final report are checksummed and atomically persisted.
Archive passphrases and database encryption material remain borrowed inputs and
never enter durable state. Reopen resumes at the next unverified node, while
report publication is a separate retry-safe phase.

## Compaction and index construction

Compaction has a governed entrypoint that obtains the compaction lane before
creating its durable checkpoint or rewriting a collection. The permit is held
through the existing phase-checkpointed operation and released on every return.

For PB-scale offline maintenance, incremental compaction pins the starting
commit sequence and advances exactly one collection, log-checkpoint phase, or
publication phase per governed tick. Its canonical collection cursor, reports,
options, resource limits, phase, and timestamps are checksummed and atomically
persisted. Reopen resumes after completed collections. A write between ticks
fails closed before log truncation; the operator can validate and discard that
incomplete coordinator state, retain all committed data, and restart from the
new commit sequence. A crash after a rewrite but before its checkpoint merely
repeats an idempotent collection rewrite.

Online index creation and the resumable full-text prepare, batch-ingestion,
tokenization-finish, merge, and atomic-publication boundaries have index-build
entrypoints. Admission occurs before workspace creation or mutation. BicDB also
rejects any index demand whose declared resident memory is below
`fts_build_memory_bytes`; a supervisor cannot reserve a token-sized amount and
then let the builder allocate its larger configured budget.

Paged B-tree creation is likewise a governed, restartable state machine. Its
`max_batch_rows`, `max_batch_bytes`, and state-file limit are explicit and
persisted. Each tick performs one bounded source batch, verification batch,
physical-count batch, or publication step. Entries are written beneath an
unreachable physical generation; the logical definition catalog is the
visibility commit marker. The builder pins BicDB's commit sequence and refuses
promotion after any intervening write. After promotion, exact and prefix reads
range-scan the page store through the bounded buffer pool, and reopen does not
materialize the complete B-tree in RAM. A retry reconciles a crash after
catalog publication but before the final checkpoint, while discard refuses
every catalog-referenced generation. Incomplete, validated generations are
removed in entry- and byte-bounded key pages.

## Distributed reads and metrics

The governed scatter/gather entrypoint reserves the foreground-read lane before
starting its worker pool. Its demand must cover the maximum encoded result
bytes plus fixed per-shard coordinator overhead, actual worker concurrency,
concurrent shard response buffers, and the maximum rate-charged bytes. Rejected
or under-declared queries contact no shard. The permit spans fan-out and merge
and is released after every success, cancellation, or shard error.

`ResourceGovernor::prometheus_text` exports 61 fixed series: four usage values
for each of three fixed aggregate scopes, and seven values for each of seven
fixed lanes. Only compile-time scope and lane labels exist. Tenant IDs, range
IDs, query text, file paths, plugins, and other user-controlled values cannot
become metric labels.
