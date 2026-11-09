# Join materialization envelope

SQL joins check owned intermediate-row allocation estimates before cloning
values into retained results. This prevents a filtered Cartesian join from
building a huge intermediate even when its candidate-pair count is below the
existing CPU limit. Hash, outer, lateral, indexed lookup, spatial and joined
UPDATE/DELETE candidate paths share the execution envelope.

The default extra ceiling is 512 MiB. Set
`BICDB_MAX_JOIN_MATERIALIZED_BYTES` to another byte count when the process
starts. `0` disables this extra ceiling; a tighter host/role
`QueryMemoryScope` still applies. Nested query and routine execution inherits
the same counter. A fresh top-level execution starts a fresh counter, including
after an error. Guards cannot be transferred between threads.

A rejected allocation returns SQLSTATE `53200`. The query does not fall back to
another materializing plan after a memory-limit error. Existing host memory
observations and deadlines are checked at execution cancellation points;
deadline-only scopes return `57014`. Normal SQL transaction error and rollback
rules apply. The connection remains usable after the appropriate rollback.

This is a cooperative, conservative materialization-work envelope, not a hard
RSS or allocator quota. It accumulates prior intermediate allocations even
when those intermediates have been freed. Provisional joined DML candidates
are counted before predicate evaluation because they may become retained
rows. It includes new slot storage, cloned owned payloads, selected scratch
vectors and spatial candidate offsets. The optimized primary-key join charges
payload copies while preserving moves of uniquely used values. Shared JSONB
and LOB payloads remain shared; JSON text owns both spelling and parsed data,
including arbitrary-precision numeric digits.

Source-table materialization, all index/R-tree preparation, caches, result
projection, transient predicate rows and other allocations are not all covered
by this counter. Existing source/host guards remain separate. The envelope
does not establish a total process-memory guarantee or replace host admission
and memory limits. A legitimate query may need a different envelope or a plan
that materializes fewer intermediate rows. The CPU candidate-pair limit stays
independent, including joins whose predicate would retain no output rows.

Validation preserves a small negative control: a 256 KiB active query envelope
previously retained a 3.7 MiB product before rejection. Regression tests check
rejection before the whole product is retained, healthy subsequent execution,
optimized join paths, joined DML with unchanged rows after rejection, and
owned-versus-shared JSON memory. Exact source-scoped test results and negative
control outcomes belong in the completion evidence; an earlier revision's
passing suite does not qualify a later guard change.
