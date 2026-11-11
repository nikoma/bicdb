# Native range-image foundations (unqualified)

This source checkpoint contains local mutation exclusion and an explicit fresh-database native genesis producer. It does not complete distributed handoff, native suffix recovery, target installation, or strong reads. No cluster endpoint or automatic placement controller uses this new capture path yet. The complete protocol must remain unmerged until its source/configuration, suffix, installation and quorum controls compose and pass the retained handoff/compaction witnesses.

A range mutation permit belongs to one live database and exact range. The coordinator retains its owned permit through the actual WAL callback. Replica apply, snapshot, repair, catch-up and cleanup helpers carry the same permit into their native transaction; they do not reacquire after a drain starts. Unscoped native/import commits use a conservative whole-database permit. A capture drains existing permits outside database/service locks; entry under existing locks fails immediately when busy. Other-range mutations remain admissible. Private transaction claims include an exact lease and expire with their actual owner.

The additive range scanner checks cancellation for each raw record and limits raw rows, including records outside the selected range. Its cursor advances over that work after flushing all matching rows. One retry step is retained, with bounded JSON serialization. A local content seal describes copied records; it is not a quorum certificate or a production readiness result.

`BicDb::create_new_range_image_database` atomically creates a new directory, syncs its parent, enables the local UUID and image reader fences, then commits a native birth in the page WAL. Existing and partially initialized directories are refused and preserved. Its refusal-only preflight sidecar cannot grant lineage: strict reopen verifies the actual native birth and exact cluster/bootstrap/member/config bindings. Empty initial ownership is allowed for a new learner and grants no range genesis.

Fresh source genesis additionally requires that factory authority, an exact initial range/config, a local exclusive mutation permit, trusted local namespace UUIDs, verified schema/readers, and an actual empty range scan. The code separately records range index zero, a captured global reservation bound, the actual applied snapshot, and native page transaction ID. It waits for the actual shippable prefix without raising an MVCC snapshot to a reserved counter. An ordered native initialization-complete record closes the initialization sequence; ordinary writes remain closed until all initial receipts exist. Completed genesis cannot be reset from later emptiness.

Native receipts occupy the reserved page-tree prefix `[0,0,16]`, outside collection and index keys. Logical and imported physical collection names are validated; ordinary empty-name page-record mutations are refused. Raw byte-key storage administration remains trusted storage access. A native page receipt orders and syncs earlier native work; separate catalog/data transactions are not described as one atomic transaction.

Native imports and physical namespace changes durably invalidate affected base lineage before mutation. Range-scoped invalidation preserves unrelated ranges; whole-database imports and DDL invalidate all bases. Hard drop/recreate receives a new local namespace UUID. Neither an exclusion permit nor an old native receipt authorizes a new certified floor.

The source capture constructor now requires a non-deserializable `SourceRangeImageLineage`, not just a resolved counter. Its only current producer is actual fresh empty genesis. The copy path is not ready to wire: the fresh committed cut must also validate the native base's span, schema, bootstrap/restore domain, voter incarnations and certificate pins through the actual learner-allocation transition. This allows the required epoch-1 to epoch-2 allocation without accepting arbitrary configuration changes. That helper is a separately composed dependency.

Still required before this becomes a working range-image protocol:

- A private actual-WAL apply receipt and recoverable certificate/abort proof that extend an existing native base through a contiguous certified range suffix, including reopen and uncertain-outcome cases.
- Actual target whole-range verification, including extra destination records, followed by an ordered native installed-base receipt and durable invalidation/recovery handling.
- Authenticated old/new data-quorum handoff, final metadata/configuration validation, and cleanup only after the destination/new quorum is ready.
- Positive versions of the retained 1,025-command compaction and late-ACK cleanup-first witnesses, plus relevant broad regressions. These are independent of an eventual strong-read API.

[Focused local evidence](evidence/range-image-native-primitives-2026-11-11/README.md) records the precise source manifest and checks. It contains no physical failover, throughput, production qualification, or deployment result.
