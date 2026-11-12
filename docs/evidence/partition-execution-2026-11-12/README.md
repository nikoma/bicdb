# Partition execution: evolving correctness batch

This is development evidence, not a release, completed partition implementation,
application acceptance result, or distributed sharding qualification. The branch
inherits source version 1.0.485-beta from its development base. Its eventual
delivery must integrate current main and receive a fresh unused source version.

The independent PostgreSQL 16.13 control uses only a new disposable schema and
role inside one rolled-back transaction. Its actual output establishes parent
read union, physical INSERT routing, child bound rejection (23514), DEFAULT
attachment/routing, and the accessed relation's grants and row policies. A
direct child query does not inherit the parent's RLS flag or policies. The
control does not claim byte-identical PostgreSQL files before and after a
transaction. See `native-pg16-reference/` and the primary PostgreSQL documentation:
[partitioning](https://www.postgresql.org/docs/16/ddl-partitioning.html) and
[row security](https://www.postgresql.org/docs/16/ddl-rowsecurity.html).

## Observed checks, scoped to their actual source

Each product test uses EmbeddedMemory and ServerPaged stores with fsync enabled,
independent acknowledged rows, and two clean reopens. The new tests retain
mandatory positive requirements. No original upstream application assertion or
timeout was changed.

| Source | Actual result | Scope |
| --- | --- | --- |
| `e86d6fc1f2f2afcbcf2f6db5622db02b89733561` | 0 pass, 7 fail | Baseline execution defects: parent reads/inserts/update/delete, child bounds, DEFAULT routing, parent/child grants and RLS. |
| `8bcd3c1fa502a674a8ea284a3ef2032bb3f61d5a` | 6 bounds pass; execution 1 pass, 6 fail | Typed RANGE/LIST/DEFAULT bounds, attachment preflight, existing default rows, direct writes, child policy independence. |
| `8a4bcfa818d4cf69df6007091c874fb4a91e088b` | 6 bounds pass; execution 3 pass, 5 fail | Parent scans now retain actual physical sources and apply the accessed parent's SQL policy. New query-shape test reaches a pre-existing bare `tableoid` projection failure. |
| `1d126acb226cce0c89ead6b0cbeb4d229d07ccd2` | 8 bounds/routing pass; execution 5 pass, 3 fail | Plain INSERT routes before staging writes, including INSERT SELECT, multirow refusal/rollback and an attached partitioned subtree. Remaining execution failures: UPDATE, DELETE and bare physical system columns. |
| `3267c029cbe7244a7fa6ca06deb1dc6bd9c9c8b6` | 8 bounds/routing pass; execution 6 pass, 2 fail | Bare system columns fixed; actual parent/child tableoid/xmin/ctid equality, aggregates and join shapes pass. UPDATE/DELETE remain failing. |
| `f19ddd769b708ae01503308fa00f68974dc89cf5` | 9 bounds/routing/delete pass; execution 7 pass, 1 fail | Physical DELETE, USING, RETURNING and rollback pass. UPDATE row movement remains a mandatory failure. |

The early `f9d2000d` submission lacked a remote Git prerequisite and ran no
tests. Its rebundled attempt and `6992ffcb` did not compile because the new test
harness consumed `BicDb` before a conditional reopen. `e86d6fc1` fixes fixture
ownership while preserving all seven assertions. These are harness failures,
separate from the seven subsequent actual product failures above.

## Required work before this batch is deliverable

- Complete UPDATE/DELETE across physical descendants with physical provenance,
  row movement, duplicate row IDs across leaves, transactional rollback and
  concurrency/conflict checks. Parent privileges and policies must remain the
  authority for access through the parent; Native context admission still
  applies to each physical store.
- Finish and qualify hash partition routing against PostgreSQL's seeded,
  type-specific hash semantics. The present bounds parser validates hash bound
  syntax/overlap, but row routing explicitly refuses it; metadata acceptance
  does not establish execution support.
- Qualify ON CONFLICT, leaf and inherited triggers, constraints and foreign
  keys, DDL transaction rollback, nested partitions, namespaces, ONLY, row locks,
  statement snapshots and all fast paths. Parent-only heaps/indexes/statistics
  cannot stand in for a partition tree.
- Prove upgrade/admission behavior on files created by a real old binary,
  including historical parent-heap rows, out-of-bound child rows and copied
  parent RLS state. Do not erase, silently hide or reinterpret acknowledged rows
  or independently authored policies. Resolve old-reader compatibility and
  index generation semantics before delivery.
- Complete the source-pinned broad SQL/protocol regressions and independent
  process/fault recovery checks after the implementation batch stabilizes.
  Oracle partition behavior remains a separate qualification path.

The broader campaign still requires its recovered durability/security,
authenticated portable rename, original iDempiere Oracle/PostgreSQL inventory,
UI400/encrypted backup/restore, HA and approved disabled experimental-track
qualification. Local SQL partitioning does not establish distributed placement,
automatic node rebalancing, or elastic capacity.
