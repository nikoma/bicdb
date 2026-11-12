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
| `e0dafabf590d66c1b8e4f6e78b3e7c0847c5f4b6` | 0 pass, 1 mandatory fail | New atomicity check against unchanged f19 runtime observes two physical commits for one parent INSERT. Earlier preflight/rollback tests did not prove cross-heap autocommit atomicity. |
| `751ae3d25195efe2013d8387d5c6a59cbd424150` | 21 pass | Statement transaction guard plus typed UPDATE movement; PK/FROM/RETURNING/rollback, destination conflict and parent RLS controls. |
| `030ff8c91c01f2810abd2f90fdaa5a9f88a0ec35` | 25 pass | Adds scalar hash routing checked against 106 actual PostgreSQL physical placements plus existing text-hash regressions. |
| `d9ff5aea602a0629e72a6b4248fa6c0dc6535767` | 0 pass, 1 mandatory fail | Actual 030 runtime plus new real-outer-heap EXISTS test: empty parent index incorrectly yields no rows. Temporary Git-index construction preserves production source exactly; see proof-source.json. |
| `c7f6ee724866c72ec87d8e2ffbad81f5d7f9945a` | 26 pass | EXISTS falls back from parent-only candidate indexes; refreshed physical UPDATE/DELETE images recheck accessed-parent policies. |
| `989b89f4f301c2763e00ee59a8f9f83cbac0fec7` | 0 pass, 1 mandatory fail | Actual c7 runtime with new parent/child locking contract: parent FOR UPDATE omits the acknowledged child row. |
| `22a347869620d46519c0665926fa1a4cc644eee2` | 42 pass | Physical parent/child locks for PK and hidden heap identities, plus partition execution/hash/bounds and independent existing lock/system-type/Oracle-partition/authorization regressions. |

The early `f9d2000d` submission lacked a remote Git prerequisite and ran no
tests. Its rebundled attempt and `6992ffcb` did not compile because the new test
harness consumed `BicDb` before a conditional reopen. `e86d6fc1` fixes fixture
ownership while preserving all seven assertions. These are harness failures,
separate from the seven subsequent actual product failures above.

## Required work before this batch is deliverable

- Qualify UPDATE/DELETE physical provenance, row movement and rollback beyond
  the passing focused controls, including duplicate row IDs across leaves,
  independent process/fault recovery, and concurrency/conflict checks. Parent privileges and policies must remain the
  authority for access through the parent; Native context admission still
  applies to each physical store.
- Scalar hash routing now uses PostgreSQL's seeded type-specific semantics
  for integer, OID, boolean, deterministic text and floating types. Actual
  PostgreSQL 16.13 INSERT/tableoid fixtures cover 106 integer/text/float/composite
  placements including NULLs, signed boundaries, UTF-8, NaN and infinities.
  Numeric/temporal/array/UDT and custom hash operator-class support remains
  incomplete and explicitly refuses unsupported types; metadata acceptance
  alone does not establish their execution support. Complete those required
  semantics and qualify hash-key movement and existing-store upgrades.
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

The physical row-lock follow-up at `22a347869620d46519c0665926fa1a4cc644eee2` passed 42 focused tests (0 failed/ignored): 1 partition-lock contract, 1 hash contract covering 106 native PostgreSQL placements, 13 bounds/routing/update controls, 9 execution/query-shape controls, 6 existing row-lock tests, 3 existing transaction-system-type tests, 4 existing Oracle-partition tests and 5 existing raw-DDL authorization tests. It preserves physical heap identities and resolves source tableoids only inside the admitted partition tree. This is still not the full batch's broad/application/fault qualification.


## Upsert and inherited trigger follow-up

These are focused development results, before integration with a fresh main
version. They do not qualify the whole partition feature or an application.

| Source | Actual result | Scope |
| --- | --- | --- |
| `8d6faa0f01530ec5ee1b9c3700b10308f0f7e0b2` | 0 pass, 5 fail | Mandatory parent upsert positives against the preceding runtime; physical duplicates, policy enforcement and movement refusals. |
| `2f034a22f935ee9551e4255453b09f0589d5be9b` | 66 pass, 0 fail/ignored | Physical leaf arbiters and Native reservations; accessed-parent SQL policy; same-leaf typed identity changes, cardinality refusal and independent Oracle regressions. |
| `eb7bcac0201ab33f575f649c20977a79f94d58cc` | 0 pass, 4 fail | Mandatory inherited trigger catalog and rollback positives against unchanged upsert runtime. |
| `747a76edfe7afd2794096b7a86eb2a9170058e69` | 38 pass, 0 fail/ignored | Persisted immediate-parent trigger lineage, pg_trigger.tgparentid, CREATE/ATTACH/DETACH propagation, dependency/mode controls and atomic Native catalog publication with retryable batch undo. |
| `005bc04f48bcff4abe41b882db34a1ce0a0594cc` | 0 pass, 3 fail | Actual preceding runtime reports parent rather than physical trigger context, omits movement DELETE/INSERT events and accepts a forbidden leaf BEFORE INSERT reroute. |
| `a15eb0af82cc158cef614939c44b04ec413b8d1a` | 80 pass, 0 fail/ignored | 17 focused targets: physical row trigger execution/event order, partition execution/upsert/bounds/locks/hash, existing trigger authorization/rename/deferred rollback and read-committed upsert/RLS controls; independent Oracle partition checks. |
| `a7bb3593349534280a65ee7e0fbd8cf195532a47` | 4 pass, 0 fail/ignored | Test-only addition over identical a15 product source: actual PostgreSQL mixed-upsert BEFORE interleaving and queued AFTER events, both storage modes and two reopens. This repeats the preceding three event tests and adds one; it is not 84 distinct tests. |

The native reference contains actual PostgreSQL 16.13 results: fourteen events
for insert/direct-child insert/movement/delete/upsert, four multirow insert
events, and eight mixed upsert events. The original reference container stopped
after ENOSPC and was preserved. Follow-up controls ran in bounded, isolated
temporary containers with an ephemeral tmpfs data directory and were removed
after the checks. They prove event semantics, not physical durability. Every
control creates its own scratch schema inside a rolled-back transaction.

Execution routes before leaf BEFORE INSERT triggers; a trigger cannot redirect
the admitted row to another leaf. Parent UPDATE invokes source UPDATE triggers
and physical DELETE/INSERT triggers for movement, with parent SQL RLS retained.
AFTER events preserve input row order. Scoped trigger identity reconstruction
does not relax the unrelated ordinary UPDATE primary-key guard.

Trigger suppression and trigger-side query visibility during movement still
need complete native controls. Constraint-trigger admission, function cascade,
quoted/namespace identity, rename integration, old-reader admission and
independent crash/fault qualification remain required. Passing catalog tests
alone never established correct event execution. Broader partition and campaign
gates listed above remain open.
