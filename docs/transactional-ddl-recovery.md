# Transactional recycle moves and namespace recovery

Source candidate: **1.0.445-beta**. This is source qualification, not a published
release, deployment, or full iDempiere acceptance result.

An interrupted DROP could leave the original row backing under its recycle name
while schema and index names still referred to the original table. Startup now
recovers a durable before-image intent before primary-key index backfill. It
restores actual schema, grants, triggers, owned sequences, and index identities;
it does not guess that an arbitrary orphan collection may be removed.

## Decision and recovery order

The intent is durable before the first rename. Its removal is buffered in the
same core transaction as the user's row writes. A surviving intent means that
the move must be reversed; its absence after recovery means that the outer
transaction decided. FLASHBACK records a corresponding restore intent. Recovery
reverses these operations in descending native transaction order and tolerates
repeated recovery.

A transactional DROP followed by CREATE under the same name requires a second
proof. Before allocating the replacement, SQL records a fresh staging backing
identity. The backing is created under that identity and then renamed. Recovery
may remove only the recorded physical backing, before reversing the earlier
DROP. A different backing with the same logical name is an ambiguity error and
is preserved. The creation decision shares the outer row commit. Repeated
DROP/CREATE cycles use the same reverse chronological recovery order.

The `sql_catalog_intents_v1` compatibility flag is persisted before journaled DDL
and remains set after completion. Older binaries that do not understand this
flag must refuse the store. Core storage recovery precedes SQL catalog recovery;
an embedding that uses SQL must run the SQL startup repair before serving it.
Repair requires a healthy database without pending transactions or row locks.

## Transactional rename and purge

Rebinding a transaction's pending writes requires its own pending transaction
from the same database instance. Another pending transaction or retained row lock
causes a retryable conflict before the catalog changes. The supplied transaction's
row locks move with the logical name, preserving acquisition order and savepoint
suffixes; rollback and commit release the renamed identities.

For supported, nonpartitioned tables, a transactional PURGE or recycling-disabled
drop retains the exact backing until the outer decision. Reconstructing rows with
fresh versions on rollback would hide them from the transaction's retained
snapshot. The purge marker is instead written in that outer transaction. A durable
marker hides the entry from FLASHBACK and permits physical cleanup. Exclusive SQL
COMMIT performs cleanup; a shared COMMIT retains the hidden marker for the next
exclusive cleanup or startup. If interrupted, startup completes the marker. Cleanup
failure after the durable decision must not run DDL undo against committed work.
The native server COMMIT path invokes decided-purge cleanup after durability and
quorum finalization, releasing its transaction before taking the catalog lease.
This also fixes rollback of DROP SCHEMA public CASCADE for these tables.

Built-in SQL hosts perform startup repair before exposing queries: the server
before index backfill, CLI before queries/analytics or migration application, the
standalone application host before initialization, RESP before HotView startup,
and the WebAssembly ABI before publishing a database handle. Embedders supplied
with an already-open database retain the startup-repair obligation above.

## Validation and remaining gates

Focused source `1fb2a302`: all 14 namespace and recovery integration tests pass,
including the unchanged namespace savepoint assertion, both storage modes, and
recycling-disabled purge rollback. Source `e3455a60`: all seven journal test
functions pass, including 106 process-cut cases across both storage modes,
repeated reopen, real grants/triggers/owned sequences, replacement publication,
repeated name reuse, and the outer purge decision. Conflicting physical backing
identities are preserved. The original failing results remain in the local
qualification evidence, rather than being replaced by weaker assertions.

The full 224-target SQL run at versioned source `4663f20f` completed with 2,297 passed, 36 failed and four existing
ignores across all 224 targets. All 36 failures came from unconditional exclusive
purge cleanup on shared COMMIT; its library alone had 807 passes and 11 failures.
The correction restricts physical reclamation to exclusive sessions;
the full original failing result is preserved. A new shared-COMMIT
regression checks durable visibility, hidden purge entries and startup cleanup.
Corrected source `5e3a9454` passed 959 tests across the full SQL library and nine
integration targets, with zero failures and four existing ignores. Full SQL at
`c53ec0a9` passed 2,334 tests across 224 Cargo targets, with zero failures and four
existing ignores. Core source `61a6a13b` passed 1,271 tests, zero failures and ten
existing helper/measurement/external-model ignores across 119 Cargo/doc targets
plus one invoked subprocess result. Fresh crash helpers and locked workspace
all-target compilation passed at that source. Core code and dependencies are
unchanged by the subsequent frontend integration.

These broad results precede the final frontend additions. Source `8558ab3b`
reproduced missing native-COMMIT cleanup and missing CLI query/migration and host
startup repair. Source `6e306a65` reproduced the RESP and native ABI startup gaps.
All six frontend regressions pass after the corrections (`a72a3191` for server,
CLI and host plus the fenced cleanup API; `bf538d4e` for RESP and the native ABI).
The complete frontend package regressions and updated workspace compilation are
separate gates. Final source-scoped results and artifacts are tracked in
[PR126](https://github.com/nikoma/bicdb/pull/126). Broad regression, final head CI,
remote merge and final-artifact qualification remain distinct; earlier scoped
results do not imply all later gates. This version follows `1.0.444-beta`.


The prior actual VM-cut recovery proof remains pinned to `823f9726`; it is not a
test of this integrated candidate. Qualification requires a rebuilt final artifact and a VM campaign against its
exact source; the prior proof does not establish these gates. Generic transactional catalog operations outside these
journaled moves, partitioned-table physical-drop compensation, replicated or
distributed catalog changes, and complete application acceptance retain their
separate requirements. This document makes no universal transactional-DDL or
Oracle-equivalence claim.
