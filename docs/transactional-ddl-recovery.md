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
marker hides the entry from FLASHBACK and permits physical cleanup. Ordinary SQL
COMMIT performs cleanup; if interrupted, startup completes the marker. Cleanup
failure after the durable decision must not run DDL undo against committed work.
This also fixes rollback of DROP SCHEMA public CASCADE for these tables.

## Validation and remaining gates

Focused source `1fb2a302`: all 14 namespace and recovery integration tests pass,
including the unchanged namespace savepoint assertion, both storage modes, and
recycling-disabled purge rollback. Source `e3455a60`: all seven journal test
functions pass, including 106 process-cut cases across both storage modes,
repeated reopen, real grants/triggers/owned sequences, replacement publication,
repeated name reuse, and the outer purge decision. Conflicting physical backing
identities are preserved. The original failing results remain in the local
qualification evidence, rather than being replaced by weaker assertions.

The versioned source `4663f20f` starts full SQL qualification across 224 Cargo
targets. Its broad result, workspace compilation, final head CI and remote merge
are separate gates; focused results alone do not establish them. The merge order
reserves this version after predecessor `1.0.444-beta`.

The prior actual VM-cut recovery proof remains pinned to `823f9726`; it is not a
test of this integrated candidate. A rebuilt final artifact and VM qualification
are still required. Generic transactional catalog operations outside these
journaled moves, partitioned-table physical-drop compensation, replicated or
distributed catalog changes, and complete application acceptance retain their
separate requirements. This document makes no universal transactional-DDL or
Oracle-equivalence claim.
