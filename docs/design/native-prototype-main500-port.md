# Registered native prototype integration with main500

This source composition starts from main `a06e09336c70fecbc38232d677c330984959388f`
(`1.0.500-beta`) and merges the frozen native prototype
`81733c1fa601fea3bcac1ad5efac8435dd02b090`. Their common ancestor is
`a1936a6e34f71dd2c7866425017ab1b5147695de`. It excludes the separate hot-joint
publication-version-gate work and the optional later diagnostic fixture changes.
The source version remains main500 until the final integrated change is reviewed,
validated and versioned for delivery.

This is a source integration, not a runtime qualification result. The earlier
prototype's evidence remains pinned to its original revisions. Passing main500
checks do not validate this combined source. No build, new binary, hosted test,
release, deployment or Azure experiment was produced during this port.

## Preserved main behavior

The three-way merge has twelve paths changed by both branches. All 1,690 paths
changed only by main since the common ancestor remain identical to main500.
Conflicts in `db.rs` and `format.rs` were resolved by retaining both module lists
and both sets of supported feature flags. The following main behavior is retained:

| Main contract | Source retained in the composition |
| --- | --- |
| SQL/native catalog admission, allocation and namespace identity | `db/native_origin.rs`, `native_import_origins.rs`, `native_catalog_allocations.rs`, `native_collection_birth.rs`, `native_wal_backings.rs`, and the main SQL implementation remain present. |
| Completed import mapping follows recovered WAL durability | In `db.rs::open_inner`, the shared page/log durability health is initialized early. `repair_pending` modifies a separate candidate catalog; `copy_transaction_log_tail(..., true)` completes before publishing that catalog with the shared fail-closed health. The old late catalog publication was not restored. |
| Physical root custody | Native factory creation retains its actual fresh-directory fact and activates origin-root, namespace, collection-birth, write-time WAL-backing and catalog-allocation contracts. `ensure_writable` still checks the actual origin. A copied stopped root needs the official `authorize_native_cold_restore` / `backup adopt-copy` workflow; a raw forensic copy is not opened or authorized by this port. |
| Historical allocation and reader facts | The actual main commit encoder receives `tx.native_catalog_allocations`; native WAL capture and checksummed reader requirements remain the main implementation. Remote manifests or decoded DATA cannot manufacture in-process `NativeWalWriteCapture`. |
| Routing latch safety | `require_for_coordinator_with_topology`, `require_native_writer_host_context_with_topology`, and their hot-recovery callers retain use of the already-held topology guard. Main recursive-read fixes remain. |
| Native command contention | `certified_prefix.rs` retains the bounded 250 ms native-journal acquisition at prepare, apply and certification. Coordinator voter fan-out, stall tracking and bounded busy retry remain. |
| Recovery and worker custody | Main metadata-supervisor/hot-recovery contention, debt and pacing fixes remain. `ParticipantPool` retains and drains workers on drop, with the existing 250 ms bounded wait. |
| Test environment | Main Core dependency optimization and separate serial native suites remain unchanged. These environment changes do not establish the cause of an older prototype timeout. |

The imported Registered private row path keeps its actual admission/hook/journal
scope, retained original transaction, pre-WAL intent and same-open retention pin.
Its inner WAL-durable outcome remains distinct from applied-known visibility.
The main commit encoder still captures the target's actual native backing facts.
The restrictive pending-row startup check runs after actual page recovery and
before generic logical replay or cutting, in addition to main's import-origin
recovery ordering.

Main added `RangeSnapshotBatch.required_reader_features`. The Registered importer
now explicitly requires each batch's reader vector to equal the preinstalled
reader contract already pinned by its manifest and actual target bindings. It
refuses a mismatch before buffering a row transaction; it does not mutate format
flags while a frozen image is being copied. The existing common snapshot field
validator also checks supported reader requirements. All four imported local
Registered/writer-DATA feature flags are recognized locally and remain excluded
from the unchanged main portable reader allowlist.

## Review and validation boundary

Port checks comprise Rust formatting/parser validation, `git diff --check`, the
29-package source-version consistency check, and static comparison of the
main-only paths and the explicit contracts above. These checks are not Rust type
checking or runtime execution. The old prototype and other agents' unmerged work
are preserved.

Before delivery, the final integrated head needs compile/type checks and the
existing actual Registered proposal, Installing, history/rows, final-image,
native-base, catalogue and cold-preservation controls. Revalidate the actual
same-open proven-abort retry and committed/unknown refusal branches; source
composition alone does not demonstrate their behavior. Main origin/import/WAL
ordering, reader-contract, SQL regressions and native routing/recovery suites
must also run against that final head. A failure remains evidence rather than a
reason to extend authority deadlines or remove an application assertion.

Further product gates remain separate: the real node-process controller and
phase-RPC attachment, complete joint activation/Open/release and ordinary writer
admission, a populated second-range move after first-range Serving, restart/cold
recovery, bounded independent-range progress during stalled I/O, and a new
isolated four-to-five-machine experiment with external-ledger reconciliation.
General PostgreSQL cross-range SQL/transactions, a stable post-convergence
throughput increase, production RPO/RTO, and production qualification are not
established by this integration. The original Azure scale-out result remains
failed.
