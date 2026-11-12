# Native range transaction effects

Status: source implementation and proposed controls, not yet compiled or executed.
The source basis is unmerged candidate `05819560209227b233e13756e83ee5cd775fa132`;
subsequent working-branch changes add the refusal described below. This is not a
production qualification or a claim about published main.

The native range command currently carries record mutations. A local transaction
can also buffer broker publications, while record auditing, sync outboxes and
memory indexing have additional local publication paths. Row preservation alone
does not establish preservation of those effects after failover or relocation.

Source inspection found that the native writer preflight returned immediately
for an empty record-write list. A broker-only transaction could therefore avoid
range admission even on a newly enrolled database without a writer policy.
This acceptance path has not been independently executed as a loss experiment.

The working branch refuses explicit broker effects before range Prepare and
application WAL, for both broker-only and mixed record/broker transactions.
The proposed regressions check the actual WAL, catalogue, row, queue and native
prefix, and separately retain the ordinary local transactional-outbox contract.
Their status is **not tested** until execution evidence is recorded.

This refusal is an interim safety boundary. It does not implement replicated
broker transactions and does not satisfy the broader transaction-effects gate.
Automatic record-audit events, CDC/sync outboxes, memory jobs, queue settlement,
and replay/relocation of their receipts still need explicit protocol coverage
and independent reconciliation. No audit or sync setting is disabled by this
change, and no complete-effects guarantee follows from an acknowledged row.

Relevant sources are `db/range_image_native/writer_enrollment.rs`,
`db/spatial_paged.rs`, `db/commit_snapshot.rs`,
`db/range_image_native/certified_prefix.rs`, and
`db/range_image_native/writer_enrollment/tests.rs` under
`crates/bicdb-core/src/`.
