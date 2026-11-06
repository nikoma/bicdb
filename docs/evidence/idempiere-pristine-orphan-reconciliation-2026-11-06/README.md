# Pristine saved-source orphan reconciliation

All **3,826 source-declared foreign-key relationships** have matching unmatched-row
counts on the pristine BicDB target and the retained Oracle reference replay.
Every count is zero. The inventory rechecked the complete catalog and all **888
affected table fingerprints** before and after counting; all fingerprints match
the completed FK migration baseline. No repair or validation-state change was
performed. [summary.json](summary.json) pins the source snapshot, reports, tools
and native development executable without publishing rows or connection details.

The same completed run repeated source-value equality for **915 tables and
185,471 rows**, then checked **160 view projections and 11,983 column labels**
against their saved source labels and broad type families. Value verification,
orphan inventory, source-count comparison and view metadata each exited zero.
The database then stopped cleanly with exit zero.

The inventory resumed a retained 128-relationship prefix after an audited clean
resource stop. Before reusing counts, the public tool authenticated its snapshot,
endpoint and completed migration identity, checked exact ordered declarations,
and repeated the complete current catalog and row baseline. It counted the
remaining relationships and repeated the final checks. An independent offline
rerun of the strict comparison accepted the completed source and target reports.
Operator-asserted quiescence remains required; the tool does not acquire a writer
lease or protect against concurrent hostile changes.

The executable source is `5e5fecc3ca279f2e3ea3e37af0c244e11bbb69ea`; tools were
frozen from `285072504e2cc3563da6bdd548eb32294cde0397`. This is the local
compatibility example server, not a qualification of the production CLI or its
allocator. Use the [orphan inventory guide](../../oracle-foreign-key-orphan-inventory.md)
with a quiesced disposable target and retained migration journal to reproduce.

This closes the saved-source orphan-count gate on the pristine schema/data
baseline. It is separate from the earlier [FK installation](../idempiere-pristine-foreign-keys-2026-11-06/README.md),
[source-value reconciliation](../idempiere-post-fk-source-values-2026-11-06/README.md)
and [view metadata](../idempiere-pristine-view-metadata-2026-11-06/README.md)
evidence. The underlying snapshot/reference replay is not a continuously current
Oracle database or a change-capture position. Broad view type families do not
establish exact precision, scale, width, nullability or view result values.
Routine execution and ownership, all original application tests/workflows,
cutover/recovery and production qualification remain open. The accepted running
application was not upgraded by these checks.
