# Paged original-schema baseline qualification

The original Oracle-schema baseline now passes the storage-mode qualification
on `server_paged`: all **915 tables and 185,471 source rows** match their exported
values; all **3,826 foreign-key declarations and orphan counts** match the source,
with zero orphan rows. Relation and routine owners, stored definitions and the
160 view projections with 11,983 column labels and broad type families survive
the offline copy. Missing owners or relationships were not repaired to pass.

Before SQL probes, both closed native integrity checks passed and readback
confirmed 923 native collections, 188,817 native records and 2,087 valid index
inventories. The protected original's file contents were independently rehashed
unchanged after qualification. The source and target native manifests, exact
artifact identities and phase-report hashes are captured in the
[provenance record](summary.json).

The copy CLI uses source `e5babd1`; the server uses the independently qualified
`a97545f` backend. These are distinct artifacts and revisions. The owning-mode
source was a cold clone. Original-source rows, definitions and owners were
proved before operational grants or capability probes. Fingerprints before and
after relationship checks confirm unchanged target application data.

An explicit monitoring SELECT grant was then added to the target. The original
eight-column free-space aggregate executed as the mapped non-superuser role.
It returned no rows, consistent with an empty free-extent inventory; this is not
a nonzero fragmentation workload benchmark. Authentication, TLS and broader
security qualification are separate gates. The database exited zero and its
process was confirmed stopped.

This establishes the copied schema/data baseline on these revisions. It does
not establish all routine execution, full view values, exact view descriptor
precision/scale/width/nullability, complete iDempiere tests or UI workflows,
production operation, distributed recovery or the full Oracle programme.
Earlier resource-interrupted integrity attempts and all original test failures
remain retained evidence. The protected original and qualified paged target
remain available for subsequent acceptance.
