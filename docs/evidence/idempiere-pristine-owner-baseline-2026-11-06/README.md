# Pristine relation-owner baseline and original application result

A cold clone of the saved-source pristine database was mapped to a dedicated
non-superuser application role using the public relation-owner migration phase.
The namespace, 915 tables and 160 views have the mapped owner. All phases and the
final clean database shutdown exited zero. The engine and tool source is
`51cfb588721113d828e787be0648669b5e441c64`.

Following owner mapping, qualification repeated:

- Saved-source value equality across 915 tables and 185,471 rows.
- All 3,826 foreign-key declarations and 888 affected-table value fingerprints.
- Orphan counts for all 3,826 relationships, all zero and equal to the retained
  Oracle reference replay.
- Labels and broad type families for all 160 views, without executing view rows.
- Preservation of stored definitions, allowing only the intended relation-owner
  change, and one nonempty source table count through the application role.

The subsequent unchanged original foundation batch ran all 89 selected tests on
another disposable clone: **40 passed, 42 errored, 7 failed, none skipped**. Of
those errors, 27 directly reported that the lowered `IS JSON` constraint helper
required superuser privileges. Execution of imported routines was also denied to
the application role. Business assertions that failed alongside these errors
remain unresolved; their causes require retesting after permission corrections.
The test JVM exited 255; its database shut down cleanly with exit zero. The
failed database and original reports were retained for diagnosis.

This checkpoint qualifies the relation-mapped saved-source baseline and records
an unsuccessful application run. It does not qualify routine ownership or
execution, authentication, exact view descriptors/values, complete application
behavior, recovery, live change capture, or production operation. Importing 62
routine definitions does not establish execution compatibility.

[summary.json](summary.json) contains sanitized counts, source/binary identity
and hashes of retained private reports. It omits application rows, credentials,
endpoints, and private paths. Hashes identify evidence; they are not an
independently reproducible substitute for the retained reports.
