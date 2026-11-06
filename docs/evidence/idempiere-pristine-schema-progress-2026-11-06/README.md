# Original-schema index and routine installation checkpoint

The pristine original Oracle-schema target now passes the complete logical-index
phase: 268/268 installed indexes, with every saved catalog definition revalidated
and executable status checked. The former missing index on TRUNC(DATEACCT) is
backed by the general timestamp day transform, without application-name handling
in the engine. The source's physical LOB indexes remain explicitly represented
by native LOB-storage adaptations rather than an identical physical layout.

All 62 exported standalone routines also pass the public installation phase and
catalog checks. The installer consumes the exported bodies without application
workarounds and qualifies their declaration namespaces. DDL acceptance and catalog
stability do not establish complete routine execution or Oracle owner privileges;
the actual installation SQL user remains `bicdb`.

The [record](validation.json) identifies the delivered main source and freshly
built binary, tool revision, manifest, completed phase report hashes, 154 passing
native-tool tests with three skips, and independently satisfied Oracle/PostgreSQL
day-index controls. Both source CLIs pass 3/3. The older-target negative control
returns failure with its source still qualified, and an older default-profile
binary refuses the new index catalog. All fixture objects, temporary databases
and source containers are cleaned up or restored. The inherited full regression
and later focused integration retain their exact source identities in the
[day-index checkpoint](../sql-timestamp-day-indexes-2026-11-06/README.md).

This target started from the [pristine saved-export values](../idempiere-pristine-source-values-2026-11-06/README.md):
915 tables and 185,471 rows. Temporal, character and BLOB type reconciliation
preceded this resume. Full value equality must be repeated after all schema phases;
the earlier data result is not promoted into post-assembly validation.

Source-ordered views are being installed in the same guarded run. Their final
result, all 3,826 foreign keys and orphan comparisons, view metadata/values,
application tests and recovery remain separate gates. No full-schema, whole
application, production or complete Oracle compatibility qualification is claimed.
The accepted application deployment has not been upgraded by this checkpoint.
