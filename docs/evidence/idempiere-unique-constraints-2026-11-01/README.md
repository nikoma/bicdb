# Oracle preview UNIQUE constraints

The preview implements Oracle's immediate nullable UNIQUE rules in ordinary
column/table constraints. Equal partially NULL composite keys conflict; wholly
NULL keys may repeat. Explicit table `NULLS DISTINCT`/`NULLS NOT DISTINCT` clauses
retain their named policy. The default PostgreSQL profile keeps its usual NULL
semantics. A durable database profile, rather than a session GUC, selects the
Oracle default.

A distinct schema wire tag records the extended policy. Existing constraints
keep their prior semantics until explicitly replaced. Older SQL schema readers
reject the new tag. Native NULL-aware indexes retain the existing capability
fence; this is not a qualified cluster upgrade/downgrade procedure.

Tests cover both resident and server-paged stores, reopening, exact numeric
keys, NULL versus empty native text, UPDATE rollback, ON CONFLICT, validation of
existing rows and inline UNIQUE on ADD COLUMN. ALTER TABLE cannot reuse an index
with a different NULL policy. Constraint backing indexes now have catalog entries
and a `pg_constraint.conindid` link. The standard `nulls_distinct` field is NULL
for Oracle's hybrid all-nullable composite rule, and NO when a non-NULL anchor
makes the all-NULL exception impossible.

The driver regression also covers failed COPY statement rollback and subsequent
connection reuse. It exposed quote loss in COPY without a column list; metadata
discovery/export now retains the SQL relation spelling, while native storage
uses normalized identifiers. Quoted mixed-case table/column COPY is checked in
both profiles. This does not qualify arbitrary punctuation in COPY identifiers
or all COPY variants.

The importer probes actual ordinary UNIQUE behavior and checks nullable
composite constraints against their executable backing indexes. Resume, data
and verification phases reject older weaker policies instead of accepting table
and row counts as schema equivalence.

Deferrable UNIQUE and foreign keys remain separate requirements. Inline column
NULLS clauses, general ALTER COLUMN transitions affecting index NULL policies,
complete Oracle error equivalence and complete application acceptance are not
qualified by this batch.

Nineteen synthetic insertion outcomes matched Oracle Free 23.26.3.0.0 through the
PostgreSQL driver against an isolated rebuilt preview server: see
[comparison](comparison.json). The earlier development binary disagreed in four
cases ([negative control](before-comparison.json)). Cases include both-null keys,
partial-null duplicates in either position, numeric spelling, a non-NULL anchor,
single-column NULLs, non-NULL duplicates and Oracle empty text.

A single-column constraint fixture also isolates the SQL schema guard from the
core NULL-aware index capability: the new runtime creates it, while the older
runtime exits with an unknown schema wire tag. The
[older-reader check](old-reader-check.json) used an isolated database and never
opened the application database with the older reader.

The rebuilt server also passed an [index collision check](index-guard.json): an
existing weaker native index is rejected before the UNIQUE constraint appears
in the catalog. Failed DDL leaves no recorded constraint.

Live application-schema verification additionally exposed a pre-existing
`pg_constraint.connamespace` constant pointing to public
([negative control](namespace-negative-control.json)). All table constraint
rows now use their owning schema's namespace OID. Regression cases include
primary, unique, foreign-key, check and NOT NULL rows in a non-public schema,
under PostgreSQL and preview profiles. The importer retains the namespace join;
it does not bypass this catalog error to approve an incomplete verification.

The release-13 development application fixture now has all three source nullable
composite UNIQUE constraints replaced and verified:
`AD_REPLICATIONSTRATEGY_VALUE`, `AD_TABLEATTRIBUTE_ATTRIBUTEKEY` and
`U_POSTERMINAL_VALUE`. Their tables contain two, zero and one rows respectively;
complete row-value digests matched the Oracle source snapshot before and after
replacement. See the [upgrade report](application-upgrade.json).
A [read-only restart check](application-reopen.json) confirmed the persisted
policies, valid/ready native indexes, matching catalog namespaces and source
values without repairing or reinstalling constraints. Final synthetic comparison
also ran against this reopened application fixture.

Validation passed: 345 SQL unit tests (three ignored), all 416 main SQL integration
tests, 117 pgwire unit tests, thirteen Oracle-profile driver tests, 167 protocol
tests and fourteen migration-tool tests. Formatting, licensing and open-core
boundary checks passed. The development example server was rebuilt and restarted;
no release was published and no production deployment was performed.

This does not qualify the complete Oracle schema or application. The 3,826 source
foreign keys all require deferred behavior; their validation state, remaining
stored programs/views and full business workflows still need implementation and
acceptance.
