# Ordinary view definitions and joined scalar result types

Three general rules improve original Oracle schema loading and driver metadata.
ANSI CHARACTER VARYING and CHAR VARYING type phrases lower to their variable
character type without rewriting ordinary identifier/alias pairs. Fully typed
ordinary view definitions obtain metadata without executing a UNION query.
Scalar subqueries derive their type from their single projected column through
joined sources, independently of matching rows. No application, schema or table
identity selects these rules.

Independent actual database comparisons cover each rule:

| Fixture | Oracle baseline → current | PostgreSQL baseline → current |
| --- | --- | --- |
| [ANSI type spellings](../../../examples/oracle/reference/ansi_character_types.py) | [2/6](ansi-oracle-baseline.json) → [6/6](ansi-oracle-controls.json) | [6/6](ansi-postgres-baseline.json) → [6/6](ansi-postgres-controls.json) |
| [Ordinary UNION view definitions](../../../examples/oracle/reference/view_definition_metadata.py) | [3/6](union-oracle-baseline.json) → [6/6](union-oracle-controls.json) | [3/6](union-postgres-baseline.json) → [6/6](union-postgres-controls.json) |
| [Joined scalar subquery types](../../../examples/oracle/reference/joined_scalar_types.py) | [0/6](scalar-oracle-baseline.json) → [6/6](scalar-oracle-controls.json) | [0/6](scalar-postgres-baseline.json) → [6/6](scalar-postgres-controls.json) |

Each baseline uses the preceding implementation for its rule, with its binary
and fixture digests recorded separately. All final source expectations qualify;
nonce object cleanup reports no errors. These comparisons retain raw labels,
compare numeric transport families and preserve exact tested values. They do not
qualify every numeric width/type modifier, overlength cast or temporal precision.

The ordinary-view fixture creates a division-by-zero projection successfully,
describes it without evaluating it, and observes the error only during execution.
The scalar fixture checks numbers, timestamps and text, unmatched correlations,
empty descriptions, stored views and a fresh connection. SQL regressions cover
both profiles and database reopen. The ABI, routine compiler 26 and durable
metadata format 5 are retained. [Reader controls](reader-controls.json) pass
24 checks across new/prior/new readers in both profiles on disposable databases;
the original application database is never opened with the prior reader.

An initial [native source observation](initial-native-alias-source-observation.json)
qualified only five expectations: PostgreSQL rejects the implicit identifier/alias
phrase that Oracle accepts. Final native controls use its quoted explicit-AS
form, while Oracle controls retain the valid implicit form. The raw SQL forms are
recorded. The initial source failure is not a green compatibility comparison;
native rejection of that Oracle-only input remains unqualified.

The application-copy investigation first accepted 33 refreshed definitions and
matched all 160 result column descriptions. Rebuilding every definition in source
dependency order exposed a stronger [type baseline](application-scalar-type-baseline.json):
159/160 views matched transport families, with three numeric scalar-subquery
price fields incorrectly described as text. That failure is retained and led to
the general scalar result-typing correction above. Declared output names come from
source column metadata sorted by numeric position; original view query bodies are
not rewritten to satisfy the checks.

This qualifies bounded development behavior. View result values across the full
application dataset, updateable/materialized views, full view security, all
application workflows and the wider compatibility programme remain open. It is
not a published release or production deployment.

The final [full definition rebuild](application-view-definitions.json) accepts all
160 original ordinary views in dependency order and matches exact output names,
order and transport type families for **11,983 columns across 160/160 views**.
All 1104 Oracle DATE result columns advertise timestamp OID 1114, preserving
room for time-of-day; this is metadata qualification, not a dataset-wide value
comparison. Materialized view population continues through its existing path.

Final local validation passes **1,506 Rust test executions in 21 batches**, with
three existing ignores, and 58 Python tests. Nine preparation and 23 executed-join
controls retain matching results against each reference database on this binary.
See [Oracle preparation](describe-oracle-controls.json), [native preparation](describe-postgres-controls.json)
and the [validation record](validation.json). Cleanup reclaimed 4.39 GiB after the
initial batch and 4.69 GiB after the final batch; the task target remains about
7.5 GiB, with frozen binaries and reusable dependencies preserved.

The four owned reference fixtures now purge Oracle tables during cleanup.
[Cleanup](reference-cleanup.json) removed 31 previously dropped nonce tables from
Oracle recycling storage, preserving unrelated recycle-bin entries. Subsequent
paired comparisons use the corrected fixtures with identical before/after hashes.

A separate [application restart observation](application-restart-observation.json)
on the preceding merged binary reloads all 62 original routines and reads order
50000 as Completed with totals 123.50 through a fresh browser and independent SQL.
Its database RSS was about 3.04 GiB after 66.2 minutes, with no JDBC SQL exceptions
or column-decoding errors in the application log during that observation. The
[screenshot](application-completed.png) records the completed order. This does not
qualify sustained production memory behavior or the remaining business workflows.
