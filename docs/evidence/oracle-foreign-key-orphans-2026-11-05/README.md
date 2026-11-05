# Read-only legacy foreign-key orphan inventory

The [operator guide](../../oracle-foreign-key-orphan-inventory.md) describes the
public command, completed migration prerequisite, operator-asserted quiescence,
per-relationship counts and separate qualification flags. This checkpoint adds
no engine behavior selected by an application or schema name.

The [validation record](validation-results.json) reports all 26 Python tests
passed with no skips: 13 migration tests and 13 inventory tests. Native inventory
cases check reporting without repair, zero-orphan results after a separate data
change and migration rerun, changed rows/projections, missing relationships,
disabled enforcement, partial failures and refusal to certify a validated-orphan
anomaly. Planning checks protect journal identity/completeness, quoted key order,
NULL filtering, invalid count results, separate output paths and replacement of
a stale success report before connection access.

Independent query comparisons pass **9/9 Oracle and 9/9 PostgreSQL controls**.
They require explicit expected counts for duplicate unmatched rows, partial NULL
composite keys, crossed column order, empty parents/children, nullable parents,
self references, fixed-character padding and empty character values. Oracle and
PostgreSQL empty-string expectations differ deliberately. Equal failures cannot
count as matching behavior. The same public query builder runs on the native
candidate and the independent references.

The first reference fixture incorrectly used embedded double quotes in table
names, which Oracle rejected during setup. Final shared fixtures use quoted
names containing dots and spaces; embedded-quote escaping remains covered by
PostgreSQL/native tests. Oracle documents its restriction as
[ORA-25716](https://docs.oracle.com/en/error-help/db/ora-25716/).
All final fixtures passed and their tables were removed. Native comparison
servers stopped cleanly and the disposable PostgreSQL container was removed.

The native engine binary is the separately validated constraint-catalog build;
its source commit and digest are recorded separately from the Python tool's
source. No engine rebuild, GitHub workflow modification or manual dispatch was
needed for these Python/documentation changes.

This checkpoint does not count the full application's relationships as
inventoried. Its schema migration runs separately. Source orphan-count equality,
legacy-row policy, online consistency, full application acceptance and production
qualification remain uncompleted gates. The inventory output keeps
`orphan_inventory_qualified`, `data_compared_to_source` and
`application_qualified` false even when its bounded counting task succeeds.
