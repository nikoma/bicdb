# Dictionary contracts and logical column catalogs

This milestone fixes a shared catalog bug: `information_schema.columns` now
reports the logical schema and relation name for schema-isolated tables and
views. Schema predicates are pushed down against the real namespace rather than
`public`. A regression covers identical table names in two schemas, view
columns, filtering and reopen. Native PostgreSQL controls exercise the same
introspection through the PostgreSQL driver.

The Oracle dictionary comparison is deliberately **failing** and is separate
from the catalog fix. USER_TABLES and USER_TAB_COLUMNS are not implemented by
this change. The public fixture creates its own table and view in dedicated
schemas, observes Oracle dictionary rows, and records unsupported target queries:

```sh
python examples/oracle/reference/dictionary_contracts.py \
  --oracle-config /private/reference.json --output /private/dictionary.json
```

Set BICDB_PG_DSN privately to the dedicated Oracle-preview target. The current
fixture returns a nonzero exit status because its target contracts are not met.
It checks table/view inclusion, column order, scalar type spelling, precision,
scale, nullability, defaults and byte/character metadata. It also records two
supplementary-character assignment cases separately from dictionary totals.
All fixture-created objects are removed; no application routines are executed.

Under the qualified Oracle AL32UTF8/BYTE environment, VARCHAR2(3) has DATA_LENGTH
3 and CHAR_USED B; VARCHAR2(3 CHAR) has DATA_LENGTH 12 and CHAR_USED C. Oracle
rejects a four-byte emoji in the former and accepts it in the latter. The target
currently persists both as the same VARCHAR typmod and accepts both assignments.
That existing mismatch is retained explicitly, not counted as a passing case.
Generating Oracle byte lengths from the current native typmod would be wrong.

The target's native catalog observations are diagnostic evidence, not Oracle
metadata matches. Before the catalog correction, a logical table-name filter
returned no information_schema rows; pg_attribute still exposed the physical
VARCHAR descriptors. The correction restores logical introspection but does not
recover lost Oracle length-unit provenance or enforce byte capacity.

The separately recorded source descriptor inventory and namespace observation
are implementation inputs. Changing CURRENT_SCHEMA to SYS did not make the
source USER_TABLES expose SYS-owned DUAL. Dictionary visibility must follow the
correct effective Oracle user, preserve local-object/synonym resolution, and
honor dictionary access rules. Source dictionary widths, exact defaults and
logical type provenance must not be invented from native storage types.

For native owner/admin controls, set ORACLE_CONTROL_PG_DSN to PostgreSQL and
BICDB_PG_DSN to a dedicated native target. Provision matching catalog_a/catalog_b
schemas on both databases before running:

```sh
python examples/oracle/reference/postgres_catalog_columns.py \
  --schemas catalog_a catalog_b --output /private/native-column-catalogs.json
```

Restricted-role catalog visibility is separate qualification. The running
application is unchanged, its definition baseline is not requalified here, and
no full Oracle/application compatibility claim is made.

The initial native fixture matched all six catalog cases but failed schema
cleanup with FeatureNotSupported. That attempt is recorded separately. The
corrected fixture requires pre-existing schemas and removes only its own tables
and view; it retains the same six catalog assertions. DROP SCHEMA support remains
a separate DDL qualification gap.

Local regression validation passes **1,313 Rust tests**, with three existing
ignores, and **25 Python reference tests**. Native PostgreSQL catalog comparisons
pass **6/6** after fixture correction. Oracle dictionary comparisons remain
**0/11**, and the separate character assignment checks remain **1/2**. All source
observations qualified successfully; these target failures are not skipped or
counted as passing Oracle cases. The current binary identities and observed
resource limits are recorded in validation.json. Build concurrency is one,
incremental compilation is disabled, and an active 8 GiB RAM headroom guard
monitors the owned process group. Cleanup reclaimed 2,902,296,560 bytes of
completed test executables; the Oracle build cache remains 6.8G.
