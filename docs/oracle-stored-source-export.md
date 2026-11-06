# Stored-source export for Oracle migrations

The source exporter captures standalone functions/procedures, type declarations,
type bodies, package specifications/bodies and triggers. Oracle exposes these as
separate source categories and identifies each source line numerically. The
`USER_SOURCE` view is scoped to the connected owner. [Oracle source dictionary reference](https://docs.oracle.com/en/database/oracle/oracle-database/26/refrn/ALL_SOURCE.html).

Use the existing owner-only export command with a fresh private directory:

```sh
python examples/oracle/idempiere/export_reference.py \
  --oracle-config /private/reference-connection.json \
  --directory /private/new-export
```

Both `catalog.json` and the completed `manifest.json` contain:

| Field | Captured categories |
| --- | --- |
| `routines` | `FUNCTION`, `PROCEDURE` |
| `types` | `TYPE` |
| `type_bodies` | `TYPE BODY` |
| `packages` | `PACKAGE`, `PACKAGE BODY`, distinguished by `kind` |
| `triggers` | `TRIGGER`, including its source enabled/disabled `status` |

Each definition retains its name, kind, reconstructed `CREATE OR REPLACE` DDL,
source line count and SHA-256 of that DDL's UTF-8 representation. Source lines
must be complete and ordered from line 1. NULL source-text lines are represented
as blank lines. Source text is retained without SQL rewriting; wrapped text is
not unwrapped, translated or certified executable.

Before publishing either catalog or manifest, the exporter requires exactly one
source definition for every listed stored object in `USER_OBJECTS`. It rejects
missing or empty definitions, unexpected identities, duplicate/gapped/reordered lines,
package/type bodies without catalog declarations, and unsupported line/text
encodings. Trigger enabled-state metadata must cover exactly the trigger set;
missing, duplicate or unknown status values fail. Enabled/disabled state is
distinct from object validity: a valid disabled trigger remains disabled in the
export metadata. [Oracle trigger dictionary reference](https://docs.oracle.com/en/database/oracle/oracle-database/26/refrn/ALL_TRIGGERS.html).

`stored_source_encoding.version=1` records the covered source kinds and
verification scope. An older manifest without this marker does not establish
that package bodies, type bodies or triggers were captured. Existing `routines`
and `types` fields retain their categories, and row-file/value encoding is
unchanged. A failed source-coverage check leaves no published catalog or
manifest; the exclusive export directory is retained for investigation.

Source capture is distinct from executable compatibility. Package compilation,
overloads, initialization and session state, object-type method execution,
trigger firing/order/replication properties, dependency ordering, grants and
other stored languages remain separate implementation and qualification gates.
Only the listed source categories are covered here; this is not a complete
physical database export. Keep schema writers stopped for a migration capture;
catalog coverage checks do not acquire a DDL lease or establish CDC positioning.

The [bounded evidence](evidence/oracle-stored-source-export-2026-11-05/README.md)
contains offline fixtures and a legacy-DDL reconstruction control. Live Oracle
qualification of the new categories is pending. No package or trigger execution
support on BicDB is claimed by a successful export.

## Executable live-source acceptance

The Oracle acceptance test requires an explicitly configured empty disposable
owner. The connection JSON uses the same fields as the exporter. The owner needs
session/table/procedure/type/trigger creation privileges, table quota and access
to `SYS.V_$DATABASE` for the exporter's diagnostic SCN query. Keep this separate
from the application source owner. The test refuses a nonempty owner before any
DDL and removes only its uniquely named fixture objects afterwards.

```sh
ORACLE_SOURCE_CAPTURE_CONFIG=/private/source-capture-owner.json \
  python3 -m unittest discover -s examples/oracle/idempiere \
  -p test_export_stored_source_oracle.py -v
```

The test creates a standalone function/procedure, an object type with a body,
a package specification/body and two triggers, disabling one. It requires valid
compiled objects, checks Oracle function/package results, performs a fresh
export, and compares every captured definition and line count with independent
bound dictionary queries. It checks both trigger statuses and complete fixture
cleanup. Without the configured owner it is reported as skipped, not passed.
This is a source-export acceptance test; it does not run those objects on BicDB.
