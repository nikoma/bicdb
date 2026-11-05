# Constraint diagnostic identity and logical storage names

An Oracle-configured application must be able to identify the constraint that
rejected a write. PostgreSQL transport retains SQLSTATE `23505`; the Oracle
preview profile now adds the selected Oracle diagnostic identity, while schema,
table and constraint fields refer to SQL objects rather than storage keys.
Default PostgreSQL error messages remain PostgreSQL messages.

## Implementation

The engine carries the resolved constraint namespace and logical table/name
through uniqueness checks. It records the operation that failed, distinguishing
row writes, primary-key validation, unique-constraint validation and unique-index
builds. The selected Oracle identities are ORA-00001, ORA-02437, ORA-02299 and
ORA-01452 respectively. The existing structured Oracle error stack carries the
signed code. SQLCODE/SQLERRM use the shared operation-specific mapping.

Logical quoted table/index names are separated from path-safe storage names.
Existing safe names retain their storage layout. Public names that need encoding
use the existing schema encoding; unsupported index-storage spellings use a
namespace-qualified digest. Catalogs and diagnostics retain logical names. A
structured core index-build conflict avoids classifying an error by its rendered
message and preserves the failed build's table data.

Legacy public view records retain their stored keys, OIDs and grants. Resolution,
materialization, default aliases, quoted CTEs and GRANT/REVOKE use the appropriate
logical or stored identity. Regression controls include ownerless views and
negative access checks.

No application, tenant or schema name selects special engine behavior.

## Qualification

The [independent reference comparisons](candidate-reference.json) pass all 15
selected cases against Oracle 23.26.3 and all 15 against PostgreSQL 18.4. The
[previous binary](baseline-reference.json) passes zero of these complete diagnostic
contracts in either profile; quoted public table setup was also unsupported in
that binary. The fixture checks selected identity and structured fields, rather
than equality of complete native Oracle messages. The baseline records its setup
and cleanup diagnostics explicitly.

The [validation record](validation.json) includes 1,116 passing tests: 632 SQL
unit tests, 370 PostgreSQL protocol/service tests, 106 SQL integration controls,
and eight core index controls. `cargo check --workspace --all-targets --locked`
and `cargo fmt --all -- --check` also pass. Three pre-existing SQL benchmarks are ignored;
none of the new tests are skipped. All 13 retained independent reference families
pass in both profiles, and the stock PostgreSQL JDBC optimistic-update probe
returns one updated row.

An [upgrade from the actual previous binary](legacy-upgrade.json) preserves
quoted public view reads and relation OIDs. A previously granted reader still
reads the granted view, while an ungranted view and the base table both remain
inaccessible with SQLSTATE `42501`. Unit controls additionally cover REVOKE,
regrant, replacement, deletion and reopening.

The nine selected original application test classes execute 170 tests: 168 pass
and two fail. The previously failing optimistic update and constraint-name
identification pass. The remaining failures are
`POInsertSQLTest.testToInsertSQLAndExecute` (Oracle BLOB export block) and
`TrxTest.testReleaseSavepoint` (the test expects a mocked JDBC call that the
application's Oracle branch bypasses). These remain failures in the record;
they are not excluded or rewritten. Tests run on a disposable clone with only
loopback networking. After clean shutdown, 67 prior workflow observations,
27 settled business queries, routine/view fingerprints, seven price rows and
four product rows retain their expected values, including source timestamps and
update users. The primary application database is not used for test mutations.

The independent [diagnostic reference](../../../examples/oracle/reference/constraint_diagnostics.py)
checks ordinary/quoted primary keys, unique constraints and unique indexes for
INSERT and UPDATE, three duplicate-data DDL failures, PostgreSQL structured
fields and rows after rollback. Oracle and PostgreSQL reference connections use
the existing reference harness with disposable neutral objects.

## Limits

These controls cover selected diagnostic identities, rather than complete Oracle
error-message details or all errors. PostgreSQL JDBC's vendor-code behavior is
unchanged; Oracle identity is available through SQLSTATE, message and structured
DETAIL metadata. Unchanged Oracle-driver protocol support remains separate scope.

The existing implicit primary-key behavior of a column named `id`, complete
identifier-length/case rules, namespace relocation, all cross-owner index cases,
DDL implicit-commit semantics, broader LOB APIs and full application acceptance
remain separate compatibility work. This checkpoint does not establish Oracle
conformance, certification or production qualification.

## Subsequent runtime qualification

Browser qualification found a stack overflow in recursive query analysis on the
diagnostic candidate, after the desktop was reached. That runtime upgrade was
rejected and the previous runtime restored. The
[recursive query qualification](../sql-recursive-query-stack-2026-11-04/README.md)
records the neutral reproducer, independent sources and corrective visitor
change. The test and reference results above remain results for their recorded
binary; they do not establish acceptance of the rejected runtime.
