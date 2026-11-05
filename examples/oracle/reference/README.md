# Oracle reference queries

`foreign_key_writer_fence.py --current-server /path/to/current/oracle_server
--previous-server /path/to/previous/oracle_server --output /private/fence.json`
qualifies an upgrade on a disposable database created by the previous binary.
The previous binary must reproduce the former unchanged-key exemption. The
current binary must retain legacy values, reject explicitly assigned invalid
references, and block the previous executable from serving the upgraded
database. Restart, repaired references, BLOB preservation and a subsequent
BLOB schema save are checked. Both paths must be local compatibility-example
executables with the documented `oracle_server` command-line interface.

`foreign_key_timing.py --reference oracle --oracle-config /private/oracle.json
--output /private/foreign-key-timing.json` checks 17 Oracle and 21 PostgreSQL expectations for
deferred foreign keys: catalogs, child-before-parent writes, commit failure,
SET CONSTRAINTS, savepoints, NOVALIDATE legacy rows, validation and delete
actions. PostgreSQL uses `--reference postgres` and `ORACLE_CONTROL_PG_DSN`;
both modes use `BICDB_PG_DSN` for the target. The target profile must match the
reference. Each case creates and drops nonce tables in a dedicated schema.
Oracle and PostgreSQL have separate expectations for unchanged-key assignments
and named non-deferrable constraints. Composite child-before-parent writes,
partially NULL composite references and tuple self-assignments have separate
cases. PostgreSQL SQLSTATEs are also compared;
Oracle numeric errors are recorded but not claimed equivalent. These checks
do not qualify application constraint migration, all concurrency schedules,
all composite/null-key combinations or recovery. PostgreSQL-only cross-width
cases check immediate/deferred diagnostics, deleting the invalid child before
commit and validation of existing rows; Oracle SMALLINT has a different numeric
domain and is not treated as the same declaration.

`integer_bounds.py --reference oracle --oracle-config /private/oracle.json
--output /private/integer-bounds.json` checks 18 independent expectations for
integer primary/secondary index bounds, numeric promotion, fractional/large
values, reversed comparisons and prepared numeric parameters. Use the same
PostgreSQL reference and BicDB target variables described below. Other index
types, user-defined numeric types and concurrency remain unqualified.

`row_update.py --reference oracle --oracle-config /private/oracle.json
--output /private/row-update.json` checks 18 independent expectations for
row-valued UPDATE: correlated aggregates, constructors, source cardinality,
NULLs, simultaneous old values, primary keys, constraint/statement rollback,
quoted columns, prepared values and volatile evaluation. PostgreSQL has its own
native controls via `--reference postgres` and `ORACLE_CONTROL_PG_DSN`;
`BICDB_PG_DSN` selects the target. Nonce objects are removed. DEFAULT, generated
columns, broader statement snapshots, exact Oracle error numbers and full
UPDATE conformance remain unqualified.

`nvl2.py --reference oracle --oracle-config /private/oracle.json
--output /private/nvl2.json` checks 59 explicit expectations for typed NVL2,
empty-character identity, DATE/TIMESTAMP conversion and arithmetic, quoted
builtin names, declared national literals, prepared parameters, and eager
argument evaluation in SQL and stored routines. Shared NVL, national length,
padding and NULL controls protect the literal provenance rule. The independent
PostgreSQL controls use native CASE/COALESCE and ordinary text semantics.
Both references must satisfy their own expectations. `--reference postgres`
uses `ORACLE_CONTROL_PG_DSN`; both use `BICDB_PG_DSN` for the target. Nonce
objects are removed. These checks do not qualify every result type, NLS option,
LOB, floating-point precedence, modifier or exact descriptor width.

`primary_key_extrema.py --reference oracle --oracle-config /private/oracle.json
--output /private/key-extrema.json` checks 20 explicit expectations for numeric
primary-key MIN/MAX and ranges: exact 30-digit integers, fractional composite
keys, numeric-looking text suffixes, empty results, prepared prefixes, pending
inserts/key updates/deletes and rollback. Both references must independently
satisfy the expected rows. Use `--reference postgres` with
`ORACLE_CONTROL_PG_DSN` for PostgreSQL; both use `BICDB_PG_DSN` for the target.
Nonce tables are removed, with Oracle PURGE. These controls do not qualify all
primary-key types, collations or concurrent schedules.

`set_operand_builtins.py --reference oracle --oracle-config /private/oracle.json
--output /private/set-operands.json` checks 14 explicit expectations for typed
builtins in individual set-operation SELECT scopes: joined aggregates, UNION,
INTERSECT, Oracle MINUS/PostgreSQL EXCEPT, stored views and counts, CTE inputs,
local alias reuse, DATE defaults, nested scalar queries and ordered pagination.
Oracle NVL and native PostgreSQL COALESCE have independent evaluation-order
expectations. `--reference postgres` uses `ORACLE_CONTROL_PG_DSN`; both modes use
`BICDB_PG_DSN` for the target. Nonce tables and views are removed, using Oracle
PURGE for tables. These cases do not establish arbitrary set-query compatibility.

`numeric_format_pictures.py --reference oracle --oracle-config /private/oracle.json
--output /private/numeric-pictures.json` checks 120 fixed numeric pictures using
9/0 digit positions, G/D separators and FM padding. Independent Oracle and
PostgreSQL expectations retain their differences at rounded negative zero and
overflow. The recorded locale is `.,` on Oracle and `en_US.utf8` on PostgreSQL.
Use `--reference postgres` with `ORACLE_CONTROL_PG_DSN` for the PostgreSQL source;
both modes use `BICDB_PG_DSN` for the target. No persistent objects are created.
These cases do not qualify other locales, currency, sign directives, exponent
pictures or malformed-format error codes.

`scalar_subquery_defaults.py --reference oracle --oracle-config /private/oracle.json
--output /private/scalar-defaults.json` checks seven scalar-subquery cases:
character and number defaults, nested defaults and concatenation, DATE
arithmetic, timestamp fractions and eager Oracle NVL evaluation. The PostgreSQL
reference uses COALESCE with independent null-concatenation and lazy-evaluation
expectations. Set `ORACLE_CONTROL_PG_DSN` with `--reference postgres` and
`BICDB_PG_DSN` for the target. Both references must meet explicit expectations;
nonce tables are removed, purging Oracle fixture tables. These cases do not
qualify arbitrary scalar queries, national character types or numeric format
pictures.

`parenthesized_queries.py --reference oracle --oracle-config /private/oracle.json
--output /private/parenthesized-queries.json` checks named statement descriptions
and rows for parenthesized SELECTs and UNION ALL operands with independent row
limits. All 12 expectations must hold on the actual reference, for both empty
and populated nonce tables. `--reference postgres` uses
`ORACLE_CONTROL_PG_DSN`; both modes use `BICDB_PG_DSN` for the dedicated target.
Oracle native cursor parsing is compared with PostgreSQL protocol statement
Describe. This checks the selected metadata and row shapes, not a whole JDBC
driver or application. Fixtures remove their nonce tables, with Oracle PURGE.

`reparsed_view_routines.py --reference oracle --oracle-config /private/oracle.json
--output /private/reparsed-views.json` compares 180 repeated calls to a stored
function reading a view containing numeric and character CASE expressions,
NULL casts and UNION ALL. Every call must return its exact expected decimal.
`--reference postgres` independently runs the PostgreSQL function syntax against
`ORACLE_CONTROL_PG_DSN` and BicDB's native profile. Set `BICDB_PG_DSN` to the
dedicated target. Nonce functions, views and tables are removed afterward;
Oracle tables are purged to avoid accumulating fixture recycle-bin entries.
This qualifies the repeated-call workload, not the full application or general
view performance.

This directory compares a fixed set of public queries against an actual Oracle
database and BicDB's `oracle-hr-v1` profile. The comparison checks query values,
including nulls, character padding, exact numbers and time components. It does
not qualify type metadata, error semantics, transactions or general Oracle
compatibility. There is no allowlist of ignored differences: any difference or
unexpected execution error makes `compare.py` exit with status 1, after writing
its report. A fixture may explicitly assert an error with `expected_error`,
which must name and match both Oracle and BicDB codes; returned rows or any
other error fail that assertion.

## Local reference database

Use a dedicated database and schema. The development reference uses the official
[Oracle Free Lite image](https://container-registry.oracle.com/ords/ocr/ba/database/free),
version `23.26.3.0-lite`, pinned to this multi-architecture manifest:

```text
container-registry.oracle.com/database/free@sha256:cf540c3fa190d7cffad08c491652ac07fc70314e510f6b87890449517d565e94
```

Review the image's Oracle Free Use Terms before downloading it. Oracle is an
external test dependency; its binaries, database files and credentials are not
part of BicDB's source distribution. Lite omits Data Pump, RMAN, XML DB and other
features listed in the registry documentation. Use a separate full-image
container and volume to qualify those features. Results on this newer Free
edition do not establish behavior on Oracle 19c or Enterprise Edition.

An example local setup, with Docker already installed:

```bash
install -d -m 700 "$HOME/.local/state/bicdb/oracle-reference"
python3 - <<'PY'
import os
import secrets
from pathlib import Path
path = Path.home() / '.local/state/bicdb/oracle-reference/oracle_pwd'
# The containing directory is private; the bound file must be readable by the
# container's Oracle user. Never put this directory inside the repository.
fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o444)
with os.fdopen(fd, 'w') as secret:
    secret.write('BicR7' + secrets.token_hex(12))
PY
docker volume create bicdb-oracle-reference-data
docker run -d --name bicdb-oracle-reference \
  --label org.bicdb.purpose=oracle-reference \
  --restart unless-stopped --cpus 2 --memory 4g --shm-size 1g \
  --publish 127.0.0.1:1521:1521 \
  --mount "type=bind,src=$HOME/.local/state/bicdb/oracle-reference/oracle_pwd,dst=/run/secrets/oracle_pwd,readonly" \
  --mount type=volume,src=bicdb-oracle-reference-data,dst=/opt/oracle/oradata \
  container-registry.oracle.com/database/free@sha256:cf540c3fa190d7cffad08c491652ac07fc70314e510f6b87890449517d565e94
docker logs bicdb-oracle-reference
docker port bicdb-oracle-reference 1521/tcp
```

Wait for `DATABASE IS READY TO USE!`. The named volume survives container
recreation. `docker restart bicdb-oracle-reference` restarts it without deleting
its data. Inspect the existing container and volume before reusing their names;
these commands do not replace existing containers. Choose another fixed host
port if 1521 is occupied. The port is bound only to localhost; using a fixed
port keeps client configuration valid across restarts. The PDB service is
`FREEPDB1`.

Create a dedicated HR schema using an administrative session:

```bash
docker exec -it bicdb-oracle-reference sqlplus / as sysdba
```

```sql
ALTER SESSION SET CONTAINER=FREEPDB1;
CREATE TABLESPACE bicdb_reference
  DATAFILE '/opt/oracle/oradata/FREE/FREEPDB1/bicdb_reference01.dbf'
  SIZE 64M AUTOEXTEND ON NEXT 16M MAXSIZE 256M;
SET VERIFY OFF
ACCEPT fixture_password CHAR PROMPT 'New HR fixture password: ' HIDE
CREATE USER bicdb_hr IDENTIFIED BY "&fixture_password"
  DEFAULT TABLESPACE bicdb_reference QUOTA 64M ON bicdb_reference;
UNDEFINE fixture_password
GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE SEQUENCE,
  CREATE PROCEDURE, CREATE TRIGGER TO bicdb_hr;
```

Store the fixture password in a private file outside the checkout. A private
connection JSON file should have the following shape; `port` is the actual port
reported by Docker and `password_file` is relative to the JSON file or absolute:

```json
{
  "host": "127.0.0.1",
  "port": 1521,
  "service_name": "FREEPDB1",
  "user": "BICDB_HR",
  "password_file": "hr-password"
}
```

## Import and compare

Install the two Python drivers in a virtual environment:

```bash
python3 -m venv /tmp/bicdb-oracle-reference-venv
/tmp/bicdb-oracle-reference-venv/bin/pip install 'oracledb==26.1.0' 'psycopg[binary]==3.3.6'
python3 examples/oracle/hr/prepare.py --check
/tmp/bicdb-oracle-reference-venv/bin/python examples/oracle/reference/load_hr.py \
  --oracle-config "$HOME/.local/state/bicdb/oracle-reference/hr-connection.json"
```

`load_hr.py` refuses administrative accounts and nonempty schemas. It loads the
same adapted fixture as the BicDB example, checks all seven row counts, and
rejects invalid compiled objects. Oracle DDL commits independently, so a failed
import may leave objects behind: recreate only the dedicated fixture schema
before retrying. The loader never drops objects automatically.

Load the [BicDB HR example](../../../docs/oracle-hr-profile.md) into a separate,
new BicDB database, then compare:

```bash
export BICDB_PG_DSN='host=127.0.0.1 port=5441 user=bicdb dbname=bicdb'
/tmp/bicdb-oracle-reference-venv/bin/python examples/oracle/reference/compare.py \
  --oracle-config "$HOME/.local/state/bicdb/oracle-reference/hr-connection.json" \
  --output /tmp/bicdb-oracle-comparison.json
```

The report includes versions, a fixture hash, and every comparison result. It
contains no DSNs or credentials. Its queries read only the public sample. The
history query deliberately selects the original sample period, excluding the
additional 2020 procedure-validation row created by `hr/run.py`.

Run driver-independent harness checks with:

```bash
python3 -m unittest discover -s examples/oracle/reference -p 'test_*.py'
```

The regular CI suite runs these harness checks. Live Oracle comparisons require
the external reference database and must be reported separately from CI checks.

The preview's standalone datetime extraction fixtures use
`--profile oracle-preview-v1 --cases examples/oracle/reference/extract-cases.json`.
They check date year/month/day fields, timestamp clock fields, conditional
aggregates and an application column named `YEAR`. These are value comparisons;
they do not qualify all Oracle datetime types, errors, NLS rules or metadata.

`date-offset-cases.json` checks known numeric day values, rounding boundaries,
nullable operands, timestamp-to-DATE conversion and nested query metadata.
NUMBER expression evaluation is a separate acceptance requirement; the
[day-offset evidence](../../../docs/evidence/oracle-day-offsets-2026-11-01/README.md)
records the initial failing integer-division composition probe. The subsequent
division implementation is checked with `number-division-cases.json` and one
hundred deterministic synthetic cases in `number-division-generated-cases.json`.

## Generated routine namespaces

`routine_namespaces.py` provides an independent Oracle comparison for static
function namespace binding. Use dedicated test schemas and a BicDB
`oracle-preview-v1` database. BicDB needs an existing schema with the same name
as Oracle's current schema; its connection also needs CREATE permission in that
schema and `public` for shadow tables. The runner creates uniquely named test
tables/functions and removes only objects it successfully created. Oracle DDL
commits independently. Cleanup failures make the run fail and are recorded with
their object names for inspection. DSNs, passwords and driver error messages are
excluded from reports.

```bash
python3 examples/oracle/reference/routine_namespaces.py \
  --oracle-config "$HOME/.local/state/bicdb/oracle-reference/hr-connection.json" \
  --output /tmp/bicdb-routine-namespaces.json
```

The runner reads `BICDB_PG_DSN` by default. Twenty-four generated cases cover
bound cursors, CTEs, SELECT INTO, query FOR loops, nested cursors and explicit
qualifications. Different-valued BicDB public shadow tables detect unintended
fallback. Both databases must match each other and the fixture's expected typed
values. Any execution error or mismatch fails; there is no ignored-difference
list. This qualifies those static relation lookups, not full PL/SQL or Oracle
AUTHID rights. The ordinary `compare.py` command remains a read-only query runner.
Run each with the same preview profile and an explicit `--cases` path. Expected
values come from the Oracle reference, independently of BicDB's implementation.

## Generated routine temporal semantics

Pass `--suite temporal` to the same `routine_namespaces.py` runner to compare
logical DATE declarations, numeric day offsets, exact NUMBER date differences,
TIMESTAMP intervals, fractional-second coercion, casts, literals and cursor
numeric fields. This suite uses the version-3 preview routine compiler and
checks both backends against explicit typed expectations. Its 29 cases run
through ordinary text driver decoding, including PostgreSQL interval values.
It does not qualify timezones, source column temporal provenance, timestamps
beyond six fractional digits or the full PL/SQL language.

```bash
python3 examples/oracle/reference/routine_namespaces.py --suite temporal \
  --oracle-config "$HOME/.local/state/bicdb/oracle-reference/hr-connection.json" \
  --output /tmp/bicdb-routine-temporal.json
```

## Generated Gregorian calendar semantics

Pass `--suite calendar` to `routine_namespaces.py` to compare ADD_MONTHS through
both scalar calls and independent stored functions. The 36 cases check month-end
carry, leap-century rules, time preservation, NULLs, fractional month counts and
exact values immediately below integer boundaries. The runner checks and records
the reference connection's Gregorian NLS calendar; a different calendar fails
before creating objects. PostgreSQL has separate native interval-arithmetic and
user-function regressions. Alternate calendars, timezone conversions, BC date
boundaries and Oracle-specific error codes are outside this suite's acceptance.

## Generated week truncation and procedural formats

Pass `--suite week` to `routine_namespaces.py` for 102 assertions covering W, WW,
D/DY/DAY, IW and ISO-year truncation, scalar SQL, stored expressions and embedded
SELECT statements, plus exact numeric truncation with integer and fractional
precision arguments. The reference session must use the Gregorian calendar and
AMERICA territory; the runner records and checks both before creating objects.
D/DY/DAY follow the preview's AMERICA default, with Sunday as the first day.
Alternate NLS territories remain unsupported by the preview session contract.

Oracle SQL returns NULL for a NULL datetime format. A PL/SQL expression raises
ORA-01899 when the datetime is non-NULL; BicDB reports SQLSTATE 22023 over its
PostgreSQL transport. A NULL datetime and numeric TRUNC retain NULL propagation.
The two expected-error cases assert both codes explicitly. They do not ignore
an execution difference. A final valid call checks recovery after those errors.
New preview definitions use compiler version 4; existing version-3 definitions
retain their earlier procedural-format behavior until recreated from source.

SQL truncates fractional numeric precision toward zero. Procedural expressions
convert that precision to an integer by rounding halfway away from zero. The
fixture checks both contracts, including positive and negative midpoints.

## Generated table-column anchors

Pass `--suite anchors` to `routine_namespaces.py` for 41 assertions covering
quoted table/column names, NUMBER(5,2) local variables and numeric returns,
VARCHAR2(3 CHAR) local lengths, NULLs, argument and character-return sizing,
exact numeric literals, overflow errors and function exception handling. The
reference session must use dot-decimal `NLS_NUMERIC_CHARACTERS='.,'`; a different
setting is rejected before DDL. Objects are generated and removed in the
reference schema and matching target namespace.

Six explicit error assertions require ORA-06502 from Oracle and either SQLSTATE
22003 (numeric overflow) or 22001 (character length) from BicDB's PostgreSQL
transport. They do not qualify a general ORA error mapping. Source-only checks
establish the expectations independently of BicDB. Engine and driver tests also
cover definition-owner selection against conflicting namespaces, missing-table
rejection, native PostgreSQL behavior and database reopen.

A constrained numeric return differs from an unconstrained argument and from a
character return length. PostgreSQL discards declaration modifiers from its
function signatures, as specified by [CREATE FUNCTION](https://www.postgresql.org/docs/18/sql-createfunction.html).
These profiles have separate regressions. Full dependency invalidation after
DDL, temporal type provenance, BYTE length semantics and cross-schema Oracle
privileges remain outside this fixture's acceptance.

## Generated procedural CASE statements

Pass `--suite case` to `routine_namespaces.py` for 34 assertions covering
simple and searched CASE, sequential and lazy conditions, NULLs, nested CASE
and IF/loop bodies, scalar SQL CASE expressions, handler bodies and a sequence
selector evaluated once. Three error assertions check ORA-06592 against
SQLSTATE 20000 and ORA-01476 against SQLSTATE 22012. An omitted ELSE raises an
error for a statement and yields NULL for the scalar expression form.

These expectations are independently exercised on Oracle before comparing
BicDB. PostgreSQL has separate engine and prepared-driver regressions, including
its integer division behavior. The fixture creates and removes its own
functions and sequence in dedicated test databases. It does not qualify
labels, Oracle qualification of comma-separated selector alternatives, arbitrary type conversions,
DDL validation timing or full PL/SQL.

## Independent nested exception-block contracts

`nested_blocks.py` checks three synthetic write/handler scenarios separately on
Oracle and PostgreSQL. Its expectations intentionally differ: a successful
UPDATE before a handled error survives the Oracle block and is undone by the
PostgreSQL exception block. Local variables retain their values in both.
The runner also checks failing-statement atomicity, handler propagation and
rollback, using fresh dedicated connections and cleaning up its tables.

Supply `--oracle-config`, `--output` and a dedicated PostgreSQL connection in
`ORACLE_CONTROL_PG_DSN`. Without target options the runner is reference-only.
Optional `--bicdb-oracle-dsn-env` and `--bicdb-postgres-dsn-env` name environment
variables containing dedicated BicDB connections. They run the same scenarios
through the PostgreSQL driver under the selected profile and record each
backend independently. Synthetic writes are rolled back and tables removed.
[Measured contracts and implementation dependencies](../../../docs/evidence/oracle-nested-block-contracts-2026-11-02/README.md)
remain separate from full transaction or application qualification.

`rownum-cases.json` adds 15 plain prefix-query comparisons over ordered derived
rows, with explicit expected values. They cover equality, upper/lower bounds,
zero/negative bounds, reversed operands, conjunctions and ordinary filtering.
They do not qualify same-block sorting/grouping, projected ROWNUM or dynamic bounds.

## Typed routine concatenation and native controls

Use dedicated reference and preview databases with the private connection format
above. The generated fixture checks 76 bounded cases, including numeric/date
conversion, NULL operands, UTF-8 byte limits, SQL versus procedural callers and
single evaluation of sequence operands:

```sh
python3 examples/oracle/reference/routine_namespaces.py --suite concat \
  --oracle-config oracle-reference.json --output routine-concat.json
```

The fixture checks its required Oracle NLS defaults and AL32UTF8 character set
before creating objects. It drops its uniquely named functions and sequences.
The independent native control runner uses two dedicated PostgreSQL connections:
`ORACLE_CONTROL_PG_DSN` for an actual PostgreSQL reference and `BICDB_PG_DSN` for
BicDB in its default PostgreSQL profile. These variables remain private.

```sh
python3 examples/oracle/reference/postgres_concat.py --output native-concat.json
```

This control checks 62 values, result-type OIDs and error SQLSTATEs. It includes
48 numeric format cases and an exact integer above the floating-point precision
boundary, and removes its temporary sequence. Both runners exit nonzero for a
difference or cleanup error. See the
[qualification record](../../../docs/evidence/oracle-routine-concatenation-2026-11-02/README.md)
for the measured scope and remaining gaps.

## Parameterized cursors and NUMBER arithmetic

The `cursors` suite checks 34 Oracle cases: positional/named/mixed arguments,
OPEN-time values, repeated calls, query/outer scopes, defaults, error cleanup and
logical DATE formals. The `numberops` suite checks 218 NUMBER cases, including
finite mantissa precision, SQL/procedural overflow and infinity operations:

```sh
python3 examples/oracle/reference/routine_namespaces.py --suite cursors \
  --oracle-config oracle-reference.json --output routine-cursors.json
python3 examples/oracle/reference/routine_namespaces.py --suite numberops \
  --oracle-config oracle-reference.json --output routine-numbers.json
python3 examples/oracle/reference/postgres_cursors.py --output native-cursors.json
```

The native runner uses `ORACLE_CONTROL_PG_DSN` and `BICDB_PG_DSN` as described
above. Its 28 cases compare values, type OIDs and SQLSTATEs independently against
PostgreSQL, retaining its ambiguity error where Oracle gives a column precedence.
All synthetic objects are uniquely named and removed after the comparison.

`compare.py` explicitly normalizes the Oracle reference driver's NUMBER infinity
sentinels, +/-1e126, to numeric `Infinity`/`-Infinity`. This policy applies only to
the Oracle reference side. Native PostgreSQL permits finite values at those
magnitudes, which retain their exact value. A DUMP/TO_CHAR reference probe and
unit regressions distinguish the representations. These tests do not qualify
NUMBER infinity export/import for arbitrary application datasets. See the
[qualification record](../../../docs/evidence/oracle-parameterized-cursors-2026-11-02/README.md)
for measured behavior and remaining gaps.

The `routine_namespaces.py --suite columns` fixture compares Oracle DATE and
TIMESTAMP storage and `%TYPE` anchors with frozen source observations. BicDB
receives values through PostgreSQL COPY, while Oracle uses typed SQL literals.
It checks quoted names, logical source metadata and the intentionally native
PostgreSQL transport OIDs. Its timezone case covers declaration and NULL metadata
only. Use the same private connection arguments as the other routine suites.

`postgres_column_types.py --output /private/native-columns.json` independently
compares native PostgreSQL DATE assignment, COPY, timestamp rounding and quoted
ALTER TYPE with a native BicDB database. Set `ORACLE_CONTROL_PG_DSN` and
`BICDB_PG_DSN` as above. The runner removes its uniquely named tables.

The `routine_namespaces.py --suite characters` fixture covers fixed AL32UTF8
CHR values and nested default TO_CHAR in SQL, stored expressions and embedded
queries. The frozen `chr-reference.json` records source characters, DUMP bytes
and numeric-range observations. Invalid packed characters and NUL transport
remain explicitly unsupported. Numeric-error comparisons distinguish SQL
ORA-01426 from procedural ORA-06502 wrapping; the preview transports both as
SQLSTATE 22003. This does not establish full Oracle error identity.

`postgres_characters.py --output /private/native-characters.json` independently
checks Unicode scalar chr, NULL, SQLSTATEs and an unselected failing COALESCE
argument against PostgreSQL. It uses `ORACLE_CONTROL_PG_DSN` and `BICDB_PG_DSN`,
creates no objects and retains native PostgreSQL semantics.

The `routine_namespaces.py --suite nvl` fixture compares eager NVL evaluation,
first-operand type selection, empty character identity, CHAR padding, datetime
conversion and timestamp precision in SQL and stored routines. It also binds
typed values through both actual drivers. `nvl-reference.json` freezes source
observations; the runner rechecks the expected values and error codes against
Oracle. Projection metadata asserts the selected PostgreSQL transport OIDs.
The fixture creates and drops only its uniquely named tables and routines.
Use the private connection arguments shown for the other routine suites.

The scope is fixed preview NLS and supported character/NUMBER/DATE/TIMESTAMP
operands. Binary floating point, other NLS settings, full Oracle diagnostics and
submicrosecond values are not qualified by this fixture. Existing compiled
routines retain their earlier contract until recreated from Oracle source.

`aggregate_conditionals.py --reference oracle --oracle-config /private/oracle.json
--output /private/oracle-aggregates.json` separately checks query-scoped aggregate
typing, parameterized stored routines, eager NVL conversion, and conditional
scalar evaluation. `--reference postgres` uses `ORACLE_CONTROL_PG_DSN` for an
independent PostgreSQL control. Both modes use `BICDB_PG_DSN` for a dedicated
target with the corresponding profile. Aggregate inputs in an unused CASE or
COALESCE branch still execute; an unused scalar fallback or scalar subquery
does not. Explicit values and selected error codes must agree with the actual
reference before a case counts as matched. The fixture creates and drops only
its nonce-named table and routine. It does not qualify every aggregate, optimizer
evaluation order, or Oracle aggregate/subquery legality rule.

`column_scope.py --reference oracle --oracle-config /private/oracle.json --output
/private/column-scope.json` checks columns that share names with local values,
including projection, filters, qualification and a variable without a matching
column. The Oracle target must use an existing dedicated non-public application
schema. `--reference postgres` independently checks SQL-language function
arguments through `ORACLE_CONTROL_PG_DSN`; it does not qualify PL/pgSQL's separate
variable-conflict policies. Both modes use `BICDB_PG_DSN`, prepared query parameters,
explicit source expectations, and owned nonce-named tables/functions that are
removed afterward. No reserved internal-function prefix is used.

`scalar_subquery_types.py --reference oracle --oracle-config /private/oracle.json
--output /private/scalar-types.json` compares scalar aggregate values and result
types. `--reference postgres` uses `ORACLE_CONTROL_PG_DSN` for the native control;
both modes use `BICDB_PG_DSN`. Six shapes run with prepared parameters in text and
binary result formats, including NULL and empty results. The Oracle source must
report NUMBER; the selected PostgreSQL transport must report NUMERIC OID 1700
independently of row values. The native source checks that OID directly. This
qualifies the bounded one-relation aggregate shapes, not general subquery typing.

`pending_index_visibility.py --reference oracle --oracle-config /private/oracle.json
--output /private/pending-index.json` compares NUMERIC-key transaction workloads
with and without a secondary index. `--reference postgres` supplies the independent
native control through `ORACLE_CONTROL_PG_DSN`; both use `BICDB_PG_DSN`. Prepared
mutations and reads cover equality/range aggregates, a correlated parent update,
inserted rows, key changes, deletion, savepoint rollback, full rollback and commit.
The 24 explicit expectations must pass on the actual reference before target
matches count. Only nonce-named tables/indexes are created and removed. This
qualifies these own-write visibility rules, not complete transaction isolation.


The `routine_namespaces.py --suite procedures` fixture checks actual Oracle CALL
effects, replacement, defaults, qualified nested calls, OUT/IN OUT copyback and
exception state. `procedure-reference.json` freezes source values and errors;
the runner compares them again against Oracle. Five extra driver cases compare
Oracle output binds with PostgreSQL output rows, including DATE fractional-second
removal and NULL output type metadata. The Oracle NUMBER outputs use text binds
to preserve decimal values without a binary floating-point intermediary.

`postgres_procedures.py --output /private/native-procedures.json` independently
checks PostgreSQL CALL inputs, named ordering, writable outputs, metadata, defaults
and recovery. Use `ORACLE_CONTROL_PG_DSN` and `BICDB_PG_DSN` for dedicated native
databases. Both runners remove their uniquely named objects. Neither establishes
full Oracle PL/SQL, complete overload resolution or application qualification.


The `routine_namespaces.py --suite errors` fixture compares fixed-preview
RAISE_APPLICATION_ERROR inputs, numeric rounding and ranges, messages, current
SQLCODE/SQLERRM and nested handler/call context against actual Oracle. It checks
saved-message invalidation, cleared-error reraising, direct PL/SQL versus SQL
entry, local name shadowing, dynamic diagnostic reads, parameter defaults and
UTF-8 byte limits. Raw procedure calls compare logical error stacks with the
PostgreSQL driver's primary message, SQLSTATE and JSON `oracle_error_stack`
DETAIL field. Multiline messages are preserved. Source fixtures record the
observed American diagnostic language and fixed preview NLS environment.

`postgres_errors.py --output /private/native-errors.json` independently checks
native rethrow messages, SQLSTATE, DETAIL, constraint fields, nested handlers,
trailing spaces, user variable names, procedure calls and recovery. Set
`ORACLE_CONTROL_PG_DSN` and `BICDB_PG_DSN` to dedicated native databases. Both
fixtures remove their uniquely named objects. Full Oracle error catalogs,
SQLERRM(code), EXCEPTION_INIT and Oracle backtraces remain unqualified.

`primary_key_updates.py --oracle-config /private/reference.json --output
/private/key-updates.json` compares schema-qualified primary-key UPDATEs with
Oracle through PostgreSQL transport. Use the same pre-existing dedicated schema
on each database. The fixture checks aliases, bound decimal inputs, old-row
assignment values, defaults, composite keys, uniqueness, foreign-key errors,
statement recovery and explicit rollback. Oracle permits statement-end key swaps;
PostgreSQL immediate checks can reject them. Each backend has independent
expectations, including ordered shifts into keys vacated by preceding rows.

`postgres_primary_key_updates.py --schema reference_schema --output
/private/native-key-updates.json` compares native BicDB with PostgreSQL, including
column UPDATE grants, UPDATE-only authorization, RLS USING/WITH CHECK and SELECT
visibility. Set ORACLE_CONTROL_PG_DSN and BICDB_PG_DSN privately. The supplied
schema must already exist on both dedicated databases; role-management privileges
are required for the temporary role. Fixtures remove their own tables, role and
grants; they retain the supplied schema. Parent keys referenced by foreign keys,
row triggers, UPDATE FROM/RETURNING and concurrent key moves remain unqualified.

`autonomous_transactions.py --oracle-config /private/reference.json --output
/private/autonomous-transactions.json` compares independent commit/rollback,
parent/observer visibility, nested calls, active-return errors, caught failures,
locking reads, parent-lock deadlocks, SERIALIZABLE snapshots and default-argument
ownership with Oracle. Set BICDB_PG_DSN privately to a dedicated preview database.
The matching source schema must already exist on the target. The fixture uses
two connections to each database and removes its uniquely named routines and
table. It does not run source application routines or qualify a complete
application. Native PostgreSQL behavior is checked by the separate native
control fixtures; transport Describe and restart are covered by Rust tests.


`national_characters.py --oracle-config /private/reference.json --output
/private/national-characters.json` compares version-17 routine-local NVARCHAR2
and NCHAR behavior with an AL16UTF16 Oracle reference. The supplied schema must
already exist on the dedicated preview target selected by BICDB_PG_DSN. Fixtures
check UTF16 lengths, supplementary characters, fixed padding, assignment errors,
complete-character counts, formal/return provenance, static/dynamic host values,
and a prepared PostgreSQL-driver return codec. An explicit negative case records
Oracle's invalid-function error for NVARCHAR and the preview's unsupported-type
error; it does not claim invalid-object catalog parity.

`postgres_national_controls.py --output /private/native-characters.json` checks
ordinary VARCHAR assignment, trailing spaces, empty strings, Unicode lengths and
overflow against PostgreSQL in a separate native-profile target. Set
ORACLE_CONTROL_PG_DSN and BICDB_PG_DSN privately. Both fixtures remove only their
own created functions. General national SQL table types, national literals,
NLS settings, positional operations and larger NCHAR declarations remain outside
this bounded contract.

`dictionary_contracts.py --oracle-config /private/reference.json --output
/private/dictionary.json` records dictionary and character-capacity contracts
against dedicated Oracle and preview schemas. This is currently a failing
acceptance fixture: USER_TABLES/USER_TAB_COLUMNS and Oracle BYTE capacity remain
unimplemented. Its nonzero status and mismatches must be retained explicitly.
It also records native target descriptors without counting them as Oracle matches.

`postgres_catalog_columns.py --schemas catalog_a catalog_b --output /private/native-columns.json` independently
checks logical names, identical table names in different schemas, schema filters
and view columns against PostgreSQL. It uses ORACLE_CONTROL_PG_DSN and a native
BICDB_PG_DSN with two pre-existing dedicated schemas, and removes its own tables
and view while retaining those schemas. Restricted-role visibility
is outside this owner/admin fixture.


The preview's SQL character-column fixture uses dedicated schemas and explicit
BYTE/CHAR declarations under AL32UTF8 and STANDARD string limits:

```sh
python examples/oracle/reference/character_columns.py \
  --oracle-config /private/reference.json --output /private/character-columns.json
```

Set BICDB_PG_DSN privately to an Oracle-preview target with a matching schema.
The fixture removes its own tables. It compares saved values, fixed padding,
capacity errors, prepared UPDATE and PostgreSQL COPY behavior against Oracle
INSERT behavior, including multibyte and supplementary characters. See the
[qualification and remaining scope](../../../docs/evidence/oracle-character-columns-2026-11-03/README.md).


`postgres_view_dependencies.py` compares explicit-column view dependency guards
across two pre-existing schemas, including identical quoted table names and
quoted identifiers containing periods. It checks refused type changes, retained
catalog widths and successful changes after removing a dependency:

```sh
python examples/oracle/reference/postgres_view_dependencies.py \
  --schemas dependency_a dependency_b --output /private/view-dependencies.json
```

Set ORACLE_CONTROL_PG_DSN privately to PostgreSQL and BICDB_PG_DSN to a native
PostgreSQL-profile target. The fixture removes its own tables and view. Wildcard
dependencies and general view invalidation are separate qualification.

`output_buffers.py` runs the supplied-package acceptance fixture:

```sh
python examples/oracle/reference/output_buffers.py \
  --oracle-config /private/reference.json --output /private/output-buffers.json
```

Set BICDB_PG_DSN privately to an explicit Oracle-preview target. Its 38 cases
cover output buffers, reads/writes, NULLs, Unicode byte limits, overflow recovery,
rollback/savepoints and bounded CHARARR batches. Dedicated source schemas and
nonce-named helper procedures keep schema changes isolated. The fixture removes
its helpers and returns nonzero while any case is unqualified. The current
[version-25 checkpoint](../../../docs/evidence/oracle-output-buffers-2026-11-03/README.md)
matches 32/38 against Oracle, with six GET_LINES cases still unimplemented.
Overall success remains false and the fixture exits nonzero. The earlier 27-case
[baseline](../../../docs/evidence/oracle-output-buffer-baseline-2026-11-03/README.md)
remains available. Neither checkpoint qualifies the full package or application.


`row_lock_deadlines.py` compares Oracle `SELECT ... FOR UPDATE WAIT n` against
an explicitly selected `oracle-preview-v1` target. It uses the same connection
arguments as `row_lock_waits.py`, with `--reference oracle` only. It checks
uncontended acquisition, immediate contention, expiry, release and cancellation.
A nonzero result means the complete contract is unqualified, including when a
source expectation fails. The current
[baseline](../../../docs/evidence/oracle-row-lock-deadlines-baseline-2026-11-03/README.md)
retains both the missing target syntax and a timed source-cancellation gap.

`row_lock_multiwait.py` uses the same Oracle-only connection arguments as
`row_lock_deadlines.py`. Two independent row owners distinguish successful
successive waits from expiry on the second row. The current
[qualification](../../../docs/evidence/oracle-row-lock-deadlines-2026-11-03/README.md)
retains the timed source-cancellation gap from the five-case deadline fixture.

`current_time.py` uses the common connection arguments with `--reference oracle`
or `--reference postgres`. It compares typed current-time values and clock
properties in three session zones: 75 Oracle cases or 12 PostgreSQL controls.
It makes no schema/data changes. Oracle timezone-bearing values use original
typed Describe metadata plus a TO_CHAR ISO-offset value projection because Thin
mode cannot decode named region IDs directly. The
[bounded qualification](../../../docs/evidence/oracle-current-time-2026-11-03/README.md)
documents the precision, transport and application limits.

`postgres_type_relocation.py --reference postgres` exercises native function
signatures, rollback, execution and body literals across type moves/renames.
Provision `bicdb_type_moves_a` and `bicdb_type_moves_b` in both dedicated test
databases first. They remain fixture infrastructure; each run removes its nonce
functions and types. Schema removal is outside this fixture's qualification.
The [bounded comparison](../../../docs/evidence/sql-type-relocation-2026-11-03/README.md)
records the original schema-cleanup failures separately.

`raw_character_conversion.py` uses the common connection arguments with
`--reference oracle` or `--reference postgres`. Its 13 Oracle cases check the
binary result of `SYS_GUID`, implicit hexadecimal conversion in character
operations, ordinary text prefixes, volatility and a stored routine's original
32-character buffer. Thirteen independent PostgreSQL controls retain native BYTEA
encoding, length, null and empty-value behavior. The PostgreSQL reference needs
`pgcrypto` installed for its random-byte control. Nonce routines are removed.
These cases do not qualify RAW columns, all RAW-producing expressions, explicit
`TO_CHAR(RAW)`, or general LOB support.

`typed_between.py` uses the common connection arguments with `--reference oracle`
or `--reference postgres`. Sixteen cases exercise typed range boundaries,
character controls, NULL logic, relation scopes and stored routines. Oracle
uses explicit datetime bounds; PostgreSQL also checks unknown ISO literals.
The [bounded checkpoint](../../../docs/evidence/sql-typed-between-2026-11-04/README.md)
retains two pre-existing PostgreSQL volatile-evaluation differences: its CLI
returns nonzero while those cases remain unqualified. Nonce objects are removed.

`anonymous_host_transactions.py` uses the common connection arguments with
`--reference oracle`. Eight controls compare bound anonymous-block failures,
continuation of the caller transaction, explicit commits inside a block and
rollback of later uncommitted writes. Each run creates and removes its own
nonce table. This compares protocol submission; JDBC statement splitting and
LOB locators require separate qualification.


### Routine-local BLOB values

`temporary_blobs.py` compares 18 cases for BLOB initialization, assignment,
return values, temporary aliasing and self-append, named APPEND arguments,
GETLENGTH/ISTEMPORARY, NULLs and standalone empty destination errors. It creates
nonce-named functions in dedicated reference/target schemas and drops them.
Run with the same `BICDB_PG_DSN`, `--reference oracle`, `--oracle-config` and
`--output` arguments as the other Oracle comparisons. Persistent row locators,
LOB streaming and complete application qualification are outside this fixture.

### Persistent BLOB ownership source probe

`persistent_blob_ownership.py` runs native Oracle observations without requiring
a BicDB target. It isolates locator reads, SQL copies and APPEND errors after
row deletion, replacement, primary-key changes, commit and rollback. See the
[recorded source contract](../../../docs/evidence/oracle-persistent-blob-ownership-2026-11-05/README.md)
for the eight cases, reproduction command and remaining implementation scope.

`persistent_blob_acquisition.py` complements ownership checks with ordinary and
locked acquisition, origin commit/rollback, and later-transaction reuse. It is
also a native source-only probe. See the
[acquisition evidence](../../../docs/evidence/oracle-lob-read-versions-2026-11-05/README.md)
and [read-version layer contract](../../../docs/oracle-lob-read-versions.md).
