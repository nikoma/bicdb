# Oracle HR SQL profile over PostgreSQL wire

`oracle-hr-v1` is an opt-in front end for an explicitly adapted version of
Oracle's published Human Resources sample. Applications use ordinary
PostgreSQL drivers and native PostgreSQL result OIDs/codecs. No Oracle driver,
Oracle Net listener or Oracle Database installation is required to run this
example on BicDB.

This is a first schema acceptance, not general Oracle Database compatibility.
It has not been differentially certified against Oracle Database. In
particular, its transaction, locking, error and procedural execution contracts
remain BicDB's documented PostgreSQL-shaped contracts.

## Provision and connect

Build and provision a new, dedicated standalone database:

```sh
cargo build --locked -p bicdb-cli
target/debug/bicdb serve /tmp/new-bicdb-oracle-hr --port 5440 \
  --sql-profile oracle-hr-v1
```

The selected non-default profile is persisted in the database's internal
`__bicdb_sql_compatibility` collection. Reopening it as `postgres` fails. Existing
SQL databases and user collections cannot be silently converted by this flag.
The default profile is `postgres`, with the existing parser and SQL behavior.
The first Oracle profile is not enabled for multi-database `serve --cluster`.

Every connection must explicitly select the matching profile through the
standard PostgreSQL startup `options` field:

```python
import psycopg

with psycopg.connect(
    "host=127.0.0.1 port=5440 user=bicdb dbname=bicdb",
    options="-c bicdb.sql_profile=oracle-hr-v1",
    autocommit=True,
) as connection:
    print(connection.execute("SELECT DUMMY FROM DUAL").fetchone())
```

The server reports `bicdb_sql_profile` in ParameterStatus and exposes the
read-only `current_setting('bicdb.sql_profile')`. `SET`, `RESET` and `set_config`
cannot switch it. `RESET ALL` preserves it. An omitted, mismatched or unknown
profile cannot silently select Oracle semantics on a PostgreSQL database.
Use separate pools for the two database profiles.

For the Rust tokio-postgres driver:

```rust,ignore
config.options("-c bicdb.sql_profile=oracle-hr-v1");
```

Oracle `NUMBER` maps to PostgreSQL `numeric`. Rust integer values therefore
need an explicit compatible parameter type with `prepare_typed`, or a driver
numeric representation. This is the ordinary PostgreSQL typed-bind contract;
the profile does not change wire encodings to accept mismatched Rust types.

## Run the first schema

Install psycopg in your chosen Python environment, then run:

```sh
python3 examples/oracle/hr/prepare.py --check
python3 examples/oracle/hr/run.py \
  'host=127.0.0.1 port=5440 user=bicdb dbname=bicdb'
```

The loader uses an empty schema, creates seven related tables, indexes,
constraints, three sequences, the six-table `EMP_DETAILS_VIEW`, and the
`ADD_JOB_HISTORY` procedure. It loads 216 source rows: 5 regions, 25 countries,
23 locations, 27 departments, 19 jobs, 107 employees and 10 job-history rows.
The view returns 106 rows. The procedure acceptance adds one valid history row
and verifies that a rejected call does not add another. It also checks a
parameterized employee query and Oracle-style scalar expressions.

The loader does not drop existing tables. Re-running it against a populated
database fails at object creation; choose another new database for a new run.
The JSON result records the upstream revision, row counts, adaptations and
driver version.

The source is pinned to Oracle's MIT-licensed
[sample schema repository](https://github.com/oracle-samples/db-sample-schemas/tree/6660bad68c07bd143430ace58565b3f727e17263/human_resources).
Unmodified source files and their license are retained under
`examples/oracle/hr/upstream/`; `upstream.json` records SHA-256 hashes.
`prepare.py` verifies those hashes and produces a reproducible migration with
these explicit adaptations:

- Map the COUNTRIES index-organized table to native heap storage.
- Use BicDB's read-only view execution contract in place of `WITH READ ONLY`.
- Use the fixed English date formats in the sample instead of its
  `ALTER SESSION SET NLS_LANGUAGE` command.
- Create and validate the cyclic department-manager foreign key after loading,
  instead of disabling and enabling it around the data load.
- Import `ADD_JOB_HISTORY`; defer `SECURE_DML`, its disabled statement trigger,
  and `UPDATE_JOB_HISTORY`. Those objects are not claimed to execute here.

SQL*Plus client commands are handled by the pinned sample preparation tool,
not by the server SQL parser. Send each Oracle procedural block as a separate
driver statement. The sample's objects occupy the dedicated database's native
public namespace; Oracle user/schema/synonym resolution is not implemented.

## Implemented front-end subset

The front end tokenizes source with the Oracle lexer and lowers a bounded
subset into native statements before parameter inference, Describe and
execution. The ordinary PostgreSQL parser and wire codecs remain unchanged.
Stored procedures persist their lowered native definitions, so their bodies
are not reinterpreted under a caller-selected SQL dialect.

The covered subset includes ordinary HR DDL/DML, parenthesized ALTER ADD
constraint lists, explicit primary-key target resolution for REFERENCES,
NUMBER/NUMBER(p,s), VARCHAR2, DATE mapped to timestamp(0), English sample
TO_DATE calls, NOCACHE/NOCYCLE, unqualified sequence NEXTVAL/CURRVAL from DUAL,
the DUAL dummy row, empty character literals/parameters, same-type NVL,
null-tolerant scalar concatenation, anonymous INSERT blocks, and the sample's
procedure with table-column `%TYPE` parameters. Native authorization and
constraint checks still apply to every lowered operation.

The profile is deliberately incomplete:

- Oracle precision/exponent rules for unconstrained NUMBER, byte-length
  VARCHAR2 semantics, national character sets and full NLS behavior are not
  implemented. These types use their documented native mappings.
- Mixed-type NVL conversions, general implicit conversion and complete Oracle
  built-in function semantics are not established.
- Procedural concatenation is rejected. General PL/SQL, packages, mutable
  package state, autonomous transactions, Oracle triggers and supplied DBMS/UTL
  packages are not implemented.
- Sequence projections that also refer to DUAL's row are rejected.
- Oracle DDL implicit commits, statement-error continuation, blocking-lock
  semantics, Oracle SERIALIZABLE and Oracle error numbers are not provided.
  Use autocommit for this schema import and the native transaction contract for
  explicitly tested application operations.
- Cross-schema `%TYPE`, synonyms, database links, Oracle catalogs, Oracle
  protocol drivers and distributed Oracle SQL are not qualified.
- Arbitrary Oracle applications, performance at scale and production migration
  compatibility have not been established by this sample.

## Validation

```sh
cargo test --locked -p bicdb-sql --lib oracle::tests
cargo test --locked -p bicdb-sql --test prepared_typed_join
cargo test --locked -p bicdb-pgwire --test oracle_hr
python3 -m unittest discover -s examples/oracle/hr -p 'test_*.py'
```

The pgwire acceptance uses a real tokio-postgres connection to import the
sample, query data, exercise typed/binary parameters, preserve the time in a
DATE-mapped column, use a sequence and invoke the stored procedure. A separate
PostgreSQL database verifies unchanged empty-string/concatenation behavior and
transactional DDL rollback. Startup mismatches and profile switching are
negative controls. Full PostgreSQL suites remain release gates.

The sample also exposed a native prepared-index join defect: typed numeric
probes did not use the stored index's typed encoding. The shared executor now
uses the same conversion as ordinary predicates. `prepared_typed_join` checks
the PostgreSQL result before and after adding a numeric index.
