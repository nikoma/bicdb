# Deterministic function declarations

The development candidate matches **10/10 Oracle function-attribute contracts**
and **4/4 independent PostgreSQL controls**. A disposable application copy now
accepts **62/62 original routine definitions** through PostgreSQL drivers.

The preview compiler maps an unquoted DETERMINISTIC attribute in a function's
declaration header to the existing native IMMUTABLE declaration. Parameter names,
qualified type names, quoted identifiers and routine bodies retain their own
meaning. Reference cases cover numeric, fractional and NULL values, parameterless
functions, quoted parameters and replacement with a different result.

User-routine `pg_proc.provolatile` metadata is derived from the declaration AST:
IMMUTABLE is `i`, STABLE is `s`, and VOLATILE or the omitted default is `v`.
Procedures report `v`. Declaration words inside arguments and body literals do
not classify volatility. Native controls verify values, metadata and replacement
against PostgreSQL 18.4. Interpreter execution and optimizer qualification remain
separate concerns; this change preserves the declaration's promise.

Six reader checks cover legacy routines, newly declared deterministic routines,
current-reader metadata and reopening with separate processes. The prior reader
can execute the native representation, so this translation needs no new compiler
language or database-format marker. The metadata correction is qualified on the
current reader.

Reproduce the Oracle comparison:

```sh
python examples/oracle/reference/function_attributes.py \
  --oracle-config /private/reference.json --output /private/attributes.json
```

Set BICDB_PG_DSN privately to an Oracle-preview target with the same dedicated
fixture schema. The fixture creates and removes its own three small functions.
The [validation report](validation.json) records both reference comparisons,
the binary hash, reader checks and the unchanged application execution gaps.

Validation passes 1,349 Rust tests, with three existing ignores, and 58 Python
tests. All 47 retained reference-report families pass on the same candidate,
including the 19 hierarchy cases, separate native PostgreSQL controls and
character migration checks. Saved hierarchy views and routines retain their
reopen and older-reader fencing contracts. Formatting, whitespace, licenses and
the open-core dependency boundary pass. One compiler and active memory guards
keep build resources bounded.

Definition acceptance is not full schema or application acceptance. Five
read-only calls to the original collection function qualify on Oracle and match
**0/5** on BicDB: its collection return type is not registered for PostgreSQL
transport. Collection types, collection operations, Oracle regular expressions,
dictionary rows, control-flow statements, package APIs, schema constraints and
application workflows remain incomplete. Only one of 14 NULL-input procedure
diagnostics completes; those diagnostics are not source execution comparisons.
