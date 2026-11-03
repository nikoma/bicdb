# Dense procedural nested tables

This checkpoint extends the experimental `oracle-preview-v1` profile with
registered collection variables and parameters, constructors, COUNT, EXTEND,
indexed reads and indexed assignment. It does not qualify the complete application.

The runtime source is `577c17d87c428c7f25707aab20e6787758e840db`. The tested development binary
has SHA-256 `089ddacc751cf62860b8374e433fd9fe4a19ea2bbc9fcdfb1dfcd2090e5fe9ea` and is retained locally at
`/tmp/bicdb-dense-collection-qualified-ojwdw4d1/oracle_server`. It is unoptimized, with debug information and incremental
compilation disabled and one compiler job.

## Semantics and scope

The new routine compiler language is `bicdb_oracle_preview_v19`. Existing
version-18 definitions retain their compiler behavior; opening a database does
not silently upgrade them. Registered collection return types resolve in the
Oracle routine's declaring schema. Native PostgreSQL functions preserve their
own name-resolution rules, quoted type names, return declarations and domain
constraints. Saved custom-type return metadata is derived from the declaration
rather than an older, incorrectly case-folded cache field.

Collection constructors and methods distinguish an uninitialized collection,
an empty collection and NULL elements. EXTEND supports default extension, explicit
counts and copies of existing elements. Index conversion follows the qualified
PLS_INTEGER rounding rules. Invalid access and uninitialized collections retain
Oracle error numbers. Character element assignment checks the declared byte or
character capacity before changing the variable. Lexical declarations and query
aliases retain separate scope.

Extension is bounded by platform limits of 100,000 elements and an estimated
8 MiB of element storage. Exceeding these limits reports SQLSTATE 54000 before
allocating the copied array. These are platform resource limits, not a claim of
Oracle capacity parity. Sparse methods, DELETE, TRIM, FIRST/LAST, PRIOR/NEXT,
EXISTS, TABLE(collection), object elements, associative arrays, VARRAYs and
collection type replacement remain outside this contract.

## Independent qualification

The public fixtures are
[Oracle methods](../../../examples/oracle/reference/nested_table_methods.py),
[Oracle function results](../../../examples/oracle/reference/collection_function_results.py)
and [PostgreSQL routine types](../../../examples/oracle/reference/postgres_routine_types.py).
Their source engines establish expected behavior independently. Target queries
use PostgreSQL prepared execution; collection-result tests register standard
array loaders for the actual logical result OIDs.

The source comparisons cover 23 method cases, 7 function-result cases and 11
PostgreSQL controls. Eleven reader checks exercise the actual previous binary,
retain old compiler behavior, reject new compiler execution on the old reader,
and recover saved quoted native return identities without redefining functions.
The previous reader already supports format-5 SQL collection metadata; the new
procedural contract is fenced by compiler language 19.

Pure, registered Oracle value helpers are available to ordinary invokers.
Unregistered helper names and administrative database operations retain their
privilege checks. The regression suite exercises both allowed value evaluation
and denied administrative access.

## Application status

A disposable application copy can register the source's simple VARCHAR2 nested
table definition after an explicit fresh-create adaptation of its editioned DDL.
That adaptation does not implement editions or replacement semantics. The original
CSV function still fails on the Oracle TRIM/regular-expression expression. Its
five source-qualified calls remain 0/5 on the target at this checkpoint. Accepting
62/62 stored definitions is not evidence that those routines execute correctly.

Startup, complete schema objects and dictionary behavior, business workflows,
recovery, performance and the wider migration programme remain unqualified.
The retained application listener has not been upgraded by these disposable
checks. This checkpoint is development source and a locally tested binary, not
a published release or a verified application deployment.

## Validation

The [validation record](validation.json) records 1,372 passing Rust tests and
three existing ignores, 58 passing Python tests, and passing formatting, license,
repository-boundary and whitespace checks. Fifty retained reference reports pass
on the same binary, alongside the new procedural contracts and the earlier
15-case SQL collection, 7-case native domain transport and 6-check metadata reader
contracts. The separately tracked dictionary comparison remains 0/11 and the
known NVL/conversion gap batch remains 2/7.

The first full run exposed a national-character metadata regression: re-deriving
custom return identities classified Oracle NVARCHAR2 as an ordinary custom type.
The fix establishes the saved routine's language scope while reading metadata.
Regression tests cover both the Oracle physical VARCHAR contract and a native
domain named NVARCHAR2 under an Oracle caller scope.

All final build and reference guards completed normally with bounded compiler
concurrency and an 8 GiB required host-memory reserve. These are local checks;
GitHub CI, complete application operation and release qualification are separate.
