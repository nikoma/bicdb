# PostgreSQL 18 Data-Type Parity - Implementation Todo

Status: **complete**
Target: PostgreSQL 18.4 behavior over embedded SQL and pgwire  
Baseline: BicDB `0.9.4-beta`, commit `0472f01`  
Last updated: 2026-07-21

This is the authoritative execution list for PostgreSQL 18 data-type parity.
It drives the generated type-capability fixtures under
`fixtures/postgres-compat`. A checked item means
the behavior is implemented, covered by BicDB tests, and compared with a stock
PostgreSQL 18 oracle when PostgreSQL exposes the behavior.

## Active execution priority

The full-text-search block through DT-1205, the Cognee compatibility slice
DT-1251 through DT-1262, UUID/JSON/JSONB/jsonpath/XML through DT-0707, and
domains in DT-0902, shell and registered base types in DT-0903, and composite
table boundaries in DT-0807, durable user-type OID allocation in DT-0905, and
user-type management in DT-0906, canonical `inet` storage in DT-1001,
canonical `cidr` storage in DT-1002, and MAC address storage in DT-1003 are
complete. The network type family through DT-1006 and the geometric type family
through DT-1306, unsigned OID semantics in DT-1401, and catalog-reference alias
semantics in DT-1402, and transaction/system-column types in DT-1403 are
complete. `pg_lsn` semantics in DT-1404, snapshot semantics in DT-1405, and
supported pseudo-types in DT-1406 and their declaration boundaries in DT-1407,
and complete built-in and user-defined `pg_type` catalogs and dependencies in
DT-1501 and accurate `pg_attribute` type metadata in DT-1502 are complete.
`format_type`, `to_regtype`, and the type/routine information-schema surfaces in
DT-1503 are complete. PostgreSQL common-type selection in DT-1504 and the
PostgreSQL 18 cast catalog, contexts, and costs in DT-1505 are complete.
Type-aware statistics, histogram selectivity, sort/hash semantics, and
operator-class selection in DT-1506 and the complete ALTER COLUMN TYPE
lifecycle in DT-1507 and stock PostgreSQL 18 full, schema-only, and data-only
dump/restore round trips in DT-1508, multi-database type/OID isolation and
cluster-wide role behavior in DT-1509, generated compatibility evidence in
DT-1510, and the complete PostgreSQL 18 type classification in DT-1601 are
complete. The seven-gate container and user-defined family matrix in DT-1602 is
complete. Typed fallback rejection in DT-1603, the complete prepared/binary
client gate in DT-1604, and the zero-difference PostgreSQL 18 differential in
DT-1605, the full workspace acceptance gate in DT-1606, and the 0.9.34-beta
release milestone in DT-1607 are complete. PostgreSQL 18.4 data-type parity is
accepted for the documented, exercised surface.
The Cognee slice
remains an application-level regression gate
and must continue to run Cognee unchanged rather than accepting BicDB-specific
SQL or client codec workarounds.

The immediate application-compatibility regressions for UUID ACL joins, empty
ungrouped aggregates, and `pg_has_role` result OID/binary encoding are complete.
These cross-cutting regression gates do not add separate items to the
datatype-parity denominator. Named composite types in DT-0806 are complete;
enum types in DT-0901 and the typed range foundation in DT-1101 are complete.
Discrete range canonicalization in DT-1102, range operators/functions in
DT-1103, all six built-in multirange constructors in DT-1104, and cross-type
range/multirange operators and aggregates in DT-1105 are complete. Exclusion
constraints and GiST/SP-GiST-equivalent index semantics in DT-1106 are
complete. Range/multirange arrays, catalogs, dump contracts, wire protocols,
statistics, and planner selectivity in DT-1107 are complete. Arrays of every
registered base and user-defined type in DT-0801, complete array operations in
DT-0803, array integration in DT-0804, and composite table boundaries in
DT-0807 are complete. DT-0904 user-defined ranges and automatic multiranges and
DT-0905 durable collision-free user-type OIDs and DT-0906 ownership, schema
movement, privileges, comments, dependencies, and dump contracts are complete.
DT-1001 validated and canonical IPv4/IPv6 `inet` host+mask storage, DT-1002
validated and canonical `cidr` network storage, DT-1003 `macaddr` and
`macaddr8` storage and conversion, DT-1004 network operators, functions,
ordering, and aggregates, DT-1005 network-aware indexes, and DT-1006 scalar and
array text/binary pgwire codecs and geometric input/output, canonical storage,
planar operations and indexes, arrays, catalogs, COPY, dump contracts, and wire
codecs through DT-1306, unsigned OID semantics in DT-1401, and all eleven
catalog-reference aliases in DT-1402, and `xid`, `xid8`, `cid`, `tid`, and
system-column integration in DT-1403, and `pg_lsn` parsing, arithmetic, WAL
mapping, catalog, and protocol integration in DT-1404, and `pg_snapshot` and
`txid_snapshot` parsing, visibility, output, and protocol integration in
DT-1405, and PostgreSQL's supported pseudo-type registry, catalogs, regtype
lookups, and routine signatures in DT-1406, including PostgreSQL-compatible
declaration boundaries in DT-1407, and `pg_type` catalogs, physical metadata,
I/O functions, ownership, ACLs, defaults, links, and dependencies in DT-1501,
are complete. Accurate `pg_attribute` identity, typmods, dimensions, collation,
physical layout, and compression metadata in DT-1502 are complete. `format_type`,
`to_regtype`, domains, element types, structured UDTs, routines, parameters, and
domain-aware columns in DT-1503 are complete. Common-type selection across
conditional expressions, arrays, row sets, set operations, parameters, and
polymorphic array functions in DT-1504 and cast catalog and coercion context
integration in DT-1505 are complete. Type-aware statistics and planner
selectivity, complete built-in operator-class metadata, and catalog-driven
index operator-class selection in DT-1506 and ALTER COLUMN TYPE rewrites,
validation, rollback, partition propagation, generated columns, and dependency
checks in DT-1507 and bidirectional PostgreSQL 18 dump/restore interoperability
in DT-1508 and database-local user-type catalogs and OID allocation plus
cluster-wide transactional roles and memberships in DT-1509, generated
compatibility evidence in DT-1510, and the complete PostgreSQL 18 type
classification in DT-1601, and the seven-gate container and user-defined family
matrix in DT-1602, typed fallback rejection in DT-1603, the complete
prepared/binary client gate in DT-1604, and the zero-difference PostgreSQL 18
differential in DT-1605, the full workspace acceptance gate in DT-1606, and the
0.9.34-beta release milestone in DT-1607 are complete.
Progress: 151 of 151 items complete; 0 remain.

DT-1503 evidence: the PostgreSQL 18.4 type differential passes 84/84 fixtures
in three clean-state sequential shards with zero expected differences, including
the dedicated `083-information-schema-types.json` contract. The `bicdb-sql`
library suite passes 183 tests with 3 benchmark-only tests ignored.

DT-1504 evidence: the PostgreSQL 18.4 type differential passes 85/85 fixtures
in clean-state sequential shards of 40, 38, and 7 cases with zero expected
differences, including `084-common-type-selection.json`. The `bicdb-sql` library
suite passes 183 tests with 3 benchmark-only tests ignored. Focused parameter
inference and pgwire Parse/Describe tests prove PostgreSQL-compatible `int8` and
text resolution and parameter/result OIDs across CASE, COALESCE, arrays, VALUES,
UNION, and anycompatible array calls.

DT-1505 evidence: the PostgreSQL 18.4 type differential passes 86/86 fixtures
in clean-state sequential shards of 40, 38, and 8 cases with zero expected
differences, including the complete 235-row built-in `pg_cast` catalog and
dynamic user range-to-multirange casts in `085-cast-catalog.json`. The shared
cast registry drives implicit common-type selection and explicit cast
validation, models assignment and array coercion costs, and preserves
PostgreSQL SQLSTATE behavior. The `bicdb-sql` library suite passes 185 tests
with 3 benchmark-only tests ignored; the OID and type-consistency integration
suites pass 2/2 and 7/7.

DT-1506 evidence: the PostgreSQL 18.4 type differential passes 87/87 fixtures
in three clean-state sequential shards of 40, 38, and 9 cases with zero expected
differences. `086-type-statistics-opclasses.json` verifies typed statistics,
exact numeric sorting, grouping and histogram selectivity, default and explicit
index operator-class selection, `pg_index.indclass`, and matching failure
SQLSTATEs. The PostgreSQL 18.4 built-in catalogs match exactly: 177
`pg_opclass`, 146 `pg_opfamily`, 945 `pg_amop`, and 714 `pg_amproc` rows.
The `bicdb-sql` library suite passes 188 tests with 3 benchmark-only tests
ignored; type consistency, typed comparison/index behavior, exact decimal
storage, and OID semantics pass 7/7, 5/5, 2/2, and 2/2. The serial broad SQL
suite passes 351/362; its 11 failures reproduce on the DT-1505 `main` baseline
and remain explicit DT-1606 acceptance debt rather than DT-1506 regressions.

DT-1507 evidence: the PostgreSQL 18.4 type differential passes 88/88 fixtures
in clean-state sequential shards of 40, 38, and 10 cases with zero expected
differences. `087-alter-column-type-lifecycle.json` verifies assignment casts,
arbitrary row-local USING expressions, failed-rewrite atomicity, transaction
rollback, partition propagation and restrictions, index operator-class
revalidation, defaults, unique and foreign-key constraints, generated-column
INSERT/UPDATE/upsert behavior, user-defined targets, catalogs, and dependencies.
The `bicdb-sql` library suite passes 188 tests with 3 benchmark-only tests
ignored; focused type consistency, typed comparison, exact decimal, and OID
suites pass 7/7, 5/5, 2/2, and 2/2. The serial SQL integration suite passes
352/363 with exactly the 11 failures already present on the main baseline.

DT-1508 evidence: `scripts/pg18-dump-restore.sh` uses stock PostgreSQL 18.4
clients to create full, schema-only, and data-only custom archives from BicDB.
Full and split archives restore into clean BicDB databases, the full archive
restores into stock PostgreSQL 18.4, PostgreSQL re-dumps that database, and the
re-dump restores into another clean BicDB database. Every stage produces the
same 2,409-byte canonical result over scalar, exact numeric, temporal, JSON,
XML, network, geometric, full-text, range/multirange, enum, domain, composite,
OID, LSN, snapshot, transaction, system, and array families. The checked-in
fixtures are under `fixtures/postgresql-18/pg-dump`. The `bicdb-sql` library
suite passes 188 tests with 3 benchmark-only tests ignored; user-defined range
tests pass 8/8 and the two changed pgwire protocol tests pass. The PostgreSQL
18.4 type differential passes 88/88 fixtures in three clean-state sequential
shards of 40, 38, and 10 cases. The broad SQL suite improves from the main
baseline's 352/363 with 11 failures to 361/369 with the same 8 inherited
failures and no new failures; DT-1508 fixes the three baseline failures for
pg_dump access-method regproc output, `pg_get_partkeydef`, and
`pg_get_viewdef`.

DT-1509 evidence: the pgwire cluster integration suite passes 2/2. It proves
independent durable user-type catalogs and OID allocators across two physical
databases on one listener, including the same type name receiving
database-local OIDs, database-selected RowDescription OIDs, isolated DROP
behavior, and restart persistence. Roles, attributes, stable role OIDs, and
role memberships are copied into newly-created databases, propagated in both
directions for CREATE, ALTER, DROP, GRANT, and REVOKE, and reconciled from the
default database when a cluster reopens. Explicit transactions do not publish
uncommitted role changes; ROLLBACK and ROLLBACK TO SAVEPOINT remain invisible
to other databases, while COMMIT publishes the complete ordered change set.
Database-local object and type ACL grants are never propagated as shared role
membership DDL. The `bicdb-sql` library suite passes 188 tests with 3
benchmark-only tests ignored and the `bicdb-pgwire` library suite passes 46/46.

DT-1510 evidence: the checked-in PostgreSQL 18.4 type inventory contains 473
oracle catalog rows, explicitly distinguished from a count of general-purpose
column types. The reproducible clean-state differential runner aggregates
40-, 38-, and 10-case shards into an 88/88 zero-difference report. The generated
compatibility report and machine-readable evidence publish the exact roadmap
status, all fixture results, support boundary, and remaining acceptance gates.
The generated client matrix declares seven cross-target clients and one
BicDB-only driver regression; the cross-target gauntlet passes psql,
node-postgres 8.16.3, psycopg 3.2.9, SQLAlchemy 2.0.41, SQLx 0.8.6, PostgreSQL
JDBC 42.7.7, and libpq against both BicDB and PostgreSQL 18.4. Prisma and GUI
clients are not represented as passing automated coverage.

DT-1601 evidence: the generated classification accounts for all 473/473
PostgreSQL 18.4 `pg_type` oracle rows with zero unclassified rows. All 51/51
general-purpose roots are backed by BicDB's canonical type registry and passing
matrix evidence. PostgreSQL's four internal-only roots (`aclitem`, `gtsvector`,
`pg_brin_bloom_summary`, and `pg_brin_minmax_multi_summary`) have explicit,
approved rationales; their derived arrays inherit those classifications.
`refcursor`, the only uncovered general-purpose root, now has scalar and array
storage, catalog, cast, text/binary protocol, and unsupported comparison/index
parity. The PostgreSQL 18.4 differential passes 89/89 fixtures in clean-state
shards of 40, 38, and 11 with zero expected differences.

DT-1602 evidence: the generated acceptance matrix accounts for 42/42 gates
across arrays; anonymous records, table rows, and named composites; enums;
domains; shell and registered base types; and ranges/multiranges. Every family
passes DDL, DML, catalog, dependency, dump, restart, and protocol gates with no
not-applicable exceptions. The policy generator verifies every referenced
fixture, source file, and named test. Stock PostgreSQL 18.4 full, schema-only,
and data-only archives restore into BicDB; the full BicDB archive restores into
PostgreSQL, whose re-dump restores into a clean BicDB database with identical
canonical output. Registered base types retain numeric I/O routine OIDs and
custom array delimiters, table-row and named-composite arrays retain typed text
and binary values, and `ALTER FUNCTION ... OWNER TO` restores successfully.

DT-1603 evidence: the exhaustive registry/catalog/planner consistency suite
passes 7/7, typed comparison passes 5/5, exact and typed scalar storage pass
5/5, and the raw pgwire structured scalar and every-registered-array binary
matrices pass. Unknown result, parameter, and composite-field types now fail
explicitly instead of substituting OID 25. Binary `xid`, `cid`, and `xid8`
parameters retain their declared types through SQL substitution, and the
internal binary `"char"[]` envelope cannot be misclassified as a JSONB cast.

DT-1604 evidence: the registry-exhaustive prepared/binary scalar and array
protocol matrices pass before the external client run. The cross-target
gauntlet then passes psql, node-postgres 8.16.3, psycopg 3.2.9, SQLAlchemy
2.0.41, SQLx 0.8.6, PostgreSQL JDBC 42.7.7, and libpq against both BicDB and
PostgreSQL 18.4. The in-process tokio-postgres and SQLx prepared/binary
regressions also pass. Driver-native assertions cover each driver's supported
codecs, while byte-level raw protocol assertions cover every registered scalar
codec and its array codec. The generated client report records both exhaustive
protocol tests and refuses compiler concurrency above 10 jobs.

DT-1605 evidence: a fresh PostgreSQL 18.4 differential passes 89/89 fixtures in
three clean-state BicDB shards of 40, 38, and 11 cases with zero expected or
unapproved differences. The `pg_lsn` fixture now uses state-independent type
assertions for receive/replay positions because PostgreSQL can retain a replay
LSN after promotion. BicDB's database-aware query typer reports PostgreSQL's
`pg_lsn` OID 3220 for `pg_lsn` plus or minus numeric offsets and numeric OID
1700 for `pg_lsn` subtraction. The differential runner refuses compiler
concurrency above 10 jobs.

DT-1606 evidence: the serialized workspace acceptance run passes every suite
through 369/369 broad SQL tests and 148/148 pgwire protocol tests; the sole
late failure was a stale IPv6 display expectation corrected to PostgreSQL's
canonical `2001:db8::1/64`, whose focused migration suite and all downstream
package tails then pass. Clippy passes under the repository warning policy,
format and packaging gates pass, crash/reopen, snapshot, WAL, backup,
replication, and sync recovery suites pass, and the PostgreSQL 18 differential
remains clean at 89/89. The optimized WASI build passes, real Chromium passes
the browser OPFS, encryption, locking, compaction, attachment, sync, and RLS
matrix, macOS desktop and production configuration validators pass, and every
verification command is capped at 10 compiler jobs.

Guardrail evidence: repository-local Cargo defaults to 10 build jobs, every
parity and Cognee runner rejects larger values, and all final verification ran
with serialized tests. Legacy record, WAL, segment, snapshot, backup, and sync
fixtures pass; no durable format changed in the final acceptance series. Exact
numeric, temporal, UUID, and binary storage suites pass without lossy
conversion. Registry/catalog/planner consistency and explicit fallback
rejection pass, the generated 89-fixture differential needs no output repair,
and DT-1603 through DT-1606 were committed and pushed as independent
milestones.

DT-1607 evidence: every BicDB crate and internal path requirement is versioned
0.9.34-beta, the release notes and generated compatibility evidence are
published, and workspace check and package gates pass with the updated lockfile.
GitHub Actions run 29858581502 completed successfully on an Apple Silicon
`macos-14` runner: it built arm64 BicDB and integration release binaries,
passed the native Swift tests and binary smoke tests, packaged and verified the
signed application plus DMG, PKG, and ZIP, validated SHA-256 checksums, and
uploaded the 329 MB `bicdb-0.9.34-beta-macos-arm64` artifact. Cargo and Swift
compiler fan-out were capped at 10 throughout.

## Definition of done

A type is not considered supported merely because its name parses. Complete
support requires all applicable items below:

- DDL preserves the declared type, schema qualification, dimensions, collation,
  and type modifiers.
- Input functions validate and canonicalize values with PostgreSQL SQLSTATEs.
- Assignment, implicit, explicit, and binary coercions match PostgreSQL.
- Equality, ordering, hashing, grouping, DISTINCT, joins, and indexes use the
  type's semantics rather than generic string or JSON comparison.
- Defaults, generated values, primary/foreign/unique keys, COPY, arrays,
  functions, procedures, triggers, and restart/recovery preserve the value.
- `pg_type`, `pg_attribute`, `format_type`, information schema, and dumps report
  the same logical type and typmod.
- pgwire advertises the correct OID and supports PostgreSQL text and binary
  parameter/result formats.
- Unsupported operations fail explicitly; they never silently fall back to
  `text` OID 25.

## Guardrails

- [x] DT-0001 Keep `CARGO_BUILD_JOBS` at 10 or less for all verification.
- [x] DT-0002 Preserve backward reads of existing BicDB records, WAL, segments,
      snapshots, and sync bundles.
- [x] DT-0003 Version every durable encoding change and add upgrade fixtures.
- [x] DT-0004 Keep exact values exact: no decimal-to-`f64`, timestamp-to-local
      time, UUID-to-lossy-string, or binary-to-Unicode conversion.
- [x] DT-0005 Use one canonical type registry for DDL, casts, catalogs, planner,
      indexes, COPY, and pgwire; no duplicated OID/type-name match tables.
- [x] DT-0006 Add no generated-output rewrites or post-generation repair scripts.
- [x] DT-0007 Commit and push each completed milestone independently.

## P0 - Truth, oracle, and single type registry

- [x] DT-0101 Introduce a canonical `PgTypeRegistry` keyed by stable type ID,
      qualified name, aliases, OID, array OID, category, length, alignment,
      storage, collation, element type, range subtype, and wire codecs.
- [x] DT-0102 Replace SQL DDL's hard-coded scalar mapping with the registry.
- [x] DT-0103 Replace `pg_type` and array catalog hard-coded lists with registry
      rows.
- [x] DT-0104 Replace pgwire's independent `oid_for_type_name` table with the
      registry.
- [x] DT-0105 Make unknown result types an internal error instead of OID 25.
- [x] DT-0106 Model PostgreSQL typmods for numeric, character, bit, temporal,
      interval, and vector declarations.
- [x] DT-0107 Preserve declared type and typmod in `ColumnSchema`, ALTER TABLE,
      LIKE, partitions, views, functions, and procedure signatures.
- [x] DT-0108 Add a typed scalar codec interface: parse, canonical text, binary
      decode/encode, cast, compare, hash key, and index key.
- [x] DT-0109 Add PostgreSQL SQLSTATE helpers for invalid text, numeric range,
      string truncation, datetime, invalid parameter, and undefined type errors.
- [x] DT-0110 Add a generated PG18 type inventory fixture sourced from the
      PostgreSQL 18 oracle catalogs.
- [x] DT-0111 Add a differential DDL/input/output/cast matrix against
      `postgres:18.4`.
- [x] DT-0112 Add a pgwire text/binary matrix using prepared parameters and
      binary results for every supported scalar and array type.
- [x] DT-0113 Add restart, WAL recovery, backup/restore, replication, and sync
      round trips for typed values.
- [x] DT-0114 Add catalog consistency tests proving registry, `pg_type`,
      `pg_attribute`, planner result types, and pgwire OIDs agree.
- [x] DT-0115 Replace the stale unsupported-types fixture with capability-specific
      expected differences generated from this checklist.

## P1 - Typed value and durable-storage plumbing

- [x] DT-0201 Define canonical in-memory representations for signed integers,
      float4, float8, exact numeric, money, text, bytes, bit strings, temporal
      values, UUID, network values, ranges, arrays, composites, and snapshots.
- [x] DT-0202 Keep the existing generic `SqlValue` API source-compatible while
      carrying logical type information through expressions and result rows.
- [x] DT-0203 Add exact decimal storage preserving value and display scale.
- [x] DT-0204 Add typed date, time, timetz, timestamp, timestamptz, and interval
      canonical encodings independent of host timezone and locale.
- [x] DT-0205 Add typed byte and bit encodings without JSON-array or UTF-8
      intermediates.
- [x] DT-0206 Add array dimensions and lower bounds; do not represent a
      PostgreSQL array as an untyped JSON array.
- [x] DT-0207 Add canonical network, range/multirange, geometric, OID-alias, LSN,
      and snapshot encodings.
- [x] DT-0208 Add schema-aware compare/hash/index encoders for every type.
- [x] DT-0209 Ensure primary, unique, foreign, exclusion, GROUP BY, DISTINCT,
      UNION, joins, and ORDER BY call typed comparison.
- [x] DT-0210 Add versioned record metadata encoding for types JSON cannot
      preserve exactly.
- [x] DT-0211 Add migrations and golden fixtures for records written before the
      typed encoding.
- [x] DT-0212 Preserve typed values through event streams, audit events, browser
      sync, WASM, Arrow export, and backup formats.

## P2 - pgwire protocol parity

- [x] DT-0301 Advertise exact scalar and array OIDs from planner result types.
- [x] DT-0302 Implement PostgreSQL binary parameters/results for bool, int2,
      int4, int8, float4, float8, numeric, and money.
- [x] DT-0303 Implement binary parameters/results for date, time, timetz,
      timestamp, timestamptz, and interval.
- [x] DT-0304 Implement binary parameters/results for text families, bytea,
      bit, varbit, UUID, JSON, JSONB, XML, and jsonpath.
- [x] DT-0305 Implement binary parameters/results for inet, cidr, macaddr,
      macaddr8, ranges, multiranges, geometric types, OID aliases, pg_lsn, and
      snapshots.
- [x] DT-0306 Implement binary array results and multidimensional binary array
      parameters/results with nulls and lower bounds.
- [x] DT-0307 Preserve parameter OIDs across Parse/Describe/Bind/Execute and
      infer unknown parameters using PostgreSQL coercion rules.
- [x] DT-0308 Return correct `typlen`, typmod, table OID, and attribute number in
      RowDescription.
- [x] DT-0309 Test psql, node-postgres, SQLx, tokio-postgres, psycopg, JDBC, and
      libpq in both text and binary modes.

## P3 - Numeric, Boolean, and sequence families

- [x] DT-0401 Enforce int2 range `-32768..32767` on input, casts, arithmetic,
      assignments, COPY, defaults, and binary output.
- [x] DT-0402 Enforce int4 range `-2147483648..2147483647` on the same paths.
- [x] DT-0403 Preserve full int8 behavior and PostgreSQL overflow errors.
- [x] DT-0404 Match integer division, modulo, unary operations, promotion,
      aggregates, rounding, and overflow semantics.
- [x] DT-0405 Store float4 at IEEE-754 binary32 precision and preserve float8 as
      binary64.
- [x] DT-0406 Match NaN, positive/negative infinity, signed zero, ordering,
      equality, casts, and text output for floating types.
- [x] DT-0407 Enforce numeric precision/scale, including negative scale and scale
      greater than precision allowed by PostgreSQL 18.
- [x] DT-0408 Match unconstrained numeric limits, rounding, NaN, infinity,
      arithmetic, comparison, aggregates, and display scale.
- [x] DT-0409 Implement `money` storage, locale-independent input tests,
      arithmetic, casts, output, and wire codec.
- [x] DT-0410 Complete boolean accepted spellings, casts, three-valued logic,
      aggregate, COPY, and protocol tests.
- [x] DT-0411 Complete smallserial/serial/bigserial and generated identity range,
      ownership, dependency, restart, and overflow behavior.

## P4 - Character, collation, binary, and bit strings

- [x] DT-0501 Preserve `text`, `varchar(n)`, `character varying(n)`, `char(n)`,
      and `character(n)` as distinct declarations.
- [x] DT-0502 Enforce varchar/character length in characters with PostgreSQL's
      explicit-cast truncation rules.
- [x] DT-0503 Implement fixed-character blank padding and comparison semantics.
- [x] DT-0504 Implement database, column, expression, and index collations with
      deterministic catalog identities.
- [x] DT-0505 Match Unicode, NUL rejection, normalization neutrality, pattern
      operators, and locale-sensitive ordering.
- [x] DT-0506 Complete `name` and internal `"char"` semantics used by catalogs.
- [x] DT-0507 Store `bytea` as bytes and complete hex/escape input/output,
      comparisons, functions, COPY, and indexes.
- [x] DT-0508 Implement `bit(n)` exact-length semantics, literals, operators,
      casts, indexing, and wire codec.
- [x] DT-0509 Implement `bit varying(n)`/`varbit(n)` maximum-length semantics,
      operators, casts, indexing, and wire codec.

## P5 - Date, time, and interval families

- [x] DT-0601 Implement strict PostgreSQL date parsing, validation, arithmetic,
      Julian/Gregorian behavior, BC dates, infinity, and canonical output.
- [x] DT-0602 Complete `CURRENT_DATE`, transaction timestamp stability, timezone
      handling, COPY, indexing, and binary protocol tests.
- [x] DT-0603 Implement `time(p)` precision, parsing, rounding, 24:00:00, and
      arithmetic.
- [x] DT-0604 Implement `timetz(p)` offset storage, comparison, conversion, and
      wire semantics.
- [x] DT-0605 Implement `timestamp(p)` precision, parsing, BC/infinity, arithmetic,
      extraction, truncation, and binary semantics.
- [x] DT-0606 Implement `timestamptz(p)` UTC storage, session timezone rendering,
      DST rules, named zones, BC/infinity, and binary semantics.
- [x] DT-0607 Implement interval fields/precision typmods, year-month-day-time
      representation, normalization, style settings, arithmetic, comparison,
      extraction, and binary semantics.
- [x] DT-0608 Add oracle tests across DST gaps/folds, leap years, timezone changes,
      infinity, epochs, and precision boundaries.

## P6 - UUID, JSON, JSONB, jsonpath, and XML

- [x] DT-0701 Validate every UUID text assignment/cast and canonicalize output.
- [x] DT-0702 Verify UUIDv4/v7 generation, ordering, extraction, indexing, binary
      protocol, restart, and uniqueness against PostgreSQL 18.
- [x] DT-0703 Make `json` preserve exact input text, whitespace, key order, and
      duplicate keys while still validating syntax.
- [x] DT-0704 Complete JSON operators, functions, aggregates, subscripting,
      casts, Unicode behavior, COPY, and wire semantics.
- [x] DT-0705 Complete JSONB numeric limits, canonicalization, containment,
      existence, deletion, concatenation, subscripting, functions, aggregates,
      equality/order, and GIN-compatible indexing behavior.
- [x] DT-0706 Implement native `jsonpath`, SQL/JSON path parsing, strict/lax
      modes, predicates, variables, datetime methods, operators, and indexing.
- [x] DT-0707 Implement `xml` validation, constructors, predicates, table
      functions, casts, encoding rules, and wire format.

## P7 - Arrays and composite values

- [x] DT-0801 Support arrays of every registered base, enum, domain, composite,
      range, and user-defined type.
- [x] DT-0802 Preserve arbitrary dimensions, lower bounds, empty dimensions,
      null elements, and rectangular-shape validation.
- [x] DT-0803 Complete array literals, constructors, subscripts, slices,
      assignment, concatenation, comparison, containment, overlap, and functions.
- [x] DT-0804 Complete `ANY`, `ALL`, `UNNEST`, `WITH ORDINALITY`, variadic
      functions, FOREACH/SLICE, aggregates, COPY, and indexes.
- [x] DT-0805 Implement anonymous `record` values and table row types.
- [x] DT-0806 Implement named composite types with CREATE/ALTER/DROP TYPE,
      constructors, field access, assignment, casts, arrays, dependencies, and
      wire codecs.
- [x] DT-0807 Preserve composite constraints at table boundaries and match
      PostgreSQL's standalone composite behavior.

## P8 - Enums, domains, and extensible types

- [x] DT-0901 Implement CREATE/ALTER/DROP TYPE AS ENUM with stable ordering,
      rename/add-before/add-after, comparison, indexes, arrays, catalogs, dumps,
      dependencies, and wire OIDs.
- [x] DT-0902 Implement CREATE/ALTER/DROP DOMAIN with base typmod, defaults,
      NOT NULL, CHECK constraints, collation, arrays, casts, dependencies, and
      domain-specific error reporting.
- [x] DT-0903 Implement shell and user-defined base types with registered
      input/output/receive/send functions and safe extension boundaries.
- [x] DT-0904 Implement CREATE TYPE AS RANGE and automatic multirange type
      creation.
- [x] DT-0905 Allocate and persist stable user-type OIDs without collisions
      across restart, backup, replication, and multi-database clusters.
- [x] DT-0906 Complete type ownership, schema movement, privileges, comments,
      dependency tracking, DROP RESTRICT/CASCADE, and pg_dump round trips.

## P9 - Network address types

- [x] DT-1001 Implement validated/canonical `inet` IPv4/IPv6 host+mask storage.
- [x] DT-1002 Implement validated/canonical `cidr` network storage and reject
      host bits with PostgreSQL errors.
- [x] DT-1003 Implement `macaddr` and `macaddr8` accepted formats,
      canonicalization, conversion, and validation.
- [x] DT-1004 Complete network comparison, containment, bitwise, arithmetic,
      extraction, conversion, aggregate, and formatting functions.
- [x] DT-1005 Add network-aware B-tree/hash/index keys and correct selectivity.
- [x] DT-1006 Complete scalar/array text and binary pgwire codecs.

## P10 - Ranges and multiranges

- [x] DT-1101 Replace string-backed int4range/int8range/numrange/tsrange/
      tstzrange/daterange with typed bounds, inclusivity, infinity, empty state,
      and subtype metadata.
- [x] DT-1102 Implement PostgreSQL canonicalization for discrete ranges.
- [x] DT-1103 Complete range constructors, input/output, comparison, containment,
      overlap, adjacency, union, intersection, difference, merge, and bound
      functions.
- [x] DT-1104 Implement all six built-in multirange types and constructors.
- [x] DT-1105 Complete range/multirange cross-type operators and aggregates.
- [x] DT-1106 Complete exclusion constraints and GiST/SP-GiST-equivalent index
      behavior for supported access methods.
- [x] DT-1107 Complete arrays, catalogs, dumps, text/binary protocol, statistics,
      and planner selectivity for ranges and multiranges.

## P11 - Full-text search types

- [x] DT-1201 Implement canonical tsvector lexemes, positions, weights, parsing,
      output, concatenation, stripping, length, and comparison.
- [x] DT-1202 Implement tsquery AST, parsing, normalization, boolean/phrase
      operators, prefix and weight matching, output, and comparison.
- [x] DT-1203 Complete dictionaries, configurations, parsers, templates,
      `to_tsvector`, `to_tsquery`, plainto/phraseto/websearch, headline, rank,
      and query rewrite functions.
- [x] DT-1204 Add GIN/GiST-equivalent indexes, expression indexes, statistics,
      planner selectivity, and maintenance.
- [x] DT-1205 Complete arrays, catalogs, dumps, COPY, and text/binary wire codecs.

## P11A - Cognee PostgreSQL compatibility slice

This slice tracks the requirements in
an external compatibility report, which audited
BicDB `0.9.3-beta`. A 2026-07-17 audit of this branch found that timestamptz
identity/codecs, array codecs, JSONB SRF infrastructure, and pgvector distance
execution have advanced since that baseline, but the exact Cognee paths still
need the gates below. Existing generic parity items remain authoritative for
the complete PostgreSQL feature; these items prove the Cognee application
contract without duplicating its implementation.

- [x] DT-1251 Replace advisory-lock no-ops with a database-scoped lock manager
      implementing bigint and two-int key spaces, session/xact/shared/try/
      blocking/reentrant semantics, disconnect and transaction cleanup,
      deadlock errors, and exact prepared parameter/result OIDs.
- [x] DT-1252 Complete DT-0901 and DT-0905 for Cognee's `pipelinerunstatus` and
      `syncstatus`: transactional persistent enum DDL, declaration-order
      comparison, validation, dependencies, companion arrays, stable OIDs,
      `pg_type`/`pg_enum`, reflection, reopen, backup, and asyncpg codecs.
- [x] DT-1253 Complete contextual prepared-array inference for casts, DML target
      columns, either side of ANY/ALL, constructors, containment, polymorphic
      functions, parallel `unnest`, CASE, and upsert expressions; assert exact
      text/varchar array OIDs and text/binary round trips through asyncpg.
- [x] DT-1254 Prove timestamp and timestamptz remain distinct through every
      prepared parameter/result path, with OIDs 1114 and 1184 respectively;
      run Cognee's timezone-aware SQL cache TTL lifecycle through asyncpg.
- [x] DT-1255 Add PostgreSQL-compatible `jit` GUC behavior (`off`) for SHOW,
      `current_setting`, and `set_config`, including session/transaction scope
      and correct missing-GUC SQLSTATEs, then pass asyncpg's unmodified recursive
      type-discovery query with an `oid[]` bind.
- [x] DT-1256 Preserve quoted identifier identity across relation resolution,
      schema/catalog keys, ownership, ACLs, grants, errors, reopen, and
      reflection for tables, columns, indexes, constraints, schemas, types, and
      sequences; never case-fold an already-resolved quoted name.
- [x] DT-1257 Complete polymorphic array mutation/query functions in the common
      expression evaluator, including `array_append`, prepend, cat, remove,
      replace, position(s), cardinality, dimensions, bounds, and length, in
      SELECT/DML/CASE/default/RETURNING/upsert contexts with atomic concurrent
      Cognee provenance merges.
- [x] DT-1258 Complete the Cognee pgvector contract: prepared text/binary vector
      codecs, persisted database-local type identity and typmod, dimension
      errors, `<=>`, `<->`, `<#>`, and `<+>` in projections/aliases/order/CTEs/
      joins, quoted PascalCase collections, SQLAlchemy reflection, and
      differential ordering against PostgreSQL plus pgvector.
- [x] DT-1259 Fix JSON/JSONB set-returning relation and alias semantics so bare
      table aliases, optional column aliases, correlated access, ANY/ALL
      filtering, and aggregation receive scalar values; pass Cognee's complete
      belongs-to-set select/update/delete flow.
- [x] DT-1260 Complete grouped HAVING after aggregate evaluation, including
      `COUNT(DISTINCT expression)`, derived-table columns, parameters, and CTEs
      referenced more than once; pass Cognee's AND nodeset-subgraph query.
- [x] DT-1261 Add one differential PostgreSQL 18 + pgvector fixture covering all
      ten Cognee areas, exact SQLSTATE and failed-transaction recovery, catalog
      identity, prepared OIDs, reopen, and two-connection concurrency.
- [x] DT-1262 Run Cognee v1.4.0 unchanged with PostgreSQL graph, cache, and
      pgvector backends: migrations twice; node/edge provenance; arrays;
      nodeset OR/AND; JSONB tag removal; cache TTL and locks; vector search;
      cascade deletes; restart and introspection. No BicDB-specific query,
      serialized-array, fake-enum, no-op-lock, or client-codec workaround.
      Gate: `COGNEE_DIR=/path/to/cognee-v1.4.0 scripts/cognee-v140-e2e.sh`.

## P12 - Geometric types

- [x] DT-1301 Implement PostgreSQL `point` independently from BicDB geographic
      geometry.
- [x] DT-1302 Implement `line`, `lseg`, `box`, `path`, `polygon`, and `circle`
      input/output and canonical storage.
- [x] DT-1303 Complete geometric equality, ordering where defined, containment,
      intersection, distance, transformation, and accessor operators/functions.
- [x] DT-1304 Implement geometric indexes and planner integration.
- [x] DT-1305 Complete arrays, catalogs, COPY, dump, and text/binary wire codecs.
- [x] DT-1306 Document and test the boundary between PostgreSQL planar geometry,
      BicDB Spatial geographic geometry, and optional PostGIS compatibility.

## P13 - Object identifiers, LSNs, snapshots, and pseudo-types

- [x] DT-1401 Enforce unsigned 32-bit `oid` semantics and symbolic lookup/output.
- [x] DT-1402 Complete regclass, regcollation, regconfig, regdictionary,
      regnamespace, regoper, regoperator, regproc, regprocedure, regrole, and
      regtype lookup, qualification, overload resolution, output, and dependency
      semantics.
- [x] DT-1403 Implement xid, xid8, cid, and tid representations and system-column
      integration where BicDB exposes equivalent concepts.
- [x] DT-1404 Implement `pg_lsn` parsing, arithmetic, comparison, WAL mapping,
      catalogs, and protocol.
- [x] DT-1405 Implement `pg_snapshot` and compatibility `txid_snapshot` parsing,
      visibility functions, output, and protocol.
- [x] DT-1406 Complete supported pseudo-types: record, void, trigger,
      event_trigger, cstring, internal, language_handler, fdw_handler,
      table_am_handler, index_am_handler, tsm_handler, and polymorphic any-types.
- [x] DT-1407 Reject pseudo-types in invalid column/argument/return positions with
      PostgreSQL errors.

## P14 - Catalogs, DDL lifecycle, planner, and tooling

- [x] DT-1501 Make `pg_type` complete for built-ins and user types, including
      typtype, category, preferred type, element/array/range links, basetype,
      typmod, collation, I/O functions, ACL, owner, and dependencies.
- [x] DT-1502 Make `pg_attribute.atttypid`, `atttypmod`, `attndims`, collation,
      alignment, storage, and compression accurate.
- [x] DT-1503 Complete `format_type`, `to_regtype`, information_schema domains,
      element types, UDTs, routines, parameters, and columns.
- [x] DT-1504 Implement PostgreSQL common-type selection for CASE, COALESCE,
      arrays, VALUES, UNION/INTERSECT/EXCEPT, parameters, and polymorphic calls.
- [x] DT-1505 Implement assignment/implicit/explicit cast catalogs and costs;
      remove ad-hoc conversion decisions.
- [x] DT-1506 Add type-aware statistics, selectivity, sort support, hash support,
      and index operator-class selection.
- [x] DT-1507 Complete CREATE/ALTER TABLE type changes with USING, validation,
      rewrite/no-rewrite decisions, rollback, partitions, indexes, constraints,
      generated columns, and dependencies.
- [x] DT-1508 Complete pg_dump/pg_restore and schema-only/data-only round trips
      for every type family.
- [x] DT-1509 Complete multi-database type/OID isolation and shared-role behavior.
- [x] DT-1510 Update compatibility reports, README, client matrix, and published
      support guarantees from generated evidence.

## P15 - Final PostgreSQL 18 acceptance gate

- [x] DT-1601 Every PostgreSQL 18 general-purpose type either passes the full
      parity matrix or has an explicit, approved out-of-scope rationale.
- [x] DT-1602 Every container and user-defined family passes DDL, DML, catalog,
      dependency, dump, restart, and protocol gates.
- [x] DT-1603 No supported type falls back to text storage, text comparison, or
      OID 25 without that being PostgreSQL's behavior.
- [x] DT-1604 Stock psql and all declared client drivers pass prepared/binary
      round trips for every supported type.
- [x] DT-1605 PostgreSQL 18 differential suite reports no unapproved differences.
- [x] DT-1606 Full workspace tests, clippy policy, format policy, crash recovery,
      replication, backup, WASM/browser, and packaging gates pass.
- [x] DT-1607 Bump the BicDB patch version, publish release notes and compatibility
      evidence, build macOS arm64 artifacts, and push the final release milestone.
