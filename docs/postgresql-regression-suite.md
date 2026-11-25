# PostgreSQL 18.4 regression suite against BicDB

This page records how BicDB does on PostgreSQL's own regression suite (`src/test/regress` of the
18.4 release, schedule `parallel_schedule`, 231 tests) and turns the failures into a work queue
clustered by root cause. The harness is `scripts/pg18-regress/`.

## Baseline (2026-11-25)

BicDB `main` at `1e106b4c8` (source 1.0.529, debug build), harness run `main-1e106b4c8`:

| | tests | pass | statements that differ from PostgreSQL |
|---|---|---|---|
| in scope (183) | 183 | **2** (md5, comments) | 26,046 |
| out of scope (48) | 48 | 0 | not clustered |
| PostgreSQL 18.4 reference | 231 | **231** | 0 |

The campaign branch `feat/pgsql-c` at `4dd664ee` (built on slot c) gives the same 2/231 and
25,791 differing in-scope statements. Pass/fail per test is a coarse measure here: one unsupported
statement fails a whole file, so the work queue below counts differing *statements* (each echoed
statement plus its output, aligned with the expected file) and attributes each `diff -U3` hunk to
the first differing statement it covers. Raw data: `docs/evidence/pg18-regress-2026-11-25/`
(`main-summary.json`, `main-status.tsv`, `main-clusters-in-scope.json`, `reference-summary.json`,
`c-branch-summary.json`).

Of the 26,046 differing in-scope statements, 3,711 are cascades (2,823 "object missing" after an
earlier CREATE failed, 888 "current transaction is aborted") and 1,440 never ran because the test
stopped early (crash or hang). They disappear when their cause is fixed and are not ranked.

## Stability findings (fix first)

These stop the suite (and would stop an application), so they are ahead of every parity cluster.

| # | finding | reproduction | where |
|---|---|---|---|
| S1 | **Server process aborts with a stack overflow** on trivial queries: any user can kill the server. | `SELECT 1 AS x ORDER BY x;` (select), `select 1 as one group by rollup(one) order by one nulls first;` (groupingsets); infinite_recurse also aborts where PostgreSQL raises `stack depth limit exceeded`. | ORDER BY output-alias resolution (`crates/bicdb-sql/src/order_by_ordinal.rs` and its callers); tokio worker stack size in `bicdb-pgwire` |
| S2 | **The store cannot be reopened** after `DROP TABLE` of a partitioned table that has partitions: the DROP fails with `SQL partition upgrade: missing certified partition parent`, and every later start fails with `collection not found: <parent>` (also after a clean stop). 5 of the run's 15 restarts hit an unopenable store. | `CREATE TABLE p (a int primary key, b text) PARTITION BY LIST (a); CREATE TABLE p1 PARTITION OF p FOR VALUES IN (1); DROP TABLE p;` then restart | `crates/bicdb-sql/src/session/partition_admission/catalog.rs:265` and the catalog replay on open |
| S3 | **COPY FROM STDIN error before the data desynchronizes the protocol**: BicDB accepts the copy-in, reports the error after the data, and the next COPY on the session fails with `expected CopyData, CopyDone, or CopyFail` and then hangs. Caused 5 of the 7 test timeouts (rowsecurity, generated_stored, generated_virtual, copy2 and the cascade in with). | `COPY nosuch FROM stdin;` + data + `\.`, then `CREATE TABLE t(a int); COPY t FROM stdin;` | pgwire COPY-in path (`crates/bicdb-pgwire/src/`, `application_copy.rs`) |
| S4 | Arithmetic overflow panics (contained) instead of PostgreSQL range errors: `internal query execution panic was contained`. | timestamp/interval/numeric overflow cases in timestamp, interval, numeric, window | `eval/temporal_json_oid.rs:3614` (multiply overflow), `pg_scalar_extra.rs:991`, `eval/value_ops2.rs:569`, `engine/joins_windows.rs:3190` (add overflow) |
| S5 | Correlated `EXISTS` over two 10k-row tables runs for minutes (no semi-join decorrelation); a recursive `UNION` over `varbit` never terminates. | subselect: `select max(unique1) from tenk1 a where exists (select 1 from tenk1 b where b.thousand = a.unique2)`; join: the same pattern under a prepared statement; with: `WITH RECURSIVE t(n) AS (VALUES ('01'::varbit) UNION SELECT n \|\| '10'::varbit FROM t WHERE n < '100'::varbit) SELECT n FROM t;` | planner/executor (`engine/exists_probe.rs`, `select_exec.rs`); recursive CTE duplicate elimination for varbit |

## Work queue: in-scope failures by root cause

Ranked by differing statements (hunks in brackets). "In flight" refers to the campaign branches
as of 2026-11-25 (A: citext/pgcrypto/hstore, VIRTUAL/UNLOGGED, exclusion constraints, inheritance;
B: RowDescription types, statement-end FK, COPY HEADER/binary, column GRANT, and earlier
CTAS/updatable views/REFRESH/INSERT defaults/column names/error texts; C: CREATE COLLATION,
custom ranges, 2PC, event triggers, and earlier MERGE/grouping sets/EXCLUDE/ROWS FROM; D:
CREATE AGGREGATE/OPERATOR/CAST, multi-array unnest, cursor/JSON_TABLE, pg_export_snapshot, and
earlier planner GUCs/named args/SIMILAR TO/SRF in select list/SQL cursors/SQL/JSON). Counts are on
`main`; clusters marked in flight should shrink when those branches merge.

| # | root cause | stmts [hunks] | example (PostgreSQL → BicDB) | code area | status |
|---|---|---|---|---|---|
| 1 | **ERROR text differs from PostgreSQL**: BicDB category prefixes (`invalid SQL:` ~1,150, `unsupported SQL:` ~610, `authorization denied:` ~330) and different wording (~1,100, e.g. `value "100000" is out of range for type smallint` → `smallint out of range`; `relation "x" does not exist` → `collection not found: x`) | ~4,050 [455] | `SELECT int2 '100000'` | `bicdb-pgwire` error rendering (`protocol_wire.rs` `error_response_for_error`), `SqlError` display in `bicdb-sql`; per-message wording at each raise site | partly in flight (B2 "PG error messages + codes"); prefixes and `collection not found` remain on main |
| 2 | **Parser gaps** (227 distinct parser errors). Largest: `x isnull`/`notnull` postfix (138); `ALTER/DROP ROUTINE`, `ALTER ... FOREIGN DATA WRAPPER/SERVER`, `ALTER TEXT SEARCH ...`, `DROP PUBLICATION`, `DROP STATISTICS` (~330); `CREATE STATISTICS` (87); `REINDEX` (72); `PASSING` in SQL/JSON and XMLEXISTS (55); `WITHOUT OVERLAPS`/`PERIOD` (48+); `CREATE AGGREGATE` (57); array-element targets `INSERT INTO t (f[1])`, `UPDATE t SET e[0]=` (~80); `WITH CHECK OPTION` (37); `RANGE BETWEEN '1 day'::interval PRECEDING` (34); `CREATE FOREIGN TABLE` (34); `COLLATE` after an expression in index definitions (52); `CREATE TEXT SEARCH` (30); `REFRESH MATERIALIZED VIEW` (19); `VIRTUAL` (18); `XMLSERIALIZE ... INDENT` (18) | ~2,000 [291] | `select * from t where b isnull;` → `sql parser error: Expected: end of statement, found: isnull` | `crates/bicdb-sql` parser front end (sqlparser dialect + raw DDL handlers in `session/raw_ddl_analyze.rs`) | CHECK OPTION, REFRESH, CREATE AGGREGATE, VIRTUAL, EXCLUDE in flight (B/D/A/C2); isnull, ROUTINE, FDW, TEXT SEARCH, STATISTICS, REINDEX, array targets, interval frame offsets **unassigned** |
| 3 | **Functions rejected in a FROM-less SELECT** (`function X is not supported without FROM`, 256 functions): ts_lexize 73, regexp_matches 42, json_value/json_query/json_exists 69, regexp_instr/substr/count 40, array_sort 19, float4send 17, json(b)_to_tsvector 34, make_timestamptz 17, pg_partition_root 15, brin_(de)summarize_range 26, aggregates `var_pop`/`stddev_pop` without FROM 22, getdatabaseencoding 10, pg_current_xact_id 10, to_regrole/to_regtypemod/to_regcollation 18, width_bucket, parse_ident, num_nulls, ... | ~900 [122] | `SELECT regexp_matches('foobarbequebaz', '(bar)(beque)');` | `crates/bicdb-sql/src/virtual_tables.rs:1883`, `engine/row_eval.rs:1939` (constant-SELECT evaluator lacks the general function table) | SRF in select list in flight (D2); the general gap **unassigned** |
| 4 | **EXPLAIN**: only plain SELECTs are explainable (`EXPLAIN supports only SELECT statements`, `... table joins only`, `... only simple SELECT`), plan text never matches PostgreSQL | ~920 [235] | `explain (costs off) insert into ...` | `plan_diagnostics.rs`, `planner.rs` | plan shapes are planner output (low value); EXPLAIN of DML/UNION/window being refused is a real gap, **unassigned** |
| 5 | **GUCs rejected**: `SET/RESET enable_seqscan/enable_indexscan/enable_bitmapscan/enable_sort/...`, `synchronous_commit`, `allow_in_place_tablespaces`, ... (34 names) | 517 [120] | `SET enable_seqscan = off;` → `SET enable_seqscan is not supported` | `session/raw_ddl_analyze.rs:1066`, `session.rs:6379` | in flight (D planner GUCs); recheck after merge |
| 6 | **psql `\d`/`\d+` describe output**: psql's own catalog queries fail or differ (`table function unnest is not supported`, `constraint catalog OID N ... collides`, missing pg_catalog columns) | 525 [36] | `\d+ numeric_view` | `engine/pg_catalog.rs`, `catalog_rows/` | **unassigned** (catalog fidelity; large payoff because almost every DDL test uses `\d`) |
| 7 | **JSON / jsonpath / SQL-JSON**: `jsonb @@`/`@?` with unknown literal, jsonpath parse/eval differences, `invalid input syntax for type json` wording, JSON SRFs with named args (60), JSON_VALUE/QUERY/EXISTS clauses (DEFAULT ON EMPTY, PASSING, RETURNING domain) | ~1,060 [179] | `select jsonb '{"a": 12}' @? '$.b + 2';` → `f` (PostgreSQL: NULL); `j @@ '$.wait == null'` → `operator does not exist: jsonb @@ unknown` | `jsonb.rs`, `jsonpath.rs`, `eval/temporal_json_oid.rs` | SQL/JSON in flight (D2); jsonpath engine **unassigned** |
| 8 | **Partitioning**: multi-level partitions (`PARTITION OF ... PARTITION BY` → `unexpected trailing partition bound clause`), `ATTACH PARTITION` column checks, routing errors (`no partition ... found for row`), `pg_partition_tree/ancestors/root`, and S2 | ~745 [104] | `create table p1 partition of p for values from (1,40) to (1,50) partition by range (c);` | `partition.rs`, `session/partition_*`, `partition_admission/` | **unassigned** |
| 9 | **Privileges / RLS behaviour and texts** beyond wording: privileges test result sets, view permission checks on `updatable_views` (`permission denied for table base_tbl` raised by PostgreSQL where BicDB differs) | ~970 [102] | updatable_views security-barrier and view-owner checks | `engine/eager_privileges.rs`, `session/privilege_projection.rs` | column GRANT in flight (B3); verify fail-closed direction per case (no bypass found by the classifier) |
| 10 | **Date/time input and output**: special values (`today`, `tomorrow`, `epoch`, `infinity` for interval), time-zone names and abbreviations (`time zone "..." not recognized`), `to_timestamp`/`to_char` tokens (HH12, -infinity), field-range wording | ~540 [49] | `INSERT INTO TIMESTAMPTZ_TBL VALUES ('today');` → `invalid input syntax` | `eval/temporal_json_oid.rs`, `typed_value.rs` | **unassigned** |
| 11 | **Text search**: `tsvector @@ 'literal'` operator resolution with unknown literal (186), ts_lexize/ts_debug/ts_stat, `CREATE TEXT SEARCH ...`, tsvector input syntax | ~370 [23] | `SELECT to_tsvector('simple', '1 2 3 1') @@ '1 <-> 2';` → `operator does not exist: tsvector @@ unknown` | `fts.rs`, operator resolution in `pg_operators.rs` | **unassigned** |
| 12 | **TEMP table shadowing / `relation already exists`**: `CREATE TEMP TABLE char_tbl` when a permanent `char_tbl` exists; index/constraint names reused across tables | ~360 [32] | `CREATE TEMP TABLE CHAR_TBL(f1 char);` → `relation "char_tbl" already exists` | `search_path.rs`, catalog name uniqueness (`catalog_identifiers.rs`) | **unassigned** |
| 13 | **`stats_ext` helper**: PL/pgSQL `FOR r IN EXECUTE format(...)` with a non-SELECT (EXPLAIN) | 381 [13] | `check_estimated_rows(...)` | `routines/` PL/pgSQL `FOR ... IN EXECUTE` | **unassigned** (one test; plan estimates themselves are planner output) |
| 14 | **Result column names / widths** (`?column?`, function names as column names) | 253 [31] | `SELECT m + '123' FROM money_data` → column `m + '123'` instead of `?column?` | `select_exec.rs` output naming | in flight (B2 "PostgreSQL output column names") |
| 15 | **Missing ERROR position** (`LINE n:` + `^`): BicDB sends no error position | 245 [29] | `SELECT bool 'test';` lacks `LINE 1: ...` | pgwire ErrorResponse `P` field; positions from the parser/eval | **unassigned** (mechanical once positions are tracked; touches every input-error test) |
| 16 | **Collations**: `collation "C"/"en_US"/ICU ... for encoding UTF8 does not exist`, `COLLATE` in expressions/indexes | ~250 [19] | `SELECT 'a' COLLATE "en-x-icu" < 'B'` | collation catalog | in flight (C CREATE COLLATION) |
| 17 | **Updatable views**: `INSERT into views is not supported`, `UPDATE through views`, CHECK OPTION wording | ~200 [13] | `INSERT INTO rw_view1 VALUES (...)` | `session/` view DML | in flight (B, done on branch) |
| 18 | **INSERT with fewer values / DEFAULT in VALUES** (`INSERT expected 3 values, got 2`) | 159 [30] | `insert into inserttest values (DEFAULT, 7);` | `session/functions_insert.rs` | in flight (B2) |
| 19 | **SQL cursors**: SCROLL/backward FETCH, `FETCH ABSOLUTE/PRIOR/LAST`, MOVE, WITH HOLD | 135 [160] | `FETCH BACKWARD 1 IN foo1;` | `bicdb-pgwire/src/sql_cursors.rs` | in flight (D2) |
| 20 | **Rows in a different order** without ORDER BY (window 57, inet, create_index) | 111 [94] | window-function queries whose output order PostgreSQL derives from the sort | executor order | low priority; many hunks, few root causes |
| 21 | **INHERITS** (`invalid SQL: expected identifier` on `CREATE TABLE x () INHERITS (y)`) | 111 [14] | `CREATE TABLE ihighway () INHERITS (road);` | DDL front end | in flight (A inheritance) |
| 22 | **`pg_input_error_info` / soft input errors** | 108 [71] | `SELECT * FROM pg_input_error_info('junk', 'int4');` | table functions (`engine/predicates_locators.rs`) | **unassigned**; one function, 71 hunks across ~40 type tests |
| 23 | **CREATE TABLE AS / SELECT INTO** | 83 [26] | `CREATE TABLE onek2 AS SELECT * FROM onek;` | | in flight (B, done on branch) |
| 24 | **MERGE** beyond one matched UPDATE | 94 [69] | `MERGE ... WHEN NOT MATCHED THEN INSERT` | `session/merge.rs` | in flight (C, done on branch) |
| 25 | **VACUUM/ANALYZE forms** (`VACUUM t`, `VACUUM ANALYZE t`, `ANALYZE t` options) rejected | 68 [25] | `VACUUM ANALYZE onek;` → `only plain VACUUM is supported` | VACUUM statement handling | **unassigned**; accepting them as store-wide no-ops is cheap and unblocks 18 statements in test_setup alone |
| 26 | **Constraint catalog OID collision** `constraint catalog OID N is invalid or collides with another constraint` (also breaks `\d`) | 73+ [13] | `\d unique_tbl`, `ALTER TABLE ... ADD CONSTRAINT` | `catalog_rows/`, constraint OID allocation | **unassigned** (catalog bug) |
| 27 | **Geometric types as columns** (`column type box/polygon is not supported`), geometric operators (`~`, `@@`, `#`) | ~110 [11] | `CREATE TABLE BOX_TBL (f1 box);` | `geometric.rs`, column type registry | **unassigned** |
| 28 | **Server-side COPY file I/O** (`COPY ... FROM/TO 'file'`) | 33 statements adapted by the harness | `COPY onek FROM '/path/onek.data';` → `COPY FROM supports only STDIN` | COPY | **unassigned**; the harness works around it (see Adaptations) |
| 29 | **NOTICE/WARNING differences** (missing `merging multiple inherited definitions`, `drop cascades to ...`, `table does not exist, skipping`) | 360 [83] | | per statement | **unassigned** |
| 30 | **DETAIL/HINT/CONTEXT lines** (`Key (a)=(1) already exists.`, `Failing row contains (...)`) | 193 [83] | FK/unique/check violations | constraint error construction | **unassigned** |

Per-test values that differ without an error (`values` category, 3,346 statements) are listed per
test in `main-clusters-in-scope.json` (keys like `timestamptz: SELECT result values differ`); the
largest are in timestamptz/horology/date (date-time output and arithmetic), jsonb_jsonpath,
geometry, updatable_views, rules, numeric and subselect, and mostly share causes with clusters
7, 10 and 27.

### Out-of-scope tests

48 tests are out of scope (column `scope` below and `scripts/pg18-regress/scope.tsv`): catalog
consistency checks (type_sanity, opr_sanity, misc_sanity, oidjoins, sanity_check), psql meta
commands (psql, psql_crosstab, psql_pipeline), monitoring (stats, sysviews, numa), maintenance and
physical storage (vacuum, vacuum_parallel, maintain_every, cluster, reloptions, compression,
compression_pglz, tid, tidscan, tidrangescan, combocid, mvcc, indirect_toast, tablespace),
replication (publication, subscription, replica_identity), planner-internal plan shapes (explain,
memoize, predicate, join_hash, incremental_sort, equivclass, partition_prune, select_parallel,
write_parallel), C extension loading (create_function_c, create_am, conversion, encoding,
security_label), dump-tool support (stats_import, init_privs), passwords (password), object
addresses (object_address), access-method introspection (amutils) and a Windows-only test.
Tests that only partly load `regress.so` (test_setup, create_type, misc, triggers, ...) stay in
scope; the C-function statements in them fail by design.



## How the harness runs the suite

`scripts/pg18-regress/pg_regress_compat.py` does what `pg_regress` does for `make installcheck`,
except that the tests run one at a time in schedule order (later tests depend on objects that
earlier ones create, starting with `test_setup`):

- Source: the 18.4 tarball from ftp.postgresql.org, cached once under
  `/root/pg18-regress-cache` on the host that runs the suite (`sql/`, `expected/` with the
  alternative `*_N.out` files, `data/`, `parallel_schedule`). PostgreSQL 18 no longer has
  `input/`/`output/` templates; tests read paths through `\getenv`.
- `regress.so`: the suite's C helper library is compiled from the release's `regress.c` against
  the 18.4 server headers (in a throwaway `postgres:18.4` container) so the reference server can
  load it. BicDB cannot load C libraries; statements that need it fail by design and are counted
  in their own cluster.
- Client: psql 18.4 from the stock `postgres:18.4` image (output formatting must be the release's),
  run as `psql -X -a -q -d regression -v HIDE_TABLEAM=on -v HIDE_TOAST_COMPRESSION=on < sql/T.sql
  > results/T.out 2>&1`, with stderr merged inside the container so messages keep their place.
- Environment as set by pg_regress: `PGTZ=America/Los_Angeles`, `PGDATESTYLE=Postgres, MDY`,
  `LC_MESSAGES=C`, `PGOPTIONS=-c intervalstyle=postgres_verbose`, `PGAPPNAME`, and
  `PG_ABS_SRCDIR`, `PG_ABS_BUILDDIR`, `PG_LIBDIR`, `PG_DLSUFFIX` for `\getenv`.
- Database: `DROP DATABASE IF EXISTS "regression"`, `CREATE DATABASE "regression"
  TEMPLATE=template0`, then the six `ALTER DATABASE ... SET` (lc_messages, lc_monetary,
  lc_numeric, lc_time, bytea_output, timezone_abbreviations) pg_regress issues.
- Comparison: a test passes when its result equals `expected/T.out` or any `expected/T_N.out`
  exactly; otherwise the `diff -U3` against the closest one is kept in `diffs/T.diff`.
- Outputs: `results/`, `diffs/`, `status.tsv`, `summary.json`, `summary.md`.
- Crashes: after every test the harness checks the server. If it died, it is restarted on the same
  store; if the store cannot be reopened, the store is kept for evidence, a fresh one is created,
  `test_setup` is replayed and the run continues. Both events are recorded in the test's note.

Validation: against a stock PostgreSQL 18.4 server (container `pgregress-ref`, 127.0.0.1:57434,
`--locale=C --encoding=UTF8`, 1 GiB) the harness passes **231 of 231** tests, including the ones
that load `regress.so`. Harness causes found and fixed while getting there: stdout/stderr
interleaving, the server user not being able to read the bind-mounted data under `/root`, psql
and the server needing to share a uid for `\o` files later overwritten by `COPY TO`, and the
`HIDE_TABLEAM`/`HIDE_TOAST_COMPRESSION` psql variables.

### Adaptations for BicDB

The SQL files and expected files are never edited. For BicDB only:

1. Tests run as BicDB's bootstrap superuser `bicdb` (the reference runs as `postgres`).
2. Single-line server-side `COPY t FROM :'file'` / `COPY ... TO :'file'` statements are sent as
   psql `\copy` with the variable resolved, because BicDB's COPY reads only STDIN and writes only
   STDOUT. The echoed line in the result is restored to the original statement before comparing.
   Without this, `test_setup` loads none of the shared fixture tables (onek, tenk1, person, ...)
   and most later tests fail for that reason alone. Server-side COPY file I/O is listed as its own
   cluster. `--no-adapt` turns this off.

### Rerunning

From a development checkout (runs everything on the remote host; nothing runs locally):

```sh
# reference (validates the harness; expect 231/231)
scripts/pg18-regress/run-remote.sh root@BUILD_HOST ref --target ref
# BicDB, a built binary on that host
scripts/pg18-regress/run-remote.sh root@BUILD_HOST main-1.0.529 --target bicdb \
    --bicdb-bin /path/to/target/debug/bicdb --timeout 180
# clusters (on the host)
python3 -I /root/pg18-regress-cache/harness/analyze.py /root/pg18-regress-cache/runs/<run> \
    --out clusters.json --top 60
```

`--only a,b,c` runs a subset (test_setup is always run first). A full BicDB run takes about
30 minutes plus 180 s for each test that hangs.

## Results per test (main `1e106b4c8`)

Run `main-1e106b4c8` (2026-11-25T10:00:36+00:00), 2018.1 s.

| | tests | pass | fail | failing diff hunks |
|---|---|---|---|---|
| in scope | 183 | 2 | 181 | 3692 |
| out of scope | 48 | 0 | 48 | 562 |
| all | 231 | 2 | 229 | 4254 |

### Per test

| # | test | scope | PostgreSQL 18.4 | BicDB | differing statements | diff hunks | note |
|---|---|---|---|---|---|---|---|
| 1 | test_setup | in | pass | fail | 35 | 18 | adapted: 7 COPY file statement(s) sent as \copy |
| 2 | boolean | in | pass | fail | 26 | 12 |  |
| 3 | char | in | pass | fail | 18 | 7 |  |
| 4 | name | in | pass | fail | 24 | 4 |  |
| 5 | varchar | in | pass | fail | 12 | 6 |  |
| 6 | text | in | pass | fail | 48 | 8 |  |
| 7 | int2 | in | pass | fail | 56 | 19 |  |
| 8 | int4 | in | pass | fail | 58 | 19 |  |
| 9 | int8 | in | pass | fail | 68 | 15 |  |
| 10 | oid | in | pass | fail | 14 | 5 |  |
| 11 | float4 | in | pass | fail | 55 | 12 |  |
| 12 | float8 | in | pass | fail | 113 | 26 |  |
| 13 | bit | in | pass | fail | 55 | 11 |  |
| 14 | numeric | in | pass | fail | 365 | 44 |  |
| 15 | txid | in | pass | fail | 17 | 5 |  |
| 16 | uuid | in | pass | fail | 14 | 4 |  |
| 17 | enum | in | pass | fail | 54 | 16 |  |
| 18 | money | in | pass | fail | 51 | 8 |  |
| 19 | rangetypes | in | pass | fail | 178 | 28 |  |
| 20 | pg_lsn | in | pass | fail | 10 | 5 |  |
| 21 | regproc | in | pass | fail | 88 | 7 |  |
| 22 | strings | in | pass | fail | 272 | 36 |  |
| 23 | md5 | in | pass | pass | 0 | 0 |  |
| 24 | numerology | in | pass | fail | 81 | 6 |  |
| 25 | point | in | pass | fail | 33 | 8 |  |
| 26 | lseg | in | pass | fail | 5 | 2 |  |
| 27 | line | in | pass | fail | 19 | 7 |  |
| 28 | box | in | pass | fail | 81 | 6 |  |
| 29 | path | in | pass | fail | 8 | 2 |  |
| 30 | polygon | in | pass | fail | 47 | 5 |  |
| 31 | circle | in | pass | fail | 6 | 2 |  |
| 32 | date | in | pass | fail | 211 | 29 |  |
| 33 | time | in | pass | fail | 27 | 9 |  |
| 34 | timetz | in | pass | fail | 41 | 4 |  |
| 35 | timestamp | in | pass | fail | 150 | 17 |  |
| 36 | timestamptz | in | pass | fail | 344 | 17 |  |
| 37 | interval | in | pass | fail | 378 | 45 |  |
| 38 | inet | in | pass | fail | 35 | 14 |  |
| 39 | macaddr | in | pass | fail | 15 | 4 |  |
| 40 | macaddr8 | in | pass | fail | 36 | 5 |  |
| 41 | multirangetypes | in | pass | fail | 189 | 39 |  |
| 42 | geometry | in | pass | fail | 115 | 34 |  |
| 43 | horology | in | pass | fail | 338 | 14 |  |
| 44 | tstypes | in | pass | fail | 110 | 20 |  |
| 45 | regex | in | pass | fail | 52 | 8 |  |
| 46 | type_sanity | out: system-catalog consistency checks of pg_type/pg_class internals (catalog contents, not SQL behaviour) | pass | fail | 0 | 19 |  |
| 47 | opr_sanity | out: system-catalog consistency checks of pg_proc/pg_operator/pg_amop internals | pass | fail | 0 | 51 |  |
| 48 | misc_sanity | out: system-catalog consistency checks (pg_depend, toast tables, catalog columns) | pass | fail | 0 | 3 |  |
| 49 | comments | in | pass | pass | 0 | 0 |  |
| 50 | expressions | in | pass | fail | 46 | 16 |  |
| 51 | unicode | in | pass | fail | 18 | 1 |  |
| 52 | xid | in | pass | fail | 43 | 15 |  |
| 53 | mvcc | out: MVCC storage internals (index bloat after aborted inserts measured via relation size) | pass | fail | 0 | 2 |  |
| 54 | database | in | pass | fail | 10 | 1 |  |
| 55 | stats_import | out: pg_restore_*_stats statistics import (dump/restore tool support) | pass | fail | 0 | 61 |  |
| 56 | encoding | out: server-encoding conversion internals tested through regress.so C functions | pass | fail | 0 | 5 |  |
| 57 | euc_kr | in | pass | fail | 3 | 1 |  |
| 58 | copy | in | pass | fail | 55 | 13 | adapted: 10 COPY file statement(s) sent as \copy |
| 59 | copyselect | in | pass | fail | 13 | 5 |  |
| 60 | copydml | in | pass | fail | 31 | 2 |  |
| 61 | copyencoding | in | pass | fail | 6 | 3 | adapted: 8 COPY file statement(s) sent as \copy |
| 62 | insert | in | pass | fail | 290 | 17 |  |
| 63 | insert_conflict | in | pass | fail | 138 | 24 |  |
| 64 | create_function_c | out: LANGUAGE C function loading from regress.so / shared libraries | pass | fail | 0 | 1 |  |
| 65 | create_misc | in | pass | fail | 71 | 4 |  |
| 66 | create_operator | in | pass | fail | 35 | 18 |  |
| 67 | create_procedure | in | pass | fail | 68 | 13 |  |
| 68 | create_table | in | pass | fail | 222 | 20 |  |
| 69 | create_type | in | pass | fail | 69 | 10 |  |
| 70 | create_schema | in | pass | fail | 24 | 1 |  |
| 71 | create_index | in | pass | fail | 458 | 80 | adapted: 2 COPY file statement(s) sent as \copy |
| 72 | create_index_spgist | in | pass | fail | 200 | 1 |  |
| 73 | create_view | in | pass | fail | 242 | 30 | adapted: 1 COPY file statement(s) sent as \copy |
| 74 | index_including | in | pass | fail | 92 | 8 |  |
| 75 | index_including_gist | in | pass | fail | 21 | 7 |  |
| 76 | create_aggregate | in | pass | fail | 43 | 23 |  |
| 77 | create_function_sql | in | pass | fail | 89 | 23 |  |
| 78 | create_cast | in | pass | fail | 19 | 3 |  |
| 79 | constraints | in | pass | fail | 344 | 34 | adapted: 2 COPY file statement(s) sent as \copy |
| 80 | triggers | in | pass | fail | 474 | 90 |  |
| 81 | select | in | pass | fail | 50 | 11 | SERVER CRASHED or stopped responding during this test; STORE COULD NOT BE REOPENED (kept at /root/pg18-regress-cache/runs/main-1e106b4c8/bicdb-data.unrecoverable-after-select); continued on a fresh store with test_setup replayed |
| 82 | inherit | in | pass | fail | 607 | 48 |  |
| 83 | typed_table | in | pass | fail | 28 | 1 |  |
| 84 | vacuum | out: VACUUM/ANALYZE maintenance internals | pass | fail | 0 | 18 |  |
| 85 | drop_if_exists | in | pass | fail | 134 | 1 |  |
| 86 | updatable_views | in | pass | fail | 775 | 75 |  |
| 87 | roleattributes | in | pass | fail | 34 | 1 |  |
| 88 | create_am | out: table/index access methods (CREATE ACCESS METHOD with C handlers, heap2) | pass | fail | 0 | 13 |  |
| 89 | hash_func | in | pass | fail | 37 | 21 |  |
| 90 | errors | in | pass | fail | 75 | 7 |  |
| 91 | infinite_recurse | in | pass | fail | 2 | 1 | SERVER CRASHED or stopped responding during this test; STORE COULD NOT BE REOPENED (kept at /root/pg18-regress-cache/runs/main-1e106b4c8/bicdb-data.unrecoverable-after-infinite_recurse); continued on a fresh store with test_setup replayed |
| 92 | sanity_check | out: catalog check that every table has the expected relhasindex (catalog internals) | pass | fail | 0 | 2 |  |
| 93 | select_into | in | pass | fail | 52 | 4 |  |
| 94 | select_distinct | in | pass | fail | 68 | 14 |  |
| 95 | select_distinct_on | in | pass | fail | 13 | 8 |  |
| 96 | select_implicit | in | pass | fail | 17 | 9 |  |
| 97 | select_having | in | pass | fail | 6 | 2 |  |
| 98 | subselect | in | pass | timeout | 186 | 11 | timeout after 180s; server restarted to stop the runaway statement; restarted on the same store |
| 99 | union | in | pass | fail | 112 | 39 |  |
| 100 | case | in | pass | fail | 14 | 9 |  |
| 101 | join | in | pass | timeout | 528 | 62 | timeout after 180s; server restarted to stop the runaway statement; restarted on the same store |
| 102 | aggregates | in | pass | fail | 386 | 77 | adapted: 1 COPY file statement(s) sent as \copy |
| 103 | transactions | in | pass | fail | 92 | 39 |  |
| 104 | random | in | pass | fail | 40 | 7 |  |
| 105 | portals | in | pass | fail | 241 | 11 |  |
| 106 | arrays | in | pass | fail | 262 | 51 | adapted: 1 COPY file statement(s) sent as \copy |
| 107 | btree_index | in | pass | fail | 58 | 23 | adapted: 4 COPY file statement(s) sent as \copy |
| 108 | hash_index | in | pass | fail | 21 | 7 | adapted: 4 COPY file statement(s) sent as \copy |
| 109 | update | in | pass | fail | 166 | 21 |  |
| 110 | delete | in | pass | fail | 2 | 1 |  |
| 111 | namespace | in | pass | fail | 26 | 5 |  |
| 112 | prepared_xacts | in | pass | fail | 86 | 3 |  |
| 113 | brin | in | pass | fail | 44 | 9 |  |
| 114 | gin | in | pass | fail | 30 | 10 |  |
| 115 | gist | in | pass | fail | 31 | 14 |  |
| 116 | spgist | in | pass | fail | 13 | 7 |  |
| 117 | privileges | in | pass | fail | 726 | 77 |  |
| 118 | init_privs | out: pg_init_privs catalog (extension privilege tracking for dump tools) | pass | fail | 0 | 1 |  |
| 119 | security_label | out: SECURITY LABEL needs a loadable label provider module (dummy_seclabel) | pass | fail | 0 | 1 |  |
| 120 | collate | in | pass | fail | 69 | 15 |  |
| 121 | matview | in | pass | fail | 128 | 11 |  |
| 122 | lock | in | pass | fail | 25 | 10 |  |
| 123 | replica_identity | out: REPLICA IDENTITY for logical replication | pass | fail | 0 | 10 |  |
| 124 | rowsecurity | in | pass | timeout | 415 | 17 | timeout after 180s; server restarted to stop the runaway statement; STORE COULD NOT BE REOPENED (kept at /root/pg18-regress-cache/runs/main-1e106b4c8/bicdb-data.unrecoverable-after-rowsecurity); continued on a fresh store with test_setup replayed |
| 125 | object_address | out: pg_get_object_address / pg_identify_object catalog-address internals | pass | fail | 0 | 6 |  |
| 126 | tablesample | in | pass | fail | 47 | 7 |  |
| 127 | groupingsets | in | pass | fail | 197 | 13 | SERVER CRASHED or stopped responding during this test; restarted on the same store |
| 128 | drop_operator | in | pass | fail | 8 | 5 |  |
| 129 | password | out: password hashing / authentication (pg_authid rolpassword) | pass | fail | 0 | 6 |  |
| 130 | identity | in | pass | fail | 159 | 19 |  |
| 131 | generated_stored | in | pass | timeout | 223 | 8 | timeout after 180s; server restarted to stop the runaway statement; restarted on the same store |
| 132 | join_hash | out: parallel hash join executor internals (plan shapes, batch counts) | pass | fail | 0 | 13 |  |
| 133 | brin_bloom | in | pass | fail | 28 | 4 |  |
| 134 | brin_multi | in | pass | fail | 65 | 11 |  |
| 135 | create_table_like | in | pass | fail | 115 | 10 |  |
| 136 | alter_generic | in | pass | fail | 266 | 12 |  |
| 137 | alter_operator | in | pass | fail | 55 | 6 |  |
| 138 | misc | in | pass | fail | 35 | 6 | adapted: 4 COPY file statement(s) sent as \copy |
| 139 | async | in | pass | fail | 7 | 2 |  |
| 140 | dbsize | in | pass | fail | 25 | 4 |  |
| 141 | merge | in | pass | fail | 233 | 107 |  |
| 142 | misc_functions | in | pass | fail | 124 | 11 |  |
| 143 | sysviews | out: pg_stat/system monitoring views | pass | fail | 0 | 4 |  |
| 144 | tsrf | in | pass | fail | 61 | 13 |  |
| 145 | tid | out: ctid / tid physical tuple identifiers | pass | fail | 0 | 4 |  |
| 146 | tidscan | out: ctid physical tuple scans | pass | fail | 0 | 3 |  |
| 147 | tidrangescan | out: ctid physical tuple range scans | pass | fail | 0 | 5 |  |
| 148 | collate.utf8 | in | pass | fail | 57 | 1 |  |
| 149 | collate.icu.utf8 | in | pass | fail | 570 | 1 |  |
| 150 | incremental_sort | out: planner internals (Incremental Sort plan nodes) | pass | fail | 0 | 23 |  |
| 151 | create_role | in | pass | fail | 92 | 2 |  |
| 152 | without_overlaps | in | pass | fail | 546 | 80 |  |
| 153 | generated_virtual | in | pass | timeout | 277 | 2 | timeout after 180s; server restarted to stop the runaway statement; restarted on the same store |
| 154 | rules | in | pass | fail | 263 | 66 |  |
| 155 | psql | out: psql meta-commands (client tool) | pass | fail | 0 | 64 | adapted: 8 COPY file statement(s) sent as \copy |
| 156 | psql_crosstab | out: psql \crosstabview (client tool) | pass | fail | 0 | 6 |  |
| 157 | psql_pipeline | out: psql pipeline meta-commands (client tool) | pass | fail | 0 | 24 |  |
| 158 | amutils | out: index access-method property introspection (pg_indexam_has_property) | pass | fail | 0 | 7 |  |
| 159 | stats_ext | in | pass | fail | 633 | 33 |  |
| 160 | collate.linux.utf8 | in | pass | fail | 181 | 1 |  |
| 161 | collate.windows.win1252 | out: runs only on Windows (skips elsewhere) | pass | fail | 0 | 1 |  |
| 162 | select_parallel | out: parallel query executor/planner internals (Gather plans, workers) | pass | fail | 0 | 11 |  |
| 163 | write_parallel | out: parallel query executor internals | pass | fail | 0 | 1 |  |
| 164 | vacuum_parallel | out: parallel VACUUM maintenance internals | pass | fail | 0 | 2 |  |
| 165 | maintain_every | out: database-wide ANALYZE/VACUUM maintenance internals | pass | fail | 0 | 1 |  |
| 166 | publication | out: logical replication publications | pass | fail | 0 | 16 |  |
| 167 | subscription | out: logical replication subscriptions | pass | fail | 0 | 1 |  |
| 168 | select_views | in | pass | fail | 33 | 5 |  |
| 169 | portals_p2 | in | pass | fail | 28 | 1 |  |
| 170 | foreign_key | in | pass | fail | 726 | 93 |  |
| 171 | cluster | out: CLUSTER physical reordering (maintenance) | pass | fail | 0 | 14 |  |
| 172 | dependency | in | pass | fail | 33 | 3 |  |
| 173 | guc | in | pass | fail | 166 | 21 |  |
| 174 | bitmapops | in | pass | fail | 4 | 2 |  |
| 175 | combocid | out: cmin/cmax tuple-header internals | pass | fail | 0 | 3 |  |
| 176 | tsearch | in | pass | fail | 285 | 48 | adapted: 1 COPY file statement(s) sent as \copy |
| 177 | tsdicts | in | pass | fail | 131 | 3 |  |
| 178 | foreign_data | in | pass | fail | 484 | 13 |  |
| 179 | window | in | pass | fail | 309 | 122 |  |
| 180 | xmlmap | in | pass | fail | 30 | 2 |  |
| 181 | functional_deps | in | pass | fail | 26 | 8 |  |
| 182 | advisory_lock | in | pass | fail | 2 | 2 |  |
| 183 | indirect_toast | out: TOAST storage internals via regress.so | pass | fail | 0 | 4 |  |
| 184 | equivclass | out: planner equivalence-class plan shapes | pass | fail | 0 | 15 |  |
| 185 | json | in | pass | fail | 285 | 44 |  |
| 186 | jsonb | in | pass | fail | 558 | 70 | adapted: 1 COPY file statement(s) sent as \copy |
| 187 | json_encoding | in | pass | fail | 22 | 9 |  |
| 188 | jsonpath | in | pass | fail | 148 | 26 |  |
| 189 | jsonpath_encoding | in | pass | fail | 21 | 6 |  |
| 190 | jsonb_jsonpath | in | pass | fail | 574 | 72 |  |
| 191 | sqljson | in | pass | fail | 105 | 33 |  |
| 192 | sqljson_queryfuncs | in | pass | fail | 285 | 7 |  |
| 193 | sqljson_jsontable | in | pass | fail | 116 | 29 |  |
| 194 | plancache | in | pass | fail | 59 | 8 |  |
| 195 | limit | in | pass | fail | 61 | 8 |  |
| 196 | plpgsql | in | pass | fail | 452 | 182 |  |
| 197 | copy2 | in | pass | timeout | 187 | 5 | timeout after 180s; server restarted to stop the runaway statement; STORE COULD NOT BE REOPENED (kept at /root/pg18-regress-cache/runs/main-1e106b4c8/bicdb-data.unrecoverable-after-copy2); continued on a fresh store with test_setup replayed |
| 198 | temp | in | pass | fail | 105 | 16 |  |
| 199 | domain | in | pass | fail | 222 | 50 |  |
| 200 | rangefuncs | in | pass | fail | 255 | 63 |  |
| 201 | prepare | in | pass | fail | 17 | 5 |  |
| 202 | conversion | out: CREATE CONVERSION / encoding conversion procs in C (regress.so) | pass | fail | 0 | 13 |  |
| 203 | truncate | in | pass | fail | 62 | 18 |  |
| 204 | alter_table | in | pass | fail | 873 | 132 |  |
| 205 | sequence | in | pass | fail | 156 | 13 |  |
| 206 | polymorphism | in | pass | fail | 277 | 42 |  |
| 207 | rowtypes | in | pass | fail | 123 | 38 |  |
| 208 | returning | in | pass | fail | 115 | 10 |  |
| 209 | largeobject | in | pass | fail | 84 | 6 |  |
| 210 | with | in | pass | timeout | 208 | 2 | timeout after 180s; server restarted to stop the runaway statement; STORE COULD NOT BE REOPENED (kept at /root/pg18-regress-cache/runs/main-1e106b4c8/bicdb-data.unrecoverable-after-with); continued on a fresh store with test_setup replayed |
| 211 | xml | in | pass | fail | 145 | 42 |  |
| 212 | partition_join | in | pass | fail | 332 | 76 |  |
| 213 | partition_prune | out: planner partition-pruning plan shapes (EXPLAIN) | pass | fail | 0 | 54 |  |
| 214 | reloptions | out: physical storage parameters (fillfactor, autovacuum, toast options) | pass | fail | 0 | 3 |  |
| 215 | hash_part | in | pass | fail | 25 | 2 |  |
| 216 | indexing | in | pass | fail | 478 | 17 |  |
| 217 | partition_aggregate | in | pass | fail | 82 | 28 |  |
| 218 | partition_info | in | pass | fail | 58 | 2 |  |
| 219 | tuplesort | in | pass | fail | 80 | 11 |  |
| 220 | explain | out: EXPLAIN plan format/shape (planner output) | pass | fail | 0 | 4 |  |
| 221 | memoize | out: planner Memoize node internals | pass | fail | 0 | 10 |  |
| 222 | stats | out: cumulative statistics (pg_stat_*) monitoring | pass | fail | 0 | 24 |  |
| 223 | predicate | out: planner predicate-simplification plan shapes | pass | fail | 0 | 12 |  |
| 224 | numa | out: pg_numa monitoring functions | pass | fail | 0 | 1 |  |
| 225 | compression | out: TOAST compression methods (physical storage) | pass | fail | 0 | 4 |  |
| 226 | compression_pglz | out: TOAST compression methods (physical storage) | pass | fail | 0 | 1 |  |
| 227 | oidjoins | out: catalog foreign-key consistency checks | pass | fail | 0 | 1 |  |
| 228 | event_trigger | in | pass | fail | 162 | 21 |  |
| 229 | event_trigger_login | in | pass | fail | 8 | 1 |  |
| 230 | fast_default | in | pass | fail | 107 | 37 |  |
| 231 | tablespace | out: tablespaces on disk | pass | fail | 0 | 14 |  |

