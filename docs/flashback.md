# Flashback: time travel queries

BicDB keeps a durable, SCN-ordered history of the tables you archive. You can
query any archived table as it was at a past SCN or timestamp, list every
version a row went through, and restore a table to a past state in one
statement.

```sql
ALTER TABLE orders FLASHBACK ARCHIVE RETENTION 30 DAYS;

SELECT current_scn();                                   -- 1790943633515776

UPDATE orders SET status = 'cancelled';                 -- oops
DELETE FROM orders WHERE id = 3;

SELECT * FROM orders AS OF SCN 1790943633515776;        -- before the mistake
SELECT * FROM orders AS OF TIMESTAMP now() - interval '5 minutes';

SELECT id, status, versions_operation, versions_starttime
FROM orders VERSIONS BETWEEN SCN MINVALUE AND MAXVALUE
ORDER BY id, versions_startscn NULLS FIRST;

FLASHBACK TABLE orders TO SCN 1790943633515776;         -- undo it
```

## Syntax

| Form | Meaning |
| --- | --- |
| `t AS OF SCN expr` | Oracle: `t` as of an SCN |
| `t AS OF TIMESTAMP expr` | Oracle: `t` as of a timestamp |
| `t VERSIONS BETWEEN {SCN \| TIMESTAMP} lo AND hi` | Oracle: every version alive in `[lo, hi]`; `MINVALUE` / `MAXVALUE` mean the oldest retained history / now |
| `t FOR SYSTEM_TIME AS OF expr` | SQL:2011 / SQL Server / MariaDB. A timestamp, or an SCN when the value is an integer |
| `t FOR SYSTEM_TIME BETWEEN lo AND hi`, `FROM lo TO hi`, `ALL` | SQL:2011 version ranges |
| `ALTER TABLE t FLASHBACK ARCHIVE [RETENTION n unit]` | Start archiving `t`. Units: `SECOND`, `MINUTE`, `HOUR`, `DAY`, `WEEK`, `MONTH` (30 days), `YEAR` (365 days); singular or plural. Without `RETENTION`, history is kept until purged |
| `ALTER TABLE t NO FLASHBACK ARCHIVE` | Stop archiving and discard the history |
| `FLASHBACK TABLE t [, u ...] TO {SCN \| TIMESTAMP} expr` | Restore tables to a past state, in one transaction |
| `current_scn()` | The current SCN (`bigint`) |
| `scn_to_timestamp(scn)` / `timestamp_to_scn(ts)` | Convert between SCNs and `timestamptz` |

The clause goes directly after the table name and before its alias, as in
Oracle: `FROM orders AS OF SCN 42 o`. Bounds may be any constant expression,
including `$n` parameters (typed `bigint` for SCN, `timestamptz` for
TIMESTAMP), `now() - interval '...'`, `current_scn()`, or subexpressions.

Historical relations work everywhere a table does: joins with current data or
other points in time, subqueries, aggregates, `WHERE`, `ORDER BY`, views,
`INSERT ... SELECT`, prepared statements, and the extended protocol. Their
columns carry the table's declared types.

### `VERSIONS` pseudocolumns

| Column | Type | Meaning |
| --- | --- | --- |
| `versions_startscn` / `versions_starttime` | `bigint` / `timestamptz` | When this version was created. `NULL` when it was created before the lower bound or before archiving began |
| `versions_endscn` / `versions_endtime` | `bigint` / `timestamptz` | When it was replaced. `NULL` while current, and for delete versions |
| `versions_xid` | `bigint` | Transaction that created it; `NULL` for the archive baseline |
| `versions_operation` | `text` | `I`, `U` or `D`; `NULL` for the archive baseline |

As in Oracle, `*` and `alias.*` do not expand to the pseudocolumns; select them
by name. A delete appears as its own version (`D`) carrying the deleted row's
values.

## SCNs

An SCN is a hybrid logical clock: `max(previous SCN + 1, Unix microseconds)`.
SCNs only ever increase, including across restarts and checkpoints. Each SCN
is its commit time in microseconds, so `AS OF TIMESTAMP t` equals
`AS OF SCN timestamp_to_scn(t)`, and `scn_to_timestamp` round-trips exactly.
Oracle maps timestamps to SCNs at roughly 3-second granularity; BicDB's mapping
is exact. Two commits only share a microsecond if they arrive within the same
microsecond, and then the later one is stamped up to a few microseconds ahead.

Reads are repeatable. Reading at an SCN fences the clock, so no later commit can
receive an SCN at or below a point that has already been read: the same
`AS OF` query returns the same rows every time. Points in the future are
rejected.

## Guarantees

- **Atomic with the data.** History rows are written inside the transaction
  that changes the data, after commit-time conflict repair. They are logged,
  replicated, checkpointed and recovered exactly like the data. A crash can
  never leave a committed change without its history.
- **States that actually existed.** Commits that touch archived tables take the
  flashback clock for their whole commit, so SCN order equals visibility
  order: every `AS OF` answer is a state the archived tables really passed
  through, including across several tables in one query.
- **Same authorization as current data.** A historical relation needs `SELECT`
  on the table. The table's collection policy (tenant scoping, field masking)
  and SQL row-level security are applied to historical rows exactly as to
  current rows. Every relation fast path (primary-key lookups, index scans,
  `count(*)` shortcuts, join fast paths, plan caches) is structurally unable
  to answer a historical relation from current data.
- **Both storage engines.** Embedded and server-paged databases behave the same.

## Retention and storage

Each archived table gets a hidden history collection (`__bicdb_fb_<table>`).
Enabling the archive copies the current rows into it as the baseline. After
that, every committed insert, update or delete adds one history row per changed
row per transaction.

With a retention, `bicdb serve` purges expired history every
`BICDB_FLASHBACK_PURGE_SECS` seconds (default 600; `0` disables it). It first
raises the table's queryable floor, which takes a brief exclusive lock. It then
deletes the history no query at or after the floor can need, as ordinary
transactions that run alongside client commits. For each row it keeps the
newest version at or before the floor, unless that version is a delete. Asking
for a point below the floor returns SQLSTATE `72000` (`snapshot too old`), with
the oldest available point in the message.

Embedded applications use the same operations from Rust: `enable_flashback`,
`disable_flashback`, `flashback_rows[_with_context]`,
`flashback_versions[_with_context]`, `purge_flashback`,
`purge_expired_flashback` and `current_scn`.

## FLASHBACK TABLE

`FLASHBACK TABLE` computes the difference between the current table and its
state at the target point. It applies that difference in one transaction,
inside the caller's transaction if one is open. It requires `SELECT`,
`INSERT`, `UPDATE` and `DELETE` on every named table. The restore is itself
archived history, so you can flash back over a flashback. As with Oracle's
default (`DISABLE TRIGGERS`), triggers do not fire. Unique indexes and
collection policies are enforced.

It refuses, instead of half-restoring, tables that have row-level security, have
foreign keys, or are referenced by foreign keys. Use `INSERT ... SELECT ... AS
OF` for those.

## Errors

| SQLSTATE | When |
| --- | --- |
| `72000` snapshot_too_old | The point is older than the retained history (or than the archive) |
| `22023` invalid_parameter_value | The point is in the future, or `VERSIONS` bounds are reversed |
| `55000` object_not_in_prerequisite_state | The table is not archived, or its baseline is still being written |

## Compared with Oracle

Matches Oracle: `AS OF SCN|TIMESTAMP`, `VERSIONS BETWEEN` with
`MINVALUE`/`MAXVALUE` and the `VERSIONS_*` pseudocolumns, `FLASHBACK TABLE ...
TO SCN|TIMESTAMP`, per-table archive retention, and SCN/timestamp conversion.

Goes further than Oracle:

- Archived history is durable and exact. It does not depend on undo retention
  sizing, so there is no `ORA-01555` in the middle of the window.
- Timestamps map to SCNs at microsecond precision, not about 3 seconds.
- Reads are fenced, so they are repeatable.
- SQL:2011 `FOR SYSTEM_TIME` works on the same history.
- Historical reads apply row-level security and tenant policies.

Not yet available:

- Flashback Query on tables without an archive. Oracle serves recent history
  from undo for every table; BicDB requires `FLASHBACK ARCHIVE` first.
- `FLASHBACK TABLE ... TO BEFORE DROP` and the recycle bin. Dropping an
  archived table drops its history.
- Flashback Transaction Query, `ORA_ROWSCN`, `VERSIONS ... AS OF`, named
  archives (`CREATE FLASHBACK ARCHIVE`), and Flashback Database. For a
  whole-database rewind, use [PITR](backup-recovery.md).
- `AS OF` on read-only standbys. History rows replicate, but the archive
  setting is not yet part of the replicated catalog.

## Costs

Tables without an archive pay one atomic load per commit. Commits that write
archived tables are serialized with each other, which is what makes SCN order
equal visibility order, and they write one extra row per changed row. History
reads currently scan the table's history collection. They are fine for
auditing and recovery, but not meant to be a hot query path on very large
histories.
