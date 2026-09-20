# Large Result Streaming

BicDB supports memory-bounded client patterns for large transactional reports and exports through
pgwire portals and `COPY TO STDOUT`.

Ordinary simple queries still enforce `max_result_rows`. Use one of the
streaming paths when a report or export can legitimately return more rows than
that limit.

## Client Patterns

Use pgwire cursors or driver fetch APIs for interactive reports. The current
server behavior is the extended-query portal flow: bind a named portal, execute
with a positive fetch row count, then repeat `Execute` until the server returns
`CommandComplete` instead of `PortalSuspended`.

```text
Parse "SELECT id, posted_at, account_id, amount FROM ledger_entries ORDER BY posted_at, id"
Bind portal "erp_export"
Execute portal "erp_export" max_rows=5000
Execute portal "erp_export" max_rows=5000
Close portal "erp_export"
```

PostgreSQL drivers usually expose this as a named portal or cursor with a fetch
size. Keep fetch sizes in the low thousands for interactive screens and tens of
thousands for offline exports, then tune from server metrics.

SQL-level `DECLARE CURSOR`, `FETCH`, and `CLOSE` statements are not the contract
for this release.

Use `COPY TO STDOUT` for file exports:

```sql
COPY (
  SELECT id, posted_at, account_id, amount
  FROM ledger_entries
  ORDER BY posted_at, id
) TO STDOUT CSV;
```

`COPY TO STDOUT` sends rows as `CopyData` frames as they are rendered. The server
does not retain the rendered export payload after sending it.

## Limits And Cancellation

- Simple query protocol results are still capped by `max_result_rows`.
- Extended-query `Execute` with a positive row count may fetch past
  `max_result_rows`; the portal returns `PortalSuspended` until all rows are
  consumed.
- `COPY TO STDOUT` may export past `max_result_rows`.
- Cancellation and query timeouts are checked while streaming batches and COPY
  rows.
- Client disconnect, portal close, timeout, and shutdown release portal stream
  accounting.

The current SQL engine exposes a row-stream iterator at the execution boundary.
ORDER BY, GROUP BY, and DISTINCT use byte-bounded external sort runs when their
working set exceeds the configured memory allowance. Joins, aggregate state,
final result assembly, and some storage scans can still materialize intermediate
records before rows are drained to pgwire. Cursor and COPY clients should still
prefer selective predicates, stable ordering, and bounded fetch sizes.

## External Sort Limits

The limits are part of `DbConfig` and can also be set before opening BicDB:

| Environment variable | Default | Meaning |
| --- | ---: | --- |
| `BICDB_QUERY_WORK_MEMORY_BYTES` | 64 MiB | Maximum resident bytes for one external-sort run buffer. A single larger row is rejected with SQLSTATE `53200`. |
| `BICDB_QUERY_TEMP_SPACE_BYTES` | 8 GiB | Hard peak temporary-space quota for one sort, including input and output merge generations. Exhaustion returns SQLSTATE `53100`. |
| `BICDB_QUERY_SERVER_TEMP_SPACE_BYTES` | 64 GiB | Aggregate process-local quota shared by concurrent sort operators using the same canonical spill root. The strictest live configuration wins. |
| `BICDB_QUERY_MERGE_FAN_IN` | 32 | Maximum run files opened in one merge group (range 2–128). |
| `BICDB_SPILL_DIR` | `<database>/tmp/sql-spill` | Optional dedicated parent directory for uniquely owned sort workspaces. Do not share an override between independent BicDB server processes. |

Spill files are framed and checksummed. Every sort owns a unique temporary
directory that is removed when the sort succeeds or unwinds after an error.
The first spill after process startup removes abandoned BicDB-owned workspace
directories and legacy run files, without touching unrelated paths or following
symlinks. Page-backed batch scans check cancellation before decoding every row,
and external run generation and merge passes check cancellation at bounded
intervals. Cancellation drops the active cursor/workspace, releases page pins
and shared temporary-space reservations, and is never converted into a
materializing fallback. Resource-limit failures have the same fail-closed
behavior.

## Metrics

`SELECT * FROM bicdb_server_stats;` includes:

| Column | Meaning |
| --- | --- |
| `rows_streamed` | Rows sent through pgwire portal fetches or `COPY TO STDOUT`. |
| `bytes_streamed` | DataRow/CopyData payload bytes sent for streamed rows. |
| `cursor_count` | Active suspended portal streams. |
| `cursor_memory_bytes` | Estimated memory held by active suspended portal streams. |
| `spilled_to_disk_bytes` | Reserved pgwire counter; external sort exists, but wiring its per-operator byte count into server statistics remains pending, so this currently reports `0`. |

Alert on sustained cursor memory growth, long-lived cursor counts, or stalled
exports with no increase in `rows_streamed`.
