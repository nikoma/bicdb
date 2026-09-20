# Redis-compatible cache server (bicdb-resp)

bicdb can serve the Redis wire protocol (RESP2) as a drop-in cache for
internal applications. Entries are ordinary bicdb records under the hood, so
the cache is **durable**: keys *and their TTLs* survive restarts and crashes
via the WAL — something Redis itself only approximates with RDB/AOF.

```
bicdb cache-serve /var/lib/bicdb-cache --port 6379
```

Any Redis client works unchanged:

```
redis-cli -p 6379 SET session:42 '{"user":"niko"}' EX 3600
redis-cli -p 6379 GET session:42
```

## Scope

v1 implements the **string + TTL command family** — what Rails.cache, the
Django cache backend, session stores, rate limiters, and generic memoization
actually use:

| Area | Commands |
|---|---|
| Strings | `GET` `SET` (EX/PX/EXAT/PXAT/NX/XX/GET/KEEPTTL) `SETNX` `SETEX` `PSETEX` `GETSET` `GETDEL` `GETEX` `MGET` `MSET` `MSETNX` `APPEND` `STRLEN` |
| Counters | `INCR` `DECR` `INCRBY` `DECRBY` `INCRBYFLOAT` |
| Keys | `DEL` `UNLINK` `EXISTS` `TYPE` `RENAME` `KEYS` `SCAN` `DBSIZE` |
| TTL | `EXPIRE` `PEXPIRE` `EXPIREAT` `PEXPIREAT` `TTL` `PTTL` `PERSIST` |
| Databases | `SELECT` (0–15) `FLUSHDB` `FLUSHALL` |
| Connection | `PING` `ECHO` `AUTH` `HELLO` `CLIENT` `COMMAND` `INFO` `QUIT` `RESET` |

Not implemented (commands return a clear error): lists, hashes, sets, sorted
sets, pub/sub, Lua scripting, `MULTI`/`EXEC` transactions, cluster protocol.
RESP3 (`HELLO 3`) is refused with `NOPROTO` so clients fall back to RESP2
automatically.

## Semantics

- **Values are binary-safe** (arbitrary bytes), as are keys.
- **Expiry** is enforced lazily on read *and* by a background sweeper
  (100 ms cadence), so expired keys stop being visible immediately and are
  physically reclaimed shortly after.
- **TTLs are persistent.** A key set with `EX 3600` that expires while the
  server is down is gone after restart; one with time remaining keeps its
  remaining TTL.
- **Atomicity**: read-modify-write commands (`INCR`, `APPEND`, `SET NX`,
  `MSETNX`, `GETDEL`, `RENAME`) execute under one exclusive engine lock, so
  they are atomic under concurrent connections, like Redis.
- **Logical databases**: `SELECT 0..15` map to bicdb collections
  `cache_db0..cache_db15` in the same data directory.

## Options

```
bicdb cache-serve <path>
    --host 127.0.0.1        bind address
    --port 6379             bind port
    --requirepass <pw>      require AUTH (or set BICDB_CACHE_PASSWORD)
    --fsync                 fsync every write (durable to power loss; slower)
    --max-keys <n>          total key budget across all databases
    --eviction <policy>     noeviction | allkeys-random | volatile-ttl
    --max-connections 1000  concurrent client cap
    --hotview               enable the SQL + HOTVIEW.* surface (off by default)
    --ephemeral             memory-only cache entries (see below)
```

`--ephemeral` keeps cache entries in memory only — no engine commit per
write, so SET runs at memory speed (97% of Redis on the same host; durable
mode is bounded by the WAL's always-synchronous group commit). Keys are lost
on restart, but SQL tables and HotView definitions remain durable, and
materialized views recompute at startup — derived entries come back hot even
though the cache itself is ephemeral. Semantics (TTLs, NX/XX, KEEPTTL,
counters, eviction) are identical in both modes.

By default writes are committed to the WAL without fsync — the right
trade-off for a cache (survives process crashes and restarts; a power loss
may lose the last instants of writes). Use `--fsync` for full durability.

`--max-keys` bounds the cache by entry count. At the cap, new-key writes
either fail (`noeviction`, the Redis default), evict a pseudo-random key
(`allkeys-random`), or evict the key closest to expiry (`volatile-ttl`).

## HotView: commit-aware materialized cache (causal cache)

**Opt-in: start the server with `--hotview`.** It is off by default because
the `SQL` command exposes arbitrary SQL execution on the cache port — that
should be a deliberate choice, not a surprise. When off, `SQL` and
`HOTVIEW.*` return a clear error, no view machinery runs, and persisted view
definitions lie dormant (their last materialized values remain readable as
plain keys); re-enabling revives them, recomputed at startup.

A normal cache guesses when data went stale (TTLs, versions, glue code).
Because the cache and the database are the same engine, bicdb doesn't have to
guess: a **hotview** binds a cache key to a SQL query, and when a committed
write changes anything that query depends on, the entry is recomputed (or
invalidated) **before the write's reply is sent**. The writer — and anything
it triggers afterwards — can never observe a stale cache entry.

```
redis-cli> SQL "CREATE TABLE orders (id TEXT PRIMARY KEY, customer TEXT, total BIGINT)"
redis-cli> SQL "INSERT INTO orders VALUES ('o1','acme',100), ('o2','acme',250)"
redis-cli> HOTVIEW.CREATE dash:acme "SELECT COUNT(*) AS orders, SUM(total) AS revenue
                                     FROM orders WHERE customer = 'acme'"
(integer) 1
redis-cli> GET dash:acme
"[{\"orders\":2,\"revenue\":350}]"
redis-cli> SQL "UPDATE orders SET total = 999 WHERE id = 'o1'"
"{...\"hotviews_refreshed\":1...}"
redis-cli> GET dash:acme
"[{\"orders\":2,\"revenue\":1249}]"        # already hot — no TTL, no rebuild race
```

One write cascades into every dependent entry at once — product page,
category rollup, and a JOIN'd cart total all refresh from a single
`UPDATE products SET price = ...`, because they all declared (via their SQL)
that they depend on `products`.

### Commands

- `SQL <statement>` — run SQL over RESP (DDL/DML/SELECT; results as JSON).
  Write replies include `hotviews_refreshed` / `hotviews_invalidated` counts.
- `HOTVIEW.CREATE <key> <select-sql> [MODE refresh|invalidate]` — bind a key
  to a query and materialize it immediately. `refresh` (default) recomputes
  on every dependent commit; `invalidate` deletes the entry instead (the app
  recomputes on its own schedule via `HOTVIEW.REFRESH`).
- `HOTVIEW.REFRESH <key>` — force a recompute; returns the new generation.
- `HOTVIEW.STATUS <key>` — sql, mode, deps, generation, staleness, last
  refresh time, last error.
- `HOTVIEW.LIST` / `HOTVIEW.DROP <key>`.

The materialized value is an ordinary cache key (a JSON array of row
objects), so any Redis client reads it with plain `GET` — no new client
library needed.

### How invalidation stays honest

- **Read dependencies** come from the query AST at CREATE time: every
  relation the SELECT references (FROM, JOINs, subqueries, CTEs).
- **Change detection trusts nothing parsed.** The SQL executor diffs bicdb's
  per-collection generation counters around each write under the same
  exclusive lock, so triggers, cascades, and multi-statement scripts
  invalidate exactly the views whose tables actually changed.
- **Timing, precisely**: recompute happens synchronously after commit, inside
  the write command, before its RESP reply. A concurrent reader racing the
  write may briefly see the previous value (never a torn one); once the
  writer has its reply, every dependent entry is consistent with the commit.
- **Restart-hot**: definitions persist in the engine; on startup every view
  is recomputed before the server accepts connections.

Current limits: dependencies are table-level (any change to a dependent
table refreshes the view, even if the specific rows wouldn't affect it), and
invalidation triggers on writes made through this server's `SQL` command —
the engine is single-process, so that is currently every SQL write path.

## Design notes

- Crate: `crates/bicdb-resp`, structured like `bicdb-pgwire` (thread per
  connection, engine behind `RwLock<BicDb>`); commands go straight to
  `bicdb-core` — the SQL layer is not involved.
- A key becomes the record id (UTF-8 keys verbatim under an `s` prefix,
  others base64 under `b`), the value lives in `Record.payload`, and the
  expiry deadline (epoch ms) rides in `Record.timestamp`.
- The sweeper drains a min-heap of deadlines and re-checks the live record
  before deleting, so stale heap entries from overwritten keys are harmless.
- `SCAN` cursors are offsets into the id-sorted key listing — weaker than
  Redis's reverse-binary cursor, but honors the client contract (iterate
  until cursor 0).
