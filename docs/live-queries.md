# Live queries: SUBSCRIBE

`SUBSCRIBE <select>` returns a query's result and then keeps streaming every
change to it, for as long as the client stays connected. A dashboard, a
notification feed or a social timeline is one subscription instead of
polling, triggers, a queue and a push server.

```sql
SUBSCRIBE SELECT p.id, p.author, p.body
FROM posts p JOIN follows f ON f.followee = p.author
WHERE f.follower = 'alice'
ORDER BY p.id DESC LIMIT 50;
```

```text
commit_seq | change | id | author | body
-----------+--------+----+--------+---------------
        41 | +      |  3 | carol  | hi alice          <- the initial result
        41 | +      |  1 | bob    | hello
        42 | +      |  4 | bob    | second post       <- bob posts
        43 | -      |  3 | carol  | hi alice          <- carol edits post 3
        43 | +      |  3 | carol  | edited
        44 | +      |  5 | dave   | ...               <- alice follows dave
        44 | -      |  1 | bob    | hello             <- and post 1 leaves the top 50
```

Every row of the subscription carries two leading columns:

| Column | Meaning |
| --- | --- |
| `commit_seq` | The commit the result reflects after this change |
| `change` | `+`: the row entered the result; `-`: it left. An update is a `-` for the old row and a `+` for the new one |

Any `SELECT` works: filters, joins, aggregates, `ORDER BY ... LIMIT` (a
feed's top N), parameters (`$1`). The subscription runs as the subscriber:
their privileges, row-level security policies, roles and session settings
apply to the initial result and to every change, so a user never receives a
change to a row they may not read.

## Reading the stream

- **Drivers** receive the rows as the result of one statement that stays
  open. Read it incrementally: `cur.stream(...)` in psycopg, `query_raw` in
  tokio-postgres, `rows.Next()` in pgx, a server-side cursor or single-row
  mode in libpq. Closing the stream (which cancels the statement) or a
  `CancelRequest` ends the subscription with SQLSTATE `57014`; the
  connection stays usable.
- **`psql` and scripts**: `COPY (SUBSCRIBE ...) TO STDOUT` prints each change
  as it arrives, tab-separated (or `WITH (FORMAT csv)`).
- The subscription also ends when the client disconnects.

A subscription cannot start inside a transaction block. `SUBSCRIBE` is a
statement of the server; it is not valid in a view or a function.

## How it works

Changes are found by re-evaluating the query as the subscriber, so every row
a subscriber receives is one they may read. How much is re-evaluated depends
on the query:

| Query shape | Per change |
| --- | --- |
| One table, filtered: `SELECT ... FROM t WHERE ...` (no join, group, `LIMIT`, `DISTINCT`, subquery) | The changed rows only: the table remembers the ids of recently written rows, and the query runs for each with `AND <key> = <id>`, a key lookup |
| One table, grouped by one column: `... GROUP BY g` (aggregates, `HAVING` allowed) | The affected groups only: the groups the changed rows belonged to and belong to now are re-aggregated |
| Anything else: joins, `LIMIT`, `DISTINCT`, subqueries, views, several `GROUP BY` columns, global aggregates | The whole query, then a diff against the previous result |

Measured on a 200 000-row table with the writer's commit included: a change
to an unindexed filter arrived in 20 ms incrementally versus 600 ms for the
whole re-run; a count over 5 000 groups the same. The incremental paths need
a single-column primary key of an integer or text-like type; other tables use
the whole re-run. A table whose window of remembered writes (64 k rows) was
exceeded, or a round with more than 200 changed rows, re-reads the whole
query once.

Commits that land during an evaluation are picked up by the next one: a
burst of writes costs one evaluation. Row-level security and field masking
apply to every evaluation; if the key column itself is masked for the
subscriber, the subscription switches to whole re-runs.

`bicdb_live_full_runs_total` and `bicdb_live_incremental_runs_total` on the
metrics endpoint count the two kinds of evaluation;
`bicdb_live_subscriptions` on the metrics endpoint counts open
subscriptions.

## Example: a social timeline in a browser

1. The API server holds one database connection per connected user (or one
   per feed shape, parameterized by user) and runs
   `SUBSCRIBE SELECT ... WHERE f.follower = $1 ORDER BY created_at DESC LIMIT 50`.
2. It forwards each `+` and `-` row over the user's websocket; the browser
   inserts, removes or replaces the post.
3. Posts, follows, blocks and deletions are ordinary writes to the tables;
   every affected timeline updates within a commit.
4. A reconnecting browser simply subscribes again and receives the current
   feed.
