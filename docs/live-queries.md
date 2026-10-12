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

The server re-runs the query, as the subscriber, whenever a commit changed a
table the query reads, and streams the difference to the previous result
(rows are compared by value, so duplicates are counted). Commits that land
during a re-run are picked up by the next one: a burst of writes costs one
re-run, not one per commit. Queries over views or relations the server
cannot map to tables re-run on every commit.

This makes any query correct, at the cost of re-running it; a large query
subscribed by many clients is re-run once per client per change. Keep
subscribed queries selective (an indexed `WHERE`, a `LIMIT`) and the server
will keep up with thousands of subscribers. Incremental maintenance of
simple filters and aggregates, which avoids the re-run, is the next step.

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
