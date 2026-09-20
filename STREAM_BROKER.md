# Stream Broker

Durable embedded broker semantics on top of the append-only `EventStream`:
consumer groups, ack/nack, retry with visibility timeouts, per-group
dead-letter queues (Slice 1); durable queue configuration, retention and
trimming, DLQ redrive/purge (Slice 2); a `broker_*` SQL function surface
usable cross-process over pgwire (Slice 3); a handler-driven drain pump plus
observability snapshot (Slice 4); automatic retention enforcement (Slice 5);
per-queue role ACLs (Slice 6); transactional publish-on-commit (Slice 7);
and push delivery over pgwire via LISTEN/NOTIFY (Slice 8).

## Model

The broker is **event-sourced on the event store itself**. Every state
transition — group creation, a delivery batch, an ack, a nack, a dead-letter —
is appended as a control event to the same durable segment that holds the
messages, so broker state:

- survives restarts by replay (rebuilt into an in-memory projection at open),
- inherits the stream's fsync, compression, and encryption configuration,
- is fully auditable after the fact.

Streams used per queue `q` and consumer group `g`:

| Stream       | Contents                                            |
|--------------|-----------------------------------------------------|
| `queue:q`    | the messages (`QueueMessage`, shared with `db.queue`) |
| `broker:q/g` | control events for one consumer group               |
| `dlq:q/g`    | dead-lettered message copies for one consumer group |

Messages are identified by their event UUID (`message_id`) and by a per-queue
logical **sequence** (0-based publish order). Sequences — not raw segment
offsets — are used in all durable broker records, so they remain stable across
segment compaction.

## Semantics

- **Fan-out across groups, competition within a group.** Every group receives
  each message once; consumers of one group compete for deliveries. A new
  group starts from the beginning of the retained queue history.
- **Visibility timeout.** A delivered message is invisible to its group until
  acked, nacked, or the timeout elapses; then it is redelivered (attempts+1).
- **Ordering.** Fresh delivery follows publish order per group. A delayed
  message at the head of the undelivered range pauses fresh delivery until it
  becomes available rather than being skipped. Redeliveries are served first,
  in sequence order.
- **Retry budget.** `attempts` counts deliveries. A redelivery (or a
  requeueing nack) that would exceed the message's `max_attempts` (default
  16) dead-letters the message instead: a full copy — payload, headers,
  reason, last error — is appended to `dlq:q/g`.
- **Idempotent publish.** Publishing with an `idempotency_key` that was
  already used on the queue is a no-op returning the original receipt.
- **Interop.** Messages published through the plain `db.queue(name)` facade
  are consumable through the broker with default broker fields; the legacy
  facade's local in-memory cursor is unchanged.

## API

```rust
use bicdb_core::{ConsumeOptions, NackOptions, PublishOptions};

let mut broker = db.broker();

let receipt = broker.publish("commerce.orders", payload, headers)?;
// or, with delay / idempotency / retry budget:
let receipt = broker.publish_with("commerce.orders", payload, PublishOptions {
    headers,
    idempotency_key: Some("order-1234".into()),
    delay_ms: Some(5_000),
    max_attempts: Some(8),
})?;

let batch = broker.consume(
    "commerce.orders",
    "sis-enrollment",   // consumer group (created durably on first use)
    "worker-1",         // consumer id within the group
    ConsumeOptions { max_messages: 100, visibility_timeout_ms: 30_000 },
)?;

for message in batch {
    match handle(&message) {
        Ok(()) => broker.ack("commerce.orders", "sis-enrollment", "worker-1",
                             message.message_id)?,
        Err(error) => broker.nack("commerce.orders", "sis-enrollment", "worker-1",
                                  message.message_id, NackOptions {
                                      requeue: true,
                                      delay_ms: Some(5_000),
                                      error: Some(error.to_string()),
                                  })?,
    }
}

let dead = broker.dead_letters("commerce.orders", "sis-enrollment");
let info = broker.group_info("commerce.orders", "sis-enrollment");
```

`ack`/`nack` enforce ownership: they error unless the delivery is currently in
flight for that consumer. After a visibility timeout expires and the message is
redelivered elsewhere, the original consumer's ack fails (at-least-once
delivery).

## Crash recovery

The projection keeps only actionable deliveries (in-flight or awaiting
redelivery); acked and dead-lettered deliveries live only in the log/DLQ. On
reopen, control events are replayed in order: in-flight deliveries whose
visibility deadline has passed become redeliverable, acked messages stay gone,
and group cursors resume exactly where they were. Dead-letter processing
appends the DLQ copy before the control transition, so a crash between the two
redelivers rather than double-delivers.

## Queue configuration & retention (Slice 2)

```rust
broker.configure_queue("commerce.orders", QueueConfig {
    default_max_attempts: Some(8),          // publish-time default
    retention_max_messages: Some(100_000),  // explicit data-loss bound
    retention_max_age_ms: Some(7 * 24 * 3_600_000),
})?;
let report = broker.trim("commerce.orders")?; // rewrites the segment
```

`trim` always reclaims the prefix that **every** consumer group has settled
(acked or dead-lettered), tracked per group as a settled floor plus an
out-of-order set. Configured `retention_max_messages` / `retention_max_age_ms`
bounds may trim further — including unsettled messages — as an explicit
data-loss policy: in-flight deliveries of removed messages are dropped and
group cursors skip past. A queue with no groups is only trimmed by configured
bounds.

Sequences are never reused: a durable `BrokerQueueTrimmed` marker carries the
floor across restarts (it is rewritten into the log *before* the surviving
messages so replay applies it first). Control events that reference only
trimmed messages are removed with them. Idempotency keys of trimmed messages
are forgotten — the dedup window equals the retention window.

DLQ lifecycle: `redrive_dead_letters(queue, group, max)` republishes dead
letters as fresh messages (new ids, attempts reset, headers preserved), each
copy at most once (`BrokerRedriven` markers); the copies stay for audit until
`purge_dead_letters(queue, group)` rewrites them away.

## SQL surface (Slice 3)

Every broker operation is callable from SQL — including over pgwire from any
PostgreSQL client. Composite results are `jsonb`:

```sql
SELECT broker_publish('commerce.orders', '{"order": 1}', '{"tenant": "t1"}');
SELECT broker_publish_with('commerce.orders', '{"order": 2}',
       '{"idempotency_key": "order-2", "delay_ms": 5000, "max_attempts": 8}');
SELECT broker_consume('commerce.orders', 'sis-enrollment', 'worker-1', 100, 30000);
SELECT broker_ack('commerce.orders', 'sis-enrollment', 'worker-1', '<message-uuid>');
SELECT broker_nack('commerce.orders', 'sis-enrollment', 'worker-1', '<message-uuid>',
                   true, 5000, 'temporary failure');
SELECT broker_group_info('commerce.orders', 'sis-enrollment');
SELECT broker_dead_letters('commerce.orders', 'sis-enrollment');
SELECT broker_redrive_dead_letters('commerce.orders', 'sis-enrollment', 100);
SELECT broker_purge_dead_letters('commerce.orders', 'sis-enrollment');
SELECT broker_configure_queue('commerce.orders', '{"retention_max_messages": 100000}');
SELECT broker_queue_config('commerce.orders');
SELECT broker_trim('commerce.orders');
SELECT broker_stats();
```

Broker SQL functions execute under the event-store lock and are durable
immediately — like `nextval`, they do **not** roll back with an enclosing SQL
transaction.

## Worker ergonomics & observability (Slice 4)

`drain` pumps deliverable messages through a handler — acking on `Ok`,
nacking (retry, then dead-letter) on `Err` — until the group is empty or a
message budget is reached:

```rust
let report = broker.drain("commerce.orders", "sis-enrollment", "worker-1",
    DrainOptions { batch_size: 32, visibility_timeout_ms: 30_000,
                   max_messages: 0, nack_delay_ms: Some(5_000) },
    |message| handle(message).map_err(|error| error.to_string()),
)?;
// report.delivered / report.acked / report.nacked
```

`has_deliverable(queue, group)` is a cheap poll for worker loops; combine it
(or `drain`) with the existing in-process `EventStream::subscribe` on the
`queue:<name>` stream as a wake signal for push-style workers.
`stats()` (and `broker_stats()` in SQL) returns a point-in-time snapshot of
every queue and group: retained messages, sequence counters, in-flight and
pending-redelivery counts, settled floors, per-group lag, and DLQ depth.

## Automatic trimming (Slice 5)

Queues with a retention configuration are trimmed automatically: database
compaction (`db.compact()`) enforces configured retention in the same pass as
event-log compaction, and a publish that finds the queue grown past **twice**
its `retention_max_messages` bound trims down to the bound first (hysteresis,
so the segment rewrite amortizes over many publishes; it runs before the
append, so a trim failure cannot half-report a publish). Queues without a
configuration are never auto-trimmed — their history is retained for future
consumer groups unless `trim()` is called explicitly.

## Access control (Slice 6)

`QueueConfig` carries durable role ACLs, enforced at the SQL boundary against
the session's `SecurityContext` (the embedded Rust API is trusted, as are SQL
sessions without a security context and contexts with a bypass policy):

```sql
SELECT broker_configure_queue('orders', '{
  "publish_roles": ["producer"],
  "consume_roles": ["worker"],
  "admin_roles":   ["ops"]
}');
```

Restricted sessions may publish/consume on queues with no ACL for that class,
but **admin operations (configure, trim, redrive, purge) are always denied to
restricted sessions unless a role is explicitly granted** — so a restricted
user can never bootstrap an ACL onto an open queue. `broker_stats()` filters
to consumable queues for restricted sessions.

## Transactional publish (Slice 7)

`broker_publish_on_commit` / `broker_publish_with_on_commit` buffer the
publish on the open transaction and apply it when the transaction commits —
PostgreSQL-NOTIFY-style semantics:

```sql
BEGIN;
UPDATE orders SET status = 'paid' WHERE id = 42;
SELECT broker_publish_on_commit('commerce.orders', '{"order": 42}', NULL);
COMMIT;   -- the message appears atomically with the txn's effects
-- ROLLBACK would have dropped it; ROLLBACK TO SAVEPOINT respects savepoints
```

The receipt carries a pre-allocated `message_id` and `"deferred": true`
(sequence assigned, and idempotency keys deduplicated, at commit time). In
autocommit the functions publish immediately. This works across the embedded
commit, session COMMIT, and pgwire deferred-commit paths, including
multi-statement interactive transactions. Caveat: the flush runs immediately
*after* the commit applies — a crash in that window loses the buffered
messages (and cannot fail the already-applied commit); pair with an outbox
check if you need strict exactly-once across crashes.

## Push delivery over pgwire (Slice 8)

pgwire now implements `LISTEN` / `UNLISTEN` / `NOTIFY` with real asynchronous
delivery: pending notifications are flushed to idle connections by the
connection loop (sub-second latency) and before every ReadyForQuery. Every
broker publish automatically notifies the `bicdb_broker__<queue>` channel
with `{"queue": ..., "message_id": ...}`, so a worker is a plain PostgreSQL
client:

```sql
LISTEN "bicdb_broker__commerce.orders";
-- block on the socket (psycopg wait/select, JDBC getNotifications, ...)
-- on wakeup:
SELECT broker_consume('commerce.orders', 'workers', 'w1', 100, 30000);
SELECT broker_ack('commerce.orders', 'workers', 'w1', '<message-uuid>');
```

Notifications are best-effort wakeups (per-connection queue capped at 4096,
oldest dropped): consumption state lives entirely in the durable broker, so a
missed wakeup only delays delivery until the next poll or notification.

## Not yet (future slices)

- `pg_notify()` as a SQL function routes to the durable notification shim,
  not the live bus (use the `NOTIFY` statement over pgwire for live delivery).
- Strictly atomic publish-on-commit across crashes (the message append is not
  part of the transaction WAL; see the Slice 7 caveat).
- Per-consumer flow control / prefetch windows on push wakeups.
