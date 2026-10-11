# Connectors: Redis and AMQP

`bicdb serve --connectors connectors.json` moves messages between the
database and external messaging systems:

- **Sinks** drain a queue of the built-in broker (the durable queues of the
  `broker_*` SQL functions) to Redis (a stream, a list or a
  channel) or to an AMQP 0-9-1 broker such as RabbitMQ.
- **Sources** drain a Redis stream or an AMQP queue into the database: into a
  table, through a function, or onto a broker queue.

Both directions are at-least-once and crash-safe: a message is acknowledged
on one side only after the other side has it durably.

## Database changes to the outside world (transactional outbox)

A trigger publishes with `broker_publish_on_commit`, so the message exists if
and only if the transaction commits:

```sql
CREATE FUNCTION orders_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  PERFORM broker_publish_on_commit('orders',
    jsonb_build_object('id', NEW.id, 'amount', NEW.amount), NULL);
  RETURN NEW;
END $$;

CREATE TRIGGER orders_outbox AFTER INSERT ON orders
  FOR EACH ROW EXECUTE FUNCTION orders_outbox();
```

A sink then delivers the `orders` queue:

```json
{
  "sinks": [
    {"name": "orders-to-redis", "queue": "orders",
     "target": {"redis": {"url_env": "REDIS_URL", "stream": "orders", "maxlen": 1000000}}},
    {"name": "orders-to-rabbit", "queue": "orders",
     "target": {"amqp": {"url_env": "AMQP_URL", "exchange": "orders", "routing_key": "created"}}}
  ]
}
```

Each sink consumes the queue as its own consumer group (`connector-<name>`),
so several sinks each receive every message. A new sink starts at the
beginning of the queue's retained history.

| Target | What is sent |
| --- | --- |
| Redis `stream` | `XADD` with fields `id` (the broker message id), `queue`, `sequence`, `payload` and `headers` (JSON); `maxlen` trims approximately |
| Redis `list` | `LPUSH` of a JSON envelope `{id, queue, sequence, payload, headers, created_at}` |
| Redis `channel` | `PUBLISH` of the same envelope (pub/sub: subscribers that are not connected miss it) |
| AMQP | The payload as the JSON body, the broker message id as `message_id`, persistent delivery, headers `bicdb-queue`, `bicdb-sequence` and the message's own headers |

A message is acknowledged on the queue only after the target confirmed it:
the `XADD`/`LPUSH`/`PUBLISH` reply, or the AMQP publisher confirm. AMQP
publishes are mandatory: a message no queue is bound for counts as a failed
delivery and is retried, it is not dropped. Failed deliveries are retried
after `retry_delay_ms` (default 1000) and, past the message's attempt limit,
dead-lettered by the broker (`broker_dead_letters`). Receivers deduplicate on
the message id: a delivery that succeeded just before a crash is sent again.

## The outside world into the database (draining a burst)

A source reads an external queue in batches and writes each batch in one
transaction. A fast writer can fill Redis or RabbitMQ in bursts while the
database drains at its own pace:

```json
{
  "sources": [
    {"name": "clicks", "from": {"redis": {"url_env": "REDIS_URL", "stream": "clicks"}},
     "into": {"table": {"table": "clicks", "key": "id", "payload_column": "raw"}},
     "batch": 500, "flush_ms": 200, "max_per_second": 5000},
    {"name": "signups", "from": {"amqp": {"url_env": "AMQP_URL", "queue": "signups"}},
     "into": {"function": "ingest_signup"}}
  ]
}
```

| Field | Meaning |
| --- | --- |
| `batch` | Messages per transaction (default 500) |
| `flush_ms` | Longest wait for a batch to fill (default 200) |
| `max_per_second` | Upper bound on messages written per second (default: none) |
| `retry_delay_ms` | Wait after a failed write or connection loss (default 1000) |

Where messages go:

- `{"table": {"table": "t", "key": "id", "payload_column": "raw", "on_conflict": "ignore"}}`:
  message fields become columns of the same name (other fields are ignored);
  `key` receives the message's identity when the message does not set it;
  `payload_column` receives the whole message as JSON. With a key, a
  redelivered message is written once: `"ignore"` (default) keeps the first
  copy, `"update"` overwrites it.
- `{"function": "f"}`: `SELECT f(message::jsonb)` per message, inside the
  batch's transaction. Use this to transform, route or validate.
- `{"queue": "q"}`: onto a broker queue, idempotent by message identity, for
  workers inside the database.

What a message is:

- **Redis stream entries**: the fields, as text. When a `payload` field holds
  a JSON object (what a BicDB sink writes), that object is the message. The
  identity is the `id` field, or else the entry id.
- **AMQP messages**: a JSON object body is the message; any other body
  becomes `{"body": ...}`. The identity is the `message_id` property.

A batch is acknowledged upstream only after its transaction committed and,
with [synchronous replication](high-availability.md), after the synchronous
standbys confirmed it. A Redis source reads through a consumer group
(`group`, default `bicdb`; `start` `"0"` reads everything already in the
stream, `"$"` only new entries) and re-reads its own unacknowledged entries
after a restart or a failure; unacknowledged AMQP messages return to their
queue when the connection closes. A kill in the middle of a burst loses
nothing and, with a key, writes nothing twice.

## Operating

- Connectors run only on the primary, and in cluster mode for the default
  database. A standby starts them when it is promoted.
- Keep credentials out of the file: `url_env` names an environment variable
  holding the URL. `rediss://` and `amqps://` use TLS.
- `SELECT * FROM bicdb_connectors` shows each connector's state (`running`,
  `idle`, `retrying`, `standby`), messages moved, failures and the last error.
  With `--metrics-listen`, the same appears as
  `bicdb_connector_messages_total`, `bicdb_connector_failures_total` and
  `bicdb_connector_retrying`.
- Redis sources need streams (Redis 5 or newer). Redis lists and pub/sub are
  sink targets only: neither can be read without losing messages on a crash.
