# Application events on an elastic cluster

`env.EVENTS` binds a deployed application to immutable records distributed over
BicDB ranges. An operator adds a node with `cluster join`, starts `serve`, and
the public balanced controller admits it, proposes placement and copies ranges.
The application host refreshes stale routes as range epochs change. No shard
map or member certificate is supplied to JavaScript.

This binding is separate from `env.DB`. Existing SQL transactions, effect
receipts and transactional job enqueueing keep their current local/native-HA
contract. An event append cannot be committed atomically with an SQL mutation
or job. General distributed SQL forwarding, distributed joins and a durable
cross-range SQL commit service are not provided by this binding.

## Bootstrap and allocation

On the first member, initialize the event collection before serving:

```sh
bicdb cluster init /srv/bicdb --cluster-id example --node-id n1 \
  --address 127.0.0.1:9444 --replication-factor 3 --initial-ranges 64 \
  --application-events
bicdb serve /srv/bicdb --host 127.0.0.1 --port 5433
```

Join and serve two more members using [cluster operations](distributed-cluster-operations.md).
Non-loopback members require pinned mutual TLS. Three live voters are needed
to fully place this RF=3 cluster. Capacity admission uses the declared node
bytes and conservative unknown-range estimates; a small declaration can leave
otherwise empty nodes ineligible. Inspect `cluster status --json` rather than
assuming that a successful membership join means writable ranges are ready.
Use enough initial ranges for future growth; automatic range splitting is not
implemented here.

Create an operator configuration outside application releases:

```json
{
  "format": 1,
  "member_root": "/srv/bicdb",
  "applications": { "notebook": "team-notebook" }
}
```

`member_root` names a running member on the trusted host. It supplies that
member's identity and pinned cluster transport configuration. Treat access to
the directory and its TLS private keys as cluster authority. The host never
opens a second database writer. Namespace allocations must be unique across
applications in the configuration. Releases cannot choose or override them.
Save the operator configuration as a regular file with mode `0600`.

```sh
bic --data /srv/apps serve --events-config /etc/bicdb/events.json
bic --data /srv/apps deploy examples/platform/notebook/bic.json
```

Declare `"bindings": { "events": true }` in the application manifest. Deployment
and recovery reject declared bindings without an allocation. Old manifests and
their content hashes retain their previous serialization when no binding is
declared. Keep equivalent allocations on every application state host used for
failover. Preview applications require their own explicit allocation.

## JavaScript contract

```js
await env.EVENTS.append('event-id', { message: 'Saved durably' });
const value = await env.EVENTS.get('event-id');
const page = await env.EVENTS.list({ cursor: null, limit: 128 });
// page.events: [{ key, value }]; page.next: continuation or null
```

- Append commits one record through range quorum admission. The engine checks
  ownership, recovered leader epoch, schema and voter acknowledgements.
- Identical key/value retries return `{ inserted: false }`. A different value
  for an existing key is rejected. A failed transport call can have an unknown
  commit outcome: keep the same key and JSON value when retrying.
- Keys use 1–256 ASCII letters, digits, `-`, `_` or `.`. Values are at most
  64 KiB of encoded JSON. Namespace allocation is a host responsibility.
- Reads return applied state from the routed leader. They have no quorum read
  barrier and are **eventually consistent**, including during partitions and
  leadership transitions. They are unsuitable for enforcing uniqueness,
  balances or inventory decisions through read-then-write application logic.
- Each page examines at most 128 records and 1 MiB in one range. Empty pages
  may have a continuation. Walk until `next` is null. There is no global order
  or cross-range snapshot. Repeat a walk from the start to discover concurrent
  insertions before an old cursor, and deduplicate by event ID.
- Range replication is independent of disconnected edge synchronization.
  Browser queues upload accepted immutable events when connected. Mutable SQL
  edge tables continue to use the existing explicit sync policies.

For SQL-to-event delivery, use the existing transactional job outbox:

```js
await env.DB.transaction([
  { sql: 'INSERT INTO notes (id, body) VALUES ($1, $2)', params: [id, body] }
], { idempotencyKey: id, jobs: [{ note_id: id, body }] });

// In the application's queue handler:
async function queue(message, env) {
  await env.EVENTS.append(message.id, message.payload);
}
```

The SQL change and queued payload commit together. Event delivery happens
later and may repeat, so the handler uses the stable job ID and payload rather
than attempt-dependent fields. Unknown event outcomes can be retried safely.
Inspect retry/dead-letter state with `bic jobs APP`; this is an outbox workflow,
not a transaction spanning the SQL database and event cluster.

The internal RPC accepts only authenticated cluster members. It is not a public
application API and does not implement per-user permissions. The app host
checks the current execution token and declared binding and supplies only its
operator-assigned namespace. Public application handlers must enforce their
own user authorization. In-flight appends may finish after a client disconnects;
retries remain safe by event ID.
The application host caps event transport connect/I/O timeouts at 250/1000 ms
and route discovery/retries at a five-second budget plus one in-flight call.
Unavailable state returns an error; applications retain pending events and retry.

Application recovery images cover `env.DB` and local jobs. External cluster
events require the cluster backup/recovery workflow; they are not included in
an application's standalone backup or SQL preview. Immutable records currently
have no app-level delete, retention or global indexing API.

## Evidence and reproduction

The [Field Notebook](../examples/platform/notebook/README.md) reference
application demonstrates offline queuing, replay, room capabilities and live
polling. [The scaling harness](../scripts/platform/README.md) runs append/read
traffic during growth, process failure and recovery, then rechecks every
acknowledged event. This workload measures the event binding, not arbitrary SQL
or full database compatibility. Local functional runs are not independent
machine measurements or production scale certification.
