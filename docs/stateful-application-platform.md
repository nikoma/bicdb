# Durable application deployment

The `bic` binary provides an application supervisor, HTTP routing, a persistent
BicDB database for each application, and durable background delivery. It is built
from this repository and uses the public database APIs.

The default process tier runs trusted applications on a Unix host with the
host user's operating-system authority. Optional [microVM execution](platform-microvm.md)
provides a jailed Linux guest for application code and private, scoped database
access. Existing signed WASM application hosting remains available through
`bicdb app`.

Optional [worker fleets](platform-worker-fleet.md) place compute on other hosts
with lease-based recovery while keeping SQL state and jobs at the state service.

## Run an application

Install Node.js 22 or newer, then build and install the supervisor:

```sh
cargo install --path crates/bicdb-app-host --locked
bic init ./counter --name counter
bic serve
```

Leave the supervisor running. In another terminal:

```sh
bic deploy ./counter/bic.json
curl http://127.0.0.1:8788/apps/counter
curl -X POST -H 'idempotency-key: first-request' \
  http://127.0.0.1:8788/apps/counter/increment
curl -X POST -H 'idempotency-key: first-job' \
  http://127.0.0.1:8788/apps/counter/jobs
bic list
bic logs counter
bic jobs counter
```

Repeating the increment with the same key returns the original result. The
counter and queued work survive a supervisor restart. `bic stop counter` retains
state; `bic start counter` activates the saved release again.

`--data` selects the local state directory, default `.bicdb-platform`.
`--endpoint` selects an existing supervisor. `--token-file` selects a management
token file. By default `bic serve` creates `DATA/management.token` with owner-only
permissions and binds to loopback. Remote clients require an HTTPS origin;
operators provide TLS termination when exposing the HTTP listener remotely.
Treat management access as permission to execute trusted code on the host.

## Release contract

`bic.json` declares format `1`, an application name, version, runtime,
entrypoint, explicit file list, ordered migrations, resources and optional
synchronization policies. See the complete [counter manifest](../examples/platform/counter/bic.json).

The uploader rejects symlinks, traversal, hidden paths, undeclared files and
oversized releases. Releases are content addressed. Both the encoded package
and unpacked execution files are verified before launch. No dependency download,
installation script or build hook runs on the supervisor. Build applications and
include their runtime dependencies in the explicit file set before deployment.

`node` runs a default-exported `fetch(request, env)` handler. The optional
`queue(message, env)` handler receives durable background deliveries.
`node_http` runs an existing Node HTTP entrypoint; it must bind `HOST:PORT` and
return success from `/health`. It receives `BICDB_DATABASE_URL` and the scoped
`BICDB_DATABASE_TOKEN`, using the same JSON query API as the fetch adapter.

Runtime stdout and stderr go to the application log. Management log reads are
bounded to the final 64 KiB. An operator must rotate the underlying log file.
In the process tier the memory setting selects Node's old-space heap ceiling.
The VM tier applies a total guest-memory ceiling. Request timeouts, request/response body limits and application concurrency
admission apply at the gateway. Streaming responses and WebSocket forwarding
are not implemented by this gateway.

## Database and jobs

The fetch adapter supplies:

```javascript
const rows = await env.DB.query(
  'SELECT id, body FROM notes WHERE id = $1', ['note-1']
);

const results = await env.DB.transaction([
  { sql: 'UPDATE counters SET value = value + 1 WHERE id = $1 RETURNING value',
    params: ['requests'] }
], { idempotencyKey: 'operation-123', jobs: [{ action: 'refresh' }] });

const conflicts = await env.DB.conflicts('notes');

await env.JOBS.send({ action: 'refresh' }, { idempotencyKey: 'refresh-123' });
```

Each query call and transaction batch executes in one database transaction.
Parameters are bound values, never SQL interpolation. Query requests accept
SELECT, INSERT, UPDATE and DELETE; schema changes belong in migrations.
An idempotency receipt commits in the same transaction as its database effects.
Reusing a key with different statements or parameters fails. Receipts are
retained; operators must account for their storage until a retention API exists.
JSON numbers use JavaScript's numeric range; cast large SQL integers to text
when exact JavaScript representation is required.

Job delivery is at least once, with bounded attempts, visibility leases and
dead letters. A handler should use its stable message ID as a database
idempotency key, as the counter example does. External effects require the
receiving service's idempotency mechanism. A standalone `JOBS.send` is a separate
durable operation from a preceding SQL transaction. Use the transaction option
`jobs` to commit queued work atomically with SQL effects. Outbox entries move
into a database-owned job ledger in one transaction. Job IDs, idempotency keys,
delivery attempts, active leases and completion state live alongside application
data and receipts. Rebuilding execution on another host preserves those IDs;
an old delivery attempt cannot settle its replacement. Sending a job with an
existing idempotency key and a different payload is refused.
Payloads are stored separately from mutable delivery metadata, so renewing or
settling a delivery does not rewrite its payload. A job payload is limited to
8 MiB; an outbox transfer handles at most 128 jobs and 16 MiB of payload data.

Existing local-broker jobs migrate automatically in restartable batches of up to
128 messages and 16 MiB of serialized events plus delivery metadata. Migration
preserves IDs, outstanding leases, attempts and terminal state; it never consumes
or acknowledges the old broker. The historical broker directory remains in
recovery images for compatibility. New deliveries use the application database.
Recovery images now use application format 2; this host also accepts format 1
and migrates its local-broker jobs. Earlier hosts that understand only format 1
refuse the new images rather than silently missing the job ledger.
Jobs do not enter selective edge record synchronization. Application recovery
and native state replication remain separate from that synchronization protocol.
Completed jobs and dead letters are retained; operators must account for their
storage until retention and redrive management are exposed by the platform.

## Upgrades and recovery

Only one supervisor may own a platform directory. Requests and active job
handlers drain before a deployment changes its application process. Unexpected
process exits trigger a restart. Execution deadlines terminate a blocked
runtime, which the supervisor replaces while retaining its database. Linux process-tier children are terminated on supervisor death. The VM tier
records and fences surviving VMM identities during recovery. Restarting the supervisor loads the committed deployment catalog and
reopens the database with fsync enabled.

Each migration and its content hash commit together. Applied migrations must
remain an unchanged prefix of subsequent releases. Failed migrations roll back
that migration's writes. Earlier migrations from the same deployment may
already have committed. Failed application activation disables the route rather
than running old code against a potentially changed schema.

`bic rollback counter` activates the previous package only when its migration
history still covers every committed migration. It does not rewind database
contents or reverse external effects. An encrypted recovery image can transfer the application to a fresh host:

```sh
umask 077
openssl rand -hex 32 > backup.key
bic backup counter counter.bicbak --passphrase-file backup.key
# On a target host with no running supervisor:
bic --data ./recovered restore counter.bicbak --passphrase-file backup.key
bic --data ./recovered serve
```

Restores publish a complete inactive application directory and refuse existing
applications. The image contains the current and previous release, database,
request receipts, pending transactional outbox and local broker state. It
excludes management credentials and peer transport grants. Transfer the key
separately and re-provision peer grants where needed. Stop or otherwise fence
the previous writer before explicitly running `bic start counter` on the target.
Recovery retains the database node identity; activating two restored copies is
not a supported multi-writer topology. Database conflicts do not fence a former
writer. Automatic state-writer failover requires separate writer and route
authority; manual restore and a local process restart do not establish those
guarantees. Compute workers can fail over while retaining this one state service,
as described in the worker fleet guide.

## Edge synchronization

Synchronization is opt-in for explicitly named public tables. Deploy the same
release on both nodes; provision the peer node IDs, signing keys and a separate
pair-specific token. There is no automatic trust-on-first-use enrollment.

```json
"sync": {
  "notes": { "mode": "append_only" }
}
```

`append_only` allows new rows from authorized peers and refuses updates, deletes
and upserts. Use independently generated IDs. Concurrent insertion of the same
ID can produce a deterministic projection with a retained record conflict;
it does not establish a globally unique insertion across disconnected nodes.
`env.DB.conflicts(table)` exposes retained candidates for application review.
For mutable data, `single_writer` names one stable database node:

```json
"sync": {
  "catalog": {
    "mode": "single_writer",
    "node": "00000000-0000-0000-0000-000000000001"
  }
}
```

The declared writer can mutate the table offline. Other nodes are read replicas
and refuse local mutations. Writer transfer requires a separate fenced workflow;
editing the node ID is not a failover protocol. An established nonempty sync
policy is immutable for an application. Create another application to change
its synchronization scope.

Inspect both application identities:

```sh
bic identity notes
bic --endpoint https://edge.example --token-file edge-management.token identity notes
bic peer-token notes-pair.token
```

On each supervisor, add the other node's identity and URL using the same
pair-specific token file:

```sh
bic peer add notes --node-id OTHER_NODE_ID --public-key OTHER_PUBLIC_KEY \
  --peer-endpoint https://other.example --peer-token-file notes-pair.token
bic peer list notes
bic peer remove notes OTHER_NODE_ID
```

Only explicitly enabled records synchronize. SQL schemas, deployment catalogs,
credentials, job deliveries, migration receipts and request receipts stay local.
Events retain their original signatures; every author must be provisioned on
receiving nodes, including authors arriving through a relay. A peer grant covers
that application's declared sync tables. It is not row-level user authorization.

The loop retries after disconnection and derives coverage from the durable event
log. Replayed bundles deduplicate. Peers must run the same release while
exchanging changes. Each exchange transfers the next bounded prefix of missing events, up to
10,000 events and 15 MiB of event data. Large backlogs resume across exchanges
without acknowledging the untransferred tail. One oversized event is refused.
Synchronization preserves record causality, not a distributed SQL transaction
or cross-node foreign-key, inventory or uniqueness invariant.

## Validation

`cargo test -p bicdb-app-host` exercises migration atomicity, durable request
receipts, scoped authorization, immutable releases, supervisor restart, background
jobs, compatible upgrades and record synchronization. The core mesh tests cover
signature verification, conflicting histories and interrupted exchange.

A running deployment, a published binary and merged source are separate delivery
states. This document describes the source implementation and its acceptance
contracts; it does not attest a production deployment.
