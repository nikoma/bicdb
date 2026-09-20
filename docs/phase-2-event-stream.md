# BicDB Phase 2: Event Stream Architecture

Phase 2 makes BicDB event-native. The intended primitive is:

```text
append-only EventStream
  -> replay
  -> subscriptions
  -> projections
  -> queues
  -> audit
  -> sync transfer
```

This phase intentionally does not add networking, clustering, consensus,
PostgreSQL compatibility, or graph database features.

## Event Contract

```rust
pub struct Event {
    pub id: Uuid,
    pub stream: String,
    pub event_type: String,
    pub timestamp: i64,
    pub payload: serde_json::Value,
    pub metadata: serde_json::Value,
}
```

Events are stored in `events/events.seg` using the same append-frame storage
layer as records and sync entries: checksummed frames, crash recovery by replay,
optional zstd compression, and optional mmap reads during open.

## Local APIs

- `db.events_mut().append(event)` appends one event and returns its segment offset.
- `db.events().read(stream)` reads all events for a stream.
- `db.events().read_since(offset)` reads all events at or after an offset.
- `db.events().replay(stream)` replays a stream from the beginning.
- `db.events().replay_from(stream, offset)` replays one stream from an offset.
- `db.events_mut().subscribe(stream, handler)` registers an in-process handler.
- `db.events_mut().subscribe_async(stream, handler)` dispatches events through a local worker thread.

## Projections

`EventProjection` rebuilds queryable state by applying events in order.

Included demonstration projections:

- `PatientView`
- `AppointmentView`
- `DeviceView`

The rebuild path is deliberately simple: replay stream, apply events, query the
resulting in-memory state. That keeps correctness clear before adding indexing
or persisted projection checkpoints.

## Queue Mode

Queue mode is not a separate subsystem. `db.queue(name).publish(...)` appends a
`QueueMessage` event to `queue:{name}`. `consume(max)` advances a local in-memory
consumer offset for that queue and returns stored events.

Durable consumer groups and acknowledgements are future work; the important
Phase 2 decision is that queue messages are still events.

## Audit And Time Travel

When `DbConfig::with_audit_events(true)` is enabled, collection mutations emit:

- `RecordCreated`
- `RecordUpdated`
- `RecordDeleted`

These are written to `bicdb.records`. `db.snapshot_at(timestamp)` rebuilds record
state by replaying those audit events through the requested timestamp.

## Sync Foundation

The local sync foundation is event transfer:

```text
node A EventStream
  -> export_events_since(offset)
  -> transfer outside BicDB
  -> node B import_events(events)
  -> node B EventStream
```

Imported events are deduplicated by event id. Networking, authorization,
conflict policy, transport retries, and consensus remain outside this phase.

## Benchmarks

CLI benchmarks in builds with `--features bench`:

- `bicdb bench events`: append throughput, subscriber latency, replay speed.
- `bicdb bench queue`: publish throughput and consume throughput.
- `bicdb bench projections`: event append throughput and projection rebuild speed.

All support JSON and CSV export through the same `--json-out` and `--csv-out`
flags used by the existing benchmark commands.
