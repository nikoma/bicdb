# BicDB Native Streaming Replication

BicDB native streaming replication is an ordered commit-stream replication
subsystem. It is additive to the existing file-shipping HA path; file shipping
remains available as a fallback and for bootstrap/repair workflows.

## Current Implementation

- Core frame model: `Hello`, `Authenticate`, `Heartbeat`, snapshot frames,
  `CommitFrame`, `Ack`, `Nack`, `Error`, and `TopologyInfo`.
- Commit frames include cluster id, source node id, stream id, commit sequence,
  previous commit sequence, transaction id, timestamp, ordered writes,
  collection names, record ids, operation type, payload bytes, version fields,
  compression flag, optional encryption metadata, and SHA-256 checksum.
- Export API:
  - `current_commit_seq()`
  - `export_replication_frames_since(commit_seq, limit)`
  - `subscribe_replication_stream(start_commit_seq)`
  - `replication_watermark()`
  - `replication_retention_status()`
- Import API:
  - `apply_replication_frame(frame)`
  - `apply_replication_batch(frames)`
  - `last_applied_commit_seq()`
  - `replication_lag(source_commit_seq)`
- Replay safety:
  - duplicate frames are ignored
  - out-of-order frames are rejected
  - gaps are rejected
  - checksum mismatch is rejected
  - cluster mismatch is rejected
  - standby write protection remains active for ordinary writes
- Transport:
  - length-prefixed replication frames over TCP
  - TLS/mTLS config builders for production transport
  - explicit loopback-only plaintext development mode
  - protocol `Hello` plus `Ack` handshake before streaming
  - cluster id and allowed follower node id validation before frames are sent
  - `bicdb replication stream --listen` and `bicdb replication follow`
    catch-up commands
  - `bicdb replication follow --continuous` reconnects and resumes from the
    last applied commit sequence
- Public direct `BicDb::insert`, `batch_insert`, `delete`, and `batch_delete`
  now route through the transaction commit stream so they export as commit
  frames.
- SQL/PostgreSQL catalog state is stored in internal BicDB catalog collections,
  so table schemas, constraints, RLS flags, extensions, routines, and related
  compatibility metadata stream as committed records. Commit writes also carry
  collection metadata so time-series mode and core collection policies are
  recreated before replay.

## Semantics

BicDB has one writable primary in this replication topology. This does not
mean transactions on that primary execute or commit one at a time; see
[write concurrency](../TRANSACTIONS.md#write-concurrency). Streaming replication is the commit transport and
replay layer. Raft-style consensus is implemented separately in the consensus
core; when enabled, it coordinates quorum acceptance of commit frames before
replay. Automatic failover is still an operator/supervisor policy, and
operators must fence the old primary before promotion.

The primary replication source is committed transaction order, represented by
gap-free `commit_seq`. A replica resumes from its last acknowledged/applied
commit sequence. If the requested sequence is older than retained WAL, snapshot
bootstrap is required.

## CLI

```bash
bicdb replication status /srv/bicdb/primary
bicdb replication status /srv/bicdb/primary --json
bicdb replication stream /srv/bicdb/primary --from 100 --limit 1000 > frames.json
bicdb replication apply /srv/bicdb/standby frames.json
bicdb replication stream /srv/bicdb/primary --from 100 --listen 10.0.0.10:9443 \
  --tls-cert /etc/bicdb/repl/node.crt \
  --tls-key /etc/bicdb/repl/node.key \
  --tls-ca /etc/bicdb/repl/ca.crt \
  --cluster-id prod-east \
  --node-id primary-a \
  --allowed-node-id standby-a
bicdb replication follow /srv/bicdb/standby --primary 10.0.0.10:9443 \
  --server-name db-a.example.internal \
  --tls-cert /etc/bicdb/repl/node.crt \
  --tls-key /etc/bicdb/repl/node.key \
  --tls-ca /etc/bicdb/repl/ca.crt \
  --cluster-id prod-east \
  --node-id standby-a
bicdb replication follow /srv/bicdb/standby --primary 10.0.0.10:9443 \
  --server-name db-a.example.internal \
  --tls-cert /etc/bicdb/repl/node.crt \
  --tls-key /etc/bicdb/repl/node.key \
  --tls-ca /etc/bicdb/repl/ca.crt \
  --cluster-id prod-east \
  --node-id standby-a \
  --continuous \
  --reconnect-backoff-ms 1000
bicdb replication snapshot create /srv/bicdb/primary /tmp/primary.snapshot.json \
  --snapshot-id primary-bootstrap
bicdb replication snapshot restore /tmp/primary.snapshot.json /srv/bicdb/standby \
  --force
bicdb replication lag /srv/bicdb/standby --source-commit-seq 250
bicdb replication cert check --cert node.crt --key node.key --ca ca.crt
```

## SQL Views

```sql
SELECT * FROM bicdb_replication_status;
SELECT * FROM bicdb_replication_lag;
SELECT * FROM bicdb_replication_nodes;
SELECT * FROM bicdb_replication_errors;
```

The SQL views intentionally do not expose secrets, certificates, private keys,
frame payloads, or raw replicated record contents.

## Snapshot Bootstrap

The intended bootstrap flow is:

1. Replica connects with mTLS.
2. Primary validates node identity.
3. Primary sends `SnapshotStart`.
4. Primary streams bounded `SnapshotChunk` frames.
5. Primary sends `SnapshotEnd` with snapshot hash.
6. Replica verifies the snapshot hash.
7. Replica resumes `CommitFrame` replay from `snapshot_commit_seq + 1`.

`bicdb replication snapshot create` writes a frame-encoded snapshot archive.
`bicdb replication snapshot restore` verifies frame and archive hashes, restores
to a staging directory, and only replaces the target after successful unpack.
After restore, resume `CommitFrame` replay from `snapshot_commit_seq + 1`.

## Operator Notes

- Run `stream --listen` under a service manager on the primary and `follow
  --continuous` under a service manager on the standby.
- Set a stable `--cluster-id` on both sides.
- Set a stable `--node-id` for each node and configure the primary with
  `--allowed-node-id` for each permitted follower.
- Rotate certificates by adding trust for the new CA/certificate chain first,
  restarting followers with the new identity, then removing old trust after all
  peers have moved.
