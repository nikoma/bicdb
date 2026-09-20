# BicDB Clustering and Consensus

BicDB includes an additive Raft-style consensus core for clustered deployments.
It coordinates ordered replication commits; it does not change single-node
operation unless `DbConfig.consensus.enabled` is set.

## Implemented

- Durable consensus state in `consensus-state.json`.
- Config validation for cluster id, local node id, peer list, voting members,
  duplicate peers, and heartbeat/election timeout sanity.
- Raft-style roles:
  - follower
  - candidate
  - leader
- Persistent term and vote tracking.
- `RequestVote` / `VoteResponse` handling.
- `AppendEntries` / `AppendResponse` handling.
- Consensus RPC frame variants on the existing length-prefixed replication
  transport:
  - `ConsensusRequestVote`
  - `ConsensusVoteResponse`
  - `ConsensusAppendEntries`
  - `ConsensusAppendResponse`
- Log consistency checks using previous log index and term.
- Conflicting follower log truncation.
- Leader-local append of `CommitFrame` entries.
- Per-voter match index tracking.
- Majority commit-index calculation.
- Committed-prefix extraction for replay through the native replication
  importer.
- Cluster event loop via `bicdb consensus run`:
  - listens for consensus frames
  - handles inbound vote and append requests
  - starts elections after heartbeat timeout
  - sends vote requests to peers
  - becomes leader after majority vote
  - sends leader heartbeats
  - proposes locally committed `CommitFrame`s as consensus log entries
  - sends append entries over the TLS/mTLS frame transport
  - applies committed follower entries through the replication importer
- CLI status inspection:

```bash
bicdb consensus status /srv/bicdb/node
bicdb consensus status /srv/bicdb/node --json
```
- SQL/admin status view:

```sql
SELECT * FROM bicdb_consensus_status;
```

## Running A Cluster Loop

Production transport uses the same TLS/mTLS certificate material as native
streaming replication:

```bash
bicdb consensus run /srv/bicdb/node-a \
  --listen 10.0.0.10:9444 \
  --cluster-id prod-east \
  --node-id node-a \
  --peer node-a=10.0.0.10:9444 \
  --peer node-b=10.0.0.11:9444 \
  --peer node-c=10.0.0.12:9444 \
  --tls-cert /etc/bicdb/repl/node-a.crt \
  --tls-key /etc/bicdb/repl/node-a.key \
  --tls-ca /etc/bicdb/repl/ca.crt
```

For localhost development only:

```bash
bicdb consensus run /tmp/bicdb-a \
  --listen 127.0.0.1:9444 \
  --cluster-id dev \
  --node-id node-a \
  --peer node-a=127.0.0.1:9444 \
  --peer node-b=127.0.0.1:9445 \
  --peer node-c=127.0.0.1:9446 \
  --dev-localhost-plaintext
```

Start one process per node with a unique database path, `--listen`, and
`--node-id`. The peer list should be identical across nodes except for the local
path/listen address.

## Data Model

Consensus log entries contain native replication `CommitFrame`s:

- consensus log index
- consensus term
- BicDB commit sequence
- authenticated/checksummed commit frame

Committed entries are applied through the same replication importer used by
standby streaming replication. That keeps replay validation, checksum checking,
cluster-id checking, standby write protection, and audit-preserving commit
application in one path.

## Configuration Shape

The core API uses `ConsensusConfig`:

```rust
ConsensusConfig {
    enabled: true,
    cluster_id: "prod-east".to_string(),
    node_id: "node-a".to_string(),
    peers: vec![
        ConsensusPeer::voting("node-a", "10.0.0.10:9444"),
        ConsensusPeer::voting("node-b", "10.0.0.11:9444"),
        ConsensusPeer::voting("node-c", "10.0.0.12:9444"),
    ],
    election_timeout_ms: 1000,
    heartbeat_interval_ms: 250,
    lease_timeout_ms: 2000,
}
```

The local node must appear in `peers`. A majority of voting peers is required
to advance the commit index.

## Current Boundaries

The consensus core and networked event loop are implemented as database
infrastructure and a dedicated CLI daemon. `bicdb serve` does not yet embed the
cluster supervisor in the pgwire server process; run `bicdb consensus run`
alongside `bicdb serve` or under the same service manager.

There is still no multi-writer distributed SQL. BicDB remains single-leader:
writes must go through the elected leader, and followers apply committed log
entries in order.

Automatic client failover and load-balancer reconfiguration are still operator
policy. The consensus daemon elects/records a leader and replicates commit
frames, but production promotion should still include fencing rules appropriate
to the deployment.
