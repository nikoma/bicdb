# Distributed cluster operations

This guide covers BicDB's automatic range-placement control plane. A cluster
uses durable membership, virtual ranges, epoch-fenced routes, bounded
relocation state machines, and an automatic supervisor. Single-node databases
remain the default.

The cluster topology and local node configuration are stored alongside the
database as `cluster-topology.json` and `bicdb-distribution.json`.
Topology writes are checksummed and atomically replaced. Copying these files by
hand between live nodes is not a membership protocol and is unsupported.

## Bootstrap and join

Initialize the first node with enough virtual ranges to allow smooth future
placement. Range count is independent of server count.

```console
bicdb cluster init /srv/bicdb \
  --cluster-id production \
  --node-id server-1 \
  --address 10.0.0.11:9444 \
  --capacity-bytes 4000000000000 \
  --replication-factor 3 \
  --initial-ranges 256 \
  --failure-domain server \
  --failure-domain rack \
  --failure-domain zone \
  --label rack=rack-a \
  --label zone=us-west-2a \
  --cluster-tls-cert /etc/bicdb/server-1.crt \
  --cluster-tls-key /etc/bicdb/server-1.key \
  --cluster-tls-ca /etc/bicdb/cluster-ca.crt
```

The default transport is plaintext only for the default loopback address.
Every non-loopback cluster must provide all three mutual-TLS paths shown above.

Run the preferred join command on the empty server itself. It authenticates to
any seed with the new server's cluster certificate, fetches a quorum-derived
bootstrap snapshot, follows the current metadata leader, self-registers as a
learner, waits for the registration to be quorum committed, and provisions its
local directory. No shared filesystem, topology copy, or application-authored
shard map is involved.

```console
bicdb cluster join \
  --cluster-id production \
  --seed-node-id server-1 \
  --seed-address 10.0.0.11:9444 \
  --node-id server-2 \
  --address 10.0.0.12:9444 \
  --capacity-bytes 4000000000000 \
  --label rack=rack-b \
  --label zone=us-west-2b \
  --node-root /srv/bicdb \
  --cluster-tls-cert /etc/bicdb/server-2.crt \
  --cluster-tls-key /etc/bicdb/server-2.key \
  --cluster-tls-ca /etc/bicdb/cluster-ca.crt
```

The three seed options are all required for remote self-bootstrap. Repeating
the identical command is safe. Reusing a decommissioned node ID requires a
strictly greater incarnation. If the named seed is not the leader, it still
returns its committed snapshot and leader identity; the join client then talks
to the leader directly. It also retries other voters from the snapshot if the
seed fails during registration.

Possession of a client certificate signed by the cluster CA permits only the
pre-membership bootstrap exchange, so protect issuance and revocation of that
CA accordingly. Before membership commits, the RPC server permits the unknown
identity to fetch only the bounded bootstrap snapshot and to register only
that same identity as a learner. Registration stores the SHA-256 fingerprint
of the exact presented leaf certificate in quorum-committed membership. Every
later RPC claiming that node ID must present the same leaf certificate; a
different certificate signed by the same CA is rejected. Once the bootstrap
snapshot is validated, the joining client also pins each destination's
committed fingerprint instead of relying on CA and hostname validation alone.
All other pre-membership cluster RPCs remain fenced.

The older admin-assisted form remains available when provisioning another
directory on the current host:

```console
bicdb cluster join /srv/bicdb \
  --node-id server-2 \
  --address 10.0.0.12:9444 \
  --node-root /srv/bicdb-node-2 \
  --cluster-tls-cert /etc/bicdb/server-2.crt \
  --cluster-tls-key /etc/bicdb/server-2.key \
  --cluster-tls-ca /etc/bicdb/cluster-ca.crt
```

The standard failure-domain labels are `server`, `rack`, `zone`, and `region`.
The `server` label defaults to the node ID. A placement requiring more distinct
values than the live topology provides fails closed and reports an unplaced
range. `--node-root` is always required. In admin-assisted mode, the positional
path must name a running member's database directory. Each provisioned member
receives the quorum-committed topology plus its own node identity and TLS
paths; private keys are never copied from another member.

The new member is deliberately excluded from elections and range placement
while it is a metadata learner. Start `bicdb serve` on its provisioned root.
The current leader streams the committed metadata snapshot, the learner proves
its commit index and topology generation, and BicDB then promotes it with a
joint old/new voter quorum. Only after that promotion can normal automatic
placement move range replicas to the server. A failed or interrupted bootstrap
is safe to retry; it cannot reduce the existing metadata quorum.

## Inspect and route

```console
bicdb cluster status /srv/bicdb
bicdb cluster status /srv/bicdb --json
bicdb cluster status /srv/bicdb --prometheus
bicdb cluster route /srv/bicdb patients patient-42
```

Route output includes the topology generation, range ID, range epoch, and
leader. Every write must carry that range ID and epoch. Owners reject stale
epochs and return structured refresh information.

## Data write quorum

Distributed `bicdb serve` nodes install a data-plane commit authority before
accepting row mutations. Until that authority is ready, the core transaction
path rejects writes; opening a distribution catalog can no longer silently
fall back to a local-only commit. This guard is below SQL, pgwire, extensions,
and direct transaction execution.

For every non-empty transaction, BicDB hashes the exact final collection and
record IDs before assigning the local commit sequence. All mutations must map
to one range. The current leader then:

1. creates one checksummed command at the next range-log index;
2. durably prepares it locally and on the current epoch's voting replicas;
3. rejects the transaction unless a majority prepared the identical command;
4. durably records a provisional decision locally;
5. replicates that provisional decision to a majority without applying rows;
6. aborts every provisional decision if that decision quorum is not reached;
7. persists a quorum certificate on the leader, then certifies and applies
   reachable followers;
8. lets the normal BicDB WAL, record, constraint, and index path apply locally;
9. checkpoints the command as locally applied only after normal WAL durability.

The node-local log is `cluster-range-write-log.json`. It is checksummed and
atomically replaced. Command ID, checksum, index, cluster, range, epoch, and
leader are validated on every retry. A repeated identical prepare or commit is
idempotent; a conflicting command at one index, a stale epoch, an old leader,
a non-voter destination, a cross-range transaction, or a minority partition
fails closed. Resolved history is compacted to a bounded recent window per
range rather than growing with database size.

If a process stops after quorum certification but before local row application,
the data service replays certified-not-applied commands through BicDB's normal
transaction machinery at startup. A merely provisional decision never enters
crash replay and remains abortable. Therefore a minority commit-message outcome
cannot expose follower rows or resurrect after restart.

Range-write log format v2 records `prepared`, provisional `committed`,
`quorum_committed`, `applied`, and `aborted` separately. Opening a v1 log
atomically migrates its old `committed` entries to `quorum_committed`, preserving
the published v1 crash-replay semantics while enforcing the safer split for new
commands.

Only one command per range may remain between quorum decision and the leader's
local durable-applied checkpoint. Later writers wait for that boundary. If it
does not complete within the bounded admission wait, the range rejects new
writes with an explicit recovery-required error; commands can never overtake
one another on the leader.

Before preparing a new command, the leader asks every reachable voter for its
durable range-log progress. A voter behind the leader's resolved watermark is
repaired with contiguous, checksummed suffix batches containing only applied or
aborted commands. Applying a batch is idempotent: a lost acknowledgement may
repeat it without duplicating logical state. The default repair budget is
explicit and bounded to 128 commands and 4 MiB per batch, with at most eight
batches per write admission. Operators embedding the coordinator can lower
those limits with `RangeWriteRepairLimits`; invalid or unbounded values are
rejected.

A newly designated leader also performs a quorum-evidenced prefix recovery
before it assigns its first log index. It computes the highest resolved index
reported by a voter majority, fetches the bounded suffix from the voters that
hold it, and requires overlapping histories to match exactly. Divergent voter
histories fail closed. If the required prefix is outside every reachable
voter's retained window, or cannot be recovered within the configured batch
budget, the range requires a fresh snapshot instead of guessing a next index.

Leadership recovery runs in the cluster supervisor outside SQL commit locks and
replays the exact row mutations as well as the command log. The supervisor
recovers at most 16 newly led ranges per tick through a fair rotating cursor
and non-blocking reservations. A busy or failed range cannot pause metadata
supervision or prevent independent ranges from completing. SQL writes remain
unavailable for a range until the coordinator records that its exact current
epoch has completed recovery.

After the resolved prefix, the new leader probes the next log index on every
reachable current voter. One quorum certificate, an applied observation, or a
quorum of identical provisional decisions preserves and applies the command. A
prepared command that provably could not have reached a decision quorum is
aborted. If unavailable or compacted voters could change the outcome, recovery
fails closed as ambiguous and retries after more evidence becomes available.
Different commands at one index, or conflicting certified-commit and abort
evidence, require operator repair rather than an arbitrary winner.

Repair validates exact byte accounting, command checksums, batch checksum,
authenticated source voter, range epochs, voter placement, consecutive indexes,
and the resolved watermark. A continuous suffix may cross older epochs during
a safe leadership transition; command epochs must be monotonic and may never
exceed the current route epoch. A follower behind the retained 1,024-command
window fails closed with a snapshot requirement. A newly promoted leader may
reconcile a voter-majority resolved prefix before serving writes, but it is
never silently fast-forwarded over unknown or conflicting commands.

Cluster data protocol v12 carries progress, authenticated point probes, bounded
repair export and apply, schema bootstrap, backup-fence install/release,
prepare, provisional commit, certification, apply, and abort messages over the
same bounded, authenticated mTLS transport used for relocation. A follower acknowledges apply only after
its certificate, local database commit, and applied checkpoint are durable. The
pgwire node currently rejects a write received on a non-leader rather than
transparently forwarding it; gateways should use `cluster route`/the
generation-cached router and retry structured redirects.

Range consensus does not replicate DDL automatically. BicDB now computes a
canonical SHA-256 identity over collection policy, mutation policy, executable
index definitions, and structural SQL catalogs. Every `bicdb serve` member
advertises that identity in the quorum-published `bicdb.schema.sha256` heartbeat
label. Once a range leader advertises an identity, placement, relocation,
leader transfer, repair, and cluster data RPCs reject nodes that advertise or
actually load a different identity.

Production write admission is stricter: it stays closed until the leader's
advertised digest exactly matches a fingerprint freshly verified against the
live local database. Schema-changing DDL and structural-catalog commits
invalidate that verification synchronously, so later writes fail closed until
the next metadata heartbeat publishes the new identity. Legacy topologies
without the reserved label can remain readable during a rolling upgrade, but
the production application host will not recover range leadership or admit
writes until its label is present.

Operators must still repair schema drift explicitly on a prospective voter
that already contains user data. A genuinely empty node advertises a
host-derived `bicdb.schema.bootstrap=true` claim and may receive a
checksummed, byte/record-bounded schema bundle from the authenticated current
range leader before its first snapshot. Installation is idempotent and an
interrupted or mismatching target remains quarantined until its final live
fingerprint is quorum-published. BicDB refuses automatic schema replacement on
a node containing user rows. Cross-range atomic writes continue to require the
explicit distributed-commit protocol and are never inferred silently.

Prometheus output uses a fixed label-free schema:

- node counts by liveness and lifecycle;
- range, replica, and leader counts;
- active and failed relocation counts;
- under-replicated and unavailable ranges;
- replica-count, leader-count, and replica-byte skew;
- local metadata role, term, commit index, last log index, and committed
  topology generation when the metadata voter has started.

Node IDs, addresses, and user-supplied labels are deliberately excluded from
metric labels to prevent unbounded cardinality.

## Rebalance

Planning is dry-run by default:

```console
bicdb cluster rebalance /srv/bicdb
bicdb cluster rebalance /srv/bicdb --json
bicdb cluster rebalance /srv/bicdb --apply
```

The planner separately balances replica bytes, replica count, and leaders. It
does not place new replicas on suspect, dead, draining, or decommissioned
nodes. Per-node move counts and total bytes in flight are bounded. Active
relocations fence a range from receiving a second concurrent move.

The cluster supervisor owns heartbeats, topology refresh, repair planning,
rebalance planning, relocation retries, and bounded movement scheduling. The
physical relocation driver must durably perform these operations:

1. stream a range snapshot in bounded record/byte batches;
2. persist each exact resume key and snapshot checksum;
3. apply the continuous range-filtered commit suffix, including empty frames;
4. report destination durability and source parity;
5. remove the old source only after epoch-fenced promotion.

`TransportClusterRelocationDriver` is the standard adapter for physical
transports. It invokes at most one bounded operation per relocation per tick
and accepts only cumulative durable progress. Incomplete snapshot responses
cannot carry final checksum/watermark metadata, catch-up cannot report a
destination ahead of its source, and only the controller can promote or remove
a replica.

`InProcessClusterRelocationTransport` implements the complete physical data
path for embedded and integration-test clusters. It scans only records owned
by the relocating range, applies each bounded snapshot batch as a destination
transaction, follows the retained source commit suffix, and removes source
records in bounded transactions only after promotion. The destination stores
its checksummed apply state in `cluster-range-learner-apply.json`; snapshot
cursors, a chained snapshot checksum, and the last durable source commit are
therefore preserved across process restart. Replayed acknowledgements are
idempotent, while stale or out-of-order cursors fail closed.

Cleanup progress is also part of the checksummed topology. An interrupted
cleanup resumes after its last stable collection/record key and never causes
the already-promoted replica to be rolled back.

`TcpClusterRelocationTransport` exposes those same node-local operations and
the range-write prepare/commit protocol over
length-bounded RPC frames. Production mode requires CA-verified mutual TLS;
plaintext is an explicit loopback-only development mode. Every request carries
and validates the protocol version, cluster identity, caller membership,
caller leaf-certificate fingerprint, destination node identity, and request
identity. A topology created before certificate binding may issue only a
heartbeat whose declared fingerprint matches its presented leaf certificate;
normal RPCs remain fenced until metadata quorum publishes that binding.
Clients independently verify that the server leaf matches the destination
node's committed fingerprint, preventing another certificate from the cluster
CA from impersonating a server at a reused or redirected address.
Connections have bounded connect/I/O timeouts and the server rejects work
beyond its configured inbound connection limit. Snapshot and commit batches
retain the controller's existing record, byte, and frame limits, so networking
does not turn relocation into an unbounded allocation.

### Rotate a node certificate

Rotate one member at a time. The command verifies the new leaf, key, and CA,
asks the metadata leader to quorum-stage the new leaf fingerprint beside the
active fingerprint, waits for that topology generation, and then atomically
updates the local node configuration:

```console
bicdb cluster rotate-certificate /srv/bicdb \
  --cluster-tls-cert /etc/bicdb/server-1.next.crt \
  --cluster-tls-key /etc/bicdb/server-1.next.key \
  --cluster-tls-ca /etc/bicdb/cluster-ca.crt
```

Restart that member after the command completes. During the overlap, both the
active and pending leaves are accepted as that exact node identity by callers
and destinations. The first heartbeat whose declared fingerprint matches the
presented pending leaf is proof that the replacement process is running; the
metadata leader then publishes a second quorum-committed topology generation
that promotes the pending fingerprint and fences the old one.

Both phases are idempotent. If the command is interrupted before local
configuration changes, repeat it with the new material. If the replacement
cannot start, restore the still-active material and cancel the pending phase:

```console
bicdb cluster abort-certificate-rotation /srv/bicdb \
  --cluster-tls-cert /etc/bicdb/server-1.crt \
  --cluster-tls-key /etc/bicdb/server-1.key \
  --cluster-tls-ca /etc/bicdb/cluster-ca.crt
```

The abort command persists the active configuration before quorum removes the
pending fingerprint, so interruption cannot strand the member with a
configuration the committed topology rejects. Do not delete the old key or
certificate until `bicdb cluster status /srv/bicdb --json` shows the new
fingerprint active and no pending fingerprint.

If the driver returns an error, the supervisor records the relocation as
failed with its resume phase. A later tick retries from the durable phase
instead of allocating another learner.

The deterministic physical-restart gate kills the relevant in-process data
node without calling `close` and reopens the controller at every durable
boundary: learner allocation, partial snapshot, partial catch-up,
ready-to-promote, promoted, and partial cleanup. Every case must reach durable
completion, preserve catch-up writes exactly, copy no out-of-range record, and
remove source records only after promotion. Split and merge tests likewise
reopen the controller between their atomic topology publications.

The local pre-certification suite also models process loss while a replacement
backup's temporary archive is incomplete; the prior published archive must
remain byte-identical and restorable until a complete retry is atomically
renamed. Resumable FTS builds are abruptly reopened after a tokenization
checkpoint, the PK merge, and the impact merge. Torn run files are reclaimed,
completed runs are reused, and an interrupted replacement generation never
displaces the previous searchable generation. These deterministic gates cover
the failure mechanics but do not replace killing physical nodes during the
five-server certification run.

`bicdb serve` automatically starts the node data listener, supervisor, and
checked range router when `bicdb-distribution.json` is present. Distributed
databases open in `server_paged` mode so range scans stay bounded and sources
retain the ordered commit suffix needed for catch-up. Every serving member runs
durable metadata consensus; established members are voters, while newly joined
members remain non-electable learners until catch-up and joint promotion. The
elected leader first commits a current-term barrier, then drives membership,
heartbeats, placement, repair, and physical movement through
`TcpClusterRelocationTransport`.

Supervisor mutations are made on an in-memory planning fork. The durable
topology, request router, and relocation transport see a new generation only
after the metadata log entry satisfies both the old and new membership
quorums. Followers install the same committed snapshot atomically. A leader
that cannot renew its quorum lease steps down; while isolated it may calculate
a proposal, but cannot commit or publish a range epoch. A surviving majority
elects a higher-term replacement and commits a leadership barrier before
resuming topology changes.

The deterministic five-voter partition gate exercises a 2/3 split. It proves
that the isolated two-member side cannot commit a conflicting topology, the
three-member side can elect and publish, healing replaces the losing
uncommitted log, and a delayed old-term acknowledgement cannot revive it. The
live server gate also kills the elected leader, observes replacement authority,
reopens the former leader from its durable files, and requires it to rejoin as
a converged follower.

Followers send their capacity, usage, labels, incarnation, and liveness
heartbeat to the elected metadata leader. The leader validates and coalesces
the latest heartbeat per node into its next topology proposal, so member count
does not create one consensus entry per inbound heartbeat. Stale heartbeat
timestamps never move liveness backwards.

Learner registration, promotion, vote, append, snapshot, heartbeat,
topology-fetch, and certificate-rotation messages share cluster data protocol
version 5. Topology fetches used for provisioning are served only by the
elected metadata leader. In production the channel is CA-verified mutual TLS;
the request envelope also fences the cluster, caller membership and committed
active-or-pending leaf-certificate fingerprint, destination, and request
identity.

Metadata consensus state is checksummed in
`cluster-metadata-consensus.json`. `bicdb cluster status` verifies that file
without changing the running node's role and includes the local role, leader,
term, commit index, and last log index in text/JSON output. Its Prometheus mode
emits the same values as fixed-cardinality gauges.

## Planned drain and removal

```console
bicdb cluster drain /srv/bicdb --node-id server-2
bicdb cluster status /srv/bicdb
bicdb cluster remove /srv/bicdb --node-id server-2
```

Drain first transfers leaders to synchronized voters and schedules replacement
replicas. Removal refuses to proceed if it would lose quorum, violate the
replication factor, or remove a node that still owns required replicas. There
is no force flag.

## Backup and restore

Full and incremental BicDB backups include the distribution configuration and
checksummed topology because both live under the database root. Before a
cluster backup, issue an expiring `ClusterBackupPlan` from the current committed
metadata leader. The plan pins the topology hash/generation and every range
epoch, and is rejected during relocation or unstable metadata membership.
Each node's bounded `BICBAK03` archive is then bound to a
`ClusterNodeBackupArtifact`. A `ClusterBackupCertificate` is valid only when a
current leader-inclusive voter quorum captured the same fully resolved log
index for every range. After restore, verify the exact topology, metadata
authority, schema labels, range barriers, manifest hashes, and encrypted
archive hashes before serving traffic. Any mismatch fails closed. The complete
protocol and its host-orchestration boundary are documented in
[`cluster-backup-certificates.md`](cluster-backup-certificates.md).

Range-write log format v3 persists each plan-owned backup fence. Cluster data
protocol v12 installs and releases it only through the current authenticated
range leader. New prepares above the cut fail inside the range-write store.
Release selected remote voters before the leader; a failed release is retried
while the leader stays closed. Expiry does not use replica wall clocks to
silently unlock a range.

`ClusterBackupRun` is the crash-resumable all-range coordinator. Its bounded,
SHA-256-framed append-only journal checkpoints every quorum, artifact
registration, phase transition, certificate publication, abort decision, and
release. Initialization is atomically published, only one process may own a
run, torn tail frames are truncated, and complete-frame damage fails closed.
The host drives range leaders and node archive creation in configured batches;
after interruption it reopens the same run directory rather than starting a
new plan. A run is terminal only after every installed fence has a durable,
idempotent release checkpoint.

Restore a whole node into an empty directory, validate it, then rejoin using a
new node incarnation if the old identity was decommissioned. Never combine a
restored data directory with topology files from another backup generation.

For a whole-cluster restore, do not start listeners after individual archive
restores. Run `verify_cluster_restore_admission` across exactly the certificate
artifact nodes and persist its atomic admission report. The verifier streams
archive authentication plus raw SHA-256 under explicit bounds, verifies each
database and schema, checks metadata-consensus watermarks, and requires every
restored range log and plan-owned fence to match the certified barrier. Each
node must pass `validate_node_start` against that same report before readiness.
The host must then commit a `MetadataRestoreActivation` through the restored
cluster's new metadata leader. This compact record binds the exact admission
and authenticated node-acknowledgement set; it is replicated, snapshotted, and
restart durable. A local file or one node's pre-crash leadership role is not an
activation decision.

Advance fence release with `ClusterRestoreActivationRun` in bounded batches.
Its atomic state checkpoint makes process restart repeat at most the current
idempotent batch. Do not publish listener readiness until `finish` creates the
activation-bound readiness record and every node passes `validate_node_ready`;
that final gate verifies the current metadata decision and an empty restored
fence set.
See [`cluster-restore-admission.md`](cluster-restore-admission.md).

Rolling upgrades require identical distribution storage and key-hash versions.
Query and global-FTS-statistics protocols may differ by at most one generation.
Check `ClusterProtocolCapabilities` in both directions before admitting an
upgraded peer.

## Incident runbooks

### Unexpected node loss

1. Confirm the node is `suspect` or `dead` in `cluster status`.
2. Inspect unavailable and under-replicated range counts.
3. Let the grace period expire; the supervisor transfers affected leaders and
   starts bounded replacement relocations.
4. Watch active/failed relocations and skew metrics.
5. Do not remove the failed identity until replacement replicas are durable.
6. Replace the host with a new incarnation, then join it normally.

At replication factor three, one failed node remains available only while each
range retains a quorum of synchronized voters.

### Failed or interrupted relocation

1. Inspect the relocation phase and its last error in JSON status.
2. Correct storage, capacity, or connectivity faults.
3. Keep the existing source intact.
4. Allow the supervisor retry delay to elapse.
5. Verify progress resumes from the recorded snapshot key or commit watermark.

Do not delete learner data manually. Promotion is allowed only when the
destination durability watermark reaches the source watermark.

### Split-brain suspicion

1. Stop writes at gateways that report conflicting cluster IDs, topology
   payloads at one generation, or range epochs.
2. Preserve every node's topology file and logs.
3. Identify the last topology generation accepted by quorum.
4. Do not edit generations, epochs, checksums, or membership files by hand.
5. Restore or rejoin nodes only from the authoritative generation.

The router and topology cache fail closed on conflicting payloads at the same
generation. An old owner remains fenced after the range epoch advances.

### Whole-node replacement

1. Drain the node when it is still reachable.
2. Wait for zero leaders and no required voter replicas.
3. Remove the identity.
4. Restore database files or provision an empty directory on the replacement.
5. Join with the same node ID and a strictly greater incarnation, or use a new
   node ID.
6. Monitor automatic redistribution until placement and skew are compliant.

## Current production gate

Multi-terabyte claims additionally require the `server_paged` bounded-memory
certification artifacts in the automatic-distributed-sharding roadmap. Passing
unit, simulation, and fault-injection tests is necessary but does not replace a
1 TB, 5 TB, or 20 TB hardware run.

Freeze the evidence contract before starting a run:

```console
mkdir -p /var/lib/bicdb-cert/production-1tb
bicdb cluster certify-plan /srv/bicdb \
  --profile 1tb \
  --run-id production-1tb-2026-07-30 \
  --output /var/lib/bicdb-cert/production-1tb/cluster-certification-plan.json
```

Plan generation reads the database storage metadata, distribution
configuration, committed topology, and local metadata-consensus status as one
live preflight. It fails unless storage is `server_paged`; the metadata log has
an established leader and no uncommitted tail; and the source topology has
exactly five live, active, promoted voters on five distinct `server` failure
domains. Every node must advertise the same active schema fingerprint. Every
range must have exactly three live voting replicas, placement and replica-byte
skew must be inside the canonical gates, and no repair, rebalance, unplaced
range, active relocation, or failed relocation may remain.

The saved plan binds that preflight: storage mode, exact distribution-config
SHA-256, local identity, schema SHA-256, node incarnations, metadata
term/commit/leader/voters, live operational metrics, replica bytes, and zero
pending-work counts. It also binds the run ID, BicDB and protocol versions,
cluster identity, topology generation and SHA-256, required failure phases,
required raw artifacts, and non-adjustable pass/fail gates. A stale topology
file or an initialized cluster whose server is not participating in metadata
consensus cannot create a production plan. The profiles mean decimal 1, 5,
and 20 TB
(`1_000_000_000_000`, `5_000_000_000_000`, and
`20_000_000_000_000` logical bytes).

The payload of the `effective-configuration` artifact must be the exact
pretty-printed JSON configuration whose SHA-256 is in the preflight. The
`topology-before` payload must be the exact compact JSON topology frozen by
the plan. Registration and offline verification compare those inner payload
digests to the plan, so recomputing an artifact envelope or publication
manifest cannot substitute unrelated configuration or topology bytes.

Start the durable collector after freezing the plan. It copies and binds the
exact plan, source commit, and start time into an atomically replaced state
checkpoint:

```console
bicdb cluster certify-start /var/lib/bicdb-cert/production-1tb \
  --source-commit 0123456789abcdef0123456789abcdef01234567
```

`certify-start` is idempotent for the same inputs, so it is safe to repeat
after interruption. A different plan or commit is rejected. `--source-dirty`
is retained as an explicit diagnostic input but now fails before creating any
collector state; a dirty build cannot begin a production certification run.

Record one typed JSON observation at a time. The envelope is
`{"kind":"hardware|measurements|background_saturation|failure_trial|restore|expansion",
"evidence":{...}}`; the evidence object uses the fields in the corresponding
report section. Hardware can be recorded immediately. Measurements,
background-saturation, failure, restore, and expansion observations must be
recorded after every raw artifact they reference has been captured or
registered. Hardware and failure observations are keyed by node and phase, so
retrying a valid phase replaces only that checkpoint:

```console
bicdb cluster certify-record /var/lib/bicdb-cert/production-1tb \
  observations/node-1-hardware.json
```

Capture raw command output and samples with BicDB rather than constructing the
envelope by hand. The input payload may live outside the bundle; the output is
always a safe relative path inside it:

```console
bicdb cluster certify-capture /var/lib/bicdb-cert/production-1tb \
  /var/log/bicdb-cert/resource-samples.ndjson \
  --kind resource-samples \
  --output raw/resource-samples.bicdb-artifact \
  --payload-format json-lines \
  --records 150000 \
  --started-at-ms 1785400000000 \
  --completed-at-ms 1785400300000
```

`certify-capture` streams the payload through a fixed 1 MiB heap buffer into a
private file in the destination directory. It writes the final run-bound
header into a reserved 64 KiB first line, fsyncs the completed file, publishes
it with an atomic no-overwrite link, fsyncs the directory, and checkpoints its
registration. Existing files are never replaced. If the process stops after
publication but before registration, repeat the same command: BicDB verifies
the already-published bytes and finishes the checkpoint without needing the
source payload.

The resulting version-one raw-artifact envelope starts with UTF-8 JSON,
newline terminated and at most 64 KiB. All following bytes are the exact raw
payload:

```json
{"format_version":1,"run_id":"production-1tb-2026-07-30","profile":"one-tb","bicdb_version":"1.0.64-beta","source_commit":"0123456789abcdef0123456789abcdef01234567","source_dirty":false,"kind":"resource_samples","started_at_ms":1785400000000,"completed_at_ms":1785400300000,"producer_node_ids":["node-1","node-2","node-3","node-4","node-5"],"payload_format":"json_lines","record_count":150000,"payload_bytes":42791822,"payload_sha256":"<64 hexadecimal digits>"}
```

The payload begins immediately after that newline. Supported payload formats
are `json`, `json_lines`, `prometheus_text`, `text`, and `binary`. The
header must bind the exact plan/run, BicDB version, source revision, artifact
kind, run-contained observation interval, and every planned producer node
exactly once.

`certify-capture` registers its output automatically. `certify-artifact`
remains available for a separately produced envelope. Registration and offline
verification read only the bounded header into memory, then stream the payload
with a 1 MiB heap buffer while checking its nonzero byte count and SHA-256 plus
the whole-file digest. Paths are always relative to the bundle; symlinks and
path escapes are rejected:

```console
bicdb cluster certify-artifact /var/lib/bicdb-cert/production-1tb \
  --kind resource-samples raw/resource-samples.ndjson
bicdb cluster certify-artifact /var/lib/bicdb-cert/production-1tb \
  --kind background-saturation raw/background-saturation.ndjson
bicdb cluster certify-status /var/lib/bicdb-cert/production-1tb
```

After the referenced files are durable, record their typed summary:

```console
bicdb cluster certify-record /var/lib/bicdb-cert/production-1tb \
  observations/measurements.json
bicdb cluster certify-record /var/lib/bicdb-cert/production-1tb \
  observations/snapshot-failure.json
```

`certify-record` runs the immutable profile verifier before rewriting collector
state. It rejects insufficient hardware, malformed or future windows, missing
or wrong-kind artifact references, insufficient samples and work, latency or
amplification violations, quorum/data-loss/fencing failures, and invalid
restore or expansion identity/topology evidence. A rejected observation does
not replace the previous durable checkpoint. The restore and expansion pair
is cross-validated as soon as both halves exist.

Evidence must include hardware and effective configuration, topology before
and after, exact commands, resource samples, foreground and hotspot latency,
background saturation, rebalance and failure timelines, whole-node restore,
production-sized FTS index construction, and checksums. Failure trials cover
snapshot, catch-up, replica promotion, cleanup, split, merge, backup, and index
rebuild. The state file is rewritten atomically after every record, so process
or host loss resumes from the last durable observation.

Opaque legacy files, headers over 64 KiB, missing header newlines, zero-record
payloads, artifacts from another run or source revision, wrong-kind files,
partial producer sets, timestamps outside the report, and payload
truncation/extension all fail closed. Recomputing the outer artifact and
publication-manifest hashes cannot conceal a mismatched inner payload digest
or run identity.

Every failure-trial observation represents a real abrupt node loss, never a
graceful shutdown. Supported mechanisms are process kill, host power cut,
hypervisor crash, and a complete network blackhole. The observation binds the
active maintenance operation and distinct before/after checkpoint SHA-256
values, then records these ordered timestamps inside the report window:
failure injection, node-loss detection, old-identity fencing, repair start,
policy convergence, and recovery. Restarting the same incarnation and
rejoining with a strictly higher incarnation are explicit, separately checked
recovery modes.

The failed node must affect at least one range and produce measurable temporary
under-replication without making any range unavailable. Every affected range
must be repaired and digest-verified, final under-replication must be zero, at
least 1,000 quorum operations during the outage must all succeed, and every
old-identity write probe must be rejected. A fixed commit watermark must cover
at least one fifth of the selected profile's logical bytes and at least one
record; its pre-failure and post-recovery SHA-256 digests must match. Each trial
must reference both its verified `failure-timeline` and `checksums` artifacts.

Performance measurements use two explicit, ordered windows inside the report:
a baseline and an active rebalance window, each at least five minutes long.
Foreground and hotspot p99 values need at least 10,000 raw samples in each
window. Recovery start and completion must fall inside the rebalance window,
and their exact difference must equal the reported recovery duration. The run
must move at least 10% of the selected logical corpus, observe nonzero source
read, network, and destination-write bytes, and stay within the 4x aggregate
relocation-I/O bound. At least 10,000 majority-quorum operations must all
succeed while at least 1,000 minority-partition writes must all be rejected.
The measurement observation must reference exact verified `resource-samples`,
`workload-latency`, and `rebalance-timeline` artifacts; merely registering those
artifact kinds elsewhere in the bundle is insufficient.

Whole-node restore evidence names and hashes the source backup and replaces one
planned node with a strictly higher incarnation. Exact restore timestamps and
elapsed time must fit inside the report. At one fixed commit watermark, source
and restored record counts, logical bytes, and SHA-256 digests must match and
cover at least one fifth of the profile corpus. Peak restore RSS is capped at
32 GiB. The node's source and restored FTS document counts, index-byte counts,
and SHA-256 digests must match and cover at least a fair node share of the
measured full index. At least 1,000 FTS probes and 10,000 quorum operations must
all succeed, at least 1,000 old-identity writes must all be rejected, and no
range may become unavailable. The observation directly references verified
restore, topology-before, topology-after, resource-sample, workload-latency,
full-text-index, and checksum artifacts.

Expansion evidence covers one continuous five-minute-or-longer interval. It
binds the plan's topology generation and digest to one newer, distinct
generation and supplies full hardware evidence for each new node. The new
nodes must have enough RAM for the fixed 32 GiB limit and enough storage for a
fair share of the measured physical database. Peak RSS covers every old and
new node. At least one new node's fair logical share must move with nonzero
source, network, and destination counters inside the 4x I/O limit. Foreground
and hotspot baselines and active measurements each require 10,000 samples and
may regress by at most 25%; 10,000 quorum operations must all succeed.
Full-corpus and full-FTS before/after counts and digests must match at one
commit watermark, at least 1,000 FTS probes must succeed, placement skew must
remain bounded, and the final topology must have no unavailable or
under-replicated ranges or unfinished relocation. Exact hardware,
configuration, topology, resource, latency, rebalance, FTS, and checksum
artifacts are mandatory.

The background-saturation observation is one continuous five-minute-or-longer
window. It records before, saturated, and after `ResourceGovernorSnapshot`
values for all five nodes. Every snapshot must contain the exact seven fixed
lanes with internally consistent aggregates. Before and after snapshots must
show all background lanes quiescent. The saturated snapshot must show
anti-entropy, backup/restore, compaction, and index construction simultaneously
active and newly admitted on every node, plus at least one background admission
rejection per node to prove actual saturation. Each node records its validated
governor configuration plus the exact rejected lane and nonzero demand. The
demand must fit that lane's hard limit but fail at least one captured live
lane, node, noncritical, background, concurrency, or token bound. Oversized or
otherwise invalid requests do not count as saturation. Counters may never
regress.

During that same window, record at least 10,000 foreground reads, foreground
writes, and range-quorum operations, plus at least 1,000 metadata-quorum checks.
Every operation and check must succeed, no range may become unavailable, and
read and write p99 may regress by at most 25% from their recorded baselines.
The observation must reference a verified `background-saturation` artifact;
substituting a different artifact kind fails verification.

After all observations and artifacts are present, materialize and verify the
report in one operation:

```console
bicdb cluster certify-finish /var/lib/bicdb-cert/production-1tb
```

Failed gates leave the collector open for corrected measurements. A passing
finish atomically marks the state finalized, rejects subsequent evidence
mutation, and publishes `cluster-certification-manifest.json` as the final
portable bundle root. The manifest binds the exact default plan and report
files, source revision, profile, completion time, sorted artifact entries,
artifact count, raw byte total, and a canonical whole-bundle SHA-256. It is
written atomically only after the report passes. If the process stops after
state finalization but before manifest publication, rerunning `certify-finish`
recreates the same manifest. An existing different manifest is never
overwritten.

Verify the completed bundle offline:

```console
bicdb cluster certify-verify /var/lib/bicdb-cert/production-1tb
bicdb cluster certify-verify /var/lib/bicdb-cert/production-1tb --json
```

Verification is fail-closed and returns a nonzero status for an incomplete,
undersized, dirty-source, weakened, unreferenced, missing, escaped, resized,
path-substituted, unpublished, or checksum-mismatched bundle. Certification
format v7 enforces a constant 32 GiB
peak-RSS ceiling per node, at most 5x total physical database amplification, at
most 4x relocation I/O amplification, at most 25% foreground and hotspot p99
regression, the dedicated background-saturation/quorum gate above, destructive
node-loss, fencing, quorum, repair, and fixed-watermark checksum evidence for
every maintenance phase, statistically bounded and artifact-linked baseline and
rebalance measurement windows, recovery within 15 minutes, complete majority-
quorum operation success, complete minority-write rejection, zero range
unavailability, converged RF3 placement, bounded count/byte skew, a full-corpus
FTS build, and the restore/expansion integrity, FTS, workload, memory, topology,
hardware, and artifact-proof gates above.

Independent verification also requires the regular, in-bundle default plan,
report, and publication-manifest files. Symlinks and alternate external paths
are rejected. Even a self-consistent replacement manifest that omits an
artifact fails because its sorted entries, count, and byte total must equal the
verified report exactly. The verifier returns both the verified raw-artifact
byte total and publication-manifest SHA-256 for durable external cataloging.

The verifier deliberately does not manufacture or extrapolate measurements.
Creating a plan or passing its unit tests is not certification; only a complete
measured bundle for the selected physical profile can pass.
