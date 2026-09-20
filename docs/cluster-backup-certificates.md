# Cluster backup certificates

BicDB node archives are independently durable, but a set of node archives is
not automatically a recoverable cluster snapshot. A cluster backup must prove
that every range belongs to one committed topology and that a current replica
quorum captured the same resolved range-log cut.

`ClusterBackupPlan` and `ClusterBackupCertificate` provide that fail-closed
control-plane contract. The archive payload remains the bounded, encrypted,
streaming `BICBAK03` format.

## Protocol

1. The current committed metadata leader calls `ClusterBackupPlan::create`.
   Planning is rejected if metadata membership differs from the committed
   topology, the topology has an active relocation, the leader or term is
   stale, or configured range/node bounds are exceeded. The expiring plan pins
   the cluster ID, topology generation and SHA-256, metadata term and commit
   index, every range epoch, and the optional PITR target timestamp.
2. The host creates `ClusterBackupRun` in a dedicated run directory. Creation
   stages the immutable plan, checksummed manifest, artifact directory, and
   journal before atomically publishing the directory. Reopening the directory
   takes an exclusive OS lock and replays only bounded journal frames. A torn
   final frame is discarded; checksum damage in a complete frame fails closed.
3. Each current range leader validates the complete plan once with
   `RangeWriteCoordinator::begin_backup_fence_session`, then the cluster host
   calls `fence_range_for_backup` for its ranges. The unforgeable in-process
   session avoids rehashing a million-range topology for every range. The
   coordinator drains in-flight admission,
   repairs followers in bounded batches, and durably installs one exact cut on
   a leader-inclusive voter quorum over authenticated cluster data protocol
   v12. New prepares fail in the range-write store until the owning plan is
   released. The host then creates each selected node's `BICBAK03` archive,
   hashes both its authenticated manifest and the complete encrypted artifact,
   and emits a `ClusterNodeBackupArtifact`. Each observation must have no
   unresolved local log tail (`last_index == resolved_through`).
4. `ClusterBackupRun` checkpoints each exact quorum immediately, then derives
   the minimum artifact-node set from those durable leader-inclusive quorums.
   Fencing and capture advance through explicit bounded batches. A restart
   retains compact journal offsets, not every decoded quorum, and continues at
   the first missing range or node.
5. `ClusterBackupCertificate::certify` accepts a range only when a
   leader-inclusive current-voter quorum reports the identical resolved index.
   Missing, stale, duplicate, divergent, or schema-incompatible observations
   fail the complete certificate. It never chooses a merely overlapping or
   approximate cut.
   The run additionally proves that every certificate barrier matches its
   persisted write fence before publishing the certificate atomically.
6. The run releases fences in deterministic range order only after certificate
   publication, or after a durable abort decision. Each successful release is
   journaled. Release is idempotent, so a crash between the remote release and
   its local checkpoint safely retries it. The run becomes `complete` or
   `aborted` only after every durably recorded fence is released.
7. Restore tooling loads the plan and certificate, checks their SHA-256
   envelopes and bounded file sizes, and runs the fail-closed admission process
   in [`cluster-restore-admission.md`](cluster-restore-admission.md). It verifies
   every archive, restored database, schema, consensus watermark, range log,
   active fence, topology, and PITR target before producing a readiness gate.

Plans, node artifacts, and certificates are written with atomic replacement.
Their JSON control files are capped by
`MAX_CLUSTER_BACKUP_CONTROL_FILE_BYTES`; topology, artifact, range, and
observation counts have separate `ClusterBackupLimits`.

Run memory, work, and disk are independently bounded by
`ClusterBackupRunLimits`: range batch, node batch, maximum decoded event, and
maximum journal bytes. The journal is append-only and SHA-256 frames include
their length, preventing resegmentation. Beyond the immutable plan, the run
retains compact 8-byte ordered-range and journal-offset arrays rather than
decoded quorum observations.

## Trust boundary

The JSON checksums detect damage and accidental substitution; they are not
operator signatures. Node identity and artifact provenance must come from
BicDB's authenticated cluster transport (mTLS in production), and the package
or backup repository must retain its own access controls. This protocol assumes
crash-fault, non-Byzantine replicas and does not claim Byzantine consensus.

The range coordinator and authenticated transport install the fence, while
`ClusterBackupRun` owns durable iteration, artifact registration, certificate
publication, abort, and release progress. The production host supplies the
authenticated leader-routing and node-archive callbacks; the journal makes
those effects resumable and idempotent. Operators must not describe a partial
collection of local archives as an online cluster backup merely because one
range was fenced or a certificate can be constructed from supplied
observations.

Fence expiry never silently reopens writes based on a replica's ambient wall
clock. The trusted coordinator explicitly releases the owning plan, including
when recovering an expired plan. This prevents clock skew from advancing a cut
while another node is still archiving it.

## Schema and topology changes

Do not rebalance, relocate, change metadata membership, or activate a schema
while a plan is in flight. Any such committed change makes the plan invalid;
create a new plan after the topology stabilizes. Artifacts also carry the
node's committed `bicdb.schema.sha256` label so an archive from another schema
generation cannot satisfy a range quorum.

Certificates remain auditable after their short-lived plans expire: validation
uses the recorded certification time. Starting or adding artifacts to an
expired plan is rejected.

`ClusterBackupCertificate::validate_for_restore` validates the recorded
topology without treating a node's offline persisted role as a live leader.
`ClusterRestoreAdmissionReport::validate_node_start` then checks each restored
node's identity and consensus watermark. Restored fences remain installed until
the globally admitted cluster enters its separately controlled activation
boundary.
