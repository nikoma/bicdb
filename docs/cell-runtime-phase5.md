# Phase 5 cell-scoped HA and recovery

Introduced in BicDB 1.0.322-beta, Phase 5 adds a callable,
industry-neutral HA, replication, backup, restore, and failover boundary to the
single-Cell runtime. It deliberately does not admit regulated data.

## Authority separation

The `bicdb-cell-ha` crate has no application runtime, SQL, fleet controller,
Cell key provider, or general database-selector capability. A manifest-pinned
`HaTrustPolicy` assigns distinct Ed25519 keys to three roles:

- Lease authorities jointly select writer epochs and short-lived replica
  leases.
- Recovery authorities jointly certify immutable backup payloads and bounded
  restore authorizations.
- Auditor authorities certify measured topology-specific failover and restore
  drills.

One public key cannot occupy two positions. The safe policy requires at least
three Lease authorities, two Recovery authorities, two Auditor authorities,
and thresholds of at least two Lease, two Recovery, and one Auditor approval.
These authorities can authorize ciphertext movement and recovery but receive
no Cell key or database handle.

## Startup order

For a Phase-5 manifest, `CellRuntime::open` performs this order:

```text
signed CellManifest and volume identity
  -> monotonic manifest/rollback state
  -> exact running binary
  -> manifest-pinned HA trust policy
  -> quorum-certified writer epoch
  -> exact short-lived replica lease
  -> predecessor epoch + old-primary fence proof on promotion
  -> workload-local replica signing key
  -> Phase-4 fleet activation
  -> Phase-3 application/policy artifacts
  -> kernel-exclusive Cell-volume lease
  -> only now request the one-shot Cell key
  -> encrypted database open
  -> exact database/lease durable-sequence match
  -> install the database commit fence on a Primary
  -> construct an application host on a Primary only
```

An invalid HA document or wrong replica key is therefore rejected before key
release, database creation, or the kernel volume lock. Standby and Recovery
roles never construct an application host or listener.

The Cell-wide HA configuration digest intentionally excludes replica-local
volume identity, replica identity, and role. It includes Cell identity,
lineage, database/encryption format, runtime and application pins, key policy,
replication group/writer epoch, jurisdiction, and all security policy pins.
This lets distinct physical replicas prove one shared configuration without
pretending their local manifests are byte-identical.

## Single runtime and writer fencing

Every public Cell construction holds an OS-enforced exclusive lease on
`.bicdb-cell-runtime.lock` for the full runtime lifetime. A second process
cannot open the same volume with a separate in-memory fence. The stale lock
file carries no authority; the kernel releases its open-file lock on process
death.

Primary writes require an installed `CellHaCommitFence`. Every commit checks
the live role, writer epoch, and short-lived lease at BicDB's trusted commit
admission boundary. The post-WAL callback advances a create-new/fsync/rename
HA witness with the exact durable commit sequence.

Graceful fencing takes the exclusive gate crossed by every commit, revokes
future admissions, waits for already-admitted commits to finish, flushes the
database, advances the witness, and signs the exact final sequence. A new
writer epoch is accepted only after either that exact old-primary signature or
expiry plus the policy clock-skew margin. Epoch, key, manifest, and durable
history cannot move backward.

## Replication

The Primary exports contiguous native `CommitFrame` objects. Each object is:

- bound to Cell, replication group, source and destination replica, writer
  epoch, key epoch, protocol version, commit/previous sequence, and predecessor
  object digest;
- encrypted with a Cell-derived Replication-purpose key and authenticated
  metadata;
- signed by the workload-local key named in the exact source lease.

The Standby verifies source authority and signature before decryption, then
checks destination scope, AEAD, plaintext digest, frame identity, contiguous
database apply, and an atomic durable predecessor witness. A shared Cell data
key does not let one replica impersonate another.

Version 1.0.322 supports asynchronous replication with an explicitly declared
commit RPO and an Auditor-measured RPO/RTO drill result. The runtime does not
claim that the declared RPO is continuously enforced by commit admission. A
topology must declare a positive commit RPO and a bounded RTO.
`SynchronousQuorum` is explicitly refused because the current Cell commit path
does not durably replicate a quorum decision before returning. BicDB will not
turn an asynchronous stream into a false zero-RPO claim.

## Backup and restore

`create_cell_backup` obtains an exact commit boundary from the already-open
encrypted database, creates a full encrypted archive with a random one-time
key, and wraps that key through the Cell cipher's Backup purpose. The candidate
binds Cell, group, lineage, source replica, writer/key/manifest epochs, exact
archive sequence, predecessor backup, archive digest, and wrapped-key digest.

A candidate has no restore authority. `finalize_cell_backup` requires Recovery
quorum over the exact candidate and atomically advances the durable backup
head; racing certifications cannot fork the local lineage.

Restore requires all of the following:

- a Recovery-role short-lived replica lease;
- Recovery quorum over the backup;
- a second, maximum-15-minute Recovery quorum authorization bound to the
  backup, target replica, and next writer epoch;
- exact Cell, group, lineage, key epoch, and accepted durable history;
- unchanged archive and wrapped-key digests.

Restore writes only beneath `.bicdb-cell-restores/<new-name>`. It cannot replace
the active database or promote the restored copy. Route publication and writer
activation require a separately verified epoch transition.

## Failover sequencing and drill evidence

`CellFailoverSupervisor` is keyless and enforces this order:

```text
fence/expire old writer
  -> select non-rollback durable sequence
  -> verify new writer activation
  -> publish route bound to that exact epoch
  -> rebuild old primary from the winner
  -> complete
```

It cannot decrypt data or manufacture an activation. `CertifiedHaDrillEvidence`
binds a Cell, group, topology policy, measured RPO/RTO, old-primary rebuild,
backup/restore result, and evidence artifact digest to Auditor approval.

## Interfaces

The reduced Cell binary accepts:

```text
--ha-trust-policy /run/bicdb/ha-policy.cbor
--ha-writer-epoch /run/bicdb/writer-epoch.cbor
--ha-replica-lease /run/bicdb/replica-lease.cbor
--ha-replica-signing-key /run/bicdb/replica-signing.key
--ha-previous-writer-epoch /run/bicdb/previous-epoch.cbor       # promotion
--ha-previous-primary-lease /run/bicdb/previous-primary.cbor   # promotion
```

`BICDB_RUNTIME_MODE=cell` maps the equivalent paths:

```text
BICDB_CELL_HA_TRUST_POLICY
BICDB_CELL_HA_WRITER_EPOCH
BICDB_CELL_HA_REPLICA_LEASE
BICDB_CELL_HA_REPLICA_SIGNING_KEY
BICDB_CELL_HA_PREVIOUS_WRITER_EPOCH
BICDB_CELL_HA_PREVIOUS_PRIMARY_LEASE
```

Environment and CLI paths select documents; they grant no authority.

## Admission status

`regulated_data_admitted` remains `false`. Phase 5 closes the callable
cell-scoped HA/recovery implementation item. Device replicas, cross-Cell
grants, attested deployment enforcement, externally archived production
drills, independent review, and the remaining admission gates are still
absent. Zero-RPO synchronous Cell commits are also not claimed.
