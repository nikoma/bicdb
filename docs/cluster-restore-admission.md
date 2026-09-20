# Cluster restore admission

A decrypted node archive is not, by itself, evidence that a restored cluster is
coherent. BicDB therefore keeps restored nodes offline until one admission pass
has verified every node artifact referenced by the quorum-certified backup.

## Admission sequence

1. Restore every selected full `BICBAK03` archive into an offline node
   directory. If the plan has a PITR timestamp, use the same target on every
   node and retain each successful `BackupRestoreReport`.
2. Build one `ClusterRestoreNodeCandidate` per certificate artifact. Pass the
   archive path, restored directory, backup passphrase, optional database-at-
   rest encryption configuration, and the restore report's target timestamp.
   The candidate type is deliberately neither serializable nor debuggable.
3. Call `verify_cluster_restore_admission` while no restored BicDB server is
   running. The verifier requires exactly the certificate's artifact nodes and
   one byte-identical committed topology on all of them.
4. Persist the resulting `ClusterRestoreAdmissionReport` with
   `save_cluster_restore_admission`. The report is produced only after every
   check succeeds; a partial attempt has no readiness artifact.
5. Before a node campaigns for metadata leadership or starts data/HTTP/pgwire
   listeners, load the report and call `validate_node_start`. Keep external
   readiness false until every required node has passed the same certificate-
   bound gate.
6. Canonically hash those authenticated node acknowledgements, construct one
   `MetadataRestoreActivation`, and submit it through the newly elected
   metadata leader. The decision is usable only after a current voter quorum
   commits it. Metadata snapshots and process restarts retain the decision.
7. Create a `ClusterRestoreActivationRun` and call `record_metadata_commit`
   against a consensus store that has observed that exact decision. Release
   its `next_release_batch` through the current range leaders. Progress is a
   constant-size atomic checkpoint; a crash repeats at most one idempotent,
   explicitly bounded batch. Reopen uses the limits pinned into the checksummed
   activation plan, not fresh operator-supplied values.
8. Call `finish` only after all certified ranges have been released. Distribute
   the resulting `ClusterRestoreReadiness` and call `validate_node_ready` on
   every node before opening data, HTTP, pgwire, or application listeners.

## Checks performed

For each node, admission verifies:

- the archive is a regular non-symlink file within the configured size bound;
- authenticated `BICBAK03` verification and raw archive SHA-256 in one bounded
  streaming pass;
- full-backup ID and manifest hash against `ClusterNodeBackupArtifact`;
- restored distribution config, node ID, cluster ID, exact topology generation,
  topology SHA-256, and every range epoch;
- persisted metadata-consensus node identity, term, commit watermark, and
  topology generation;
- storage-format compatibility, database integrity, page integrity where
  applicable, and the live schema compatibility fingerprint;
- every artifact range's durable resolved log cut;
- range-catalog reads within the configured per-node byte bound;
- an active plan-owned range fence at that exact epoch and cut, with no
  unexpected restored backup fences; and
- one identical PITR target on every node.

The final report binds the certificate ID/checksum, plan, topology, every node
artifact checksum, archive/manifest/schema hashes, consensus watermarks, and
verified range-fence counts. It is atomically persisted, size bounded, and
checksummed. Existing symlinks are rejected on both save and load.

## Security and activation boundary

The report checksum detects damage and substitution within trusted backup
storage; it is not an operator signature. Protect the report and certificate
with the same access controls and provenance/signature layer as the backup
repository.

Admission deliberately leaves restored backup fences in place. This means a
restored node cannot accept a write above the certified cut while the operator
is still validating other nodes. `MetadataRestoreActivation` makes the global
decision compact and quorum durable by binding the certificate, admission,
topology, plan, acknowledged-node count, and canonical acknowledgement-set
hash. `acknowledge_cluster_restore_node` is the only public acknowledgement
constructor and re-runs the node startup gate. The host must still authenticate
the node identity carrying each acknowledgement, normally through the bound
cluster mTLS identity; these checksums are integrity bindings, not signatures.

Fence release and listener readiness are driven only after that exact decision
commits. Releases are reconstructed from the certificate's exact plan, epoch,
cut, quorum, and artifact-node observations. A partial batch remains
uncheckpointed and is safely repeated. Final readiness binds the metadata term
and commit index and rejects any node that still has a write fence, has not
observed the same activation, or has regressed from admission. A partially
admitted or partially activated cluster remains unavailable rather than serving
divergent data.

Offline certificate validation does not pretend that a persisted pre-crash
`Leader` role is a new election. It validates the immutable certified topology;
the startup gate separately verifies each node's durable consensus watermark.
The restored cluster must then elect authority normally.
