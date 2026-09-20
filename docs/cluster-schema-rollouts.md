# Cluster schema rollouts

BicDB separates schema rollout into validation, staging, distribution,
activation, and contraction. A bundle being syntactically valid is never
authority to mutate a non-empty voter.

## Signed bundle boundary

`SignedClusterSchemaBundle` wraps the existing bounded schema bundle with:

- a versioned, domain-separated Ed25519 signature;
- an operator-selected signer key ID;
- the complete bundle checksum; and
- an independently recomputed executable-schema fingerprint.

The host supplies the trusted public-key map. Keys are not read from ambient
environment variables or from the bundle itself. An unknown key ID, malformed
key, malformed signature, invalid signature, checksum mismatch, or fingerprint
mismatch rejects the bundle before any durable state changes.

The signed message is fixed size. It binds the bundle checksum and compatibility
fingerprint under the `bicdb.cluster-schema-bundle.ed25519.v1` domain, avoiding
an additional corpus-sized signing buffer.

## Safe staging in 1.0.47-beta

`BicDb::stage_signed_cluster_schema_bundle` currently accepts only a strict
additive superset of the live executable schema:

- new collections;
- new indexes under new names; and
- new structural catalog records.

It rejects collection replacement/removal, index replacement/removal, and
structural record replacement/removal. Those operations require migration and
contraction editions with their own compatibility and rollback boundaries.

Staging writes `cluster-schema-stage.json` atomically. The state contains a
unique stage ID, the signed bundle, base and target fingerprints, bounded change
counts, creation time, format version, and checksum. Reads enforce a configured
file-size/change-count limit, reject symbolic links, use `O_NOFOLLOW` where
available, and revalidate both checksums and the signature.

Staging is idempotent after a lost response. Re-submitting the exact signed
bundle against the unchanged base schema returns the existing stage. A different
bundle or a changed base fails closed. Validated staged state can be discarded
without touching rows or the live schema.

## Durable all-voter coordination in 1.0.49-beta

`ClusterSchemaRolloutRun` turns individual stage RPCs into one bounded,
restartable rollout. The coordinator freezes the exact cluster ID, committed
topology generation, metadata leader, sorted active-voter set, live base
fingerprint, signed target bundle, and resource limits before sending anything.
It refuses to begin unless every metadata voter is active and advertises the
same valid base fingerprint.

Each call advances at most one voter and atomically checkpoints that voter's
validated durable receipt before moving the cursor. The state file is bounded,
checksummed, signature-revalidated on every open, protected against symbolic
link traversal, and read without following a substituted final component on
supported platforms. A lost response is safe: the cursor remains at the same
voter and the destination's idempotent stage operation returns the existing
receipt on retry.

The coordinator fails closed if leadership, topology generation, voter
membership, cluster identity, or the common base fingerprint changes. Once all
frozen voters have acknowledged the exact target, the durable phase becomes
`ready_for_activation`; it does not silently activate schema.

## Crash-resumable local activation in 1.0.50-beta

`BicDb::begin_cluster_schema_activation` and
`BicDb::advance_cluster_schema_activation` apply a staged additive bundle on
one fenced voter. The durable activation state walks collections, structural
catalog records, and indexes in that order. One call inspects an explicitly
bounded number of signed bundle items and applies at most one schema change.

Every step revalidates the stage signature, activation checksum and limits,
and proves that the live schema is still a strict additive prefix of the exact
signed target. Existing objects must match byte-for-byte at the logical model
level; a replacement, removal, extra object, cursor mismatch, clock regression,
or changed target fails closed.

The schema mutation commits before the cursor advances. If the process exits
in that window, retry observes the exact existing object and checkpoints it
without recreating it. New indexes continue to use their own shadow generation
and atomic publication machinery. Activation becomes complete only after BicDB
recomputes the exact target fingerprint. Existing user rows remain intact.

An untouched activation can be aborted. Once any object has been applied, the
checkpoint and signed stage cannot be discarded through the safe API; the node
must resume to the target. This includes internal catalog collections that do
not independently change the public compatibility fingerprint.

## Quorum-committed activation fence in 1.0.51-beta

After every frozen voter has returned its durable stage receipt,
`ClusterSchemaRolloutRun::activation_fence_topology` constructs one and only one
eligible topology candidate. It changes the expected schema digest from the
frozen base to the signed target for the complete metadata-voter set in a
single generation. It refuses inactive or missing voters, changed membership,
changed leadership, base drift, active range relocation, invalid digests, or an
incomplete stage set.

The candidate has no authority by itself. The current metadata leader must
propose it through the existing replicated metadata log, and it becomes visible
only after the normal voter quorum commits it. Until that commit, the published
topology remains byte-for-byte unchanged. After commit, every old-schema node
fails BicDB's existing live-versus-published schema fence; it cannot accept
cluster data RPC or production writes until local activation reaches the exact
target digest.

`observe_activation_fence` reconciles a lost proposal response or coordinator
restart from the committed topology and durably records the exact fence
generation. It also permits a newly elected metadata leader to observe the
decision, while still requiring the same cluster, voter set, target digest, and
one-generation transition.

## Authenticated all-voter activation in 1.0.52-beta

Cluster data protocol 17 adds the bounded activation operation. The transport
continues to authenticate membership and the presented node certificate, and
the destination accepts activation only from the current metadata leader. It
also proves that the published topology is the exact committed consensus
generation, that its metadata-voter set is unchanged, and that every voter is
active and advertises the signed target digest. A staged bundle alone, an
uncommitted fence, or a request to a non-voter has no activation authority.

Each destination reopens its own durable stage and activation checkpoints,
revalidates the host trust store and configured bounds, and advances at most
one bounded local step per request. Its node-bound receipt identifies the
rollout, voter, stage, local activation, phase, target, live digest, activation
state checksum, inspected-item count, and update time. A completion receipt is
valid only when the live digest equals the exact signed target.

The rollout coordinator invokes one voter at a time. Progress receipts leave
the durable voter cursor in place; only an exact completion receipt advances
it. This makes a lost response safe because retry reaches the same idempotent
node-local checkpoint. The rollout becomes `complete` only after it atomically
checkpoints a completion receipt for every frozen voter. Coordinator restart,
destination restart, and metadata-leader replacement can resume from durable
state while the exact quorum fence remains committed.

## Verified fleet finalization in 1.0.53-beta

Cluster data protocol 18 finalizes a rollout only from the same authenticated
metadata leader and exact committed all-voter target fence used by activation.
The coordinator will not enter finalization until every frozen voter has a
validated completion receipt. It then advances one voter at a time and binds
each request to that voter's rollout, stage, activation, target digest, and
completed activation-state checksum.

The destination reopens and verifies its signed stage, complete activation
checkpoint, and live target fingerprint. It first atomically writes a compact,
checksummed finalization proof. Only then does it remove the activation cursor
and signed stage, in that order, with durable directory synchronization when
fsync is enabled. A different receipt cannot remove either file. If the process
stops after writing the proof or a response is lost, an exact retry returns the
same proof and safely finishes only matching leftovers.

The coordinator atomically checkpoints every node-bound finalization receipt
before moving its cursor. Its terminal `finalized` state therefore retains
fleet-wide evidence after the large node-local staging files are gone. The new
state fields are omitted at their defaults, so 1.0.52 activation-complete
checkpoints reopen and continue directly into finalization.

## Write-available compatibility window in 1.0.54-beta

Cluster data protocol 19 removes the whole-cluster write pause for strict
additive rollouts. After every voter durably stages the signed bundle, the
coordinator constructs a quorum-published compatibility window. The active
schema label remains the common base digest and a separate protected label
names the exact pending target. Heartbeats cannot create, replace, remove, or
overwrite either quorum-managed transition value.

Each voter revalidates the signed stage and proves its live executable schema
is an additive prefix of the target. That verified base/target/live authority
is cached in the node service and recovered from durable checkpoints after a
restart. Production writes and cluster data RPC remain admissible only when
the live digest is either the exact active base or the exact verified prefix
inside the published window. Direct schema mutation invalidates that authority
immediately.

Bounded activation therefore proceeds on every voter while foreground writes
continue. New indexes retain their shadow-generation publication boundary and
include rows committed before, during, and after their resumable build. The
coordinator still requires exact node-bound completion receipts from every
frozen voter before it can construct the promotion topology.

Promotion is a second quorum-committed topology generation. It atomically
changes every voter from the base digest to the target digest and removes the
pending label. Finalization is rejected before that exact promotion commits.
A stale node heartbeat cannot restore the base digest afterward. Only then are
the large stage and activation checkpoints replaced by compact durable fleet
proofs. Older rollout checkpoints retain the protocol-18 strict-fence path and
remain restart-compatible.

## Deliberate non-capabilities

Version 1.0.48-beta distributes a signed stage over cluster data protocol 16.
Only the current metadata leader may call the operation, the destination must
be an active voter, transport identity remains bound to cluster membership and
the presented node certificate, and every destination verifies against its own
host-configured signing keys. The destination returns a small durable receipt;
retrying a request after a lost response returns the same stage ID.

Local activation is not cluster authority. Coordinated online DDL requires the
signed all-voter stage, quorum-published compatibility window, exact completion
receipts, quorum-committed promotion, and verified fleet finalization described
above. Replacement, removal, data transformation, and contract/contraction
editions remain separate migration work and are not accepted by this additive
path.
