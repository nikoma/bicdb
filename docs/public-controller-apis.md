# Public Controller and Extension APIs

Status: public contract for BicDB `1.0.360-beta`.

BicDB is independently operable. A controller can automate it, but the
database does not require a particular controller and does not load a private
control-plane crate. The dependency rule is:

```text
controller or integration -> public BicDB crate APIs
BicDB engine              -X-> controller or integration implementation
```

This document maps the public mechanism contracts that an unrelated company
can use to build a controller, provider, synchronization service, application
registry, or operator tool. It documents code that exists in this repository;
it is not a reservation of unimplemented commercial features.

## Contract rules

A controller is a proposer and coordinator, never a bypass around BicDB's
checks. BicDB remains authoritative for:

- database, cluster, Cell, replica, and application identity;
- canonical encoding, content digests, signatures, trust roots, and threshold
  approvals;
- topology generations, range epochs, writer leases, fencing, and durable
  state transitions;
- compatibility checks, resource bounds, cancellation, and admission;
- action receipts, audit evidence, and recovery verification.

Public controller code must expect proposals to be rejected. It must persist
enough state to retry idempotently and must treat receipts—not an RPC success—as
the durable result. Controller credentials do not confer direct storage or key
access.

Versioned serialized formats use their declared `bicdb.*/*` format identifier
and canonical encoder. Consumers must reject unknown required fields, unknown
format generations, invalid canonical encodings, and signatures from keys not
present in the applicable trust policy. Rust APIs are normal Cargo/SemVer APIs;
while the release is beta, pin an exact BicDB release and test upgrades before
deployment.

## Independent community operation

A checkout containing only this repository can:

```bash
cargo build --workspace --locked
cargo test --workspace --locked
cargo run --release -p bicdb-cli -- serve ./database --host 127.0.0.1 --port 5433
```

The community CLI installs `ManualDistributionHostService` for a distributed
server. It runs the data plane and advances operator-created relocation state,
but it does not invent placement or silently retry failed moves. Backup,
restore, PITR, integrity checks, replication, failover primitives, Cell
startup, local sync, import/export, and signed-format verification remain
available without another repository.

`scripts/check-open-core-boundary.sh` enforces that public Cargo paths stay
inside this checkout. `scripts/validate-community-capabilities.sh` exercises
the security and recovery capabilities that must remain available.

## Placement and relocation

Crate: `bicdb-core`.

The principal policy seam is `ClusterPlacementPlanner`:

```rust
pub trait ClusterPlacementPlanner: std::fmt::Debug + Send + Sync {
    fn plan_failure_repair(
        &self,
        topology: &ClusterTopology,
        config: &DistributionConfig,
        options: &RebalanceOptions,
        now_ms: u64,
    ) -> Result<FailureRepairPlan>;
}
```

The planner receives a snapshot and returns a proposed, typed plan. It does not
receive a database handle, filesystem path, node TLS private key, or Cell key.
`ClusterSupervisor::with_planner` installs the policy. `ClusterSupervisor::new`
constructs manual convergence and rejects automatic placement/retry flags when
no planner exists.

Physical movement is a separate capability:

- `ClusterRelocationDriver` advances one durable relocation;
- `ClusterRelocationTransport` supplies bounded snapshot/catch-up transport;
- `DeferredClusterRelocationDriver` is the safe control-plane-only default;
- `CancellationToken`, resource limits, epochs, and fencing remain enforced by
  the database mechanism;
- `ClusterSupervisorReport` and the metrics snapshot expose outcomes without
  granting mutation authority.

A controller should capture the committed `ClusterTopology`, construct a plan,
submit it through the typed store/supervisor methods, and retain the returned
relocation IDs and receipt. It must not mutate topology files.

Minimal policy injection:

```rust,no_run
use bicdb_core::{ClusterPlacementPlanner, ClusterSupervisor,
                 ClusterSupervisorConfig};

# fn install(planner: Box<dyn ClusterPlacementPlanner>)
#     -> bicdb_core::Result<ClusterSupervisor> {
let mut config = ClusterSupervisorConfig::default();
config.automatically_rebalance = true;
config.retry_failed_relocations = true;
ClusterSupervisor::with_planner(config, planner)
# }
```

## Server lifecycle injection

Crate: `bicdb-pgwire`.

`PgWireHostService` attaches a host-owned service to pgwire startup. The
embedding binary passes implementations to `serve_with_host_services` or
installs them on `PgWireServer` before serving.

`PgWireHostContext` exposes the bounded mechanisms needed by a controller:

- read-only server configuration and the database path;
- the database handle and `ResourceGovernor`;
- the optional distribution router;
- validated topology installation;
- `start_distribution_controller` with an already constructed supervisor;
- graceful-shutdown state and accounted background tasks.

It does not expose listener internals, authentication bypasses, arbitrary
database selection, or private controller types. A long-running service must
use `spawn_background_task`, check `is_shutdown_requested`, and use
`sleep_until_shutdown` so shutdown cannot falsely report a closed database
while controller work still holds it open.

The public `ManualDistributionHostService` is both a usable default and a small
reference implementation. Third-party controllers may replace it through the
same interface used by any commercial controller.

## Schema rollout

Crate: `bicdb-core`.

The public schema-rollout mechanism consists of:

- signed schema bundles and compatibility fingerprints;
- durable rollout states and phases;
- `ClusterSchemaStageTransport`;
- `ClusterSchemaActivationTransport`;
- `ClusterSchemaFinalizationTransport`;
- stage, activation, and finalization receipts.

A controller chooses cohorts and timing. The mechanism validates bundle
identity, topology/schema generations, compatibility, activation preconditions,
and completion receipts. An operator can drive these phases manually; a fleet
service is not required to apply a migration to a Cell or cluster.

For many isolated Cells, one signed migration definition can be converged by a
controller across many stores. Each Cell still records and validates its own
activation. This is fleet automation, not 1,000 hand-authored migrations.

## Backup, restore, PITR, and HA

Crates: `bicdb-core` and `bicdb-cell-ha`.

Local full/incremental backup, verification, restore, PITR, and restore drills
are public CLI and library capabilities. Cluster modules expose backup plans,
barriers, range fences, artifacts, certificates, restore admission, and
readiness checks. A controller may schedule and aggregate those operations but
cannot turn an unverified artifact into an accepted restore.

`bicdb-cell-ha` publishes deterministic, industry-neutral HA formats and
state machines:

- `HaTrustPolicy`, authority roles, approvals, and canonical documents;
- writer-epoch statements, certified writer epochs, leases, and fence evidence;
- replication-object headers bound to Cell, generation, epoch, sequence, and
  object kind;
- certified backup and restore authorization;
- drill evidence and verification;
- `CellHaStateStore` and `CellFailoverSupervisor`;
- typed `FailoverAction` values that an operator or controller executes.

The state machine decides what is safe. Automation decides when to ask and how
to provision infrastructure. Route publication follows durable fencing and
writer activation; it is not an external override.

## Anti-entropy and replication

Crate: `bicdb-core`.

The public replication surface includes frame formats, TLS configuration,
watermarks, apply reports, retention state, transport validation, streaming
send/receive, and manual standby promotion/recovery paths.

Range anti-entropy exposes `RangeDigestTransport`,
`RangeDigestRepairTransport`, bounded manifests/scans, repair state,
certificates, and verification. `RangeAntiEntropyFenceAuthority` binds repairs
to current fencing authority. The public local safety loop is intentionally
usable; fleet-wide cadence, inventory, alerting, and repair scheduling can live
outside the engine.

## Distributed execution

Crate: `bicdb-core`; single-node execution also uses `bicdb-analytics`.

Interoperable primitives include `DistributedShardExecutor`,
`DistributedCommitProtocol`, typed plans/fragments, cancellation, accounting,
bounded scatter/gather, distributed FTS statistics, and protocol-version
constants. These are sufficient to build a scheduler without patching query or
storage code. Elastic placement, admission policy, workload prioritization, and
automatic storage tiering are controller policy, not requirements for
single-node analytics.

## Cells, keys, admission, grants, and devices

Crates: `bicdb-cell`, `bicdb-cell-admission`, `bicdb-cell-grant`,
`bicdb-cell-device`, and `bicdb-cell-ha`.

The Cell boundary remains public and fail closed:

- `CellManifest`, its storage/runtime/key/replication/policy sections,
  `VerifiedCellManifest`, trusted manifest keys, and transition checks;
- `CellKeyProvider`, provider-assurance classification, attested one-Cell key
  leases, and the development file provider (which cannot satisfy hardened
  admission);
- signed admission policies, gate evidence, authority roles, threshold
  authorization, and `VerifiedAdmissionEvidence`;
- recipient-encrypted object grants, recipient key interfaces, source and
  recipient ledgers, threshold revocation, and immutable evidence;
- device enrollment/authorization, `HardwareBoundDeviceKey`, sealed working
  sets, offline policy, amendment resolution, retirement, and parent ledger;
- deterministic HA/fencing/backup/restore contracts described above.

An external KMS implementation supplies `CellKeyProvider`. The runtime still
checks that requested Cell identity, signed manifest, storage lineage,
workload/key scope, application digest, and replication authority agree before
opening data. A provider must return only the requested scoped lease; a global
unwrap credential is not part of the interface.

Managed inventory, enrollment campaigns, attestation collection, remote wipe
coordination, and enterprise KMS/HSM policy orchestration can remain external.
The formats and local enforcement needed to implement alternatives remain
open.

## Local-first synchronization

Crates: `bicdb-core` and `bicdb-sync`.

`SyncBundle`, `SyncBundleEvent`, `SyncVector`, `SyncCheckpoint`, signatures,
strict import, conflict evidence, pending-change status, and delta export are
public. `SyncEndpoint` is the replaceable transport/store interface:

```rust
pub trait SyncEndpoint {
    fn load_checkpoint(&self, client: &NodeId) -> Result<ClientSyncCheckpoint>;
    fn save_checkpoint(&mut self, client: &NodeId,
                       checkpoint: &ClientSyncCheckpoint) -> Result<()>;
    fn push_bundle(&mut self, bundle: &SyncBundle) -> Result<PushBundleReport>;
    fn pull_bundles(&self, client: &NodeId,
                    checkpoint: &ClientSyncCheckpoint) -> Result<Vec<SyncBundle>>;
}
```

`SyncCoordinator` drives any implementation. `FileSyncEndpoint`, direct mesh
sessions, and LAN synchronization provide usable community paths. A managed
relay may implement the same endpoint semantics; it cannot bypass bundle
identity, signature, vector, authorization, or replay checks.

## Application packages and registries

Crates: `bicdb-extension`, `bicdb-app-runtime`, and `bicdb-fleet`.

`ApplicationPackage`, application components/scopes/data classes,
capabilities, frontend assets, canonical signing payloads, `TrustedSigningKeys`,
and `PackageVerifier` are public. Runtime providers are explicit traits:

- `SecretProvider`, `BlobProvider`, and `EgressProvider`;
- `RedisProvider`, `EmailProvider`, and `GrpcProvider`;
- `TokenizerProvider`, `EmbeddingsProvider`, `LlmProvider`, and
  `EvaluationProvider`;
- `RealtimeProvider`, `PluginServiceDispatcher`, `TrustedHttpHandler`, and
  `HostObservability`.

Deny-by-default implementations exist for effectful providers. A host composes
the exact provider set; packages receive declared capabilities, never ambient
host authority.

`bicdb-fleet` contains open App Root/release authorization formats rather than
a hosted registry service. Its public types include `FleetTrustPolicy`,
`ApplicationRelease`, `FleetRelease`, `SignedFleetRelease`, transparency
entries/checkpoints/proofs, `RolloutPlan`, `CohortGate`, `ActivationTicket`,
`FleetActivationBundle`, `ActivationContext`, `ConvergenceReceipt`, and the
append/verify `ConvergenceLedger`. The crate has no database, SQL, network
server, or Cell-key dependency.

A third party can store immutable application bytes, produce transparency
proofs, collect independent approvals, issue activation bundles, and verify
convergence receipts. A private registry or staged-rollout UI is one possible
implementation, not a prerequisite.

## Provider implementation checklist

Every controller/provider implementation should:

1. pin compatible BicDB crate and document-format versions;
2. use canonical encoders and compare content digests before signatures;
3. enforce input size/count/time/resource limits before allocation or I/O;
4. bind credentials to the smallest Cell, database, operation, and time scope;
5. make retries idempotent and preserve durable receipts;
6. honor cancellation and graceful shutdown;
7. redact keys, tokens, sensitive payloads, and connection secrets from logs;
8. test stale generation, replay, wrong identity, tamper, partial failure, and
   recovery paths;
9. expose audit/metrics without treating observability as authority;
10. avoid direct filesystem mutation of BicDB-owned formats.

## Boundary conformance

For a public checkout:

```bash
./scripts/check-open-core-boundary.sh
./scripts/validate-community-capabilities.sh
```

For an out-of-tree controller, the equivalent acceptance gate is:

```bash
cargo check --workspace --locked
cargo test --workspace --locked
```

with BicDB consumed only through declared dependencies and without `[patch]`
or source modification. The extracted sibling platform uses exactly this model;
it has no privileged private hook into the engine.

## Compatibility-only legacy vocabulary

Some older signed packages, SQL migrations, telemetry attributes, or persisted
documents use names from a pre-neutral application adapter. BicDB may continue
to read those exact strings so existing data remains accessible. They are not
the canonical API for new controllers. See
[`legacy-application-compatibility.md`](legacy-application-compatibility.md)
for the containment and removal rules.
