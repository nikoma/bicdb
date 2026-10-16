# BicDB Controller Boundaries

This document records the mechanism/automation split used by the open-core
extraction. It describes code that exists; it does not reserve unimplemented
features by name.

The invariant is:

```text
controller implementation -> public BicDB mechanism/API
BicDB engine             -X-> controller implementation
```

## Existing subsystem boundary

| Subsystem | Public BicDB mechanism | External controller responsibility |
|---|---|---|
| Placement and relocation | Topology/config snapshots, `RebalancePlan`, `FailureRepairPlan`, plan validation/application, manual-convergence `ClusterSupervisor`, relocation state machine, fencing, transport and driver contracts, cancellation, receipts and metrics | Implement `ClusterPlacementPlanner`; choose moves, retry policy and fleet cadence |
| Pgwire lifecycle | `PgWireHostService`, `PgWireHostContext`, `start_distribution_controller`, graceful background-task accounting, database/path/governor access and validated topology installation | Construct and install an optional controller service from the embedding binary |
| Schema rollout | Signed schema bundles, compatibility fingerprints, rollout state/phase, stage/activation/finalization transports and receipts | Select cohorts, schedule/resume runs and approve activation |
| Backup and PITR | Local backup/restore/PITR, cluster plans, barriers, range fences, artifacts, certificates and verification | Schedule and converge backups across clusters; manage retention and recovery drills |
| Restore and HA/DR | Restore manifests, readiness checks, writer epochs, leases, fencing and deterministic failover actions | Choose disaster policy, regions, timing and route publication |
| Anti-entropy | Digest protocol, bounded scans, repair state, certificates, verifiers, resource-governed local safety loop | Schedule and observe repair across a fleet |
| Compliance evidence | Open evidence/report formats, signatures, verifier and local self-check inputs | Central collection, retention, dashboards and approval workflow |
| Distributed query | Plans, fragment/executor/commit protocols, cancellation, accounting, bounded scatter/gather and single-node analytics | Elastic placement, admission policy, workload scheduling and storage tiering |
| Routing/gateway | Topology snapshots, route validation, retry/fencing semantics, static bounded connection pool | Managed pools, capacity convergence and geo routing |
| Cells and applications | Cell runtime/identity, signed manifests, admission/grant/device/HA formats and application package verification | Fleet placement, staged activation, enterprise identity/KMS workflow and device inventory |

Several open modules contain safe, usable reference behavior. That behavior is
not an enterprise controller merely because it performs bounded local work.
In particular, checkpointing, local anti-entropy, deterministic failover state
machines, manual recovery and bounded scatter/gather remain public security or
database mechanisms.

## Controller authority

A controller proposes or schedules work. BicDB independently validates the
action and remains authoritative for:

- topology generation and range epochs;
- database and Cell identity;
- signatures and approval thresholds;
- fencing and writer authority;
- resource limits and cancellation;
- durable state transitions;
- action receipts and audit evidence.

Controller hooks never grant unchecked file, key or storage-engine access.
An unrelated implementation can use the same contracts as BicDB Platform.

## CLI ownership

The community CLI owns local database operations and cluster administration.
It installs `BalancedDistributionHostService`, a public reference adapter over
the existing `build_failure_repair_plan` mechanism. The adapter enables bounded
placement and relocation retries through `ClusterPlacementPlanner`; the core
supervisor still requires an injected policy for automatic operation. This
implementation is maintained in this repository and has no private dependency.

`--cluster-controller manual` installs `ManualDistributionHostService` instead.
Embedders can choose either service or provide their own planner. Commercial
fleet policy, scheduling, regional placement and centralized operations can use
the same interfaces without changing engine authority or importing private code.
