# BicDB Open-Core Extraction Audit

Status: Phase 1 audit only

Baseline: `main` at `86bf0330a1b587a2e677242c54b2e9b787f113e0`

Workspace version: `1.0.352-beta`

Audit date: 2026-08-31

## Scope and conclusion

This audit classifies the code that exists in the repository. It does not assume that planned enterprise services already exist, and it makes no code, workspace, or licensing changes.

The proposed boundary is:

```text
bicdb (eventual Apache-2.0)
  database and storage data plane
  security, integrity, and recovery mechanisms
  Cell runtime and cryptographic identities
  protocol/formats/verifiers
  single-node execution and analytics
  basic replication, local synchronization, and manual recovery
  stable controller/provider interfaces
  usable local/reference implementations

bicdb-platform (private)
  fleet policy decisions and automation
  placement and convergence controllers
  automated cluster rollout, HA, DR, and recovery orchestration
  organizational approval and release workflows
  enterprise identity/KMS integrations
  managed devices, relays, compliance evidence, and observability
  advanced distributed query/workload/storage orchestration

product or integration repositories
  Carrier Broker
  WalkNorth application packaging
  HL7/EHR integrations and clinical fixtures not intentionally published
```

The dependency direction can be achieved without forking the engine. The principal extraction problem is not a proprietary dependency already flowing into core; none exists. The problem is that commercial-looking orchestration implementations currently live inside public engine crates and are started directly by pgwire or exposed by the general CLI.

The current `bicdb-fleet` crate is not an enterprise fleet controller. It is predominantly an industry-neutral set of signed formats, verification logic, rollout state, and reference persistence. Those contracts belong in the public repository. Moving the crate wholesale would remove precisely the public interface a third-party controller needs.

## Classification vocabulary

| Classification | Meaning |
|---|---|
| `PUBLIC_CORE` | Implementation required for a complete, secure, independently useful BicDB distribution. |
| `PUBLIC_INTERFACE` | Stable protocol, format, verifier, trait, or reference state machine required to permit independent controllers/providers. |
| `COMMERCIAL_CANDIDATE` | Existing implementation of organizational- or fleet-scale automation that can move after a public seam exists. |
| `APPLICATION_VERTICAL` | Product-, Carrier-, ERP-, WalkNorth-, HL7-, or healthcare-specific code that is not generic database infrastructure. |
| `NEEDS_REFACTOR` | A package/module whose public mechanism and orchestration or vertical concerns are mixed. |
| `UNCERTAIN` | Ownership, provenance, intended publication, or correct product boundary requires a decision before relicensing. |

A `NEEDS_REFACTOR` classification does not mean the whole package should become private. In most cases the package's dominant implementation belongs in open core and only a narrow controller, collector, or vertical adapter should move.

## Repository and dependency observations

- The Cargo workspace contains 30 packages. The local dependency graph has no Cargo package cycle.
- `bicdb-core` does not depend on `bicdb-fleet` or another enterprise crate.
- `bicdb-cell` depends directly on `bicdb-fleet` for signed activation verification and convergence ledger types.
- `bicdb-pgwire` directly constructs and starts cluster relocation and automatic anti-entropy supervisors from `checkpoint_driver.rs`.
- `bicdb-cli` links cluster automation, certification/evidence commands, vertical compatibility commands, and benchmark engines into the main binary.
- There are no existing enterprise KMS/HSM, SAML/SCIM/LDAP/AD, managed-device fleet, private registry service, compliance dashboard, or multi-region control-plane crates to move. These should not be invented as empty Phase 3 crates.
- Existing Cell admission, device, grant, HA, and fleet crates are mostly local mechanisms, cryptographic contracts, and deterministic state machines. Their names sound commercial, but their authority and behavior are appropriate for open core.

The important current dependency path is:

```text
bicdb-cli
  -> bicdb-pgwire
       -> bicdb-core distribution mechanisms
       -> constructs automatic supervisors directly

bicdb-cell
  -> bicdb-fleet
       -> signed release/activation formats and verifiers
       -> local convergence state
```

The first path needs dependency inversion. The second needs an API naming/boundary cleanup, not removal from the public repository.

## Cargo package classification

This table covers every package returned by `cargo metadata --no-deps`.

| Package/path | Classification | Proposed disposition and rationale |
|---|---|---|
| `crates/bicdb-page` | `PUBLIC_CORE` | Page store, WAL-facing page mechanics, B+tree, storage integrity, and local/tiered read mechanisms are fundamental. Storage tiering primitives stay open; fleet-wide placement policy may be private. |
| `crates/bicdb-core` | `NEEDS_REFACTOR` | The database engine, MVCC, transactions, storage, security, recovery, indexes, resource safety, and replication remain open. Several `distribution_*` modules mix these mechanisms with automatic fleet orchestration. |
| `crates/bicdb-sql` | `PUBLIC_CORE` | SQL parser, planner, execution, authorization hooks, and compatibility behavior are basic database functionality. |
| `crates/bicdb-pgwire` | `NEEDS_REFACTOR` | Pgwire and TLS remain open. `checkpoint_driver.rs` currently owns construction/startup of automatic distribution controllers and must consume an injected public controller/background-service interface. |
| `crates/bicdb-resp` | `PUBLIC_CORE` | RESP support is an open database protocol surface. |
| `crates/bicdb-sync` | `PUBLIC_CORE` | Local-first synchronization protocol and usable local implementation remain open. Managed relay and fleet device operations may be private when implemented. |
| `crates/bicdb-wasm` | `PUBLIC_CORE` | WASM execution belongs to the public embedded/application runtime. |
| `crates/bicdb-analytics` | `PUBLIC_CORE` | Single-node analytics remains complete and open. Distributed elastic orchestration is a separate boundary. |
| `crates/bicdb-extension` | `NEEDS_REFACTOR` | The extension SDK and application ABI belong open, but the public ABI is extensively named around Carrier. Generalize the contract and isolate any compiler-specific adapter. |
| `crates/bicdb-app-runtime` | `NEEDS_REFACTOR` | A usable generic application runtime and package verifier belong open. Carrier-named programs, interpreters, workflows, and resources are too tightly mixed with the generic runtime. |
| `crates/bicdb-cell` | `NEEDS_REFACTOR` | The Cell runtime, identity, signed manifest verification, key-provider interface, attested key lease, and usable file provider remain open. Its direct dependency on the mixed-name `bicdb-fleet` crate should target a stable public activation/app-registry contract. |
| `crates/bicdb-cell-admission` | `PUBLIC_INTERFACE` | Signed admission evidence, policy formats, verification, and regulated admission checks must remain open. A hosted approval/admission service may be private. |
| `crates/bicdb-cell-device` | `PUBLIC_CORE` | Enrollment/retirement formats, authorization state, replica behavior, and device sync enforcement are security mechanisms. Managed inventory, enrollment campaigns, attestation operations, and remote-wipe orchestration may be private. |
| `crates/bicdb-cell-grant` | `PUBLIC_CORE` | Cryptographic grants, key envelopes, export/import, authorization epochs, enforcement, and ledgers remain open. Approval and routing workflows may be private. |
| `crates/bicdb-cell-ha` | `PUBLIC_INTERFACE` | Lease/fencing protocol, durable state, manual failover primitives, replication objects, and the deterministic failover action state machine remain open. A controller that performs automated geo failover may be private. |
| `crates/bicdb-fleet` | `PUBLIC_INTERFACE` | Despite its name, this is currently neutral signed fleet/release/activation data, verification logic, transparency/rollout state, a local immutable artifact registry, and convergence ledger. Keep it public, potentially under a clearer API crate/module name. |
| `crates/bicdb-blob-s3` | `PUBLIC_CORE` | A usable generic blob-provider implementation supports a complete application runtime. Enterprise-certified storage connectors and organization-wide policy may be private. |
| `crates/bicdb-provider-email` | `PUBLIC_CORE` | Generic provider implementation; not fleet orchestration. |
| `crates/bicdb-provider-redis` | `PUBLIC_CORE` | Generic provider implementation; not fleet orchestration. |
| `crates/bicdb-provider-tokenizer` | `PUBLIC_CORE` | Generic tokenizer provider/reference implementation. |
| `crates/bicdb-provider-embeddings` | `PUBLIC_CORE` | Generic embeddings provider/reference implementation, subject to separate model-license notices. |
| `crates/bicdb-provider-grpc` | `NEEDS_REFACTOR` | Generic gRPC provider belongs open, but Carrier-specific terminology/contracts must be removed or isolated. |
| `crates/bicdb-provider-llm` | `NEEDS_REFACTOR` | Generic LLM provider belongs open, but Carrier-specific terminology/contracts must be removed or isolated. |
| `crates/bicdb-provider-hl7` | `APPLICATION_VERTICAL` | HL7v2/MLLP, PID/patient, visit, and healthcare-specific mapping are certified integration/product concerns, not generic database infrastructure. |
| `crates/bicdb-bench` | `PUBLIC_CORE` | Public performance/regression tooling is useful, but it should be a dev/tool target rather than a normal dependency of the shipped CLI. |
| `crates/bicdb-cli` | `NEEDS_REFACTOR` | Core database, backup, recovery, Cell, and manual replication commands remain open. Automatic fleet, evidence collection, ERP, clinical-profile, and benchmark command wiring needs separation or injection. |
| `examples/extensions/hello-extension` | `PUBLIC_CORE` | Generic public SDK example. |
| `examples/extensions/website-host` | `PUBLIC_CORE` | Generic public application/extension example. |
| `examples/extensions/website-renderer` | `PUBLIC_CORE` | Generic public application/extension example. |
| `products/carrier-broker` | `APPLICATION_VERTICAL` | Product runtime built on BicDB. It is not part of the generic database and should live with its product, without copying the engine. |

## Other relevant repository areas

| Path/area | Classification | Finding |
|---|---|---|
| `abi/application-v2` | `PUBLIC_INTERFACE` / `UNCERTAIN` provenance | A stable, open application ABI is necessary. Confirm whether any files were generated from a proprietary Carrier specification and record their source and ownership. |
| `web/bicdb-client` | `PUBLIC_CORE` | Generic browser/client support belongs open. EHR-specific demo proxy behavior should become neutral or move to an example owned by the application. |
| `apps/bicdb-macos` | `NEEDS_REFACTOR` | Generic desktop BicDB capability may remain open, but current packaging/launcher code mixes BicDB with WalkNorth Auth and Carrier Broker. |
| `jepsen/bicdb` | `PUBLIC_CORE` | Correctness testing for replication and failure behavior should be public. |
| `benches`, `tests`, `scripts` | `PUBLIC_CORE` / `NEEDS_REFACTOR` | Correctness, security, recovery, compatibility, and benchmark harnesses remain open. Product-specific fixtures and scripts need classification; license enforcement scripts are currently stale. |
| EHR/ERP schemas and fixtures | `APPLICATION_VERTICAL` / `UNCERTAIN` | Test fixtures can remain public when deliberately sanitized and owned, but should not define production engine APIs. Reconfirm rights and publication intent before Apache relicensing. |
| `.github/workflows/macos-arm64-release.yml` | `APPLICATION_VERTICAL` | It fetches pinned external/private WalkNorth Auth and Carrier revisions and packages them with BicDB. This is a product release workflow, not a clean public database release workflow. |
| `docs/future-enterprise-roadmap.md` and similar commercial strategy documents | `UNCERTAIN` | Review deliberately before a public release. Security formats/specifications should be open; detailed unpublished commercial strategy need not be. |
| `web/bicdb-client/vendor/browser_wasi_shim` | `PUBLIC_CORE` | Vendored MIT code has an included license; retain attribution and verify the exact upstream revision. |

## Detailed mixed-module audit

### `bicdb-core`

| Module or concern | Classification | Finding |
|---|---|---|
| Page/storage integration, WAL, MVCC, transactions, catalog, backup/PITR, encryption, integrity, import/export | `PUBLIC_CORE` | Required for a production-capable community database. |
| Authentication, RBAC/RLS, TLS-facing support, audit events, resource governor | `PUBLIC_CORE` | Security and safe resource behavior may not become commercial gates. |
| Full-text, vector, graph, spatial, analytics primitives | `PUBLIC_CORE` | Single-node data/query capabilities remain open. |
| `distribution_data`, `distribution_transport`, range/consensus protocol, repair certificates, backup/restore certificates | `PUBLIC_CORE` / `PUBLIC_INTERFACE` | Data-plane protocols, evidence, and verification are required for independent controllers and safe manual operation. |
| `distribution_supervisor` | `COMMERCIAL_CANDIDATE` | Implements an automatic cluster controller and relocation driver rather than a database primitive. Extract only after the engine consumes a public controller interface. |
| Automatic rebalance and failure-repair planners in `distribution.rs` | `COMMERCIAL_CANDIDATE` | Policy/placement automation can be paid. Topology, range, relocation, validation, and manual plan execution stay open. |
| `distribution_schema_rollout::ClusterSchemaRolloutRun` | `COMMERCIAL_CANDIDATE` | Durable automated multi-node rollout is fleet automation. Schema format, compatibility validation, state, receipts, and manual execution primitives stay open. |
| `distribution_backup_run::ClusterBackupRun` | `COMMERCIAL_CANDIDATE` | Cluster-wide backup scheduling/convergence is commercial automation. Backup, restore, PITR, certificates, verification, and manual workflows remain open. |
| `distribution_restore_activation::ClusterRestoreActivationRun` | `COMMERCIAL_CANDIDATE` | Automated restore activation across a topology is DR orchestration. Local restore, admission safety, verification, and manual activation remain open. |
| Evidence initialization/capture/finalization in `distribution_certification` | `COMMERCIAL_CANDIDATE` | Organization/fleet evidence collection is a compliance operation. Evidence schema, report format, signature, verifier, and community self-check remain open. |
| `distribution_query` | `NEEDS_REFACTOR` | Query plans, protocols, executor traits, and a usable basic implementation should be public. Elastic scatter/gather scheduling, admission/workload management, and fleet execution policy are commercial candidates. |
| `distribution_gateway` and `distribution_routing` | `NEEDS_REFACTOR` | Route formats, validation, and basic routing remain open. Managed gateway pools, topology convergence, elastic capacity, and organization policy can move behind public APIs. |
| `distribution_anti_entropy*` and `distribution_repair*` | `NEEDS_REFACTOR` | Digest comparison, repair, verification, and a usable local safety loop are integrity protections and remain open. Only centralized fleet scheduling, policy, and evidence orchestration may be private. |
| `phi.rs` | `NEEDS_REFACTOR` | Field encryption, blind lookup, AAD binding, and redaction are generic security features and remain open. Rename PHI/environment-specific concepts to classified/protected data and use the public key-provider model. |
| `event.rs` patient/appointment/device views | `APPLICATION_VERTICAL` | Keep the generic event system open; move healthcare views to fixtures/examples or an application repository. |
| `graph.rs::clinical_graph_projection` | `APPLICATION_VERTICAL` | Keep graph mechanics open; move the clinical projection to a healthcare example/fixture. |

The distribution area is the principal extraction seam. Its exact source-module classification is:

| Source module | Classification | Boundary |
|---|---|---|
| `distribution.rs` | `NEEDS_REFACTOR` | Keep topology/range/relocation types and validation open; isolate automatic rebalance and failure-repair planners. |
| `distribution_consensus.rs` | `PUBLIC_CORE` | Consensus mechanism and safety behavior remain open. |
| `distribution_range_consensus.rs` | `PUBLIC_CORE` | Range consensus mechanism remains open. |
| `distribution_data.rs` | `PUBLIC_CORE` | Distributed data-plane implementation remains open. |
| `distribution_transport.rs` | `PUBLIC_INTERFACE` | Transport protocol/traits must support independent controllers. |
| `distribution_anti_entropy.rs` | `PUBLIC_CORE` | Divergence detection and integrity mechanism remain open. |
| `distribution_anti_entropy_repair.rs` | `PUBLIC_CORE` | Repair mechanism remains open. |
| `distribution_anti_entropy_repair_certificate.rs` | `PUBLIC_INTERFACE` | Repair evidence and verification remain open. |
| `distribution_anti_entropy_auto.rs` | `NEEDS_REFACTOR` | Keep a usable local integrity loop open; separate fleet policy/scheduling. |
| `distribution_anti_entropy_scheduler.rs` | `NEEDS_REFACTOR` | Separate safe scheduling primitives/reference behavior from centralized fleet scheduling. |
| `distribution_anti_entropy_run.rs` | `NEEDS_REFACTOR` | Keep action/evidence/state contracts open; isolate fleet execution. |
| `distribution_anti_entropy_repair_run.rs` | `NEEDS_REFACTOR` | Keep safe repair actions and receipts open; isolate fleet execution/approval. |
| `distribution_backup.rs` | `PUBLIC_CORE` | Backup format, barrier, certificate, and verification remain open. |
| `distribution_backup_run.rs` | `COMMERCIAL_CANDIDATE` | Automated crash-resumable cluster backup coordinator; retain its contracts in open APIs. |
| `distribution_restore.rs` | `PUBLIC_CORE` | Restore format, safety, and manual recovery remain open. |
| `distribution_restore_activation.rs` | `COMMERCIAL_CANDIDATE` | Automated topology-wide restored-cluster activation; retain readiness/evidence contracts. |
| `distribution_schema_rollout.rs` | `NEEDS_REFACTOR` | Compatibility/package/action contracts remain open; `ClusterSchemaRolloutRun` is a commercial candidate. |
| `distribution_certification.rs` | `NEEDS_REFACTOR` | Report/evidence/signature verifier remains open; centralized collector is a commercial candidate. |
| `distribution_query.rs` | `NEEDS_REFACTOR` | Plans/protocol/executor/basic path remain open; elastic scheduling/workload policy may be private. |
| `distribution_gateway.rs` | `NEEDS_REFACTOR` | Basic correct routing stays open; managed elastic gateway/pool behavior may be private. |
| `distribution_routing.rs` | `NEEDS_REFACTOR` | Public route/validation contracts must be separated from managed topology convergence. |
| `distribution_supervisor.rs` | `COMMERCIAL_CANDIDATE` | Automatic controller/relocation driver belongs behind the public controller contract. |

### `bicdb-pgwire`

The most important inversion seam is `src/checkpoint_driver.rs`. It currently:

- constructs `ClusterSupervisor`;
- selects a transport relocation driver;
- starts the distribution supervisor; and
- creates an `AutomaticRangeAntiEntropyController`.

Pgwire should own protocol serving and lifecycle hooks, not decide which fleet controller implementation exists. A public background-service/controller contract should be supplied by the embedding binary. Community BicDB must still have safe checkpointing, repair primitives, manual recovery, and a usable single-cluster integrity implementation.

### Cell crates

The Cell implementation is closer to the desired boundary than its names suggest:

- `bicdb-cell` includes the runtime, manifest/identity binding, package loading, key-provider contracts, attested key leases, and a development file provider.
- `bicdb-cell-admission` verifies signed admission evidence and policy.
- `bicdb-cell-grant` implements cryptographic grant/envelope and import/export behavior.
- `bicdb-cell-device` implements security-relevant enrollment, retirement, authorization, and replica behavior.
- `bicdb-cell-ha` provides fencing/lease state and a deterministic failover action state machine; it does not itself hold cloud/network authority.
- `bicdb-fleet` has no database, key, or network authority and is not a running fleet service.

These mechanisms remain open. Commercial implementations may make decisions, obtain organizational approvals, drive the state machines, schedule many Cells, and integrate enterprise providers.

### Application runtime and providers

The public runtime already exposes useful provider contracts, including secret, blob, egress, Redis, email, gRPC, tokenizer, embeddings, LLM, evaluation, and observability providers. Those contracts should be stabilized rather than replaced with private hooks.

The runtime and extension ABI use `Carrier*` types and terminology extensively. This creates both an industry-neutrality problem and an ownership/provenance question. The generic typed program, package, interpreter, capability, and workflow mechanics belong in open core. Carrier compiler-specific metadata and product workflows should consume a neutral public ABI from outside the engine.

The relevant runtime module boundary is:

| Source module/group | Classification | Boundary |
|---|---|---|
| `auth`, `bounded_sql`, `host`, `http`, `openapi`, `otlp`, `package`, `providers`, `resource` | `PUBLIC_CORE` | Generic application hosting, package verification, bounded execution, and provider contracts remain open. |
| `runtime/application_runtime`, `runtime/program_host`, `runtime/scheduling`, `runtime/schema_defs` | `PUBLIC_CORE` | General execution mechanisms remain open after neutral naming. |
| `program.rs` | `NEEDS_REFACTOR` | Generic typed programs remain open; Carrier-shaped contracts/names must be generalized or adapted externally. |
| `runtime/carrier_workflow.rs` | `APPLICATION_VERTICAL` / `NEEDS_REFACTOR` | Move product workflow semantics out while retaining a generic workflow/runtime interface. |
| `bicdb-extension/abi_v2.rs` | `NEEDS_REFACTOR` | The ABI remains public; pervasive Carrier names and any compiler-derived assumptions require neutralization/provenance review. |
| `bicdb-extension/{host,http}` | `PUBLIC_INTERFACE` | Stable extension host and HTTP contracts remain open. |

HL7 is different: its protocol/provider is healthcare-specific and can be a separate certified connector or vertical repository. Its removal must not remove generic TCP/gRPC/extension facilities from community BicDB.

## Commercial candidates: required extraction contracts

Every existing `COMMERCIAL_CANDIDATE` is covered below. A candidate is the named implementation, not the adjacent public formats/mechanisms.

| Candidate | What it does | Why outside core | Required public BicDB interface | Community impact if moved correctly | Current extraction blockers |
|---|---|---|---|---|---|
| `distribution_supervisor` / relocation driver | Watches topology state and automatically drives range relocation and convergence. | Continuous placement/convergence across a fleet is operational automation. | Versioned topology snapshots; relocation plan/action/receipt types; `PlacementPlanner` and `RelocationExecutor`/transport traits; idempotency, fencing, audit, metrics, and cancellation hooks. | None: topology inspection, manual relocation, safety verification, and a documented reference flow remain open. | Implemented inside `bicdb-core`; pgwire constructs it directly; store and transport concrete types cross the boundary. |
| Rebalance/failure-repair planners in `distribution.rs` | Chooses moves after imbalance or failure. | Placement policy and automated repair strategy are commercial workload-management value. | Public topology/range/capacity/failure models; deterministic plan format and verifier; manual plan builder/executor; pluggable planner trait. | None if users can inspect topology, construct/validate a plan, and perform manual recovery without proprietary code. | Planning algorithms share a module and internal structs with public distribution mechanism. |
| `ClusterSchemaRolloutRun` | Persists and advances automated cluster schema rollout one voter/action at a time. | Fleet-wide rollout and migration convergence are paid automation. | Public schema package/version/compatibility contracts; rollout action/receipt state machine; executor trait; resume/idempotency/audit hooks. | None: ordinary migrations and explicit/manual multi-node application remain open. | Runner, state persistence, transport assumptions, and public rollout data live together. |
| `ClusterBackupRun` | Coordinates a crash-resumable distributed backup run. | Central scheduling and fleet convergence of backups are operational automation. | Public backup/PITR APIs; cluster snapshot/sequence/barrier contracts; certificate/manifest verifier; action/receipt interface; manual documented procedure. | None: local backup/restore/PITR and manual consistent cluster backup remain supported. | Coordinator is inside core beside required backup certificates and primitives. |
| `ClusterRestoreActivationRun` | Coordinates safe activation of a restored cluster/topology. | Automated DR activation and topology orchestration are commercial. | Public restore manifest, readiness, fencing, epoch, activation actions/receipts, verifier, and manual activation procedure. | None: restoration and manual recovery remain complete. | Coordinator shares internal state/transport with public restore safety mechanisms. |
| Certification evidence collector portions of `distribution_certification` | Initializes, captures, records, and finalizes cluster evidence/report bundles. | Continuous compliance evidence collection at organizational scale is a platform feature. | Open evidence/report schema, signed statements, verifier, audit event stream, metrics/export API, and a community self-check command. | None: integrity/security tests and verification remain open; only centralized collection/retention/workflow moves. | Verifier and collector are combined, and CLI directly exposes the complete workflow. |
| Advanced coordinator portions of `distribution_query` | Coordinates distributed scatter/gather execution and transactions. | Elastic execution, fleet admission, scheduling, and workload policy are commercial differentiators. | Public logical/physical distributed plan, fragment/executor, result stream, cancellation, admission, accounting, snapshot, error, and metrics contracts. | None if all single-node analytics and a usable basic/manual distributed path remain open. | Protocol, coordinator, engine internals, and policy are not separated; internal engine types leak into orchestration. |
| Managed gateway/pool portions of `distribution_gateway` and `distribution_routing` | Maintains topology-aware connections/routes and dispatches work. | Elastic gateway capacity and topology convergence at fleet scale are operations automation. | Public route/topology snapshot, health, endpoint, session/fencing, resolver, pool, retry budget, and observability traits. | None if direct connections, static configuration, and basic validated routing remain open. | Basic correctness routing and managed elastic behavior currently share concrete implementations. |
| Cluster automation command implementations in `bicdb-cli` | Starts rebalances, automated certification/evidence, and other cluster-wide runs. | A commercial controller should own automation commands and credentials. | Stable command/service registration, controller client contracts, serializable request/response formats, and open manual commands. | Community CLI remains fully capable for core database and manual cluster administration. | One monolithic CLI depends on implementation crates; no plugin/subcommand injection seam. |

Automatic anti-entropy and repair are intentionally not classified wholesale as commercial candidates. Integrity protection cannot be removed from community BicDB. A private platform may schedule and prove repair across thousands of Cells, but open BicDB must detect divergence, validate repairs, expose evidence, and provide a usable safety loop.

## Boundary violations and architectural findings

### Core to fleet dependencies

There is no Cargo dependency from `bicdb-core` to `bicdb-fleet`. The actual boundary violations are responsibility and construction:

1. commercial-looking fleet orchestration is implemented within `bicdb-core`;
2. `bicdb-pgwire` constructs and starts those controllers;
3. `bicdb-cli` directly includes their operational commands; and
4. `bicdb-cell` consumes activation contracts through a crate named `bicdb-fleet`, although those contracts themselves belong open.

### Core to enterprise assumptions

No proprietary/enterprise crate dependency exists. Enterprise assumptions are nevertheless embedded in generic code:

- a pgwire server is assumed to own cluster-controller lifecycle;
- cluster evidence collection lives alongside engine verification;
- automated rollout/restore/placement logic uses core-internal storage and transport types;
- the main CLI assumes all operational roles and dependencies belong in one binary;
- production macOS release automation assumes private WalkNorth Auth and Carrier components.

### Circular dependencies

Cargo metadata reports no package dependency cycle. There is an architectural ownership loop:

```text
CLI chooses runtime configuration
  -> pgwire starts controllers
     -> controllers mutate core distribution state
        -> pgwire checkpoint lifecycle drives them
```

This is not a Cargo cycle, but it prevents a clean platform-over-core dependency direction. Controller lifecycle must move to the embedding binary/control-plane side of a public API.

### Private hooks that should become stable public APIs

The following seams should be public, versioned, documented, and usable by an unrelated controller:

- cluster/controller background-service lifecycle;
- topology snapshot and watch stream;
- placement planner and validated relocation plan/action/receipt;
- schema rollout actions, compatibility checks, and receipts;
- backup/restore/PITR coordination barriers, manifests, and receipts;
- HA lease, fencing, durable-sequence selection, activation, and route publication;
- distributed query fragment/executor/result/admission/accounting interfaces;
- app artifact registry lookup/fetch and signed activation verification;
- Cell lifecycle/status/health and manifest evidence;
- device enrollment/attestation/revocation event interfaces and sync transport;
- key-provider and workload-attestation context;
- compliance evidence/event/metrics export;
- CLI/service registration for optional controller implementations.

These may remain modules in existing crates where coherent. Crate proliferation is not required.

### Mechanism currently mixed with orchestration

- Distribution topology/relocation types are mixed with automatic placement and relocation.
- Backup/restore safety evidence is mixed with automated run coordinators.
- Schema package compatibility is mixed with a fleet rollout runner.
- Certification report verification is mixed with evidence collection.
- Query/executor protocols are mixed with scheduling/coordinator policy.
- Routing correctness is mixed with managed gateway/pool behavior.
- Anti-entropy integrity mechanisms are mixed with automatic scheduling.
- pgwire protocol lifecycle is mixed with control-plane construction.
- CLI user commands are mixed with controller implementations and vertical tools.

### Enterprise identity, KMS, and compliance mixing

No enterprise identity provider or enterprise KMS/HSM integration was found in the workspace. Do not extract nonexistent implementations.

Generic security that must stay open includes:

- `CellKeyProvider`;
- the attested key-lease provider/verifier;
- the usable development/file provider;
- signed Cell manifest and identity binding;
- admission and signature verification;
- audit events and evidence formats;
- grant/device cryptography and enforcement.

Compliance evidence collection in `distribution_certification` is the main mixed enterprise-style concern. The evidence format and independent verifier remain public; fleet collection, retention, dashboarding, and separation-of-duty workflows may be private.

### Vertical-specific logic in generic crates

The following must be moved or generalized before describing BicDB as industry-neutral:

- `bicdb-provider-hl7` and its patient/visit/MLLP concepts;
- `products/carrier-broker`;
- `Carrier*` application ABI/runtime/interpreter/workflow terminology;
- `bicdb-core::phi` names and PHI-specific environment keys, while preserving the generic encryption mechanism;
- `PatientView` and `AppointmentView` in the generic event module;
- `clinical_graph_projection()` in the generic graph module;
- clinical and ERP commands/profiles in the main CLI;
- WalkNorth Auth/Carrier integration in the macOS application and release workflow;
- EHR-specific web demo behavior.

Clinical terms in deliberately sanitized tests/examples are not an engine boundary violation, but their ownership and publication intent must be recorded.

## Proposed repository boundary

### Remain in `bicdb`

- all core database correctness, storage, query, protocol, security, encryption, integrity, resource safety, backup/restore/PITR, and import/export;
- single-node analytics and all local query capabilities;
- extension SDK, neutral application ABI/runtime, WASM runtime, package format, signature verification, and usable provider/reference implementations;
- Cell runtime, immutable identity, manifests, evidence, key-provider interface, and at least one production-capable provider path;
- grants, device/local sync, admission, replication, HA/fencing, and manual recovery protocols and enforcement;
- topology, placement-plan, rollout, HA, DR, query fragment, device, registry, admission, evidence, and controller APIs;
- basic replication/manual failover and a usable community operation path;
- integrity anti-entropy/repair and a usable safety loop;
- public test suites, Jepsen tests, protocol conformance, and architectural dependency checks.

### Move to or be implemented in `bicdb-platform`

- automatic placement, rebalance, and fleet convergence implementation;
- automatic fleet schema/app rollout and staged activation;
- automated cluster backup/restore activation, DR drills, and multi-region failover;
- centralized certification/compliance evidence collection and organizational workflows;
- managed topology/gateway capacity and fleet observability;
- advanced distributed query admission, workload scheduling, elastic execution, and storage placement;
- enterprise identity/KMS/HSM connectors and organization-specific approvals when implemented;
- managed device inventory, attestation operations, relay, revoke/wipe orchestration when implemented;
- private registry and release approval/activation services when implemented.

### Move to product/integration repositories

- Carrier Broker and compiler/product-specific integration;
- WalkNorth Auth/desktop product bundling;
- HL7/EHR-specific provider and clinical projections;
- ERP compatibility/product behavior not required for generic SQL compatibility;
- application-owned EHR demo/schema assets that are not deliberately public fixtures.

## Ten highest-priority code moves/refactors

1. Introduce a public controller/background-service lifecycle interface and stop constructing distribution supervisors in `bicdb-pgwire::checkpoint_driver`.
2. Split public distribution topology, validation, action, and receipt contracts from automatic placement/rebalance/failure-repair implementations in `bicdb-core`.
3. Preserve `bicdb-fleet` formats/verifiers as a public interface, rename/reorganize it if useful, and make `bicdb-cell` depend only on the stable activation/app-registry contract.
4. Separate cluster schema rollout, backup-run, and restore-activation state machines/contracts from the automatic runners that drive an entire topology.
5. Separate certification evidence schemas/verifiers and community self-checks from centralized evidence collection and retention.
6. Split distributed query plan/executor/result contracts and a usable basic path from elastic scheduling, admission, and workload-management policy.
7. Generalize PHI-specific security into protected/classified-field encryption while retaining all encryption, blind-index, redaction, and key-provider behavior in open core.
8. Generalize the `Carrier*` ABI/runtime into an industry-neutral public application contract; move compiler/product adapters and workflows out without reducing the public runtime.
9. Make the CLI composable: retain database/security/recovery/manual-cluster commands, inject optional controller commands, and remove the normal dependency on `bicdb-bench` and its comparison engines.
10. Move HL7, clinical projections/views, Carrier Broker, WalkNorth packaging, and ERP/clinical product integrations out of generic production crates, leaving neutral examples and interfaces.

## Code that should not be Apache-licensed yet

Do not apply Apache-2.0 to the repository wholesale until the following are resolved:

- automatic distribution controller, placement/rebalance, fleet rollout, automated DR activation, evidence collection, and advanced distributed coordination implementations that are intended as commercial value;
- `products/carrier-broker` and Carrier compiler/product-specific runtime pieces;
- `bicdb-provider-hl7` and other clinical integration assets until their product boundary and ownership are explicit;
- WalkNorth Auth/Carrier macOS packaging and release automation;
- EHR/ERP fixtures or schemas without written ownership, sanitization, and publication approval;
- `abi/application-v2` or generated/runtime artifacts until their generation source and rights are documented;
- detailed commercial roadmap material that the company does not deliberately intend to publish;
- any file contributed or generated under rights that do not permit Apache-2.0 relicensing.

This is a caution against premature relicensing, not a recommendation to make generic runtime/security mechanisms proprietary. Several mixed files will be Apache candidates after commercial implementations or vertical adapters are separated.

## Proprietary-looking code that belongs in open core

The following names may sound enterprise-oriented, but their current mechanism or contract should remain open:

- `bicdb-fleet` signed trust, release, transparency, rollout, activation, and convergence formats/verifiers;
- local immutable artifact registry/reference persistence needed to exercise the app package flow;
- Cell admission evidence, policy format, and verifier;
- `CellFailoverSupervisor` as a deterministic action state machine, plus lease/fencing/manual failover;
- grant envelopes, object export/import, authorization epochs, and enforcement;
- device enrollment/retirement protocol, authorization state, filtered/local replica behavior, and sync protocol;
- Cell key-provider and attested lease verification;
- backup/restore/PITR and distribution backup/restore certificate formats and verification;
- anti-entropy, divergence detection, repair validation, integrity evidence, and a usable local safety loop;
- application package/ABI/signature verification;
- route, topology, action, receipt, and audit formats required by third-party controllers.

Charging for the automated service that drives these mechanisms is compatible with keeping the mechanisms safe, inspectable, and independently implementable.

## Licensing, copyright, and provenance audit

### Current repository license

The workspace inherits the root `LICENSE` through Cargo `license-file`. The current terms are BUSL-1.1 for the 1.0.x-beta line, with a stated change date of 2030-08-11 to AGPL-3.0-only. Phase 1 makes no license changes.

The earlier release assessments are archived privately. This historical extraction record does not determine current licensing; see [the current licensing guide](licensing-faq.md).

### Ownership/contributors

`git shortlog -sne --all` reports:

| Commits | Identity |
|---:|---|
| 882 | Nikolai Manek, one email identity |
| 626 | `nikoma`, same email identity |
| 8 | Nikolai, `@plumeria.ai` identity |
| 2 | Codex, `codex@example.local` |

Required follow-up:

- map all aliases/emails to legal contributors and applicable employment/assignment agreements;
- obtain written confirmation from every copyright holder named in current license materials, including WalkNorth, for Apache-2.0 relicensing of the selected files;
- document the ownership and review process for AI-assisted/Codex-authored commits;
- confirm that copied specifications, fixtures, generated ABI files, and product assets were created under assignable rights;
- decide on DCO versus CLA. DCO is simple for provenance; a CLA with explicit copyright/patent grants is preferable if future dual licensing or proprietary derivative distribution needs broader grants. Legal counsel should make the final choice.

No repository CLA, DCO sign-off policy, or contributor guide was found.

### Dependency licensing

The dependency graph is mostly permissive, but an Apache readiness audit must cover every feature and target, not only the default Linux build. Notable findings:

- `bicdb-core` enables `osm-import` in its default feature set.
- That path includes `osmpbfreader` and related crates with WTFPL components and `smartstring` under MPL-2.0+.
- `self_cell` offers Apache-2.0 or GPL-2.0-only; the permissive option must be documented.
- Intel `ittapi`/`ittapi-sys` offer GPL or BSD alternatives; the permissive option must be documented.
- `r-efi` offers permissive alternatives alongside LGPL.
- local model weights are not tracked, but externally distributed models such as EmbeddingGemma have separate terms and must never be represented as covered by BicDB's Apache license.
- the vendored browser WASI shim contains an MIT license and needs NOTICE/attribution handling.

`THIRD_PARTY_NOTICES.md` describes the OSM path as optional, but the current `bicdb-core` default feature list enables it. The notice and actual default graph disagree.

### Broken license gate

Both `cargo deny check licenses` and `scripts/check-licenses.sh` fail at this baseline. The immediate cause includes stale `deny.toml` clarification hashes for the root license: the configured hash is `0xe71a31ec`, while cargo-deny computed `0xc41bceeb`. The clarification list also needs review for crates added since it was written.

The broader script additionally fails the cargo-deny bans policy because wildcard dependencies are present in `bicdb-cell-ha`, `bicdb-extension-example`, and `bicdb-website-renderer-example`. Numerous duplicate-version warnings are also reported. These are dependency-hygiene/readiness findings, not evidence that those public crates should become proprietary.

The primary test workflow runs `scripts/validate-licensing.sh`, but it does not run the failing dependency-license check. Therefore current green CI would not prove license compliance. This is a release blocker, not a Phase 1 code change.

### Generated, vendored, and bundled code

- Record exact source revision and modification status for `web/bicdb-client/vendor/browser_wasi_shim`.
- Record how every file under `abi/application-v2` is produced and whether the source generator/specification is owned and publishable.
- Keep generated code headers and regeneration instructions consistent with the selected source license.
- Separate the public BicDB distribution manifest from product bundles that fetch private WalkNorth Auth or Carrier artifacts.
- Reconfirm the earlier assertion that WalkNorth EHR fixtures contain no third-party patient data and are intentionally publishable.
- Audit npm, Swift/macOS, WASM, container, model, and release-artifact dependencies independently from Cargo.

### Apache release documentation needed later

Phase 5 should propose, but not yet install:

- root Apache-2.0 `LICENSE`;
- a truthful `NOTICE` containing required attributions only;
- per-file SPDX identifiers or a documented generated/vendored exception policy;
- separate licensing manifests for proprietary product bundles;
- trademark policy and naming rules for BicDB compatibility claims;
- contributor policy and DCO/CLA process;
- a tag/commit-specific relicensing statement so prior BUSL history is not ambiguously represented.

Legal counsel must review the final file list, contributor grants, dependency choices, patents, trademarks, model licenses, and mixed public/proprietary distribution.

## Extraction risk by subsystem

| Major subsystem | Risk | Reason |
|---|---|---|
| Page/storage/WAL/MVCC/transactions | `LOW` | Coherent public core with no enterprise dependency; licensing verification still applies. |
| SQL/pgwire/RESP | `MEDIUM` | Query/protocol functionality is clearly public, but pgwire currently starts automatic distribution controllers. |
| Authentication/RBAC/RLS/TLS/encryption/integrity | `MEDIUM` | Must remain open; PHI-specific naming/key paths and provider boundaries need generalization without regression. |
| Backup/restore/PITR | `MEDIUM` | Local mechanisms are clear; distributed certificates and automated cluster run coordinators are mixed. |
| Basic replication/consensus/manual recovery | `MEDIUM` | Mechanisms belong open, but topology and automatic repair/HA orchestration share internal types. |
| Distribution/fleet automation | `HIGH` | Supervisors and policy live in core, pgwire constructs them, and the CLI owns their operation. |
| Cell runtime/identity/admission/key providers | `MEDIUM` | Conceptual boundary is strong, but the Cell crate is large and directly consumes a mixed-name fleet package. |
| Grants/device sync/Cell HA | `MEDIUM` | Security mechanisms are appropriately public; clean controller/transport seams and managed-service separation need proof. |
| Application/extension/WASM runtime | `HIGH` | Generic public value is tightly interwoven with pervasive Carrier-specific ABI and runtime terminology. |
| Provider ecosystem | `MEDIUM` | Generic provider traits are clean; HL7 is vertical and gRPC/LLM adapters retain Carrier-specific assumptions. |
| Single-node analytics/search/vector/spatial/graph | `LOW` | Clearly public, except clinical graph examples should move. |
| Distributed analytics/query execution | `HIGH` | Public execution protocols and commercial elastic scheduling/workload policy are not yet separated. |
| CLI/packaging/release tooling | `HIGH` | Core, automation, vertical, benchmarking, and private product packaging are combined. |
| Web client | `LOW` | Generic and separable; clean up vertical demo content and vendor attribution. |
| Desktop/macOS application | `HIGH` | Generic client behavior is bundled with external/private WalkNorth Auth and Carrier product components. |
| Licensing/provenance | `HIGH` | Apache grants are not established, cargo-deny currently fails, notices are stale, and mixed/generated/product assets require decisions. |

Risk describes extraction/relicensing difficulty, not an assertion of runtime insecurity.

## Phase 2 admission criteria

Phase 2 should begin only after approving this boundary and deciding:

1. whether the generic application ABI/runtime will be renamed in place and fully open;
2. which distribution automation implementations are intended commercial value;
3. whether HL7 and Carrier/WalkNorth product assets move to dedicated product/integration repositories;
4. the stable controller interfaces and versioning policy;
5. the minimum usable community path for cluster integrity, replication, backup, and manual recovery; and
6. ownership/publication intent for fixtures, generated ABI artifacts, and AI-assisted commits.

No private sibling should be scaffolded until at least one justified implementation can compile against these public seams. No engine code should be copied into that sibling.

## Phase 1 decision

Proceeding is feasible, but extraction risk is `HIGH` overall because the most valuable fleet automation is embedded in core/pgwire/CLI and the generic application runtime is heavily Carrier-shaped. The open database mechanisms themselves are not the problem.

The recommended strategy is centralized source ownership with dependency inversion:

```text
bicdb public mechanisms + stable contracts
                    ^
                    |
bicdb-platform controllers and enterprise integrations
```

Do not create a reduced-security community edition. Do not move `bicdb-fleet`, Cell security, integrity repair, key-provider primitives, recovery, or verification wholesale merely because their names resemble enterprise features.
