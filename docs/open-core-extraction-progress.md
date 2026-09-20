# BicDB Open-Core Extraction Progress

Baseline audit: [`open-core-extraction-audit.md`](open-core-extraction-audit.md)

Target dependency direction:

```text
bicdb-platform  -> bicdb public APIs
product repos   -> bicdb public APIs
bicdb           -X-> proprietary or product-specific code
```

The repository remained under BUSL-1.1 throughout this campaign. Apache-2.0
readiness was assessed in Stage 8; the later prospective relicensing decision
for `1.0.363-beta` and subsequent releases is documented in
[`licensing-faq.md`](licensing-faq.md).

## Stage ledger

| Stage | Version | Status | Commit | Validation |
|---|---|---|---|---|
| 0. Clean baseline | `1.0.353-beta` | Complete | `b678614f` | Build/policy checks pass; 3,052 tests pass, 4 baseline tests fail, 12 are ignored. |
| 1. Dependency inversion | `1.0.354-beta` | Complete | `dae72199` | Workspace build, pgwire tests, CLI tests, opt-in comparison build, license and formatting checks pass. |
| 2. Product-neutral contracts | `1.0.355-beta` | Complete | `8ae2ea9b` | Workspace check and targeted application ABI/runtime/Cell/provider suites pass. |
| 3. Mechanism/automation split | `1.0.356-beta` | Complete | `6b355597` | Workspace check and focused core/pgwire policy-injection tests pass. |
| 4. Vertical/product extraction | `1.0.357-beta` | Complete | `f0de4b5d` | Public and integration workspace checks/tests pass; vertical dependency scan is clean except documented legacy aliases. |
| 5. Private platform workspace | `1.0.358-beta` | Complete | `c23c7fbc` | Public/manual and private/automatic controller builds and focused tests pass. |
| 6. Independence proof | `1.0.359-beta` | Complete | `5aa72ec0` | CI architecture gate plus community capability and private public-API validation modes pass. |
| 7. Public API documentation | `1.0.360-beta` | Complete | `58dc2620` | Public all-target check, neutral API/profile regressions, sibling builds/tests, license policy, production examples, and architecture gate pass. |
| 8. Apache readiness | `1.0.361-beta` | Complete | `de9da7b0` | Apache-readiness evidence audit, licensing/architecture gates, workspace check, and focused gRPC contract tests pass; no relicensing performed. |
| 9. Final report | `1.0.362-beta` | Complete | Final campaign commit | Complete public/private/integration validation matrix; all four Stage 0 defects closed; final topology and Apache blockers recorded. |

## Stage 0 baseline

Baseline source commit: `86bf0330a1b587a2e677242c54b2e9b787f113e0`

Completed hygiene work:

- repaired the first-party BUSL-1.1 cargo-deny clarification hashes;
- added clarification entries for the six Cell/fleet crates added after the
  previous policy update;
- pinned three local path dependencies that violated the no-wildcards policy;
- corrected the OpenStreetMap dependency notice and policy comments to state
  that `osm-import` is currently default-enabled;
- bumped the workspace release to `1.0.353-beta`.

Validation:

| Command | Result |
|---|---|
| `cargo deny check licenses` | Pass |
| `cargo deny check bans sources` | Pass, with existing duplicate-version warnings |
| `scripts/check-licenses.sh` | Pass |
| `cargo build --workspace` | Pass |
| `cargo test --workspace` | One pre-existing/flaky key-order assertion failed; exact rerun passed |
| `cargo test --workspace --no-fail-fast` | Complete: 3,052 passed, 4 failed in three targets, 12 ignored |

The observed baseline failure was:

```text
bicdb-sql --test composite_types
named_composite_attributes_evolve_existing_scalar_array_and_nested_values
```

The semantic JSON values were identical, but an assertion compared serialized
object key order:

```text
actual:   {"member":{"code":9,"title":"nested","enabled":null}}
expected: {"member":{"code":9,"enabled":null,"title":"nested"}}
```

An exact rerun passed, but the no-fail-fast run reproduced it. The complete
baseline additionally found:

- two `json_storage` assertions where JSONB retained input key order instead of
  PostgreSQL-canonical key order; and
- one `jsonb_primary_keys` composite-prefix lookup returning no row.

The three failing targets are:

```text
bicdb-sql --test composite_types       (1 failure)
bicdb-sql --test json_storage          (2 failures)
bicdb-sql --test jsonb_primary_keys    (1 failure)
```

No engine source had changed when these failures occurred. They are recorded
as pre-existing baseline defects rather than open-core extraction regressions.
Future stages must not increase this set, and should make affected focused
suites green when they touch JSON/composite behavior.

## Stage 1 dependency inversion

Implemented boundaries:

- pgwire now exposes `PgWireHostService` and the narrow
  `PgWireHostContext`; the protocol lifecycle starts only services explicitly
  installed by its embedding process;
- the generic `serve` and `serve_cluster` entry points no longer silently
  construct an automatic distribution supervisor;
- the BicDB CLI explicitly injects the transitional automatic distribution
  service to preserve its existing clustered-server behavior;
- external comparison databases are optional `bicdb-bench` dependencies and
  enter the CLI graph only through `bench-comparison-engines`;
- `redb` and `fjall` are absent from the default production CLI dependency
  graph, while the opt-in comparison command remains buildable.

The automatic distribution implementation remains temporarily in the public
workspace behind the new lifecycle contract. Stage 3 separates its open
mechanisms from the controller, and Stage 5 moves the justified controller
implementation to `bicdb-platform`.

Validation:

| Command | Result |
|---|---|
| `cargo build --workspace` | Pass |
| `cargo test -p bicdb-pgwire` | Pass |
| `cargo test -p bicdb-cli` | Pass |
| `cargo test -p bicdb-cli --features bench-comparison-engines --no-run` | Pass |
| default CLI dependency-tree assertion excluding `redb`/`fjall` | Pass |
| opt-in CLI dependency-tree assertion including `redb`/`fjall` | Pass |
| `cargo deny check licenses` | Pass |
| `cargo fmt --all -- --check` | Pass |
| `git diff --check` | Pass |

## Stage 2 product-neutral contracts

Implemented boundaries:

- the public application ABI now uses generic `Application*` types rather
  than Carrier-owned type names;
- manifest, route, telemetry, runtime host, evaluation, provider, Cell, SQL,
  and native commit-validation contracts now expose neutral application
  terminology;
- old serialized `carrier_profile`, `carrier_program`, `carrier_request`,
  `carrier_response`, `prepared_carrier_programs`, and `carrier_invariant`
  values remain accepted through one-way compatibility aliases;
- serialization emits only the canonical neutral names;
- Carrier Broker remains a product-owned name and consumes the neutral BicDB
  application ABI.

Internal compatibility helpers and legacy fixture vocabulary are not public
contracts. They remain temporarily where changing persistent or signed wire
material would require an explicit format migration. Stages 4 and 7 remove
product implementation and documentation from the public repository without
silently breaking those formats.

Validation:

| Command | Result |
|---|---|
| `cargo check --workspace` | Pass |
| `cargo test -p bicdb-extension` | Pass: 55 tests |
| `cargo test -p bicdb-app-runtime --lib` | Pass: 134 passed, 1 ignored |
| `cargo test -p bicdb-cell -p bicdb-provider-grpc -p bicdb-provider-llm --lib` | Pass: 43 tests |
| legacy manifest input / neutral output regression | Pass |

## Stage 3 mechanism/automation split

Implemented boundaries:

- `ClusterPlacementPlanner` is the public policy seam between validated
  topology/relocation mechanisms and placement decisions;
- `ClusterSupervisor::with_planner` consumes controller policy through that
  interface before BicDB validates and applies a plan;
- `PgWireHostContext` exposes only bounded database mechanisms, graceful
  background-task lifecycle, shutdown signals and topology installation to an
  out-of-tree host service;
- the subsystem-by-subsystem boundary is recorded in
  [`open-core-controller-boundaries.md`](open-core-controller-boundaries.md);
- modules that already contain only open formats, verification or bounded
  local safety behavior remain public; nonexistent enterprise services were
  not invented.

The transitional built-in automatic controller remains present only until its
physical extraction in Stage 5. The interface used by that extraction is now
public and tested.

Validation:

| Command | Result |
|---|---|
| `cargo check --workspace` | Pass |
| `cargo test -p bicdb-core distribution_supervisor` | Pass |
| `cargo test -p bicdb-pgwire host_service` | Pass |
| injected placement-policy regression | Pass |

## Boundary decisions

- Public security, integrity, recovery, Cell, grant, device, HA fencing, and
  signed verification mechanisms are not extraction candidates.
- `bicdb-fleet` currently contains public formats/verifiers and reference state,
  not a proprietary fleet service.
- Automatic distribution supervisors, placement decisions, fleet rollout,
  centralized evidence collection, and elastic scheduling are implementation
  candidates for `bicdb-platform`.
- Carrier, WalkNorth, healthcare, and ERP concepts are product/vertical
  concerns, except where a generically named database mechanism replaces them.
- No speculative private crates will be created. A platform crate is justified
  only when it receives real extracted implementation.

## Stage 4 vertical and product extraction

Implemented boundaries:

- Carrier Broker, HL7, WalkNorth desktop packaging and product fixtures now
  live in the independent `../bicdb-integrations` workspace;
- healthcare program built-ins and healthcare event/graph projections consume
  public application-host and projection contracts from that workspace;
- the public runtime now uses neutral application-module and protected-data
  contracts while accepting old signed/persisted vocabulary as compatibility
  aliases;
- product-specific CLI benchmarks, migrations, release workflows and reports
  no longer ship in the database repository;
- graph projection definitions are data supplied through the public API rather
  than a built-in clinical choice.

The detailed move map and compatibility contract are in
[`product-integration-boundary.md`](product-integration-boundary.md).

Validation:

| Command | Result |
|---|---|
| `cargo check --workspace` in `bicdb` | Pass |
| `cargo test --workspace` in `bicdb-integrations` | Pass: 50 tests |
| focused public application/runtime/core/pgwire regressions | Pass |
| `cargo deny check licenses` | Pass |
| `cargo fmt --all -- --check` and `git diff --check` | Pass |

## Stage 5 private platform workspace

Implemented boundaries:

- `../bicdb-platform` is an independent private Rust workspace and contains no
  copy of the BicDB engine;
- its real `bicdb-platform-fleet-controller` crate owns automatic placement,
  failed-relocation retry policy and automatic controller activation;
- the built-in public automatic placement implementation was removed;
- public `ClusterSupervisor` is now a validation/convergence runtime with no
  ambient placement policy: its default is manual convergence and automatic
  behavior requires an injected `ClusterPlacementPlanner`;
- `ManualDistributionHostService` keeps the community data plane, metadata
  consensus, explicit relocation progress and local integrity work usable;
- `PgWireHostContext::start_distribution_controller` lets an unrelated host
  install its own controller through public BicDB APIs.

No empty HA, compliance, device, registry or scheduler crates were created.
Those private services do not yet exist as separable implementations. Their
public formats, verification and deterministic mechanisms remain in BicDB.

Validation:

| Command | Result |
|---|---|
| focused `bicdb-core` controller/mechanism tests | Pass |
| focused `bicdb-pgwire` host-service tests | Pass |
| `cargo check --workspace` in `bicdb-platform` | Pass |
| `cargo test --workspace` in `bicdb-platform` | Pass |

## Stage 6 repository independence proof

Implemented gates:

- `scripts/check-open-core-boundary.sh` rejects any public Cargo path outside
  the BicDB repository, any private repository dependency, and the extracted
  first-party automatic controller types;
- the public GitHub Actions workflow runs that architecture gate from a fresh
  community-only checkout before the existing all-target build and complete
  workspace test matrix;
- `scripts/validate-community-capabilities.sh` exercises the non-negotiable
  encryption, backup/restore/PITR, replication, SQL authorization/RLS, pgwire,
  Cell identity/admission/grants/HA, local sync and audit/metrics capabilities;
- `../bicdb-platform/scripts/validate-public-api-boundary.sh` rejects source
  patches and non-BicDB path dependencies, then builds and tests the platform;
- the private repository has its own CI workflow that checks out public BicDB
  as a sibling and runs the public-API-only validation.

The ordinary public CI remains the authoritative complete test run; the
focused capability mode is an admission gate and does not replace it.

Validation:

| Command | Result |
|---|---|
| `scripts/check-open-core-boundary.sh` | Pass |
| `scripts/validate-community-capabilities.sh` | Pass |
| `../bicdb-platform/scripts/validate-public-api-boundary.sh` | Pass |
| `cargo check --workspace --all-targets --locked` CI mode | Pass locally |
| public vertical-source scan | Pass except intentional legacy deserialization/storage aliases |

## Stage 7 public API and documentation cleanup

Implemented boundaries:

- [`public-controller-apis.md`](public-controller-apis.md) documents how an
  unrelated operator can run BicDB independently, inject controller policy,
  implement providers, drive recovery manually, operate sync infrastructure,
  and verify every signed format without a private repository;
- canonical trusted SQL policy settings are now `bicdb.current_*`; historical
  `carrier.current_*` settings remain host-bound, SQL-immutable compatibility
  aliases so existing policies do not lose authority or silently widen access;
- newly emitted Cell security profiles and CLI hardening profiles are neutral,
  with explicit regression coverage for legacy persisted profile acceptance;
- active database/runtime documentation and production examples use generic
  application, protected-data, tenant, and workspace terminology;
- product UI/browser/packaging/storage design records were retained in
  `../bicdb-integrations`, while the private fleet roadmap was retained in
  `../bicdb-platform`; and
- the boundary gate now rejects new product/vertical names in external Rust
  symbols and the canonical public docs.

The exact compatibility rules are in
[`legacy-application-compatibility.md`](legacy-application-compatibility.md).

Validation:

| Command | Result |
|---|---|
| `cargo check --workspace --all-targets --locked` | Pass |
| trusted neutral/legacy SQL-setting regression | Pass |
| neutral/legacy Cell-profile regression | Pass |
| `scripts/check-open-core-boundary.sh` | Pass |
| `cargo deny check licenses` and `scripts/check-licenses.sh` | Pass |
| `scripts/validate-production-config-examples.sh` | Pass |
| `cargo test --workspace --locked` in `bicdb-integrations` | Pass: 50 tests |
| `scripts/validate-public-api-boundary.sh` in `bicdb-platform` | Pass: 2 tests |

## Stage 8 Apache-2.0 release readiness

The superseded Apache-release assessment is archived privately. This stage
records historical work; it does not describe the current license or a current
publication blocker. See the [current licensing guide](licensing-faq.md).

Hygiene discovered and completed during the evidence audit:

- removed the extracted `carrier-broker` cargo-deny clarification;
- corrected a stale patch-specific phrase inside the active `1.0.x-beta`
  license notice and kept the npm mirror byte-identical;
- selected the actually bundled MIT notice for the vendored browser WASI shim;
- made the sample gRPC protobuf package and trace metadata BicDB-neutral;
- extended the architecture gate to cover protobuf, WIT and serialized example
  contracts in addition to Rust symbols and canonical docs;
- removed a dead reference to an extracted product fixture; and
- repaired the licensing validator so an intentionally absent `products/`
  directory does not expand into a false manifest path.

No license grant changed during Stage 8. The subsequent `1.0.363-beta` release
changed the active release line to Apache-2.0; older tagged releases retain the
terms under which they shipped.

Validation:

| Command | Result |
|---|---|
| `cargo check --workspace --all-targets --locked` | Pass |
| `cargo test -p bicdb-provider-grpc --locked` | Pass |
| `cargo deny check licenses` and `scripts/check-licenses.sh` | Pass |
| `scripts/validate-licensing.sh` | Pass |
| `scripts/check-open-core-boundary.sh` | Pass |
| `cargo fmt --all -- --check` and `git diff --check` | Pass |

## Stage 9 final report and campaign close

[`open-core-extraction-final.md`](open-core-extraction-final.md) records the
final repository topology, moved implementations, stable interfaces, coupling
audit, legacy-vocabulary containment, validation matrix, Apache blockers,
publication-risk analysis, and exact pre-Apache steps.

Final cleanup completed during the closeout:

- fixed all four JSON/composite defects recorded in the Stage 0 baseline;
- made PostgreSQL `json` field ordering explicit and JSONB rendering/key
  identity independent of `serde_json` feature unification;
- changed canonical application telemetry, lifecycle, encryption-request, JWT
  and named-blob metadata to neutral BicDB namespaces while preserving legacy
  decode/fallback paths;
- moved the remaining product-shaped production runbook into
  `bicdb-integrations`; and
- extended the architecture gate over canonical root documents and runtime
  emitters.

Validation:

| Command | Result |
|---|---|
| `cargo build -p bicdb-core --example chaos_child --locked` | Pass |
| `cargo test --workspace --no-fail-fast --locked` | Pass: 2,986 passed, 0 failed, 12 ignored |
| `cargo check --workspace --all-targets --locked` | Pass |
| `scripts/check-open-core-boundary.sh` | Pass |
| `scripts/validate-community-capabilities.sh` | Pass: 103 passed, 0 failed |
| license, configuration, format and diff gates | Pass |
| `bicdb-platform` public-API validation | Pass: 2 passed, 0 failed; no patches |
| `bicdb-integrations` workspace tests | Pass: 50 passed, 0 failed |

## Stop conditions

Work pauses only for:

- a data-loss or behavior-regression risk that cannot be resolved safely;
- unresolved ownership or licensing rights;
- a genuinely ambiguous high-value public/commercial implementation boundary;
- an unresolved major test regression.
