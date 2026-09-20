# BicDB Open-Core Extraction Final Report

Status: **Stages 0–9 complete**

Final release: `1.0.362-beta`

Baseline: `86bf0330a1b587a2e677242c54b2e9b787f113e0`

This historical report closes the repository-separation campaign; the campaign
itself did not relicense BicDB. A later owner decision prospectively licensed
`1.0.363-beta` and subsequent releases under Apache-2.0. Older tagged releases
retain the terms under which they shipped. See
[`licensing-faq.md`](licensing-faq.md).

## 1. Final repository topology

```text
bicdb/                         canonical database and application substrate
  public core mechanisms      no dependency on either sibling
  public controller APIs      usable by unrelated implementations
  manual/reference operation  complete without commercial code

../bicdb-platform/             private commercial control plane
  fleet-controller            -> BicDB public Rust APIs
  commercial roadmap          no database engine copy

../bicdb-integrations/         product and vertical staging repository
  Carrier Broker              -> BicDB public APIs
  HL7/healthcare providers    -> BicDB public provider/application APIs
  healthcare projections      -> BicDB public projection APIs
  WalkNorth packaging         -> BicDB CLI/pgwire/Cell interfaces
  product fixtures/runbooks   no database engine copy
```

The enforced dependency direction is:

```text
bicdb-platform      -> bicdb public APIs
bicdb-integrations  -> bicdb public APIs
bicdb               -X-> either sibling
```

Public BicDB retains storage, WAL, MVCC, transactions, SQL, pgwire, RESP,
search, analytics, application/WASM runtimes, authentication, RBAC/RLS, TLS,
encryption, backup/restore/PITR, integrity controls, Cells, signed formats,
grants, audit/metrics, replication, manual recovery/failover, local sync, and
open controller/provider protocols. It has no artificial resource or tenant
limit and no proprietary license check in a data or safety path.

## 2. Components moved

### Commercial control plane

The automatic fleet placement/retry policy and its pgwire host-service
activation moved from BicDB into
`../bicdb-platform/crates/fleet-controller`. The public repository retains the
topology, relocation, validation, transport, fencing, state-machine, receipt,
metrics, cancellation, manual convergence, and policy-injection mechanisms.

No empty private crates were invented. Automated HA/DR, compliance, device,
registry, and distributed-scheduler services named in the product boundary do
not yet exist as separable implementations; their security-relevant formats
and deterministic mechanisms remain public, while future fleet automation
must be implemented outside BicDB through the documented contracts.

### Products and verticals

The following moved into `../bicdb-integrations`:

| Component | Destination |
|---|---|
| Carrier Broker plus AMQP, MQTT, Kafka, HTTP and gRPC adapters | `crates/carrier-broker` |
| HL7 provider | `crates/bicdb-provider-hl7` |
| healthcare terminology/FHIR built-ins | `crates/bicdb-provider-healthcare` |
| patient, appointment and clinical graph projections | `crates/bicdb-healthcare-projections` |
| WalkNorth desktop/package integration | `products/bicdb-macos` |
| ERP/EHR/WalkNorth fixtures, migrations, benchmarks, scripts and reports | `fixtures/`, `tests/`, `scripts/`, `reports/`, `archive/` |
| product UI, packaging, storage and scale-out design material | `docs/archive/` and platform docs as appropriate |
| ERP-shaped production runbook | `docs/legacy-erp-production-operations.md` |

Generic event/graph projection interfaces, the provider SDK, application
runtime, broker-facing database mechanisms, pgwire, extension ABI and secure
Cell runtime remain public.

## 3. Important interfaces introduced or stabilized

| Boundary | Public contract |
|---|---|
| placement policy | `ClusterPlacementPlanner` |
| relocation execution | `ClusterRelocationDriver`, `ClusterRelocationTransport`, typed plans and receipts |
| server integration | `PgWireHostService`, `PgWireHostContext`, `start_distribution_controller`, accounted background tasks |
| community server | `ManualDistributionHostService` |
| schema rollout | signed bundles, compatibility fingerprints, stage/activate/finalize transports and receipts |
| backup/recovery | backup barriers, manifests, certificates, restore admission/readiness and manual PITR operations |
| HA/fencing | Cell writer epochs, leases, fence evidence, failover actions and `CellFailoverSupervisor` |
| anti-entropy | digest/repair transports, bounded scans, certificates and local safety loop |
| distributed execution | shard executor/commit traits, plans/fragments, cancellation, accounting and bounded scatter/gather |
| Cells and keys | `CellManifest`, `VerifiedCellManifest`, `CellKeyProvider`, scoped key leases and admission evidence |
| grants/devices | signed recipient grants, revocation evidence, device enrollment/working-set/retirement formats |
| local sync | `SyncEndpoint`, `SyncCoordinator`, signed bundles, checkpoints, vectors and strict import |
| applications | neutral `Application*` ABI, package/signature verifier, providers and component scope/data class |
| application lifecycle | App Root release, transparency, approval, activation and convergence formats in `bicdb-fleet` |

The complete third-party guide is
[`public-controller-apis.md`](public-controller-apis.md). Controllers propose
work; BicDB independently verifies identity, signatures, generations, epochs,
fences, resource limits, transitions and receipts.

## 4. Remaining public-to-private coupling

**Zero code or manifest dependencies.** The public workspace contains no path,
Git, registry, feature, patch, build-script or runtime dependency on
`bicdb-platform` or `bicdb-integrations`. The boundary check rejects Cargo
paths outside the checkout and references to extracted controller types.

The sibling repositories use local path dependencies only for development,
with matching exact version constraints. Those paths point from sibling to
public BicDB and can be replaced by versioned published crates. Neither
sibling patches or copies the engine.

Documentation may name the siblings to explain the boundary. That is not a
runtime dependency. Product compatibility strings remain inside explicit
decoders, described below.

## 5. Remaining Carrier, WalkNorth, clinical, EHR or ERP material

There is no public type, canonical manifest field, canonical ABI/protobuf/WIT
contract, newly emitted runtime metadata, canonical operator documentation, or
built-in vertical projection that requires one of those products or
industries.

The remaining occurrences fall into four quarantined classes:

1. historical release/audit/performance records that must not be rewritten;
2. compatibility fixtures and read aliases for previously signed or persisted
   package fields, SQL GUC/schema names, encryption requests, JWT headers and
   blob metadata;
3. private/internal helper or regression-test identifiers that do not cross a
   public Rust or serialized boundary; and
4. current license text naming WalkNorth, Inc. as a licensor.

Canonical output now uses `bicdb.*` telemetry attributes,
`x-bicdb-lifecycle`, `bicdb-aes-256-gcm-*`, `bicdb_key_version`, and
`bicdb.logical_key`. Old durable inputs remain accepted and receive identical
security checks. The precise containment/removal policy is in
[`legacy-application-compatibility.md`](legacy-application-compatibility.md).
The architecture gate now scans public Rust symbols, protobuf/WIT/example
contracts, canonical docs, and canonical runtime-emitter patterns.

## 6. Complete build and test status

Final local validation on `1.0.362-beta`:

| Validation | Result |
|---|---|
| `cargo build -p bicdb-core --example chaos_child --locked` | Pass |
| `cargo test --workspace --no-fail-fast --locked` | Pass: 2,986 passed, 0 failed, 12 ignored |
| `cargo check --workspace --all-targets --locked` | Pass |
| four Stage 0 JSON/composite regressions under the hostile feature graph | Pass |
| `scripts/check-open-core-boundary.sh` | Pass |
| `scripts/validate-community-capabilities.sh` | Pass: 103 passed, 0 failed |
| `cargo deny check licenses` / `scripts/check-licenses.sh` | Pass |
| `scripts/validate-licensing.sh` | Pass |
| `scripts/validate-production-config-examples.sh` | Pass |
| formatting and `git diff --check` | Pass |
| `bicdb-platform/scripts/validate-public-api-boundary.sh` | Pass: 2 passed, 0 failed; no patches |
| `cargo test --workspace --locked` in `bicdb-integrations` | Pass: 50 passed, 0 failed |

The four defects recorded in the Stage 0 baseline are closed. Their shared
root cause was accidental dependence on workspace-wide activation of
`serde_json/preserve_order`: JSONB display and primary-key identity changed
with Cargo feature unification. SQL now requests ordered maps explicitly for
PostgreSQL `json` field-order semantics while its JSONB renderer and identity
encoder sort object keys explicitly. Tests force the formerly hostile graph.

Warnings already present in the workspace remain warnings; there are no known
test failures in the final matrix.

## 7. Apache-release blockers recorded at campaign close

At campaign close, BicDB was assessed as **not ready to relicense**. The
recorded blockers were:

- chain of title and explicit Apache authority for every copyright holder;
- employer/contractor and contributor-identity reconciliation;
- human review and provenance attestation for AI-assisted work;
- exact provenance/licensing/regeneration records for vendored,
  PostgreSQL-derived, fixture, ABI and report material;
- feature/target/package-specific dependency inventories, SBOMs and notices;
- counsel-approved Apache `LICENSE`/`NOTICE`, SPDX, patent and BUSL/AGPL
  transition treatment;
- trademark policy and organization-controlled crate/npm namespaces;
- CLA/DCO and contribution/provenance policy; and
- a signed, reproducible, two-person release pipeline with secret/private-data
  scans and a clean-room public-only rehearsal.

This section records an earlier extraction campaign, not the current licensing
status. The superseded release assessment is archived privately. See the
[current licensing guide](licensing-faq.md) and [scope](../LICENSE-SCOPE.md).

## 8. Private material that may later belong in public

No current `bicdb-platform` implementation needs to move back. Its fleet
controller contains placement/retry policy and activation automation, while
all verification and safe execution stay public.

Future review should move any newly written controller code back to BicDB if it
becomes necessary for format interoperability, independent verification,
manual recovery, deterministic fencing, data access, security correctness, or
a genuinely usable reference implementation. A product adapter may likewise
become a separate public extension if it becomes industry-neutral; it should
not be moved into the engine merely to publish it.

## 9. Public material with commercial value

Cell isolation, signed application/fleet formats, cryptographic grants,
deterministic failover, anti-entropy, replication, application hosting,
distributed execution protocols, backup/PITR and controller interfaces all
have substantial commercial value. Releasing them can help competitors.

They should nevertheless remain public because hiding them would weaken basic
security/correctness, make data recovery or independent operation dependent on
a vendor, or sabotage third-party controllers. The commercial moat is the
organizational-scale automation: placement, rollout, geo-HA/DR, recovery
drills, enterprise KMS/identity, managed devices/relays, compliance evidence,
private registry approvals, fleet observability, elastic scheduling, certified
builds and support.

The most publication-sensitive public area is the neutral application ABI and
distributed protocol suite because it exposes a broad platform design. It is
not a hidden commercial implementation, however, and removing it would violate
the interoperability boundary. Its provenance must be reviewed before the
Apache tag rather than moving it behind a paywall.

## 10. Steps proposed at campaign close

1. Freeze an exact candidate commit; do not publish or add Apache headers yet.
2. Assign owners and collect signed exit evidence for AR-01 through AR-09.
3. Complete human provenance review of the candidate tree, including every
   AI-assisted, vendored, generated, derived and fixture artifact.
4. Obtain counsel approval for chain of title, Apache/BUSL/AGPL transition,
   patents, `LICENSE`, `NOTICE`, CLA/DCO and trademark policy.
5. Generate and review per-target/per-feature Rust and npm dependency reports,
   SBOMs and third-party notices.
6. Add file-level SPDX/REUSE metadata without overwriting third-party or
   generated provenance.
7. Put crate/npm/signing identities under organization ownership with hardware
   keys and two recoverable administrators.
8. Build the protected two-person release workflow: pinned actions,
   deterministic source archive, clean-room build, secret/customer-data scan,
   reproducibility check, signatures, attestations and checksums.
9. Rehearse crate, npm, container and source publication from a checkout where
   both private siblings are physically absent; rerun every Stage 6 capability
   gate and the complete workspace suite.
10. Have legal and engineering approve the exact tree and tag as a distinct,
    irreversible relicensing event; only then replace licensing files, apply
    SPDX identifiers and publish the first Apache-2.0 tag.

## Historical campaign verdict

Repository extraction: **COMPLETE**.

Public/community independence: **PROVEN**.

Commercial/product dependency direction: **CLEAN**.

Apache-2.0 publication at campaign close: **BLOCKED pending the documented
legal and provenance gates**. This verdict was superseded by the later owner
authorization for `1.0.363-beta` and subsequent releases.
