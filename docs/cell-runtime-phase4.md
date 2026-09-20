# Phase 4 App Root and fleet lifecycle

Introduced in BicDB 1.0.321-beta, Phase 4 makes the App Root and fleet
lifecycle callable without making the fleet controller a database or key
authority. It is industry-neutral and does not admit regulated data.

## Authority boundary

`bicdb-fleet` intentionally depends only on serialization, hashing, signatures,
semantic versions, UUIDs, and the filesystem. It has no dependency on:

- `bicdb-core` or storage engines;
- `bicdb-sql`;
- `bicdb-app-runtime`;
- `bicdb-cell` or a Cell key provider;
- pgwire, RESP, sync, extensions, or providers.

The fleet layer can store opaque bytes and authorize an exact transition. It
cannot open a database, run a migration, decrypt a Cell, or select a Cell key.

## Immutable App Root artifacts

`ImmutableArtifactRegistry` publishes bytes at their SHA-256 address. It:

- accepts no mutable version aliases;
- uses create-new staging plus a hard-link no-overwrite commit;
- verifies an existing digest path before treating a retry as idempotent;
- rejects symlink roots, non-regular files, size changes, and digest changes;
- fsyncs artifact and shard metadata.

Version labels remain signed release metadata. The digest is executable
identity.

## Five independent authority roles

A `FleetTrustPolicy` assigns each Ed25519 key exactly one role:

```text
publisher     signs the immutable release definition
security      independently approves release and activation policy
builder       attests reproducible source/recipe/output identity
transparency  signs the observed append-only log checkpoint
rollout       authorizes bounded cohorts and exact Cell tickets
```

The same public key may not occupy two roles. The current safe policy requires
at least one publisher, one security authority, two independent builders, one
transparency authority, and one rollout authority. A release requires the
publisher, security, and builder thresholds; later documents retain separate
security and operational approvals.

This separates publication from activation. A compromised CI credential,
registry, publisher, or CellAgent cannot independently create an accepted
fleet transition.

## Release and provenance

A signed fleet release binds:

- every application root/name/version and monotonic release sequence;
- exact package bytes and independently identified backend, frontend,
  migration, and policy components;
- scope, data class, schema generation, record-format range, and BicDB runtime
  compatibility;
- source tree, dependency lock, SBOM, provenance, and build recipe digests;
- at least two independent builder witnesses reproducing each package digest;
- an explicit rollback deadline.

Cell package signatures are still checked by Phase 3. Fleet authorization does
not replace package verification or Cell-local authority.

## Transparency

The inclusion proof starts at the exact release entry and supplies each
successor through a SHA-256 predecessor chain to a signed checkpoint head. The
checkpoint requires transparency and security approval and binds log identity,
size, head digest, and issue time. Proof length is bounded.

External monitors and production log operation remain deployment evidence;
the implementation does not claim a globally witnessed log merely because it
can verify one.

## Bounded cohort rollout

A rollout contains explicit Cell UUIDs. Wildcards and an `all Cells` operation
do not exist in the document model. Each Cell appears once, and every cohort
declares:

- a not-before time;
- observation duration;
- maximum parallel activations;
- an independently signed prior-cohort decision and metrics digest before the
  next cohort may proceed.

A Cell ticket then binds one Cell UUID, release digest, rollout/cohort,
immediately previous manifest generation/digest, exact next manifest
generation/digest, and a short validity window. Rollout and security authorities
both approve it.

## Cell startup order

For a Phase-4 profile, startup is:

```text
signed CellManifest and volume identity
  -> local monotonic manifest check
  -> exact running binary check
  -> manifest-pinned fleet policy
  -> release threshold and reproducible builds
  -> transparency checkpoint
  -> bounded rollout and prior-cohort gates
  -> exact per-Cell activation ticket
  -> signed predecessor and schema/runtime compatibility
  -> minimum-safe release policy
  -> only now request the Cell key
  -> open encrypted storage and activate packages/migrations
  -> flush database
  -> append/fsync convergence receipt
  -> anchor receipt head in monotonic Cell state
```

Non-initial transitions require the signed predecessor manifest. Compatibility
therefore derives from Cell-authorized state, not from a fleet assertion about
what used to run.

## Convergence and migrations

The App Root distributes one signed migration definition. Each Cell executes
it once inside its own runtime through the existing atomic stage/activation and
schema compensation path. A fleet of 1,000 Cells therefore means 1,000
automated, isolated executions—not 1,000 human migration deployments and not
one cross-Cell database session.

Successful activation appends a bounded deterministic receipt containing the
Cell, release, cohort, old/new manifest, old/new application state, timing, and
rollback window. Receipts are create-new, fsynced, hash chained, and their head
is recorded beside the monotonic manifest state. A crash after convergence but
before state advancement is idempotently recoverable.

Destructive synchronous fleet migrations remain prohibited by the architecture.
Applications should use add, dual-read, dual-write, background conversion,
verification, and retirement within signed compatibility windows.

## Interfaces

The reduced Cell binary accepts:

```text
--fleet-trust-policy /run/bicdb/fleet-policy.json
--fleet-activation-bundle /run/bicdb/activation.json
--previous-manifest /run/bicdb/previous-cell.cose   # generation > 1
```

The `BICDB_RUNTIME_MODE=cell` launcher maps the equivalent environment paths:

```text
BICDB_CELL_FLEET_TRUST_POLICY
BICDB_CELL_FLEET_ACTIVATION_BUNDLE
BICDB_CELL_PREVIOUS_MANIFEST
```

The environment selects files; it grants no authority. Every meaningful value
is signed and/or pinned by the CellManifest.

## Admission status

`regulated_data_admitted` remains `false`. Phase 4 closes the App Root/fleet
lifecycle implementation item only. Cell-scoped HA, device replicas,
cross-Cell grants, attested deployment enforcement, independent review, and
the remaining production evidence gates are still absent.
