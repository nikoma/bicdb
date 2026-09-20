# BicDB Cell/Application Architecture

Status: **normative target; Phases 0–8 implemented, regulated-data admission
closed**

Date: 2026-08-26

Scope: BicDB application packaging and execution, isolation, identity, storage,
keys, replication, sharing, device replicas, and fleet operation. Regulated
data is the highest-risk motivating threat profile, not an application domain
baked into BicDB.

This document defines the target architecture. It is deliberately not a claim
that the current BicDB release is suitable for a global regulated data workload. A feature
described here is not shipped until its implementation and admission evidence
are linked from this document.

The callable 1.0.314-beta Phase-1 boundary is documented in
[`cell-runtime-phase1.md`](cell-runtime-phase1.md). The industry-agnostic,
still non-admitted 1.0.316-beta application host is documented in
[`cell-application-host-preview.md`](cell-application-host-preview.md).
The callable, still non-admitted 1.0.317-beta cryptographic boundary is
documented in [`cell-runtime-phase2.md`](cell-runtime-phase2.md).
The callable, still non-admitted 1.0.320-beta Cell-native application boundary
is documented in [`cell-runtime-phase3.md`](cell-runtime-phase3.md).
The callable, still non-admitted Phase 4 through Phase 7 boundaries are
documented in [`cell-runtime-phase4.md`](cell-runtime-phase4.md),
[`cell-runtime-phase5.md`](cell-runtime-phase5.md),
[`cell-runtime-phase6.md`](cell-runtime-phase6.md), and
[`cell-runtime-phase7.md`](cell-runtime-phase7.md). The callable Phase 8
hardened-fleet evidence verifier is documented in
[`cell-runtime-phase8.md`](cell-runtime-phase8.md). It completes the software
roadmap without claiming that external review and production-attestation
evidence already exist.

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**,
and **MAY** are normative.

## 1. Decision

BicDB separates four scopes that are often accidentally collapsed:

1. **Application definition** — signed code, schemas, migrations, policies,
   assets, and compatibility metadata.
2. **Application execution** — the process or microVM in which that definition
   receives authority.
3. **Tenant data** — records owned by a person, practice, organization, device, or
   another administrative entity.
4. **Security boundary** — the maximum confidentiality and blast-radius domain
   accepted by policy.

These scopes MAY align, but BicDB MUST NOT assume they are identical.

The platform architecture is:

```text
                         BicDB platform
                               │
              ┌────────────────┴────────────────┐
              │                                 │
       Global control plane              Application Root
       no regulated payload                signed definitions
              │                                 │
              └────────────────┬────────────────┘
                               │
                          CellAgent fleet
                               │
          ┌────────────────────┼────────────────────┐
          │                    │                    │
          ▼                    ▼                    ▼
          Cell A               Cell B               Cell C
     runtime + app A      runtime + app B      runtime + app C
     key + storage A      key + storage B      key + storage C
     protected data A       protected data B       protected data C
```

The same application definition can be instantiated in many cells. Its code is
global; its runtime authority is local.

The design borrows the useful lifecycle distinction in Oracle Application
Containers: an application root holds a named, versioned master application
definition, and application PDBs synchronize to application versions or
patches. BicDB does **not** copy Oracle's process boundary. Oracle documents
that a PDB appears independent to applications while the CDB remains the
database from the operating system's perspective. Hardened BicDB cells instead
use separate processes or microVMs because BicDB cannot yet justify trusting
one address space with the aggregate regulated data of the fleet.

## 2. Security objective

The governing invariant is:

> Compromise at one layer must not automatically grant the authority of the
> layer above it.

Its fleet-wide counterpart is:

> **No globally scalable capability may simultaneously possess global
> distribution authority and regulated data-decryption authority.**

Isolation is both horizontal and vertical. Cells isolate neighboring authority
domains horizontally. Separate build, frontend, orchestration, and key
authorities prevent a compromise above the cells from combining deployment,
execution, and decryption power vertically.

### 2.1 Five independent trust domains

BicDB recognizes five crown-jewel domains. None implicitly trusts another:

1. **Cell runtime domain** — one cell's regulated data, keys, storage, replication, local
   application execution, and local authorization.
2. **Application release domain** — source, CI/build systems, backend packages,
   migrations, signing, provenance, and App Root publication. It can distribute
   code but receives no cell key or regulated-workload payload.
3. **Frontend release domain** — browser/desktop code, static assets, service
   workers, CSP, signing, provenance, and rollout. It is separate because code
   executing in the user's protected-data origin can read decrypted regulated data through
   legitimate APIs.
4. **CellAgent/orchestration domain** — placement, lifecycle, routing,
   activation, replication, snapshots, and resource management without regulated data
   decryption.
5. **KMS/recovery domain** — cell-scoped cryptographic release, rotation,
   escrow, and recovery. It neither publishes applications nor directs fleet
   rollout.

The domains cooperate through signed, bounded statements. They do not share a
super-credential. In particular:

```text
App Root       may publish digest D;        cannot unwrap Cell A
Rollout policy may authorize D for cohort;  cannot decrypt or execute it
CellAgent      may start/activate Cell A;   cannot unwrap or query it
KMS            may release A's capability; cannot choose A's application
Cell runtime   may decrypt and execute A;   cannot enumerate or open B
Frontend       may render for user/device;  has no reusable fleet credential
```

The target consequences are:

| Compromise | Maximum intended consequence |
| --- | --- |
| One user or device | That principal's authorized working set and pending actions |
| One application request | The cell and authority represented by that request |
| One cell application runtime | One cell, never neighboring cells or the App Root |
| One database/RLS/kernel failure | One cell's data |
| One CellAgent | Fleet availability and metadata, not regulated data decryption |
| One storage volume | Ciphertext for one cell |
| One replication credential | One replica role in one cell and writer epoch |
| One host | Cells placed on that host; fewer with attested confidential compute |
| One application-registry node | Artifact availability; no ability to forge an accepted release |
| One release signer | No accepted regulated-workload release without independent approvals |
| One control-plane service | Routing or availability impact; no regulated-workload payload or cell session |

The corresponding crown-jewel containment contract is:

| Crown jewel compromised | Maximum acceptable consequence |
| --- | --- |
| One CellRuntime | One cell |
| CellAgent | Infrastructure control and denial of service; no regulated data decryption |
| App Root storage/service | Cannot forge or silently activate a release globally |
| CI/build system | Cannot produce an accepted regulated-workload release alone |
| Frontend pipeline | Cannot bypass independent signing and staged activation |
| One KMS workload identity | Only the explicitly scoped cell/key operations |
| Operator account | No arbitrary regulated data-key unwrap or regulated-workload session |
| One release signer | Cannot activate a release alone |

This is an objective, not a proof. Section 20 attacks every row whose boundary
can collapse under a naive implementation.

## 3. Terminology

**Application Root**
: A registry containing no tenant payload: only immutable, signed application definitions, dependency
  locks, migrations, compatibility ranges, provenance, SBOMs, and release
  transitions. It is a distribution authority, not a database superuser.

**Cell**
: A confidentiality and blast-radius boundary. A cell contains exactly one
  cell authority, storage identity, key authority, writer lineage, application
  set, and regulated-workload authorization domain.

**Tenant**
: An administrative or commercial owner. A tenant can own multiple cells, and
  a cell can contain multiple users who intentionally share its confidentiality
  boundary. `tenant_id` is not a substitute for `CellId`.

**CellId**
: A permanent, random, non-semantic identifier. It MUST NOT encode a principal,
  organization, geography, specialty, customer number, or route. Renaming or moving a
  organization never changes its `CellId`.

**Route alias**
: A replaceable, opaque network locator for a cell. It is not an authority and
  MUST NOT be used as cryptographic identity.

**CellManifest**
: The signed, monotonic statement binding one `CellId` to its storage,
  application set, key authority, replication group, policy, and jurisdiction.

**CellRuntime**
: The hardened, single-cell construction path. It owns one `CellAuthority`, one
  BicDB handle, one application runtime, cell-local listeners and jobs, and no
  cross-cell capability.

**CellAgent**
: A keyless host supervisor that schedules, starts, stops, routes, snapshots,
  upgrades, and observes cells without receiving regulated-workload-data authority.

**Device replica**
: A separately keyed, filtered edge replica subordinate to a parent cell. It is
  not an offline copy of the parent cell's root key or unrestricted database.

**Grant object**
: A bounded, signed, encrypted transfer from one cell to another. It copies
  explicit authority; it never opens a connection from the recipient cell into
  the source cell.

## 4. Non-negotiable invariants

### 4.1 One runtime, one cell

A regulated-workload runtime MUST be constructed with exactly one cell authority. It
MUST have no database selector, cluster database manager, global key provider,
cross-cell handle, bootstrap superuser, development sync server, arbitrary
filesystem opener, or native extension loader.

The desired response to `OPEN DATABASE cell_b` is not a permission error.
There is no such operation in the regulated-workload runtime's type or protocol surface.

### 4.2 Operational authority is not data authority

A CellAgent MAY start, stop, move, replicate, snapshot, or replace a cell. It
MUST NOT possess a credential that can unwrap the cell's KEK or read decrypted
memory. Support and infrastructure administrators MUST NOT inherit regulated data access
from their operational role.

### 4.3 Identity is conjunctive

Before recovery, page decryption, WAL replay, migration, application startup,
or replication apply, all available identities MUST agree:

```text
requested CellId
    = signed manifest CellId
    = durable database CellId
    = mounted-volume identity
    = workload/KMS authorization CellId
    = replication-group CellId
    = backup lineage CellId
```

Missing identity is not compatibility. It is an enrollment or migration state
that MUST be handled explicitly outside regulated-workload serving.

### 4.4 Cell identity is below SQL

`CellId` MUST be immutable storage metadata and cryptographic associated data.
It MUST NOT be a mutable row column, GUC, RLS expression, database name, URL,
or application-supplied tenant claim.

### 4.5 RLS is an inner wall

Cells do not replace user, role, device, purpose-of-use, consent, or row-level
authorization. Within a organization cell, BicDB MUST continue to enforce RLS and
least privilege among principals, operators, billing staff, labs, data subjects, jobs,
and applications. RLS is the third wall, not the first.

### 4.6 Global code does not imply global execution

A globally defined regulated data-capable application MUST execute independently inside
each cell. Global systems MAY receive explicitly authorized, minimized outputs;
they MUST NOT acquire an ambient query path across cells.

### 4.7 No hidden regulated data concentrator

The global router MUST NOT terminate regulated data-bearing TLS, maintain regulated-workload
connection pools, produce regulated data-bearing logs, or proxy decrypted bodies through a
globally privileged application process. TLS for regulated-workload traffic terminates
inside the selected cell or a component that is itself confined to that cell.

### 4.8 Fail closed before mutation

Identity, signature, generation, key, storage-mode, and compatibility checks
MUST complete before startup mutates the database directory. A refused open
must not create metadata that makes the next open appear enrolled.

### 4.9 No dishonest offline revocation

Once an authorized device or recipient has decrypted data, the source cannot
cryptographically prove deletion of every plaintext copy. Revocation stops new
keys, new data, future access, and server acceptance; it MUST NOT be described
as retroactive erasure.

### 4.10 No last-writer-wins regulated-workload truth

Authoritative regulated-workload data MUST NOT merge by device wall-clock LWW. Offline
changes are signed proposed amendments with causal provenance. The authority
cell accepts, rejects, reconciles, or records a conflict.

## 5. Cell is a policy-selected boundary, not a synonym for principal

The default boundary for maximum isolation is one principal or deliberately
sharing practice per cell. BicDB itself MUST model the boundary generically:

| Deployment | Possible cell policy |
| --- | --- |
| Solo principal | One principal cell |
| Small practice with shared care | One practice cell |
| Large organization | Multiple department, service-line, or high-risk cells |
| Research organization | Separate regulated-workload and research cells |
| High-security customer | Principal-level or data subject-cohort cells |
| Desktop | One filtered device replica subordinate to a cell |

The security owner chooses how much data one kernel/process compromise may
expose. A practice-wide cell is an explicit acceptance of a practice-wide blast
radius, not an optimization BicDB chooses silently.

## 6. Application scope and data class

Every executable application component MUST declare both an execution scope and
a data class. Labels alone provide no security; the compiler, package verifier,
launcher, capability provider, network policy, and runtime types MUST all
enforce them.

```rust
enum ExecutionScope {
    Global,
    Cell,
    Device,
}

enum DataClass {
    Public,
    Operational,
    Sensitive,
    Regulated,
    RegulatedLocal,
}
```

The default policy matrix is:

| Scope | Public | Operational | Sensitive | Regulated | Regulated-local |
| --- | --- | --- | --- | --- | --- |
| Global | allowed | allowed with minimization | exceptional review | forbidden | forbidden |
| Cell | allowed | allowed | allowed | allowed | not applicable |
| Device | allowed | allowed | bounded | forbidden as authority | allowed working set |

`Global + Regulated` is a package-verification error, not a warning. A component that
needs global fleet results MUST define a cell-scoped producer and a separate
global consumer over a typed, minimized export contract.

One product can contain separately signed components:

```yaml
application: example-suite
components:
  directory:
    scope: global
    data_class: operational
  regulated-workload:
    scope: cell
    data_class: regulated
  desktop:
    scope: device
    data_class: regulated_local
```

Cross-scope calls MUST be explicit protocol calls. A component does not inherit
the caller's or callee's broader scope merely because both belong to the same
product.

## 7. Global control plane

The control plane MAY store:

- principal and device public identities;
- opaque cell membership and route aliases;
- cell placement, lifecycle, health, and desired application digest;
- one-time cell handoff state;
- grant routing envelopes whose regulated-workload payload is end-to-end encrypted;
- release rollout and non-regulated data compliance evidence.

It MUST NOT store:

- data subject records or regulated-workload search indexes;
- plaintext grant objects;
- cell data keys or a wildcard KMS unwrap credential;
- reusable cell sessions;
- decrypted backups or replication frames;
- logs containing request or response bodies.

Control-plane metadata is still sensitive. Cell membership, traffic volume,
organization specialty, device activity, and referral edges can reveal health facts.
It requires access control, retention limits, regional placement, audit, and
privacy review even when it contains no regulated-workload payload.

### 7.1 Authentication handoff

The global identity service establishes user and device identity and locates a
candidate cell. The cell makes the final decision.

A handoff assertion MUST be:

- short lived and single use;
- audience-bound to the permanent `CellId`, not only a route alias;
- bound to a device key by proof of possession;
- bound to a nonce and authentication strength;
- insufficient without a current cell-local membership/grant record.

After accepting the handoff, the cell issues a host-only, cell-bound session.
The global origin MUST NOT receive that session or read the regulated-workload response.
New device enrollment requires cell-local approval or a separately designed
recovery quorum; control of the global IdP alone MUST NOT enroll an arbitrary
device into every permitted cell.

## 8. Network and frontend boundary

The preferred browser flow is:

```text
login.example        non-regulated data identity and routing
        │
        └── one-time, device-bound redirect
                         │
                         ▼
r-8f3a.cells.example cell-local TLS, UI, API, and session
```

The route label is opaque and replaceable. It is not a principal name or
permanent `CellId`.

The shared ingress layer SHOULD route at L4/SNI and MUST NOT decrypt regulated-workload
HTTP. A cell-local certificate/private key terminates TLS. If a platform
component must inspect HTTP, it becomes part of that cell's boundary and must
be separately instantiated per cell.

The frontend is security-critical executable code. A compromised global
`application.js` can read and exfiltrate every active user's regulated data even if every backend
cell is perfect. Therefore:

- the cell SHOULD serve its own bootstrap HTML, CSP, service worker, and
  verified frontend artifact;
- regulated-workload pages MUST NOT execute third-party scripts;
- CSP SHOULD default to `connect-src 'self'` and narrow explicit cell-local
  endpoints;
- service-worker scope and cookies MUST be host-local;
- an external CDN MAY cache immutable bytes but the cell must pin and verify
  their digest before serving or executing them;
- frontend and backend artifacts belong to the same authorized release
  transition and transparency record.

Subresource Integrity reduces CDN substitution; it does not protect against a
maliciously signed application release.

## 9. Application Root

The App Root contains immutable artifacts, not live regulated-workload tables. It is the
BicDB analogue of the useful part of Oracle's master application definition.

An application release contains at least:

- application and component identities;
- semantic version and content digest;
- execution scope and data class;
- backend modules and frontend assets;
- schemas, additive migrations, and supported schema-generation range;
- RLS, capabilities, egress and cross-scope contracts;
- jobs, indexes, resource limits, and provider bindings;
- dependency lock, SBOM, provenance, and reproducible-build evidence;
- upgrade, rollback, and data-convergence rules;
- signatures and transparency-log inclusion proof.

Backend code, frontend code, and migrations may ship in one product release,
but each is an independently identified executable artifact with its own digest,
provenance, analysis evidence, and authorization. A migration is code running
against the most sensitive persisted state in the system; it is not harmless
schema metadata.

Migration execution is cell-local and receives a narrower capability than the
serving application: declared schema/data transforms, bounded work, migration
receipts, and no network, provider-secret, cross-cell, or arbitrary filesystem
access. The package declares the intended object/effect set so an unexpected
touch is a failed transition. A migration cannot inherit CellAgent or release
authority merely because those systems requested it.

An artifact is addressed by digest and never mutated in place. A version label
is human metadata; the digest is the executable identity.

### 9.1 Release authorization

The App Root is a delayed global regulated data authority: a malicious common application release
can read one cell at a time as the fleet activates it. Runtime isolation does
not solve this common-mode supply-chain risk.

No single online credential may authorize a regulated-workload release. Activation MUST
require independent trust domains, for example:

1. vendor/release signature;
2. security or regulated-workload-governance signature;
3. fleet-policy approval for a specific cohort;
4. cell-local manifest transition to the exact digest.

Publication and activation are different authorities:

```text
Release engineering   → may publish independently signed artifacts
Security/governance   → may approve exact digests and transition constraints
Fleet rollout policy  → may authorize an exact cohort and observation window
CellAgent             → may execute that approved transition for assigned cells
CellRuntime           → independently verifies every artifact and authorization
```

No CI, registry, signer, deployment account, or CellAgent credential can jump
directly from a new commit to all regulated-workload cells. Emergency activation uses the
same separation with a faster, predeclared quorum; it does not create a bypass.

Signing keys SHOULD be offline or hardware-backed and threshold controlled.
The registry is untrusted storage: compromising it may delete or withhold
artifacts, but cannot forge the signatures, provenance, transparency proof, or
cell transition needed for execution.

Rollouts MUST be cohort-based, observable, pausable, and reversible only within
the declared schema compatibility window:

```text
internal cells → security canaries → regulated-workload canaries → bounded cohorts → fleet
```

There is no "update all cells now" privilege for regulated data applications.

Backend, frontend, and migration cohorts MAY advance at different rates only
when the signed compatibility contract permits the exact combination. Otherwise
they activate atomically as one cell manifest transition.

### 9.2 Fleet convergence

A cell records current, desired, and minimum-compatible application state:

```text
example-suite
current_digest          sha256:...
desired_digest          sha256:...
catalog_generation      187
record_format_range     15..18
minimum_safe_digest     sha256:...
```

Inactive cells do not need to wake merely because a release exists. On wake,
the selected runtime must support the cell's current generation, then converge
through authorized transitions. The fleet service distributes one migration
definition; each cell executes and verifies it inside its own boundary.

Migrations SHOULD use:

```text
add → dual-read → dual-write → background-convert → verify → retire
```

Destructive, fleet-wide, synchronous migrations are prohibited for regulated-workload
data unless a separately reviewed emergency procedure justifies them.

## 10. Regulated-workload CellRuntime

The canonical interface is:

```bash
bicdb cell serve --manifest /run/bicdb/cell.cose
```

`BICDB_RUNTIME_MODE=cell` MAY be consumed by a container entrypoint or
CellAgent to select that command. The environment setting is not an authority,
identity, or security boundary. Raw keys MUST NOT be stored in environment
variables or command-line arguments.

The construction path is conceptually:

```rust
enum RuntimeMode {
    Development(DevelopmentConfig),
    GeneralServer(ServerConfig),
    HardenedCell(CellConfig),
}

struct CellRuntime {
    authority: CellAuthority,
    database: CellDatabase,
    application: CellApplicationRuntime,
    listeners: CellListeners,
    replication: CellReplication,
    audit: CellAudit,
}
```

This enum is a dispatch decision, not a bag of booleans. `HardenedCell` MUST
construct different types. Adding a capability to general BicDB must not make
it reachable from `CellRuntime` without an explicit type and security review.

The strongest release form is a separate `bicdb-cell` binary whose dependency
graph omits general-server cluster administration, arbitrary pgwire database
selection, shell/admin tooling, development sync, unsafe maintenance APIs, and
native code loading. A shared binary is acceptable initially only if tests and
link/dependency inspection prove the regulated-workload construction path cannot obtain
those capabilities.

Public pgwire is disabled. A loopback-only, cell-bound pgwire listener MAY be
introduced for carefully constrained compatibility after its identity,
authentication, RLS, spill, and admin surfaces pass the regulated-workload admission
gates. It is not part of the minimum regulated-workload runtime.

## 11. CellManifest and startup ceremony

The signed representation SHOULD use deterministic CBOR and COSE rather than
inventing JSON canonicalization and signature framing. Human-readable YAML may
be generated for inspection but is not signed input.

Illustrative contents:

```yaml
format: bicdb.cell-manifest/v1
cell_id: 018f7b30-4f4d-7b5c-a1f6-a183663e1240
manifest_generation: 92
previous_manifest_digest: sha256:...
jurisdiction: IN

storage:
  volume_id: vol-2d091...
  database_format: 2
  encryption_profile: cell-bound-v2

runtime:
  guest_image_digest: sha256:...
  bicdb_binary_digest: sha256:...
  isolation_profile: confidential-microvm-v1

applications:
  - root: example-root
    name: example-suite
    version: 4.19.3
    digest: sha256:7629...
    catalog_generation: 187

keys:
  authority: kms://india/cells/018f7b30...
  cell_kek_id: kek-018f7b30
  key_epoch: 31

replication:
  group_id: rg-018f7b30
  writer_epoch: 1042

network:
  cell_tls_identity_digest: sha256:...
  route_binding_key_digest: sha256:...

policy:
  security_profile: cell-native-strict-v1
  egress_policy_digest: sha256:...
  identity_policy_digest: sha256:...
  trusted_release_policy_digest: sha256:...
```

### 11.1 Manifest rules

- `CellId`, original lineage, and volume identity are immutable.
- Manifest generation increases monotonically and links to the previous digest.
- Security-relevant unknown fields or enum values cause refusal.
- A downgrade to an older generation is refused even when its signature is
  valid.
- Key, writer, authorization, and application epochs are distinct values.
- Moving a cell changes placement metadata, not `CellId`.
- Restoring a cell preserves its `CellId` and advances its recovery/writer
  epoch; cloning it requires an explicit rehome/rekey protocol and a new
  `CellId`.
- Manifest signing authority is separate from application release authority and
  KMS key authority.
- Runtime/guest image identity and the regulated-workload TLS/route-binding public
  identities are authorized transitions, not mutable DNS or host settings.

### 11.2 Boot order

The startup sequence is:

1. Parse a bounded envelope without following untrusted links or symlinks.
2. Verify canonical encoding, signatures, signer policy, expiry if any, and
   monotonic manifest generation.
3. Resolve the expected `CellId` and immutable volume identity.
4. Verify the mounted volume's identity without mutating it.
5. Attest the guest image/runtime and request only this cell's KEK capability.
6. Verify that KMS authorization, key epoch, and `CellId` match the manifest.
7. Verify durable database header, backup lineage, and replication identity.
8. Verify storage engine and complete encryption profile.
9. Open and recover storage with `CellId` in cryptographic associated data.
10. Fetch immutable application artifacts by digest; verify all release
    approvals and manifest pins.
11. Verify schema/catalog generation and complete authorized migrations.
12. Load cell-local authorization, listeners, jobs, and audit sinks.
13. Publish readiness and only then accept traffic.

Any mismatch produces a stable fatal error such as
`CELL_IDENTITY_MISMATCH`, includes no secret or regulated data, and leaves storage
byte-identical.

An empty microVM may be prewarmed. A microVM containing another cell's
decrypted pages, caches, keys, or application state MUST NOT be reassigned.
Decrypted RAM hibernation is forbidden unless memory state is encrypted to the
same cell identity and rollback protected.

## 12. Keys and encrypted storage

Each cell has an independently authorized KEK. The workload identity for Cell
A can request A's key and is structurally incapable of requesting B's key.
There is no global service credential with arbitrary `unwrap(cell_id)`.

KMS administration is itself a crown jewel. A cell-scoped runtime policy is
meaningless if one ordinary operator can rewrite that policy, mint arbitrary
workload identities, disable attestation, or invoke decrypt through a more
privileged role. KMS policy changes, recovery, key export where supported, and
attestation-policy changes require independent approval, immutable external
audit, and narrow emergency procedures. A managed KMS/HSM operator remains in
the trusted computing base unless an accepted confidential-compute or
multi-party construction removes that trust.

The key hierarchy separates purpose and rotation:

```text
Cell KEK (KMS/HSM; cell workload policy)
  ├─ page/segment DEKs
  ├─ WAL and replication DEKs
  ├─ temporary/spill DEKs
  ├─ index and search DEKs
  ├─ blob/attachment DEKs
  ├─ audit DEKs
  ├─ blind-index keys
  ├─ backup-specific DEKs
  └─ object DEKs used by explicit grants
```

Data keys are generated and used inside the cell, wrapped by the cell KEK, and
zeroized when no longer needed. Key version is authenticated metadata. Key
rotation never reinterprets old ciphertext under a new key.

All durable and transient regulated-workload representations MUST be encrypted and
authenticated, including:

- record pages and segments;
- page and transaction WAL;
- event history and MVCC prior versions;
- temporary files and SQL spills;
- indexes, dictionaries, vectors, statistics, and materialized views;
- attachment chunks and metadata;
- replication buffers, snapshots, and queues;
- backups, restore staging, crash dumps, diagnostics, and audit queues.

AEAD associated data includes at least `CellId`, storage/object kind, durable
object identity, format version, and key epoch. A page or frame copied from one
cell cannot authenticate in another.

Encryption at rest does not protect data while the cell is executing. A
malicious host can inspect ordinary VM memory. A claim that host operators
cannot access regulated data therefore requires confidential-VM hardware, measured boot,
remote attestation, KMS key release bound to measurements and `CellId`, and an
accepted residual side-channel model. A microVM without those properties is a
strong process-containment boundary, not protection from the host administrator.

Rollback protection requires a monotonic generation or durable root outside
the roll-backable volume. If a host can restore storage, manifest, audit, and
local counters together, signatures alone do not detect a valid old world.

## 13. Authorization inside a cell

The cell stores its own current memberships, roles, grants, device keys,
authorization epochs, break-glass policy, and revocations. Global identity is
evidence, not final authority.

Every request context includes, where applicable:

```text
CellId
user identity
device identity and proof
application/component digest
session and authorization epoch
authentication strength
purpose of use
roles/scopes
data subject/encounter grant context
trace and audit identity
deadline
```

Absence never means internal, bootstrap, owner, global, or bypass. The rules in
[`ambient-authority-bug-family.md`](ambient-authority-bug-family.md) are part of
this specification.

Regulated-workload tables are owned by a non-login storage owner. Application roles are
`NOSUPERUSER NOBYPASSRLS`; regulated data tables enable and force RLS. Table privileges,
RLS, mutation grants, application policies, and API authorization all apply.
No one of them is treated as sufficient alone.

Stored functions invoked by RLS, triggers, views, generated expressions, and
jobs are part of the trusted authorization kernel. Unsupported evaluation is a
startup/admission failure for an application that declares the feature. It is
never grounds for disabling RLS while retaining a regulated-workload-ready label.

Break-glass access is cell-local, purpose-bound, time-limited, strongly
authenticated, conspicuously audited, and preferably dual approved. There is no
fleet-wide break-glass regulated data role.

## 14. CellAgent and placement

The CellAgent receives narrow orchestration capabilities:

```text
start / stop / route / health / resource limit
attach the declared encrypted volume
request guest attestation
prefetch immutable application artifacts
initiate encrypted snapshot or replication transport
advance an approved rollout
```

It does not receive database handles, SQL, cell sessions, plaintext health
samples, raw crash dumps, or KMS unwrap permission.

Cells on one host MUST have separate processes or microVMs, OS identities,
mount namespaces, volumes, IPC namespaces, network policy, resource budgets,
logs, and workload identities. Process-only isolation is an intermediate tier;
microVM isolation is preferred for regulated data. The maximum cells per host is a stated
risk control because a host exploit can still create a correlated blast radius.

Shared caches, deduplication, diagnostics, metrics, and backup tooling must be
reviewed as cross-cell covert or direct data paths. Noisy-neighbor resource
exhaustion must stop at per-cell quotas.

## 15. Replication, HA, and disaster recovery

Replication is per cell:

```text
Cell A primary → Cell A replica
Cell B primary → Cell B replica
```

Every handshake, snapshot, commit frame, acknowledgement, and durable apply
record binds:

```text
CellId
replication group
source and destination replica identities
writer epoch
key epoch
commit sequence and predecessor
protocol/format version
payload digest/authentication tag
```

A valid frame for A is invalid for B. A frame from an old writer epoch is never
merged into the new writer history.

Promotion requires a cell-scoped lease and fencing protocol:

```text
{ CellId, writer_epoch, primary_replica, expiry, durable_commit_seq }
```

The supervisor fences the old writer, establishes the accepted durable
sequence, increments the epoch, promotes, and only then publishes routing. Time
alone is not sufficient fencing when clocks or partitions disagree.

The business chooses and measures RPO/RTO. Remote asynchronous replication can
lose committed data. A common regulated-workload topology is a local synchronous replica
for zero/low RPO plus a remote asynchronous disaster-recovery replica. A
cross-region synchronous policy is stronger and slower. Marketing and runbooks
must say which guarantee is active.

An idle application cell may sleep, but replication state still needs an active
trusted receiver or encrypted block/object replication. A dormant, months-stale
replica is not failover readiness.

Backups are cell-scoped, separately keyed, signed, immutable, and bound to
lineage and manifest generation. Restore drills run in a declared recovery
cell/enclave with equivalent controls. Operators cannot make a readable clone
by changing a database name or `CellId`.

## 16. Device replicas and offline work

A device replica has:

- parent `CellId` plus unique `DeviceReplicaId`;
- a device-only database key sealed by TPM/Secure Enclave/platform keystore;
- an authorized, filtered working set and expiry policy;
- device-bound user authentication and application digest;
- signed replication cursors and authorization epoch;
- bounded encrypted queues of proposed amendments.

The parent cell KEK and unrestricted database are never copied to the device.
The platform keystore protects a key at rest; it does not protect plaintext
from malware, browser compromise, screen capture, or a user session while the
device is unlocked.

Offline access is an explicit risk window. Policy defines the maximum offline
duration, data categories allowed offline, local reauthentication, retry and
conflict behavior, and remote-revocation limitations. Remote wipe is best
effort. Cryptographic erasure is achieved by destroying the device key when the
device cooperates; physical assurance depends on the platform.

Offline writes carry causal history and an immutable author/device signature.
The cloud cell validates current authorization, schema, constraints, and
conflicts before accepting them. Regulated-workload corrections append provenance rather
than silently overwriting the source fact.

## 17. Cross-cell sharing

Recipient applications never connect to source cells. A sharing flow is:

1. Cell A authorizes a precise object set, recipient, purpose, duration, and
   redisclosure policy.
2. A generates or selects object DEKs independent of A's cell KEK.
3. A produces a canonical, bounded, non-executable regulated-workload package with
   immutable source provenance.
4. Object keys are wrapped to Cell B's verified sharing public key; an HPKE
   construction is a suitable standard basis.
5. A signs the grant, object digests, policy, schema, and anti-replay data.
6. An opaque relay routes ciphertext without becoming a recipient.
7. B verifies sender identity, grant status, bounds, schema, signatures,
   recipient, replay state, and malware/content policy before import.
8. B stores received data as externally sourced regulated-workload evidence. B may append
   local annotations; it does not rewrite A's historical assertion.

Grant revocation stops future envelope/key delivery and updates. It cannot make
B forget data already decrypted or exported. If online-only revocation is
required, do not give B a durable offline key.

The import parser is a hostile-input boundary. Shared objects contain no WASM,
SQL migrations, provider configuration, templates with executable expressions,
or sender-selected filesystem/network references.

## 18. Global analytics and services

Global algorithms SHOULD execute locally when their inputs are regulated-workload:

```text
TerraGuard definition
       ├─ Cell A execution → approved minimal result
       ├─ Cell B execution → approved minimal result
       └─ Cell C execution → approved minimal result
```

An aggregate can still identify a person through small cohorts, rare events,
joins, or repeated queries. "De-identified" is not a magic scope conversion.
Every global export requires a typed contract, minimum cohort and suppression
policy where applicable, query/rate budget, purpose, retention, provenance, and
privacy review. Raw regulated-workload fallbacks are prohibited.

## 19. Audit, observability, and support

Audit is cell-scoped, append-only, tamper evident, and externally anchored. It
records actor/device/application digest, decision, purpose, object identifiers
or safe references, writer/key/authorization epochs, and result. It does not
record plaintext regulated data, secrets, tokens, query parameters, response bodies, or
decrypted grant payloads.

Fleet observability receives bounded non-regulated data health signals. Cardinality values,
error messages, SQL text, stack traces, heap/core dumps, and support bundles are
treated as potential regulated data until proven otherwise.

Support personnel diagnose using redacted evidence and cell-local workflows.
If decrypted access is exceptionally required, it is a data subject/organization-authorized
break-glass event inside that cell—not a standing support credential.

## 20. Adversarial review: where this design breaks

This section is intentionally hostile. The architecture is rejected if the
implementation answers any attack with policy prose alone.

### P0 common-mode failures

| Attack | Why cells do not save us | Required answer |
| --- | --- | --- |
| Compromise the App Root signing/update path | The same malicious application can be activated in every cell over time and exfiltrate locally | Independent threshold approvals, immutable digests, transparency, reproducible evidence, cell manifest pin, cohort rollout, narrow egress, rapid freeze |
| Compromise the BicDB/guest-image release path | A malicious measured runtime can decrypt every cell to which that measurement is rolled out | Independently signed runtime image, reproducible provenance, attestation policy transition, cohort rollout, no single platform signer/activator |
| Compromise the globally delivered frontend | Malicious JavaScript reads every active user's regulated data through legitimate cell APIs | Cell-served digest-pinned UI, no third-party code, strict CSP/origin isolation, independently approved frontend release |
| Give one runtime or CellAgent wildcard KMS IAM | RCE changes `requested_cell` and unwraps the fleet | Workload identity and KMS policy structurally scoped to one `CellId`; negative cross-cell KMS tests |
| Compromise KMS policy/recovery administration | Attacker widens a scoped identity, weakens attestation, or uses recovery to reach every key | Independent KMS/recovery quorum, policy-as-code digest, external immutable audit, no single administrator or universal recovery secret |
| Terminate regulated-workload TLS in a global gateway | Gateway memory, logs, SSRF, or RCE regain the global regulated data blast radius | L4/SNI routing or per-cell gateway instances; cell-local TLS keys |
| Claim operator-blind regulated data on ordinary VMs | Host/root can snapshot or inspect guest memory and alter boot | Confidential VM, attestation-bound key release, measured image, anti-rollback, explicit residual side-channel/vendor trust |
| Start cell mode on today's incomplete encryption surfaces | Pages, spills, indexes, logs, or staging can retain plaintext | Complete at-rest inventory and ciphertext tests for every durable/transient representation before regulated-workload admission |
| Restore/clone under a new identity | A backup becomes an untracked readable copy or wrong organization serves source data | Lineage-bound backup, same-cell restore rules, explicit rehome/rekey protocol, mismatch refusal before decrypt |

The first two attacks expose a hard truth: **physical cells contain accidental
or local compromise, but common software supply chains remain fleet-wide
authority.** This residual cannot be hand-waved away by saying the registry has
no regulated data.

Nor can egress policy prove a signed application release benign. An application must legitimately
return regulated data to an authorized browser; malicious backend code can over-return it,
and malicious frontend code can forward what it renders. Capability limits,
static analysis, and DLP reduce opportunity but cannot replace independent
release trust, narrow cohorts, observation, and the ability to freeze rollout.

### P0/P1 boundary-collapse failures

| Attack | Failure mode | Required answer |
| --- | --- | --- |
| Route Cell A user to Cell B | Confused deputy or data misdelivery | Cell-bound assertion, local membership check, cell certificate/origin binding, no trust in route alias |
| Compromise DNS, certificate issuance, or route control | Phish credentials, substitute a fake cell origin, or suppress the real cell | Manifest/device-bound cell public identity, proof-bound one-time handoff, certificate transparency/monitoring, cell-local session and membership refusal |
| Swap volume after identity check | TOCTOU serves another cell | Open-by-handle/mount namespace pinning, immutable device identity, key/AAD binding, recheck through recovery |
| Launch a second validly measured Cell A workload | Orchestrator creates an unauthorized fork or stale reader while still satisfying image attestation | Short-lived launch authorization bound to CellId, manifest generation, replica role, placement and nonce; cell-local auth; fork/rollback detection |
| Roll back disk and manifest together | Valid old grants, app, keys, and audit reappear | External monotonic root/generation and KMS refusal of stale state |
| Reuse old primary after failover | Split brain and divergent regulated-workload history | Writer epochs, lease/quorum fencing, old-epoch rejection, rebuild old primary from winner |
| Exploit RLS function/trigger/view gap | In-cell users cross roles or data subjects | Conformance corpus and startup rejection for unsupported declared policy semantics; never disable RLS as workaround |
| Reach embedded/bootstrap/unchecked API | App escapes inner authorization wall | Capability-free regulated-workload types, no embedded admin session, API/link inspection, negative mutation tests |
| Abuse extension/provider egress | Compromised app exports the cell | No native loader, signed least-authority providers, destination/method/schema limits, egress receipts and budgets |
| Exploit one remotely reachable shared parser/CVE across the fleet | Automation repeats a nominally one-cell compromise thousands of times | Minimal cell listener, cell-local auth before complex parsing, cohort/version inventory, rate/anomaly controls, rapid per-cohort fence/patch, independent red-team corpus |
| Send malicious cross-cell object | Parser RCE, decompression bomb, schema confusion, stored XSS | Memory/CPU/depth bounds, canonical non-executable format, quarantine, provenance, fuzzing, content sanitization |
| Steal a replication credential | Inject or read another cell/epoch | mTLS plus cell/replica/role/epoch authorization and payload AEAD; no wildcard follower |
| Compromise backup service | Copy or substitute fleet data | Per-cell backup DEKs, ciphertext-only service, signed lineage, anti-rollback, restore approval distinct from backup creation |

### P1 human and lifecycle failures

1. **Wrong boundary selection.** An organization placed in one cell still has
   an organization-sized blast radius. Provisioning must record and approve the chosen
   confidentiality boundary.
2. **Authorized insider.** Cell isolation does not stop a legitimately
   authorized principal from abusing access. Purpose-of-use, minimum necessary,
   anomaly detection, and audit remain necessary.
3. **Recipient persistence.** Cross-cell revocation cannot erase screenshots,
   exports, printed pages, or decrypted device copies.
4. **Desktop malware.** Secure Enclave/TPM key storage does not protect an open
   record from endpoint compromise.
5. **Emergency bypass accretion.** Disaster recovery and support procedures
   tend to create global credentials. Every emergency path must remain
   cell-scoped and expire automatically.
6. **Key loss.** Eliminating global recovery keys improves confidentiality and
   raises availability risk. Recovery must use cell-scoped escrow/quorum with
   tested succession, not an undocumented master key.
7. **Dormant fleet drift.** Old cells can miss security releases or exceed
   supported schema ranges. Wake admission must enforce minimum-safe versions
   without destructively upgrading unreadable data.
8. **Jurisdiction drift.** Failover, backup, logging, and support can silently
   move data across residency boundaries. Placement and recovery policy are
   signed manifest inputs.
9. **Metadata inference.** Even opaque cell and grant routing can reveal
   relationships and activity patterns. Minimize, partition, retain briefly,
   and audit queries.
10. **Noisy neighbor and correlated host risk.** MicroVMs share hardware,
    network, storage controllers, and sometimes caches. Placement caps and
    failure-domain diversity are required.
11. **Secure deletion claims.** Flash remapping, snapshots, backups, and device
    caches make overwrite promises unreliable. Define deletion as key
    destruction plus retention expiry and state the residual copies.
12. **Subject identity errors.** Strong cryptography can faithfully transfer
    the wrong data subject's data. Cross-cell data subject matching, consent, corrections,
    and provenance need domain-safety review independent of cybersecurity.

### 20.1 Claims this specification forbids

- "Database per tenant" means isolation when all databases share one process,
  one key authority, or one privileged application.
- "Encrypted" when pages, WAL, spills, indexes, logs, backups, or old versions
  can contain plaintext.
- "Zero trust" as a substitute for enumerated identities and capabilities.
- "No regulated data in the control plane" when it serves scripts capable of reading regulated data
  or terminates regulated-workload TLS.
- "Revoked" meaning previously decrypted offline copies disappeared.
- "Anonymous aggregate" without re-identification analysis.
- "Cross-cell access impossible" before the binary, runtime types, KMS policy,
  mounts, and tests support that claim.
- "regulatory compliant" based solely on implementation of this architecture.

## 21. Current BicDB gap analysis

The target deliberately exceeds current behavior:

| Target property | Current evidence/gap | Consequence |
| --- | --- | --- |
| One-cell regulated-workload runtime | `bicdb-cell` and `bicdb cell serve` construct one signed, volume-bound runtime without a database selector, cluster manager, sync server, RESP server, or general app installer; 1.0.326-beta admits regulated data only after the exact Phase-8 evidence bundle verifies | Process/microVM isolation, production evidence, and a threshold admission-root ceremony must be supplied by the operator |
| Cell-native application execution | The 1.0.320-beta Phase 3 path signs component authority and frontend bytes, serves only same-origin package assets, performs device-proof one-time identity handoff, derives Cell-bound host-only sessions, applies Cell-local membership, withholds raw credentials from guests, and binds declared database features to the exact binary; pgwire, lifecycle mutation, ambient blob storage, egress, raw SQL, secrets, and cross-cell handles are absent | The later phases supply fleet, HA, device, grant, and evidence-verification boundaries; independent feature, deployment, and regulated-workload review evidence remains required |
| App Root and fleet lifecycle | The 1.0.321-beta Phase 4 path provides a database-independent immutable artifact registry, distinct threshold authorities, reproducible-build witnesses, hash-chain transparency, bounded cohort gates, exact per-Cell activation tickets, predecessor-derived compatibility checks, minimum-safe release policy, and Cell-local convergence receipts anchored beside monotonic manifest state | The hardened verifier now binds release evidence to an exact deployment; external log monitoring, production authority ceremony, and independent review still must produce real evidence |
| Cell-scoped HA and recovery | The 1.0.322-beta Phase 5 path requires quorum-certified writer epochs and short replica leases before key release, fences the real database commit boundary, binds signed/encrypted native commit objects to both replicas and exact epochs, maintains a crash-safe durable witness, holds one kernel runtime lease per volume, and provides Recovery-quorum backup lineage, inert restore, keyless failover ordering, and auditor drill verification | Replication is declared-RPO asynchronous with Auditor measurement, not continuously commit-time RPO-enforced; production transport/placement evidence, externally archived drills, grants, attested deployment, and independent review remain required |
| Hardware-bound device edge | The 1.0.323-beta Phase 6 path provides disjoint threshold authorities, certified hardware-key contracts, device-only encrypted database keys, exact bounded working sets, rollback-resistant offline policy, immutable causal amendments, parent conflict classification, certified resolution, and retirement; the durable parent ledger inherits Cell HA/recovery | Production TPM/Secure Enclave/StrongBox and attestation evidence, platform sandboxing, grants, independent review, and residual-copy controls remain required |
| Selective cross-Cell sharing | The 1.0.324-beta Phase 7 path provides mutually pinned Cell trust anchors, distinct threshold roles, exact object/purpose/schema/application bounds, per-object DEKs, RFC 9180 recipient envelopes, deterministic bounded ciphertext packages, predecessor/replay protection, import-review evidence, encrypted ledgers, provenance, and honest revocation | Production HSM/attested recipient keys, opaque relay evidence, complete protocol fuzzing, independent HPKE/security review, and deployment admission remain required |
| Hardened fleet admission evidence | The 1.0.325-beta Phase 8 path provides a database-free exact twelve-gate verifier, four disjoint threshold trust domains, immutable evidence/provenance commitments, exact build/Cell/application/deployment binding, deployment attestation, a signed hash-chain checkpoint, and separate short-lived activation authorization before key release; 1.0.326-beta makes a fully verified bundle admission-authoritative | The operator must produce truthful review, confidential-compute attestation, operational, and root-ceremony evidence; repository test fixtures are not production evidence |
| Per-cell key authority | The 1.0.317-beta cell path accepts only a one-shot independently signed lease scoped to the exact attested workload and one cell key; no KMS credential or arbitrary cell selector enters the runtime | A real KMS/HSM/attestation deployment and negative IAM evidence are still required; general BicDB and legacy regulated data-field key paths are not retroactively cell-scoped |
| Complete encryption | The constructed Cell graph encrypts and path-binds records, WAL, sync/audit frames, spills, blobs, catalogs, vector/graph/search sidecars, semantic-index jobs and identity secrets; Phase 5 adds Cell-bound replication objects plus encrypted backup/key-wrap and inert restore; server-paged storage remains refused | Deployment-level transient surfaces, ciphertext inventory evidence, external data-state rollback roots, and independent review remain admission gates |
| Pgwire encrypted open | `PgWireConfig` carries no database key provider and server cluster opens through `open_with_config` | Existing pgwire cluster is not the regulated-workload cell path |
| Process isolation | `PgWireCluster` hosts multiple database servers in one address space | Database directories are not a kernel boundary |
| Immutable CellId | Phase 1 verifies a deterministic-CBOR Ed25519 manifest, signed volume identity, expected CellId/volume/lineage, runtime and app digests, and a local monotonic transition before opening storage; Phase 2 adds CellId/profile/epoch/path AEAD binding and an external exact-predecessor lease chain; Phase 5 binds replication, backup, restore, replica, group, and writer-epoch identity | External data-commit rollback roots, production authority evidence, and COSE transition policy remain required |
| Cell-local auth | Pgwire routes/opens the requested database before constructing the session security context; no cell membership binding is the outer selector | Database selection cannot be the tenant boundary |
| Strong inner authorization | RLS is substantial, but table privileges are documented catalog metadata rather than full query-time enforcement; embedded bootstrap and trusted APIs exist | RLS and runtime capability work remain admission gates |
| Regulated-workload sync | Browser sync documents shared static bearer scope, plaintext transport unless externally terminated, and record-level LWW | It is a development working-set service, not regulated-workload replication |
| Automatic safe failover | Phase 5 provides quorum writer epochs/leases, real commit-boundary fencing, a keyless ordered failover supervisor, signed bounded-RPO/RTO drill evidence, and old-primary rebuild ordering; replication is asynchronous and zero-RPO claims are refused | Production transport, route, placement, partition, and recurring drill evidence remain deployment admission gates |
| Selective sharing | Phase 7 implements a new bounded recipient-grant protocol; neither recipient nor source runtime can open the other Cell, and no Cell key crosses the boundary | Opaque production relay, HSM/attestation integration, protocol fuzzing, independent review, and deployment evidence remain admission gates |

Relevant current documents are
[`security-audit.md`](security-audit.md),
[`production-security-hardening.md`](production-security-hardening.md),
[`application-runtime-implementation-checklist.md`](application-runtime-implementation-checklist.md),
[`browser-sync.md`](browser-sync.md),
[`browser-cache-threat-model.md`](browser-cache-threat-model.md),
[`replication-streaming.md`](replication-streaming.md), and
[`high-availability.md`](high-availability.md).

## 22. Implementation sequence

Each phase produces testable boundaries. Later phases do not retroactively make
an earlier phase regulated data-ready.

### Phase 0 — types, formats, and threat fixtures

**Implemented in 1.0.314-beta for the Phase-1 boundary.** The signed envelope
uses deterministic CBOR plus a domain-separated Ed25519 signature. Migration to
standards-profiled COSE is still required before the regulated-workload admission gate.

- Assign `CellId`, `ReplicaId`, `WriterEpoch`, `KeyEpoch`,
  `AuthorizationEpoch`, and manifest-generation newtypes.
- Specify deterministic CBOR/COSE `CellManifest` and transition signatures.
- Define application component `ExecutionScope` and `DataClass`.
- Add wrong-volume, wrong-key, stale-manifest, cross-cell, rollback, and
  malicious-package fixtures before runtime code.

### Phase 1 — single-cell construction path (not yet regulated data-ready)

**Implemented in 1.0.314-beta as a callable, deliberately non-serving
boundary.** `bicdb-cell serve` and `bicdb cell serve` verify and open exactly
one encrypted store, expose no public listener, and report
`regulated_data_admitted = false`. `BICDB_RUNTIME_MODE=cell` selects the same
typed path and refuses general BicDB commands. Host/microVM isolation and the
remaining cryptographic/operational gates stay outside this phase and keep regulated data
admission closed. The optional 1.0.316-beta application host is tracked as a
partial Phase-3 implementation, not a retroactive expansion of Phase 1.

- Add canonical `bicdb cell serve` and optional `bicdb-cell` binary.
- Build `CellRuntime` from a one-cell capability object, not general server
  config.
- Omit database selection, cluster manager, public pgwire, bootstrap SQL,
  development sync, native extensions, and arbitrary path opens.
- Implement read-only, pre-mutation startup identity checks and honest banner.
- Run process/UID/mount/network/resource isolation under a minimal CellAgent.

### Phase 2 — cryptographic cell

**Implemented in 1.0.317-beta for the callable single-cell capability graph,
with regulated-data admission deliberately closed.** Phase-2 manifests require
one-shot attested signed key leases scoped to the exact cell, volume, lineage,
manifest, binary, guest image, profiles, KEK and key epoch. Cell-bound stores
derive per-purpose and per-object keys; records, WAL, sync buffers, audit
streams, SQL spills, blobs, catalogs, vector/search structures, graph
projections, semantic-index jobs and identity secrets are encrypted and bound
to their final paths. Exact-predecessor external rollback witnesses and
crash-resumable two-lease offline key rotation are callable. Ciphertext-only,
wrong-scope, replay, tamper and crash-boundary tests cover the constructed
paths. The unencrypted server-paged engine, general backup/HA controllers and
arbitrary storage APIs are absent/refused rather than silently admitted.
Production KMS deployment evidence, data-commit rollback roots, cell-scoped HA
and restore, attested microVM enforcement, and independent review remain later
admission work. See [`cell-runtime-phase2.md`](cell-runtime-phase2.md).

- Implement KMS/HSM `CellKeyProvider` with workload identity and attestation.
- Encrypt/authenticate pages, segments, all WAL, spills, indexes, blobs,
  staging, backups, and replication buffers.
- Bind `CellId` and object identity into AEAD associated data.
- Add per-purpose keys, rotation, zeroization, external anti-rollback roots, and
  ciphertext-only whole-tree tests.

### Phase 3 — cell-native application and identity

**Implemented in 1.0.320-beta as an industry-agnostic, deliberately
non-admitted boundary.** The callable construction path verifies signed
component scope/data-class/capability/egress/database-feature contracts and
signed same-origin frontend bytes before opening storage. It performs a
device-proof-bound one-time OIDC handoff, obtains final roles/scopes only from
manifest-pinned Cell-local membership, derives short-lived host-only sessions
from Cell cryptographic identity, persists replay refusal, and withholds raw
credentials from guest code. Unsupported ambient providers are absent instead
of conditionally denied. See [`cell-runtime-phase3.md`](cell-runtime-phase3.md).

- Serve signed frontend/backend artifacts from the cell origin.
- Enforce component scope/data class/capability/egress contracts.
- Implement device-bound one-time handoff and cell-local final authorization.
- Remove ambient bootstrap/unchecked authority from the hardened dependency
  graph and certify RLS/functions/triggers/jobs for declared app features.

### Phase 4 — App Root and fleet lifecycle

**Implemented in 1.0.321-beta as an industry-agnostic, deliberately
non-admitted boundary.** `bicdb-fleet` has no dependency on the database, SQL,
application runtime, Cell runtime, or key providers. It verifies independently
authorized release, transparency, rollout, observation, and per-Cell ticket
documents. `CellRuntime::open` evaluates the exact bundle before requesting a
Cell key or opening storage, then records successful convergence locally. See
[`cell-runtime-phase4.md`](cell-runtime-phase4.md).

- Build immutable artifact registry, threshold release policy, transparency,
  reproducible provenance, compatibility ranges, and cohort rollout.
- Implement signed, monotonic cell manifest transitions and dormant-cell wake
  admission.
- Support additive background convergence with receipts and rollback windows.

### Phase 5 — cell-scoped HA

**Implemented in 1.0.322-beta as an industry-agnostic, deliberately
non-admitted boundary.** `bicdb-cell-ha` separates Lease, Recovery, and Auditor
authority from Cell keys and application execution. `CellRuntime::open`
verifies the exact epoch/replica lease before key release, holds a kernel volume
lease, installs fencing at the durable commit boundary, and exposes
Cell-bound replication plus quorum-certified backup/restore. Failover route
publication is ordered after a verified, non-overlapping writer activation.
The current transport is declared-RPO asynchronous with Auditor measurement;
continuous commit-time RPO enforcement and synchronous-quorum/zero-RPO claims
are refused. See [`cell-runtime-phase5.md`](cell-runtime-phase5.md).

- Bind every replication object to `CellId`, replica identity, and writer epoch.
- Build lease/quorum fencing and automatic failover supervisor.
- Certify topology-specific RPO/RTO, backup lineage, restore, and old-primary
  rebuild.

### Phase 6 — device edge

**Implemented in 1.0.323-beta as an industry-agnostic, deliberately
non-admitted boundary.** `bicdb-cell-device` supplies disjoint threshold device
authorities, a hardware-keystore contract, device-only encrypted database
keys, exact filtered working sets, authorization epochs, bounded offline
sessions, immutable causal amendment chains, parent clean/conflict
classification, certified resolution, and retirement. The parent ledger lives
inside the encrypted Cell database and inherits Phase-5 fencing, replication,
backup, and recovery. Production hardware/attestation evidence remains an
admission gate. See [`cell-runtime-phase6.md`](cell-runtime-phase6.md).

- Bind device storage and packages to exact Cell, device, application, schema,
  authorization, package, and key epochs.
- Refuse wildcard export, parent-key export, wall-clock last-writer-wins,
  fabricated package causality, and unilateral conflict resolution.
- Retire at both parent and cooperative hardware-key layers without claiming
  deletion of previously copied plaintext.

### Phase 7 — cross-cell grants

**Implemented in 1.0.324-beta as an industry-agnostic, deliberately
non-admitted boundary.** See
[`cell-runtime-phase7.md`](cell-runtime-phase7.md).

- Implement recipient keys, object DEKs, HPKE envelopes, signed grants,
  bounded hostile-input import, provenance, updates, expiration, and honest
  revocation semantics.

### Phase 8 — hardened fleet

**Implemented in 1.0.325-beta as an industry-agnostic verification boundary
and admission-enabled in 1.0.326-beta.** `bicdb-cell-admission` verifies the exact
twelve gate records, a live deployment attestation, a hash-chain transparency
checkpoint, and a separate activation authorization under four disjoint
threshold trust domains. `CellRuntime::open` binds that bundle to the exact
Cell, manifest, binary, guest image, application/schema set, profiles, and
launcher isolation tier before key release. A single owner may operate the
ceremony with distinct threshold key domains and approval steps; concentration
of key custody remains an explicit governance risk. This is the code and
contract for evaluating real production evidence, not a substitute for
performing the work. See
[`cell-runtime-phase8.md`](cell-runtime-phase8.md).

- Move regulated data cells to attested microVM/confidential-VM profiles where operator
  blindness is required.
- Complete independent penetration testing, cryptographic review, red-team
  exercises, operational drills, privacy review, and regulated-workload risk
  analysis.

## 23. Regulated-workload production admission gates

A release cannot carry production regulated data merely because `bicdb cell serve` exists.
All applicable gates must have archived, reproducible evidence:

1. **Runtime graph:** regulated-workload binary/path has no cross-database, global-key,
   bootstrap, unchecked, native-loader, dev-sync, or public-admin capability.
2. **Identity:** wrong cell/volume/key/replication/backup/app/generation fails
   before mutation or decryption; TOCTOU and symlink cases are tested.
3. **Encryption:** every durable/transient path passes plaintext canary scans,
   wrong-key, tamper, truncation, replay, rotation, crash, and restore tests.
4. **KMS:** IAM/attestation negative tests prove Cell A cannot request B's key;
   no fleet-wide runtime credential exists.
5. **Authorization:** cell-local auth, device proof, RLS, functions, views,
   triggers, jobs, stored procedures, grants, break glass, and audit pass an
   adversarial conformance corpus.
6. **Application supply chain:** threshold approvals, pinned digests,
   transparency, reproducible provenance, SBOM/dependency audit, CSP, egress,
   canary freeze, and rollback are exercised.
7. **Isolation:** escape, filesystem, IPC, network, resource exhaustion,
   crash-dump, observability, and cross-cell negative tests pass under the
   deployment profile.
8. **Replication/HA:** writer fencing, stale epoch, network partition, replay,
   snapshot bootstrap, RPO/RTO, old-primary rebuild, and regional placement are
   demonstrated.
9. **Backup/recovery:** encrypted lineage, restore authorization, anti-rollback,
   key recovery, deletion/retention, and wrong-cell restore drills pass.
10. **Device:** working-set limits, offline expiry, lost/stolen device, malware
    assumptions, revocation, conflicts, logout, and key destruction are
    documented and tested before desktop regulated data.
11. **Sharing:** parser fuzzing, decompression/resource limits, wrong recipient,
    replay, expired/revoked grant, provenance, and redisclosure semantics pass
    before cross-cell regulated data.
12. **Independent review:** database, cryptographic, platform, regulated-data safety,
    privacy, and operations owners sign the threat model and residual risks.

The gate output must identify the exact BicDB binary digest, guest image,
application digests, manifest/security profiles, deployment isolation tier,
test evidence, and date. Evidence from a different build is not transferable.

## 24. Open design decisions

These are deployment and production-admission decisions to resolve before a
regulated workload is accepted, not excuses to weaken the invariants:

1. Which confidential-compute platforms and attestation claims are supported,
   and which operators remain in the trusted computing base?
2. What independent parties and threshold authorize vendor, emergency, and
   customer-specific application transitions?
3. Where does the external monotonic anti-rollback root live during regional
   isolation, and what is the availability tradeoff?
4. What synchronous/async replication profiles are offered, and which regulated-workload
   operations require zero RPO?
5. How are data subject identity, consent, correction, provenance, and redisclosure
   represented across cells without a global regulated-workload graph?
6. Which data may be present on devices, for how long offline, and under whose
   risk acceptance?
7. What recovery quorum prevents both catastrophic key loss and creation of a
   de facto global master key?
8. How are aggregate exports evaluated for re-identification and repeated-query
   leakage?
9. Which cell boundary defaults apply by product/customer class, and who may
   approve a larger blast radius?
10. What is the precise compatibility and rollback contract for records,
    schema generations, frontend/backend pairs, and dormant cells?

## 25. External precedents and standards

- Oracle, [About Application Containers](https://docs.oracle.com/en/database/oracle/oracle-database/26/multi/application-containers2.html): named/versioned master application definitions and application roots/PDBs.
- Oracle, [Administering an Application Container](https://docs.oracle.com/en/database/oracle/oracle-database/26/multi/administering-application-containers-with-sql-plus.html): explicit synchronization to application versions and patches.
- Oracle, [Multitenant Container Database](https://docs.oracle.com/en/database/oracle/oracle-database/26/dbiad/db_cdb.html): PDB logical independence while the CDB is the database to the operating system.
- Oracle, [Database Vault Operations Control](https://docs.oracle.com/en/database/oracle/oracle-database/26/dvadm/database-vault-operations-control.html): separation of common/container administration from PDB application-data access.
- Oracle, [Configuring Isolated Mode](https://docs.oracle.com/en/database/oracle/oracle-database/26/dbtde/configuring-isolated-mode2.html): PDB-specific keystores and TDE master keys.
- IETF, [RFC 8949 — CBOR](https://www.rfc-editor.org/rfc/rfc8949.html): deterministic structured encoding basis.
- IETF, [RFC 9052 — COSE](https://www.rfc-editor.org/rfc/rfc9052.html): CBOR signing/encryption structures.
- IETF, [RFC 9180 — HPKE](https://www.rfc-editor.org/rfc/rfc9180.html): recipient-oriented public-key envelope basis.
- NIST, [SP 800-207 — Zero Trust Architecture](https://csrc.nist.gov/pubs/sp/800/207/final): resource-focused, explicitly evaluated access rather than network-location trust.
- NIST, [SP 800-57 Part 1 Rev. 5](https://csrc.nist.gov/pubs/sp/800/57/pt1/r5/final): key-management lifecycle basis.
- NIST, [SP 800-124 Rev. 2](https://csrc.nist.gov/pubs/sp/800/124/r2/final): enterprise mobile-device security basis.

## 26. Final architectural test

For every new feature, ask:

```text
What authority does this component receive?
What immutable identity bounds it?
What key can it obtain?
What storage can it open?
What network can it reach?
What happens if its code is fully compromised?
Does that compromise automatically grant authority above its layer?
```

If the last answer is yes, the feature has crossed a Cell/Application boundary
and requires redesign or an explicit, reviewed change to the accepted blast
radius.
