# Phase 8 hardened-fleet admission evidence

Introduced in BicDB 1.0.325-beta, Phase 8 completes the industry-neutral Cell
roadmap's callable software boundary. It verifies exact-build security evidence
and deployment attestation before a Phase-8 Cell can request its key. BicDB
1.0.326-beta permits regulated-data admission when that entire verified bundle
is present. The runtime does not manufacture or waive any evidence.

## Boundary

`bicdb-cell-admission` is deliberately database-free. It has no SQL, network,
application-runtime, CellRuntime, orchestration, or key-provider dependency.
It can verify signed facts; it cannot open a Cell, unwrap a key, execute an
application, or activate itself across a fleet.

The exact admission subject binds:

- Cell and signed manifest digests;
- BicDB binary and guest-image digests;
- every application identity, digest, and schema generation;
- the Cell security and isolation profiles; and
- the launcher-selected deployment isolation tier.

Evidence from another Cell, build, manifest generation, application set, or
deployment tier is therefore not reusable.

## Exact gate set

An admission bundle contains exactly these twelve certificates in canonical
order, with no omissions or duplicates:

1. runtime graph;
2. identity;
3. encryption;
4. KMS;
5. authorization;
6. application supply chain;
7. isolation;
8. replication and HA;
9. backup and recovery;
10. device;
11. sharing; and
12. independent review.

Every certificate carries a non-empty, failure-free, skip-free test summary,
immutable artifact and provenance digests, bounded timestamps, and the exact
admission subject. The independent-review gate must additionally contain the
complete database, cryptography, platform, regulated-data-safety, privacy, and
operations discipline set.

## Independent authority domains

The manifest-pinned `AdmissionTrustPolicy` defines four disjoint Ed25519 trust
domains:

- `EvidenceReviewer` certifies the twelve gate records;
- `DeploymentAttestor` certifies the live workload measurement;
- `TransparencyWitness` signs the evidence hash-chain checkpoint; and
- `AdmissionAuthority` grants a short-lived activation authorization.

Every role requires at least two distinct trusted keys. A public key cannot be
reused between roles. Signatures are role- and document-domain separated, and
all signature arrays are strictly ordered to prevent duplicate or ambiguous
quorum counting.

Publishing evidence is not authorization to activate it. Attesting a workload
is not authority to approve its evidence. The admission authority cannot alter
the evidence set committed by the transparency checkpoint.

The organization operating BicDB may operate this ceremony itself. Authority
independence means separate keys, trust-policy roles, approval steps, and
evidence records; it does not require a particular outside vendor. One public
key cannot serve multiple roles, each role still requires a threshold of at
least two keys, and one approval cannot substitute for another role. If one
organization controls all of those keys, that concentration is an explicit
governance risk to record in the independent-review evidence, not a reason to
bypass the verifier.

## Deployment attestation

The short-lived deployment statement binds the exact subject and isolation
tier to digests for:

- the attestation report and trusted verifier;
- workload identity and KMS scope;
- the compiled capability graph;
- storage mounts and network policy;
- crash-dump policy and observability policy; and
- an activation nonce shared with the authorization.

The verifier id and isolation tier must appear in the pinned policy. This is a
portable contract for a CellAgent/confidential-compute launcher; BicDB does not
pretend that a string naming a microVM creates a kernel boundary.

## Startup order

For the cumulative Phase-8 profile, `CellRuntime::open` performs this order:

```text
signed CellManifest + volume identity + local monotonic state
        -> exact running binary and guest-image measurements
        -> manifest-pinned admission policy
        -> twelve-gate bundle + deployment attestation
        -> transparency checkpoint + activation authorization
        -> HA/device/grant/fleet/application policies
        -> one-shot Cell-scoped key release
        -> encrypted storage open
```

Missing, stale, malformed, non-canonical, under-threshold, cross-role,
cross-build, cross-Cell, or substituted evidence fails before key release.

## Construction interfaces

Both `bicdb cell serve` and the reduced `bicdb-cell serve` accept:

```text
--admission-trust-policy /run/bicdb/admission-policy.cbor
--admission-evidence-bundle /run/bicdb/admission-bundle.cbor
--deployment-isolation-tier confidential-microvm
```

All three options are required together. A Phase-8 manifest pins the exact
policy-file digest and also retains every cumulative Phase-3 through Phase-7
policy pin.

With `BICDB_RUNTIME_MODE=cell`, the launcher maps:

```text
BICDB_CELL_ADMISSION_TRUST_POLICY
BICDB_CELL_ADMISSION_EVIDENCE_BUNDLE
BICDB_CELL_DEPLOYMENT_ISOLATION_TIER
```

Runtime status exposes evidence completeness, checkpoint sequence, and
authorization expiry separately from regulated-data admission.

## Admission status

Beginning with 1.0.326-beta, `regulated_data_admitted` is `true` only when the
release enables admission and all Phase-8 documents verify for the exact live
subject. Missing evidence, a Phase-1 through Phase-7 profile, an untrusted or
stale attestation, a role/key substitution, an incomplete gate set, or an
expired activation authorization keeps it `false` or prevents startup before
Cell-key release.

Admission is continuous. The earliest evidence, deployment-attestation, or
activation-authorization expiry is an exclusive deadline, with no runtime
clock-skew grace. Status and every new HTTP request check the same live lease;
expired requests receive HTTP 503 before their bodies are read. A second check
after body reading rejects requests that crossed the deadline while uploading.
Startup rechecks admission before and after key release and before publishing
the runtime's active manifest state.

Both `bicdb-cell serve` and `bicdb cell serve` retain a dedicated OS-thread
watchdog through shutdown. It checks every 100 ms and terminates the process
with exit status 78 on expiry, stopping long-lived streams and background work
without waiting for cooperative cancellation. This is an abrupt termination;
normal encrypted-storage crash recovery applies. The polling bound assumes the
OS schedules the watchdog; deployment CPU reservations and an external fleet
fence remain necessary for a stalled or suspended process.

The lease also advances using monotonic elapsed time, so moving the wall clock
backwards cannot extend its original lifetime. Once observed expired, it stays
expired even after a clock rollback. Renewal requires a restart with freshly
verified evidence and a new key lease. There is no online signing-key revocation
or transparency-log polling; emergency revocation still requires fleet fencing.

Embedded launchers must retain `CellRuntime::start_admission_watchdog()` until
after runtime close, or supply equivalent process fencing. The HTTP and status
checks alone do not terminate embedded background work or privileged APIs.

Application pins and signed component contracts may use `regulated` or
`regulated_local` only in the coherent Phase-8 profile and only after that
evidence has verified. The same package is refused under every earlier profile
or when the admission bundle is absent or invalid.

The source repository still does not fabricate a penetration test,
cryptographic audit, privacy assessment, confidential-compute attestation, or
operational drill. An owner-operated ceremony must attach the real artifacts
and truthful failure-free, skip-free summaries it is asserting. Synthetic
fixtures prove verifier behavior but are not production evidence.
