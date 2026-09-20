# Phase 6 hardware-bound device edge

Introduced in BicDB 1.0.323-beta, Phase 6 adds an industry-neutral device
replica protocol and encrypted offline store to the single-Cell runtime. It
inherits the Phase-5 application, fleet, HA, backup, and recovery boundary. It
deliberately does not admit regulated data.

## Security boundary

A device never receives the parent Cell key, an unrestricted database, a SQL
connection, or a filter expression. It receives:

- a unique 256-bit device database key sealed to a certified X25519 hardware
  encryption key;
- every and only the exact object identities in a threshold-certified
  authorization epoch;
- a bounded signed working-set package encrypted to that device; and
- time, count, byte, schema-generation, and application-digest bounds.

The device database has its own encryption binding:

```text
device:<CellId>:<DeviceReplicaId>
  + bicdb-device-replica-bound-v1
  + device key epoch
```

The parent Cell key is not used to open it. A device-store theft therefore
does not yield the Cell database and a parent-store theft does not yield the
device hardware key.

## Independent authority roles

The manifest-pinned `DeviceTrustPolicy` requires disjoint Ed25519 authority
keys for four roles:

- Enrollment certifies Cell, principal, application, device identity,
  the exact trust-policy digest, hardware profile, attestation digest,
  encryption key, signing key,
  user-presence policy, and rollback-resistant clock contract.
- Authorization certifies an exact object set, authorization epoch, schema
  generation, offline expiry, upload grace, and queue limits.
- Resolution certifies the disposition and immutable provenance of one
  already-admitted amendment.
- Retirement certifies the exact final enrollment and authorization epoch.

Every role has a threshold of at least two. Key ids and public-key material
cannot be reused across roles. Parent export signing keys are also disjoint;
they can seal device keys and working sets but cannot unwrap a Cell key.

## Exact filtered working sets

`DeviceObjectRef` is an exact tuple of namespace, object id, and data
category. There is intentionally no wildcard, predicate, arbitrary SQL, or
caller-defined filter language at the device boundary.

A package must contain every and only the authorized references. An existing
object carries its record and source digest. A not-yet-existing exact object
uses a signed tombstone, permitting a later offline create for that one id
without granting wildcard create authority. Package envelopes form a
contiguous signed predecessor chain and are recorded in the parent ledger
before delivery. Amendments naming an unrecorded causal package are refused.

## Offline execution and amendments

Opening a device replica verifies policy, enrollment, authorization, hardware
descriptor, export signature, secure clock, key fingerprint, absolute
non-symlink storage path, and encryption binding before database use.
Device-local sessions require a fresh hardware user-presence signature.
Authorization, package, and local reauthentication expiries are enforced
against the rollback-resistant clock high-water mark stored inside the
encrypted device database.

Every new offline amendment must also advance the hardware monotonic counter;
the device and parent ledger both reject repeated or decreasing counters.
Future-dated package recording, resolution, and retirement transitions remain
inert until their certified time and are rejected when presented early.

Offline writes are immutable proposals, not direct parent mutations. Each
amendment binds:

- Cell, device, principal, application, schema, and authorization epoch;
- exact object and causal working-set package;
- source version digest and proposed result digest;
- contiguous amendment sequence and predecessor digest; and
- hardware secure-clock counter and device signature.

The parent compares the causal base digest to the current object digest and
returns `clean` or `conflict`. BicDB never applies application data from this
protocol and never uses wall-clock last-writer-wins. The industry-neutral
application layer must validate its own schema and invariants, then obtain a
threshold-certified resolution. A resolution can name only an amendment that
the durable parent ledger actually admitted.

## HA and crash behavior

The Phase-6 `CellRuntime` stores the parent device ledger inside the encrypted
Cell database. Its authorization epochs, authorized package chain, amendment
chain, pending set, resolutions, and retirement state therefore inherit the
Cell commit fence, replication stream, backup lineage, and recovery process.
State is persisted before the corresponding in-memory transition is
published; a failed durable write cannot advance only process memory.

On a Phase-6 standby the policy and credentials are verified before Cell-key
release, but no device authority is constructed. Device export and admission
exist only on the active application-executing Primary.

## Retirement semantics

After threshold-certified retirement the parent refuses new packages,
authorization epochs, and amendments. A cooperative device stores the signed
retirement, closes its encrypted database, and asks its platform provider to
destroy the hardware key.

This is honest cryptographic erasure, not a promise that bytes once decrypted
were never copied. Flash remapping, malware, screenshots, backups, or user
exports may retain plaintext. Retention and incident processes must address
those residual copies.

## Interfaces

The reduced Cell binary accepts:

```text
--device-trust-policy /run/bicdb/device-policy.cbor
--device-exporter-key-id device-export-a
--device-exporter-signing-key /run/bicdb/device-exporter.key
```

`BICDB_RUNTIME_MODE=cell` maps:

```text
BICDB_CELL_DEVICE_TRUST_POLICY
BICDB_CELL_DEVICE_EXPORTER_KEY_ID
BICDB_CELL_DEVICE_EXPORTER_SIGNING_KEY
```

The callable Rust surface includes Cell-scoped key provisioning, exact
working-set export, authorization activation, amendment admission, certified
resolution, and retirement. The device crate exposes a
`HardwareBoundDeviceKey` trait so platform TPM, Secure Enclave, or StrongBox
implementations retain private operations in hardware.

## Admission status

`regulated_data_admitted` remains `false`. The repository supplies the
protocol, cryptographic bindings, encrypted stores, runtime construction, and
adversarial software reference tests. A production hardware-keystore backend,
attestation verifier and evidence, platform sandboxing, cross-Cell grants,
independent cryptographic/security review, and the remaining admission gates
are still required.
