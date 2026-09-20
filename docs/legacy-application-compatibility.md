# Legacy Application Compatibility

BicDB's canonical public application vocabulary is industry-neutral:
`ApplicationPackage`, `ApplicationProgramV1`, `ApplicationExpressionV1`,
`ApplicationModuleContractV1`, application scopes/data classes/capabilities,
and `bicdb.*` versioned formats.

Older releases accepted packages, SQL migrations, GUC names, schema/role names,
HTTP headers, and encryption-algorithm identifiers that used the name of a
former application producer. They also emitted telemetry attributes in that
namespace. The accepted spellings can be part of signed bytes or durable data.
Removing or silently rewriting them would break signature verification,
database opening, restore, or application behavior.

They are therefore retained only as compatibility inputs under these rules:

1. New serialization emits the neutral field name. `serde(alias = "...")`
   may accept an old field name during decoding.
2. Old cryptographic domain separators and algorithm identifiers are immutable
   for old ciphertext. New formats require a new neutral, versioned identifier;
   existing ciphertext is never relabeled.
3. Old SQL schema, role, GUC, and header names remain recognized only where an
   existing migration or adapter contract requires them. New applications must
   use neutral host policy attributes and application contracts.
4. Internal helper names do not define a public API. They should be renamed
   opportunistically, but not in a way that changes durable bytes.
5. Compatibility paths receive the same validation, authorization, resource
   limits, and security fixes as canonical paths. They are not a less-secure
   mode.
6. No dependency on the old producer, its compiler, its repositories, or a
   product-specific runtime is permitted in BicDB.
7. A legacy path can be removed only after a documented format migration,
   release window, restore test, and explicit compatibility decision.

Canonical telemetry now uses `bicdb.*` names. Telemetry is an emitted
operational stream rather than persisted engine input, so collectors must
migrate old dashboard queries; BicDB does not continue emitting the former
product namespace.

The architecture check rejects reintroduction of extracted repositories or
controller types. Product adapters and vertical fixtures live outside BicDB;
compatibility strings inside the database do not imply product ownership or
runtime dependency.
