# Authoring BicDB ABI v2 applications

This guide is for compiler authors and developers building signed BicDB
application packages. The normative behavior is defined in
[`application-runtime-abi-v2.md`](application-runtime-abi-v2.md).

## Build boundary

Compile application logic for `wasm32-unknown-unknown`. Do not enable WASI.
An ABI v2 module may import exactly one function:

```text
bicdb:app/host.call (i32, i32, i32, i32) -> i64
```

The `bicdb_extension::abi_v2::guest::call` SDK owns that unsafe transport.
Application code uses the typed `HostRequest` and `HostValue` enums:

```rust
use bicdb_extension::abi_v2::{
    guest, HostRequest, HostValue, IsolationLevel, TransactionRequest,
};

let transaction = match guest::call(HostRequest::Transaction(
    TransactionRequest::Begin {
        isolation: IsolationLevel::ReadCommitted,
    },
))? {
    HostValue::Handle(handle) => handle,
    value => return Err(format!("unexpected host value: {value:?}").into()),
};
```

The SDK performs a bounded two-pass response-buffer exchange, verifies the
request ID, validates that a response contains exactly one value or error, and
returns stable `HostError` details. There is no native handle, connection
string, environment lookup, filesystem API, socket API, or Tokio handle in the
guest.

## Package contents

`ApplicationPackage` is a JSON envelope containing:

- the ABI v2 `ExtensionManifest`;
- a map of module name to WASM bytes;
- an exact dependency lock;
- a CycloneDX JSON SBOM;
- provenance identifying the builder.

The entry module name must equal `manifest.identity.name`. The entry module
embeds the same manifest. Only `package_sha256` and `signature` may contain
placeholders in that embedded copy; BicDB compares every other field exactly.

The dependency lock format is:

```json
{
  "format": 1,
  "packages": [{
    "name": "example-auth",
    "version": "2.1.0",
    "package_sha256": "<64 lowercase hex>",
    "module_sha256": "<64 lowercase hex>"
  }]
}
```

Every required extension dependency needs one exact lock entry. Activation
also checks the active provider bytes and version against the lock.

## Signing

Build all bytes first. Set the manifest package hash and signature to empty
strings, then call `bicdb_app_runtime::canonical_signing_payload`.
SHA-256 that canonical payload, store the lowercase digest in
`package.package_sha256`, and sign the ASCII digest with Ed25519. Store the
base64 signature and the operator-approved key ID in the manifest.

BicDB verifies the signature, every module digest, dependency lock, SBOM,
provenance, embedded manifest, feature profile, route/resource contract, SQL
declaration, and Wasmtime import set before staging.

## Declaring authority

Authority requires all of the following:

1. The extension capability is declared.
2. The extension permission names the relation, queue, or host.
3. The ABI v2 manifest declares the typed relation operation, service, secret,
   egress policy, blob namespace, or route.
4. Operator policy makes the provider available.
5. The trusted actor has the required roles, scopes, tenant, workspace, and
   policy attributes.
6. A write also carries a transaction-bound native `MutationGrant`.

A declaration never grants ambient authority. Dependency calls receive the
intersection of caller and callee capabilities and columns unless the exact
signed import and export both opt into `delegated_authority`. Delegation enters
only the pinned, request-validated method under the callee's own manifest; it
does not add callee capabilities to the caller. Declare the
`delegated_service_authority` required feature whenever either side opts in.

Application packages using blobs declare both a `BlobDeclaration` and an exact
`application_program.blob` contract. Reachable helper and literal signed-method sets
must match the contract exactly. Use logical keys such as
`reports/annual.pdf`; leading/trailing slashes, empty or dot segments,
backslashes, and control characters are rejected. Paths passed to
`blob.get`/`blob.put` are invocation-local virtual identifiers, never host
filesystem paths. Provider-specific storage belongs behind `BlobProvider` and
must not be exposed to the guest.

New packages should require `exact_column_authority`. With that feature,
permission column sets are exact: empty means deny all, omitted read
projections return only signed readable columns, and requesting any other
column is denied. The older empty-set wildcard is retained only for packages
that do not declare the feature.

## Resource contracts

Package producers emit one signed `ResourceContractV1` per resource. Contracts bind:

- relation, schema version, primary key, field types, nullability, generated
  fields, defaults, create/update field sets, and validation rules;
- list/item route templates, CRUD operations, filters, JSON-path filters,
  search, sorting, pagination, soft delete, restore, and optimistic version;
- tenant/workspace fields, roles, scopes, actor policy attributes, field
  visibility/redaction, immutable fields, cache and idempotency rules;
- durable audit projection, event projection/version, OpenAPI fragments, and
  operation metadata.

The reusable resource interpreter performs API semantics through ABI v2
capabilities. BicDB performs RLS, tenant/workspace injection, native mutation
authority, constraints, commit validation, durable audit, and transactional
outbox enforcement.

## Unsupported profile features

The current profile rejects repeatable-read, serializable, PL/pgSQL, advisory
locks, PostgreSQL event triggers, arbitrary SQL, unrestricted PostGIS,
Timescale, arbitrary materialized views, raw sockets, filesystem, environment,
processes, threads, and WASI.

The in-tree host also rejects inbound streaming bodies, WebSocket routes, and
online rewrite steps until their trusted adapters are available. Rejection
happens during package validation; these features are never silently buffered
or downgraded.

## Test requirements

Package CI should run:

- manifest validation and signature verification;
- native and `wasm32-unknown-unknown` builds;
- denied undeclared relation/SQL/secret/service/egress cases;
- cross-tenant and cross-workspace cases;
- mutation-grant scope, optimistic conflict, rollback, and savepoint cases;
- redaction and OpenAPI snapshots;
- outbox duplicate delivery and idempotent consumer cases;
- upgrade, failed activation, restart restore, and rollback.
