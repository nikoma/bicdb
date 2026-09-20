# BicDB Application Runtime and Extension ABI v2

Status: normative for ABI v2
ABI v1 status: supported and unchanged
Application compiler target profile: `bicdb-application-v2`

## 1. Purpose

BicDB ABI v2 is a capability-mediated application runtime. A signed package may
contain application modules, schema changes, resource contracts, routes,
services, workers, schedules, policies, dependency locks, and provenance.
BicDB owns listeners, database transactions, durable storage, secrets, network
connections, clocks, randomness, blobs, observability, and activation.

A module receives no WASI imports, filesystem, environment, raw socket, raw
database handle, Tokio handle, or native object pointer. ABI v1 modules continue
to run with no imports.

The stable ABI v2 boundary is the strongly typed `HostRequest`/`HostResponse`
contract in `bicdb_extension::abi_v2`. Its wire representation is canonical
JSON in ABI v2. Hosts may negotiate a future binary encoding without changing
operation semantics. Unknown operations, fields, enum values, handle kinds, or
contract versions fail closed.

The Rust types in `crates/bicdb-extension` are the sole ABI source of truth.
The versioned WIT projection, core-Wasm lowering, canonical payloads, and
accept/reject corpus live under `abi/application-v2`. Compiler repositories may
vendor an exact generated snapshot of that crate and artifact directory, but
must pin their provenance and verify the snapshot byte-for-byte. They must not
maintain parallel wire structs.

## 2. Compatibility rule

ABI v2 follows these source and wire compatibility rules:

- adding an optional struct field is compatible only when deserialization has
  a deterministic default and old canonical payloads remain accepted;
- removing or renaming a field, changing its meaning or default, narrowing a
  scalar, or changing a JSON tag is breaking and requires a new ABI major;
- adding a variant to a closed enum is breaking for exhaustive consumers and
  therefore requires a new ABI major unless the containing contract was
  explicitly documented as an open union;
- unknown authority, operation, handle, and contract fields remain rejected;
  permissive decoding is never used to grant future authority;
- accepted canonical fixtures must re-encode byte-for-byte, while legacy
  compatibility fixtures may normalize to the current canonical encoding;
- every ABI change must extend the versioned compatibility corpus and pass it
  in BicDB and every in-tree compiler consumer before release.

Patch releases may add validation, documentation, fixtures, or optional fields
under those rules. A behavioral reinterpretation of an accepted signed payload
is never a patch-level change. Deprecated fields remain decoded for at least
the lifetime of their ABI major; emitters stop producing them before removal in
the next major.

A compiler targeting BicDB MUST validate a program against the
`bicdb-application-v2` profile. An accepted program MUST behave equivalently to
the producer's declared reference target for the declared features. A compiler or
installer MUST reject a package before activation when it uses an unsupported
feature. It MUST NOT omit a guard, weaken isolation, change transaction
boundaries, ignore a policy, approximate a data type, or silently downgrade an
isolation level.

Initially supported:

- typed CRUD, UPSERT, filters, sorting, cursor/offset pagination, count and
  declared aggregates;
- declared FTS, vector, JSON-path, and available BicDB spatial operations;
- read-committed transactions, savepoints, row locks, commit validators, and
  publish-on-commit;
- exact and template HTTP routes, JSON/text/binary request bodies, bounded
  response streams, and SSE;
- declared services, broker consumers, jobs, schedules, secrets, cryptography,
  HTTP egress, blobs, logs, traces, metrics, audits, and evidence;
- BicDB tables, indexes, constraints, generated columns, RLS, JSONB, FTS,
  vectors, available spatial types, and ordinary compatible views.

The initial in-tree host does not yet provide inbound request streams,
WebSocket callbacks, or an online-rewrite adapter. A package that declares
`streaming_request`, a WebSocket route/feature, or `online_migrations` is
rejected during validation. This is intentional compatibility behavior, not a
runtime downgrade. A future host profile may enable those features only after
the corresponding trusted adapters are installed.

Rejected unless a later profile explicitly enables them:

- repeatable-read or serializable isolation;
- arbitrary SQL, dynamic relation or column names, PL/pgSQL, PostgreSQL event
  triggers, advisory-lock authority, unrestricted PostGIS/Timescale functions,
  arbitrary materialized views, unrestricted `pgcrypto`, raw sockets,
  filesystem access, environment access, process execution, threads, and
  unrestricted WASI.

## 3. Package identity and activation

An application package is a canonical manifest plus content-addressed artifacts.
The signed package hash covers:

- all WASM modules and ABI versions;
- schema, forward migration, compatibility, transformation, activation,
  rollback, and irreversible-operation declarations;
- resource contracts and their bound schema versions;
- route, service, worker, schedule, capability, secret, egress, and provider
  declarations;
- dependency lock and exact hashes;
- OpenAPI, provenance, signatures, and SBOM.

Installation verifies canonical hashes, signatures, manifest limits, supported
features, declared SQL, schema bindings, dependency contracts, and operator
policy. Staging never changes the active snapshot.

Activation builds a complete shadow runtime snapshot. It becomes visible by one
atomic catalog generation change only after migrations commit, dependencies
bind, routes validate, required secrets/providers exist, workers start, and all
readiness probes pass. Failure leaves the previous valid snapshot active.
Upgrade follows the same rule. Rollback activates a complete previous package
snapshot, including compatible schema/contract bindings, not merely old WASM
bytes.

Exact package and module locks can require several mutually dependent
applications to move together. Operators stage every package first and use
`bicdb app ... activate-batch APP...`. BicDB validates every staged candidate
and every unchanged active dependent against the prospective catalog, applies
and compensates schema work as one operation, prepares all supervisors, then
persists one catalog generation. No sequential intermediate dependency graph
is published. Any validation, migration, route, provider, worker, schedule, or
persistence failure leaves the prior catalog active.

## 4. Trusted invocation context

The host creates one immutable `ActorContext` for every invocation:

- user, service/client, acting-client, session, authentication method and
  assurance level;
- roles and scopes;
- tenant, workspace, and organization;
- delegation chain and request origin;
- request, trace, correlation, and causation identifiers;
- deadline and policy attributes.

The context propagates through HTTP, database calls, service calls, messages,
jobs, schedules, events, realtime subscriptions, audit records, and evidence.
Plugins may inspect manifest-approved fields. Context supplied by a plugin is
untrusted data and never replaces host context. Delegation can only reduce
authority.

Every invocation has a trace ID and deadline. The effective deadline is the
minimum of caller, route, package, host, and operator limits.

## 5. Handles and cancellation

Transaction, savepoint, message-delivery, secret, blob, upload, stream, and
service-call handles are random or monotonically unique host resources:

- created by the host for one invocation;
- typed and non-serializable;
- rejected when used with another operation kind;
- bound to package, actor, tenant, workspace, deadline, and invocation;
- invalid after close, commit, rollback, cancellation, or invocation return.

Client disconnect, deadline, host shutdown, fuel exhaustion, memory exhaustion,
or explicit cancellation cancels pending host operations, rolls back open
transactions, releases handles, terminates streams, and records a bounded
diagnostic. Cancellation is not reported as successful completion.

## 6. Database and transaction semantics

Database operations are typed and relation-bound. Read permission is checked
before planning and again at the mutation/read authority boundary. Tenant and
workspace predicates are injected by the host and cannot be removed by the
module.

Packages compiled with the `exact_column_authority` feature use signed column
sets as exact bounds. An empty readable or writable set means deny all, and an
omitted read projection resolves to the signed readable set. Packages that do
not require this feature retain ABI v2's original compatibility rule in which
an empty set is a wildcard. New compilers MUST require
`exact_column_authority`; the compatibility wildcard exists only for older
signed packages.

Raw SQL is disabled by default. A statement may run only by its manifest ID.
Installation parses and validates it, records referenced relations and
operations, rejects dynamic SQL, and binds a canonical statement hash. Runtime
execution requires the same package ID, statement ID, hash, permissions,
transaction, actor, tenant, and workspace.

Read-committed is the required isolation level. Each statement sees the latest
committed state plus its transaction's writes. `begin`, `commit`, `rollback`,
`savepoint`, `rollback-to`, and `release` are explicit. A host may create an
automatic transaction for one resource operation. Commit-time validators and
outbox records participate in the same commit. Unsupported isolation is
rejected at build and installation.

## 7. Mutation authority

A protected relation rejects every mutation without a valid native
`MutationGrant`, regardless of whether the request came from WASM, declared raw
SQL, pgwire, an internal action, a service call, cascade, lifecycle hook, or
tool. Administrative bypass is a distinct host-only operation requiring an
operator policy and durable audit reason.

A grant is bound to one transaction and includes relation, operation, record or
predicate, expected version, allowed columns, bulk flag, affected-row maximum,
cascade closure, actor, tenant, workspace, package, resource/action, statement
budget, expiry, and audit metadata. It is created only by the trusted resource
runtime or host policy, consumed below SQL/pgwire, and cannot be represented in
WASM memory.

Native integrity rules include append-only relations, immutable fields,
optimistic versions, tenant/workspace immutability, ledger and finance
validators, touched-subject commit validation, audit generation, and atomic
publish-on-commit.

## 8. Service dependencies

Services are versioned typed contracts. Activation binds each required import
to one active provider satisfying the dependency lock and operator policy.
Calls are host-brokered; modules never share memory or invoke arbitrary exports.

By default, the effective capability set is the intersection of caller
authority, callee authority, both manifests, operator policy, actor
permissions, tenant/workspace limits, transaction restrictions, and deadline.
An exact import and export may both sign `delegated_authority: true`; in that
case the request-validated exported method executes under the callee's signed
implementation authority. The caller does not receive or reuse that authority
outside the method. Any package using it must also declare the
`delegated_service_authority` required feature. Calls propagate cancellation,
trace, actor, and optionally
the current transaction. The host enforces declared imports, structured
errors, retry classification, reentrancy policy, call-depth limits, and cycle
detection. A delegation mismatch fails closed.

## 9. Broker and events

Publish, consume, ack, nack, delay, retry, dead-letter, visibility timeout, and
consumer-group operations are host mediated and manifest scoped. Envelopes carry
trace, causation, correlation, actor, tenant, workspace, origin package,
contract/event version, transaction ID, and commit sequence.

Required database events use a transactional outbox stored in the same commit
as mutations. A committed transaction cannot omit its outbox record; an
uncommitted transaction cannot publish. Dispatch is at-least-once, carries a
stable event ID, and consumers must use that ID for idempotency. Projection and
redaction happen before delivery according to the signed contract.

## 10. HTTP, streaming, and realtime

BicDB owns the listener and connection lifecycle. Requests expose route name and
parameters, multivalued query/header maps, cookies, content type, bounded body,
trusted client/origin metadata, actor context, request/trace ID, deadline, and
cancellation.

Responses expose status, repeated typed headers, repeated `Set-Cookie`, JSON,
text, binary, bounded response streams, SSE, trailers,
structured errors, and retry metadata. The host applies TLS, trusted-proxy
policy, authentication, CORS, compression, cache control, idempotency, size and
rate limits, CSRF policy, and OpenAPI aggregation. Plugins declare behavior but
receive no listener or socket.

Backpressure is mandatory. Stream quotas include bytes, frames, duration, idle
time, and concurrent streams.

The current host buffers bounded response chunks produced during an invocation;
it does not claim live bidirectional WebSocket or inbound-stream equivalence.
Those declarations fail package validation as described in section 2.

## 11. Clock, random, secrets, crypto, egress, and blobs

Clock and random imports are host capabilities. Production exposes wall and
monotonic time, secure random, and UUID generation. Tests may install a
deterministic clock and seeded generator. Ambient WASI time/random is forbidden.

Secrets are named opaque handles approved by package and operator policy.
Private material remains host-side unless an explicit narrow export policy
allows bytes. Sign, verify, HMAC, encrypt, decrypt, approved derivation, key
metadata, rotation, and version selection operate on handles.

Application programs that reach security helpers carry a versioned
`application_program.security` contract. It declares the exact helper names, one
signed auth scheme when token/user helpers require it, the host secret names
and signing algorithm, access and refresh lifetimes, magic-link verifier
secrets, and mandatory durable-replay, versioned-key, and evidence flags. The
host derives the required database, transaction, clock, random, secret, crypto,
and observability capabilities from that exact helper set and rejects surplus
or missing authority at activation.

When token issuance is present, BicDB exclusively owns `POST /auth/register`,
`POST /auth/login`, `POST /auth/refresh`, `POST /auth/logout`,
`GET /auth/sessions`, and `DELETE /auth/sessions/{session_id}`. Their methods,
paths, public/protected status, verifier, request bounds, and response bounds
are canonical ABI, not guest-selectable routing metadata. User, refresh,
magic-link replay, and OAuth state records are application-scoped and durable.
Refresh rotation consumes the presented token atomically before minting a pair
with a fresh token id. Ciphertext and signed tokens carry key-version metadata;
historical material is usable only while that version remains in the operator
secret/verifier overlap set.

Egress uses a trusted HTTP client with declared scheme/host/port, DNS and
resolved-address checks, private/link-local/loopback/metadata-address denial,
TLS validation, optional mTLS, redirect revalidation, request/response limits,
timeouts, deadlines, rate/concurrency quotas, secret-backed credentials, and
structured audit. Raw sockets are forbidden.

Blob services provide transactional metadata plus streaming create/read/delete,
hash, content type, attachment association, provider selection, signed URLs,
limits, and scanning hooks. Blob bytes do not pass through one JSON buffer.

Application programs using blobs additionally declare `application_program.blob` with
the exact reachable helper names, logical namespace, literal GET/PUT signed-URL
methods, maximum object size, and required virtual-file, durable-key, and
evidence flags. The declaration and program contract must agree. Logical keys
are validated, hashed to provider ids, and placed beneath an
application-derived physical namespace. The guest never receives that physical
namespace.

Named uploads, reads, metadata, delete, and signed URL calls use durable logical
keys. Application source paths are invocation-local virtual-file identifiers rather
than host paths. `/_bicdb/blob` accepts only the signed query fields and exact
method, verifies expiry and HMAC before access, applies the active declaration's
size/content-type/scan rules, and records audit/evidence for successful
transfers. Signed upload responses use the ABI v2 logical `BlobMetadata` shape.

The production `bicdb-blob-s3` module implements the same host `BlobProvider`
contract for S3-compatible storage. It is a trusted native provider rather than
guest WASM, keeping operator credentials and network authority outside guest
memory. Object keys are opaque, application-namespaced, optionally restricted
to an operator prefix, and use immutable generations with a metadata pointer so
an interrupted overwrite cannot remove the prior committed value. Signed URLs
continue through the BicDB host instead of exposing physical provider URLs.

## 12. AI orchestration

Application AI programs sign exact tokenizer, fixed-dimension embedding, LLM,
tool, RAG, stream, budget, agent, and evaluation contracts. Logical routed LLM
clients contain no physical endpoint or credential. Their signed policy names
the primary and fallback clients, cost target, and the exact outage,
rate-limit, and budget-pressure transitions BicDB may take. A physical client
shared by several logical routes may carry the union needed for package
validation, but each invocation passes an explicit logical tool allowlist to
the provider, including an empty set, so route composition cannot advertise
or execute excess tools.

Tool calls are untrusted model output. BicDB validates each name and recursive
argument schema, executes only its package-signed application callable, enforces
turn/tool/token/deadline bounds, and appends the result to durable scoped
conversation history. RAG embeds through a signed local provider or callable,
invokes the exact native vector retriever, bounds and serializes the selected
context, and calls the declared logical LLM without an application-side
database or network round trip.

`stream` emits compiler-named chunk, tool, completion, and redacted failure
events through the durable realtime host. Tenant, workspace, and conversation
group fields come from the trusted actor and signed request, never provider
output. Per-tenant daily budgets reserve the signed worst-case projection,
reconcile operator-priced actual usage on success or error, and persist across
restart. Typed agents use the generic durable workflow runner with signed
tools, guards, retries, fallback output, and recursive output validation.

Evaluation datasets remain operator state. A signed evaluation selects the
provider name, exact case schema and callables, timeout/case bounds, aggregate
requirement, and optional identity expressions. BicDB replaces the invoking
operator context with that least-authority identity before executing cases;
operator roles, scopes, tenant, workspace, client, and service authority are
not inherited.

Application scenario and property tests use the separate optional
`application_program.tests` contract. Each entry signs one callable, an optional
recursive case type, a deterministic seed and bounded case count, a timeout,
mandatory evidence, and an optional pure test identity. The contract requires
the Tests and Observability features and an Observability host capability;
each callable reference must exist in the same signed program.

Tests are not application capabilities. Only the operator test entry point can
install the private in-process HTTP callback. `http.request` and
`http.request_as` then dispatch through the active package's ordinary router,
coercion, authorization, and response path without opening network egress.
The invoking operator's service, client, roles, scopes, workspace, and tenant
authority are replaced when a test identity is present. A single signed case
may be selected by index so compiler tooling can provision a fresh ephemeral
database for every property case.

## 13. Observability

Application programs may carry a signed `application_program.observability` contract.
It binds the provider and protocol, service name, default sampling, exact
reachable helpers, literal metric and audit names, dynamic-name authority,
recursive redaction keys, field depth/byte limits, durable-audit requirement,
and mandatory W3C propagation. Per-route sampling is signed separately. The
manifest and program call graph must agree exactly; an omitted helper or name
is deny-all rather than wildcard authority.

Every operation records trace timing under the invocation trace ID. Plugins may
emit bounded structured logs, spans, metrics, audits, evidence, and
flight-recorder events. BicDB owns request, trace, correlation, and immediate
causation identifiers and propagates them through HTTP, broker jobs, schedules,
workflows, governed egress, and nested service calls. Parent-aware sampling is
deterministic from the trusted trace context. Unsampled traces are suppressed;
logs and metrics remain available under their exact signed authority.

Sensitive keys are replaced recursively before any operational exporter or
durable record receives the event. Explicit application audit calls commit inside
the active application transaction, or a host-owned read-committed transaction
when no transaction exists, and survive restart. Policy-required audit/evidence
is host-generated and cannot be suppressed by a plugin.

Physical collectors are operator state. The first-party host supports native
OTLP/HTTP protobuf and OTLP/gRPC exporters with bounded non-blocking queues and
optional append-only JSONL fan-out. Package readiness requires an exporter that
matches the signed provider/protocol. Endpoints and authorization headers never
enter package artifacts or guest memory; cleartext transport requires explicit
operator opt-in.

The OTLP instrumentation scope and target are `bicdb-application-runtime`.
Application log, audit, and evidence events use the
`bicdb.application.{log,audit,evidence}` names; actor attributes use `bicdb.*`
and application fields use the `bicdb.application.field.*` prefix. These are
industry-neutral public telemetry contracts.

## 14. Errors

All host errors have a stable code, class, message, retryability, optional
retry-after, and trace ID. Classes are:

- invalid request or contract;
- unauthenticated or unauthorized;
- policy or mutation-grant denial;
- not found, conflict, or optimistic-version conflict;
- constraint or commit-validation failure;
- dependency unavailable or incompatible;
- timeout or cancelled;
- resource exhausted or rate limited;
- provider/network/secret/blob failure;
- package/signature/migration/activation failure;
- internal error.

Sensitive provider, SQL, secret, and policy details are redacted from guest and
HTTP responses but retained in privileged diagnostics.

## 15. Host readiness and lifecycle

The first-party app host composes pgwire, extension HTTP, TLS/authentication,
actor construction, active snapshots, service bindings, event/job/schedule and
realtime supervisors, graceful draining, health, metrics, tracing, and package
operations.

Readiness is false unless required packages and signatures are valid,
dependencies bound, migrations complete, contracts schema-compatible, routes
active, workers/schedules healthy, providers and secrets available, and the
catalog generation is coherent. Liveness only reports process/runtime health.

## 16. Resource contract and migration rules

`ResourceContractV1` is signed and schema-bound. It declares model and key,
fields/types/nullability/defaults/generated fields, create/update DTOs,
validation, routes, CRUD, filters, relation/JSON-path filters, search, sorting,
pagination, soft delete/restore, optimistic version, tenant/workspace fields,
roles/scopes/policy, visibility/redaction/write restrictions, idempotency,
cache, audit, events, OpenAPI, and operation metadata.

Package producers emit contracts; the reusable resource interpreter applies
them. Custom business logic belongs in the application program.

BicDB migrations lower compatible DDL directly and replace PostgreSQL-specific
guards with native grants and validators. Row rewrites use a shadow/backfill/
catch-up/atomic-promotion plan. Every artifact declares compatibility,
transformation, activation, rollback, irreversibility, schema version, contract
version, and dependency compatibility.

## 17. Security boundary

The trusted computing base is BicDB's package verifier, Wasmtime configuration,
capability broker, actor/authentication boundary, transaction/mutation path,
policy engine, provider adapters, activation catalog, and durable logs.

WASM, package-provided strings, request data, broker data, JWT claims before
verification, DNS answers, provider responses, and dependency outputs are
untrusted. All authority is explicit, least-privilege, deadline-bound,
tenant/workspace-bound, auditable, and fail closed.
