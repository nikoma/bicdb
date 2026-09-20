# BicDB application runtime implementation checklist

Release target: `1.0.1-beta`
Compatibility profile: `bicdb-application-v2`

This checklist tracks the concrete in-tree host. A checked compatibility item
may mean either equivalent implementation or deterministic pre-activation
rejection; the normative distinction is recorded in
[`application-runtime-abi-v2.md`](application-runtime-abi-v2.md).

## Runtime contract and package model

- [x] Versioned ABI v2 contract with ABI v1 retained.
- [x] Exact application compatibility profile and fail-before-activation rule.
- [x] Signed manifest, modules, contracts, migrations, dependency lock, hashes,
      service contracts, routes, workers, schedules, capabilities, secrets,
      egress, OpenAPI, provenance, and SBOM.
- [x] Durable stage/install/validate/activate/upgrade/rollback/disable/remove.
- [x] Atomic coherent snapshots and previous-valid-snapshot preservation.
- [x] Dependency graph validation, exact binding, call-cycle rejection, and
      legacy privilege intersection plus bilateral signed callee-authority
      delegation for modular provider services.

## Capability ABI

- [x] Typed database CRUD/UPSERT/query/aggregate/FTS/vector/spatial/JSON paths
      and declared prepared SQL IDs.
- [x] Invocation-scoped transaction/savepoint/row-lock/validator/outbox handles.
- [x] Broker publish/on-commit/consume/ack/nack/retry/delay/dead-letter/groups.
- [x] Host-brokered typed plugin services with deadlines, cancellation context,
      actor propagation, optional transaction propagation, depth, cycles, and
      stable errors.
- [x] Host clock, monotonic clock, secure/test random, and UUID.
- [x] Opaque versioned secrets plus signing, verification, HMAC, encryption,
      decryption, derivation, rotation metadata, and narrow plaintext policy.
- [x] Trusted HTTP egress with destination/DNS/port/TLS/mTLS/redirect/SSRF,
      size/deadline/rate/concurrency, credential, pooling, audit, and quota
      controls.
- [x] Provider-backed bounded streaming blobs, metadata, hash, attachments,
      signed URLs, scanning hooks, and transaction-aware metadata.
- [x] Quota-controlled logs, traces, metrics, audit, evidence, flight recorder,
      and operation timing.

## Authority and data integrity

- [x] Immutable trusted actor context across HTTP, DB, services, broker, jobs,
      schedules, realtime, audit, and evidence.
- [x] Native unforgeable `MutationGrant`, bound below Rust/SQL/pgwire mutation
      entry points to transaction, actor, tenant, workspace, relation,
      operation, row/prefix, expected version, columns, cascade closure, row
      budget, statement budget, lifetime, and audit metadata.
- [x] Append-only and immutable-field policy, optimistic versions,
      tenant/workspace immutability, ledger/finance/touched-subject commit
      validators, durable audit evidence, and publish-on-commit.
- [x] Strict transactional outbox with stable message IDs, replay
      deduplication, causation/correlation/trace/actor/scope metadata, and
      idempotent-consumer support.

## Host, HTTP, and resources

- [x] One host composing pgwire, HTTP/TLS, JWT/OIDC, trusted proxies, actor
      construction, routes, catalog refresh, workers, schedules, realtime,
      graceful drain, health, readiness, metrics, and management.
- [x] Route templates, repeated query/header values, cookies, JSON/form/
      multipart/binary bodies, trusted client/origin/deadline/trace context.
- [x] Typed/repeated response headers, multiple cookies, JSON/text/binary,
      bounded response streams, SSE, trailers, structured errors/retry data.
- [x] Host CORS, compression policy, cache/idempotency controls, size/rate/
      admission limits, authentication, CSRF, trusted proxy, and OpenAPI.
- [x] Signed resource contracts for schema/DTO/validation/routes/CRUD/filter/
      search/sort/page/soft-delete/version/scope/privacy/cache/audit/events.
- [x] Generic reusable resource interpreter with list/get/create/upsert/update/
      delete/restore, validation, policy, redaction, idempotency, OpenAPI, audit,
      and events.
- [x] Native multi-field raw-text FTS projection without duplicate
      `search_text` rows.

## Migration, supervision, and performance

- [x] Versioned BicDB migration plans with forward/compatibility/transform/
      activation/rollback/irreversibility/schema/contract/dependency metadata.
- [x] PostgreSQL-only features mapped to native capabilities or rejected before
      staging; migration application is durably ledgered.
- [x] Worker/schedule supervision, visibility/retry/dead-letter behavior,
      durable actor/event envelopes, health, and draining.
- [x] Precompiled module reuse, shared engine/linker/epoch watchdog, bounded
      fresh stores, per-route/plugin admission, off-reactor execution,
      bounded-copy buffers, provider connection pooling, backpressure, and
      operation timings.

## Explicit profile rejections

- [x] Repeatable-read and serializable isolation.
- [x] Arbitrary/dynamic SQL, PL/pgSQL authority, advisory locks, PostgreSQL
      event triggers, unrestricted PostGIS/Timescale/materialized views/
      `pgcrypto`.
- [x] Raw sockets, filesystem, environment, process, threads, and WASI.
- [x] Inbound request streams, WebSocket callbacks, and online row rewrites in
      the in-tree host until trusted adapters implement equivalent semantics.

## Release evidence

- [x] Forged/expired/cross-transaction handles and actor metadata rejected.
- [x] Undeclared relation, SQL, dependency, secret, egress, and queue access
      rejected.
- [x] Cross-tenant/workspace, field-redaction, role/scope, grant-scope,
      optimistic-conflict, rollback/savepoint, multi-table, and pgwire-bypass
      coverage.
- [x] CRUD/UPSERT/filter/search/sort/page/soft-delete/restore/idempotency/
      OpenAPI/audit/event vertical slice.
- [x] Commit crash-window recovery, duplicate delivery, consumer retry/
      dead-letter, failed activation, process reopen, upgrade, and rollback.
- [x] `1.0.1-beta` workspace version and release notes.
