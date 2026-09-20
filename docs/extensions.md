# BicDB extensions

BicDB extensions are versioned, capability-declared WebAssembly packages. They
can register pure functions, index callbacks, HTTP routes, durable database or
queue event handlers, and observability callbacks without linking third-party
code into the database process. Storage providers use the same manifest
contract but require a trusted native adapter because they participate in
durability, recovery, replication, and backups.

This document covers ABI v1. New capability-mediated applications should use
the [ABI v2 specification](application-runtime-abi-v2.md), the
[author guide](application-runtime-author-guide.md), and the
[operator guide](application-runtime-operator-guide.md).

## Design boundaries

The host, not the module, owns every durable or privileged resource:

- BicDB owns the database, transaction, durable broker, package directory, TCP
  listeners, authentication, RLS context, retry policy, and dead-letter queue.
- The module is an import-free WASM component. It receives one bounded JSON
  invocation and returns one bounded JSON result.
- A manifest declaration is a request, not authority. Activation and dynamic
  resource creation intersect declarations with host policy.
- Published packages are immutable files named by SHA-256. A new runtime
  snapshot is completely validated before one atomic pointer swap. A failed
  load leaves the previous snapshot serving requests.
- Each invocation gets a fresh Wasmtime store and instance. Mutable globals and
  linear memory do not leak between tenants or requests.

The import-free ABI deliberately prevents arbitrary filesystem, socket, clock,
environment, and WASI access. A future host-call ABI can add narrowly scoped
database operations; ABI v1 does not give a module a raw `BicDb` or SQL handle.

## Current support

| Registration | ABI v1 behavior |
| --- | --- |
| Functions | Declared exports can be called through `ExtensionRuntime::invoke`; automatic SQL `CREATE FUNCTION` binding is not part of ABI v1. |
| Indexes | Versioned/index-format declarations and bounded callback dispatch are available; an access-method adapter must integrate a callback with a specific planner/index implementation. |
| Storage | Manifest contract only for trusted native adapters. WASM storage invocation is rejected. |
| HTTP routes | Catalog-backed routes, authorization gates, atomic route snapshots, and an optional Axum/Tokio adapter. |
| Database events | `INSERT`, `UPDATE`, and `DELETE` payloads publish to a durable BicDB queue. Explicit SQL transactions publish on commit and discard on rollback. |
| Queue events | Consumer groups, visibility timeouts, bounded retries, explicit ack/nack, and DLQ delivery through BicDB's broker. |
| Observability | Declared callbacks can be dispatched through the runtime. |
| Network egress | No network imports in ABI v1. A declaration alone does not enable egress. |
| Dependencies | Catalog-only semantic-version, ABI, capability, and optional SHA-256 constraints with dependency-first atomic activation. |

That distinction is important: an `instant_rest` extension can own route
behavior today, but arbitrary table CRUD needs a host adapter that performs the
actual BicDB operations under the request's authenticated/RLS context. Do not
smuggle database credentials or a second SQL client into the module.

## Author a module

The runnable example is
[`examples/extensions/hello-extension`](../examples/extensions/hello-extension/).
Its crate disables native host features:

```toml
[lib]
crate-type = ["cdylib"]

[dependencies]
bicdb-extension = { path = "../../../crates/bicdb-extension", default-features = false }
serde_json = "1"
```

An author may implement `BicDbExtension` and use `ExtensionRegistrar` to build
and validate a manifest:

```rust
use bicdb_extension::{
    BicDbExtension, ExtensionCapability, ExtensionIdentity, ExtensionRegistrar,
    HttpMethod, HttpRouteRegistration, Result, RouteAuth,
    EXTENSION_ABI_VERSION,
};

struct Hello;

impl BicDbExtension for Hello {
    fn identity(&self) -> ExtensionIdentity {
        ExtensionIdentity {
            name: "hello_extension".into(),
            version: "1.0.0".into(),
            abi_version: EXTENSION_ABI_VERSION,
            description: "Hello route".into(),
        }
    }

    fn register_routes(&self, registrar: &mut ExtensionRegistrar) -> Result<()> {
        registrar
            .declare(ExtensionCapability::HttpRoutes)
            .register_route(HttpRouteRegistration {
                name: "hello".into(),
                method: HttpMethod::Get,
                path: "/hello".into(),
                export: "hello".into(),
                auth: RouteAuth::Public,
            });
        Ok(())
    }
}
```

Build tooling serializes that validated manifest to static JSON. The module
exports it and one JSON handler with `export_bicdb_extension!`. See the example
for a complete handler with structured error results.

Build it:

```bash
rustup target add wasm32-unknown-unknown
cargo build --release \
  -p bicdb-extension-example \
  --target wasm32-unknown-unknown
```

The package is
`target/wasm32-unknown-unknown/release/bicdb_extension_example.wasm`.

## Declare extension dependencies

Dependencies are declarations, never download instructions. BicDB resolves
them only against packages an operator has already installed in the durable
extension catalog:

```json
{
  "dependencies": [{
    "name": "template_core",
    "version": ">=1.2, <2.0",
    "abi_version": 1,
    "optional": false,
    "capabilities": ["functions"],
    "module_sha256": null
  }]
}
```

The Rust registrar offers the equivalent `require_extension` method:

```rust
use bicdb_extension::ExtensionDependency;
use std::collections::BTreeSet;

registrar.require_extension(ExtensionDependency {
    name: "template_core".into(),
    version: "^1.2".into(),
    abi_version: 1,
    optional: false,
    capabilities: BTreeSet::new(),
    module_sha256: None,
});
```

Activation resolves the complete graph, validates every local package, detects
cycles, and activates staged dependencies before their consumers. None of the
graph is changed if preflight validation fails. Optional dependencies are
activated when installed, ignored when absent, and ignored by an active runtime
when disabled.

`ALTER EXTENSION name DISABLE RESTRICT` and `DROP EXTENSION name RESTRICT`
refuse to break required consumers. `DISABLE CASCADE` disables active consumers
as well. `DROP ... CASCADE` disables consumers but retains their immutable
installations, then removes the requested extension; reinstalling the
dependency permits those consumers to be activated again.

The manifest schema rejects unknown top-level and dependency fields. This is
intentional: a misspelled security or dependency constraint must not be
silently ignored.

## ABI v1

A module exports:

| Export | Signature | Meaning |
| --- | --- | --- |
| `memory` | WASM memory | Invocation exchange memory |
| `bicdb_extension_abi_version` | `() -> u32` | Must return `1` |
| `bicdb_extension_manifest_ptr` | `() -> u32` | Static manifest pointer |
| `bicdb_extension_manifest_len` | `() -> u32` | Manifest byte length |
| `bicdb_extension_alloc` | `(u32) -> u32` | Allocate input |
| `bicdb_extension_dealloc` | `(u32, u32) -> ()` | Release input/output |
| `bicdb_extension_invoke` | `(u32, u32) -> u64` | Return pointer in the high 32 bits and length in the low 32 bits |

Input is an `ExtensionInvocation` JSON object:

```json
{
  "id": "request-or-message-id",
  "kind": "http_route",
  "target": "hello",
  "payload": {},
  "context": {
    "role": null,
    "tenant": null,
    "deadline_unix_ms": null,
    "trace_id": "trace-123",
    "metadata": {}
  }
}
```

Output is `ExtensionInvocationResult`:

```json
{
  "status": 200,
  "headers": {"content-language": "en"},
  "body": {"message": "hello"},
  "ack": true,
  "retry_after_ms": null,
  "error": null
}
```

For event calls, `ack=false`, status `429`/`5xx`, a non-null `error`, a trap, a
timeout, or malformed output causes a nack. Delivery retries with bounded
backoff and then moves to the binding's DLQ.

## Install and activate

Package bytes are installed before catalog activation:

```rust
use std::fs;
use bicdb_core::BicDb;
use bicdb_extension::host::{
    ExtensionPackageStore, WasmExtension, WasmHostConfig,
};
use bicdb_sql::SqlSession;

let mut db = BicDb::open("./data")?;
let config = WasmHostConfig::default();
let store = ExtensionPackageStore::open(
    db.data_path().join("extensions/packages"),
)?;
let bytes = fs::read("./hello_extension.wasm")?;
let hash = store.install(&bytes, config.max_module_bytes)?;
let package = WasmExtension::load(&bytes, config)?;
let manifest = serde_json::to_string(package.manifest())?
    .replace('\'', "''");

let mut sql = SqlSession::new(&mut db);
sql.execute(&format!(
    "CREATE EXTENSION hello_extension \
     FROM MODULE '{hash}' MANIFEST '{manifest}'"
))?;
sql.execute("ALTER EXTENSION hello_extension ACTIVATE SINGLE NODE")?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

`CREATE` records a staged installation. `ACTIVATE SINGLE NODE` refuses to
activate unless the package exists locally, its SHA-256 matches, the embedded
manifest exactly matches the catalog, its ABI is supported, and every limit is
within host policy.

An already-active single-node extension can be upgraded without publishing a
partially loaded module:

```sql
ALTER EXTENSION hello_extension
UPDATE FROM MODULE '<new-sha256>' MANIFEST '<new-manifest-json>';
```

Install the new package first. Then refresh the runtime with
`sync_extension_runtime`; that call builds the complete new module/route
snapshot before atomically publishing it. On error, the prior snapshot and
prior immutable package remain usable.

Other lifecycle commands are:

```sql
ALTER EXTENSION hello_extension DISABLE;
DROP EXTENSION hello_extension RESTRICT;
DROP EXTENSION hello_extension CASCADE;
```

`RESTRICT` protects dependent resources, websites, subscriptions, and
extensions. `CASCADE` removes owned resources and disables extension consumers.
All catalog DDL participates in SQL transaction/savepoint rollback.

## Dynamic REST resources

The extension manifest must declare the selected route export, the
`http_routes` capability, and read permission for the relation:

```sql
CREATE RESOURCE patients
USING EXTENSION instant_rest
FROM TABLE public.patients
WITH (
  path = '/patients',
  methods = 'GET,POST',
  auth = 'rls',
  openapi = true,
  export = 'handle_resource'
);
```

Route identities are unique by method and exact path. Authorization modes are
`public`, `authenticated`, `rls`, and `admin`. The runtime fails closed unless
the host supplies a sufficient `RouteAuthorization`; a bearer token merely
being present does not assert that RLS was checked.

Enable the optional Tokio/Axum adapter:

```toml
bicdb-extension = { version = "...", features = ["http"] }
```

```rust
use bicdb_extension::http::{serve_extension_http, ExtensionHttpConfig};
use bicdb_extension::host::{
    ExtensionPackageStore, ExtensionRuntime, WasmHostConfig,
};
use bicdb_sql::sync_extension_runtime;

let packages = ExtensionPackageStore::open(
    db.data_path().join("extensions/packages"),
)?;
let runtime = ExtensionRuntime::new(packages, WasmHostConfig::default())?;
sync_extension_runtime(&db, &runtime)?;

let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
serve_extension_http(listener, runtime, ExtensionHttpConfig::default()).await?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

The embedding host chooses and binds the listener. `serve_extension_http`
runs the already-built router on that listener; it never exposes the listener,
Tokio runtime, or a socket host call to WASM.

The default authorizer is anonymous and therefore serves only `public` routes.
Production hosts should provide an `ExtensionHttpAuthorizer` that validates
their session/token and returns `Authenticated`, `RowLevelSecurityChecked`, or
`Administrator` with the corresponding `InvocationContext`.

## Versioned websites

The runnable
[`examples/extensions/website-renderer`](../examples/extensions/website-renderer/)
module turns a versioned JSON site bundle into HTML. It does not run Tokio.
BicDB's native host runs Tokio/Axum, matches the hostname and mount path,
applies authorization, and sends a bounded invocation to the renderer.

Build and install the renderer exactly like any other extension:

```bash
cargo build --release \
  -p bicdb-website-renderer-example \
  --target wasm32-unknown-unknown
```

After placing the resulting module in `ExtensionPackageStore`, create and
activate its executable extension installation, then create a website:

```sql
CREATE WEBSITE docs
USING EXTENSION website_renderer
WITH (
  host = 'docs.example.com',
  path = '/',
  export = 'render_website',
  auth = 'public'
);
```

`host = '*'` creates a host-agnostic mount. A specific hostname wins over a
wildcard, and the longest matching mount path wins. Exact dynamic REST
resources are checked before website mounts. Websites serve `GET` and `HEAD`.

Publish a complete immutable release and activate it with one statement:

```sql
PUBLISH WEBSITE docs VERSION '1.0.0'
CONTENT '{
  "stylesheet": "body { font-family: system-ui }",
  "pages": {
    "/": {"title": "Docs", "html": "<main><h1>Docs v1</h1></main>"},
    "/404": {"title": "Not found", "html": "<h1>Not found</h1>"}
  }
}'
ACTIVATE;
```

The example directory contains `site-v1.json` and `site-v2.json`. Real tooling
should read a bundle, validate it, escape SQL single quotes, and submit it
through a parameterized SQL call when the client supports parameters. Release
versions are semantic versions and immutable; publishing the same website and
version twice is rejected.

Deploy a second release:

```sql
PUBLISH WEBSITE docs VERSION '2.0.0'
CONTENT '<site-v2-json>'
ACTIVATE;
```

Then refresh the native runtime:

```rust
sync_extension_runtime(&db, &runtime)?;
```

Publication writes the immutable release first and changes one active-version
catalog pointer last. Runtime refresh builds the complete module, route, and
content snapshot before swapping it into service. A failed refresh leaves the
previous in-memory snapshot serving. Existing requests retain their previous
snapshot until completion.

Instantly switch the pointer back and publish a new runtime snapshot:

```sql
ALTER WEBSITE docs ROLLBACK;
```

```rust
sync_extension_runtime(&db, &runtime)?;
```

`ROLLBACK` swaps the active and previous pointers, so it can also be used to
roll forward again. Any retained release can be selected explicitly:

```sql
ALTER WEBSITE docs ACTIVATE VERSION '1.0.0';
```

`DROP WEBSITE docs RESTRICT` preserves release history and refuses the drop.
`DROP WEBSITE docs CASCADE` removes the website and its releases. Both forms,
publishing, activation, and rollback participate in SQL transaction/savepoint
undo.

Website JSON is capped at 16 MiB by the catalog and must also fit the
extension's requested input limit and the host's `WasmHostConfig` input limit.
The effective bound is the smallest limit. The current renderer deliberately
accepts trusted administrative HTML from a release and adds a restrictive CSP;
do not publish untrusted user HTML without a sanitizer.

## Database and queue events

A database binding publishes full before/after `Record` values to a durable
queue. The manifest must declare the export, `database_events`, relation read
permission, and permission to publish to the queue:

```sql
CREATE EVENT SUBSCRIPTION patient_changes
USING EXTENSION instant_rest
ON TABLE public.patients EVENTS (INSERT, UPDATE, DELETE)
QUEUE 'patients.changed'
EXECUTE 'on_patient_changed'
WITH (max_attempts = 8, visibility_timeout_ms = 30000);
```

Queue-origin work uses an existing queue/group and requires `queue_events` plus
consume permission:

```sql
CREATE EVENT SUBSCRIPTION patient_worker
USING EXTENSION instant_rest
ON QUEUE 'patients.work' GROUP 'instant-rest'
EXECUTE 'on_patient_work'
WITH (max_attempts = 5, visibility_timeout_ms = 30000);
```

A supervisor calls `run_extension_event_once` for each value returned by
`active_extension_event_bindings`. Multiple processes may use the same group:
BicDB's broker distributes messages, maintains visibility, and prevents an ack
from the wrong consumer. Handlers must still be idempotent because delivery is
at least once.

Explicit SQL transactions provide rollback-coupled publication:

- commit durably applies row writes and then flushes prepared broker publishes;
- rollback/savepoint rollback truncates buffered event publishes;
- the queue and broker control log survive restart;
- failed handlers retry and eventually land in `dlq:<queue>/<group>`.

There is currently a documented crash window between the durable row commit and
the broker flush (`STREAM_BROKER.md`). This is rollback-correct but is not a
strict transactional-outbox guarantee across process/power failure. Workloads
that cannot tolerate that gap should write an application outbox row in the
same transaction and relay it idempotently. Autocommit statements publish
immediately after their row write and have the same ordering constraint.

## Limits and failure behavior

`ExtensionLimits` requests:

- linear memory bytes;
- Wasmtime fuel;
- wall-clock milliseconds;
- input/output bytes;
- concurrent invocations.

The effective value is the lower of manifest and host policy. Overload is
rejected rather than queued without a bound. Each invocation starts from a
fresh instance, and a watchdog interrupts code that exceeds its deadline.

The package store:

- validates lowercase 64-character SHA-256 identities;
- rejects symlinks and non-regular package files;
- fsyncs a same-directory temp file before rename;
- atomically renames to `<sha256>.wasm`;
- removes only its own incomplete temp files on open;
- never removes a published package implicitly;
- exposes explicit `remove_unreferenced(retained_hashes)` garbage collection.

## Cluster rollout

Do not run `ACTIVATE SINGLE NODE` for a distributed deployment. A cluster
controller should:

1. copy the immutable package to every required node;
2. verify SHA-256 and embedded manifest on every node;
3. build a candidate runtime snapshot on every node;
4. quorum-commit an `ExtensionActivation` containing catalog/topology
   generations plus the ready/required node sets;
5. publish the catalog generation;
6. garbage-collect an old package only after no retained catalog/snapshot
   references it.

`ExtensionActivation::validate` fails closed when any required node is absent
or a clustered activation lacks a quorum commit. This prevents routing traffic
to a node that cannot execute the active generation.

## Testing checklist

Before distributing an extension, test:

- manifest validation and duplicate registrations;
- unsupported ABI and missing exports;
- package hash mismatch and truncated package files;
- input, output, memory, fuel, deadline, and concurrency limits;
- anonymous/authenticated/RLS/admin route decisions;
- transaction commit, rollback, retry, visibility expiry, and DLQ redrive;
- handler idempotency under duplicate delivery;
- failed upgrade retaining the previous runtime snapshot;
- restart/reopen with the catalog and queue state intact;
- all target operating systems and the exact WASM target used for release.
