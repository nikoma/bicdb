# TypeScript and Lua workflows

`bicdb-script` compiles TypeScript using OXC 0.152.0. The compilation receipt
contains JavaScript, the TypeScript source SHA-256, and the compiler version.
Workers execute the JavaScript, with no runtime TypeScript compilation.
OXC validates syntax and bindings; it does **not** perform TypeScript's full
type checking. Publication pipelines should run `tsc --noEmit` when type
checking is required. Runtime host boundaries must validate all values.

Define a global `function workflow(event)` or `async function workflow(event)`.
Imports, exports, Node APIs and external modules are unsupported. The fresh
QuickJS VM has no filesystem, network, timers, or module loader. Defaults bound
source to 1 MiB, VM memory to 64 MiB, JSON inputs/results to 1 MiB, host calls
to 1,000, pending promise jobs to 10,000, and execution time to ten seconds.
OXC compiler allocation is bounded by source size rather than the VM allocator.
Native host calls must enforce their own deadlines; VM interrupts cannot
preempt blocking Rust code.

Both languages expose the same host method contract:

| API | Host method | Arguments |
| --- | --- | --- |
| `db.one/scalar/execute` | `db.one/scalar/execute` | statement, bound parameters |
| `db.transaction(callback)` | `db.begin`, `tx.*`, `db.commit/rollback` | synchronous callback |
| `http.post` | `http.post` | URL, options |
| `secrets.get` | `secrets.get` | secret name |
| `jobs.retry` | `jobs.retry` | retry options |
| `json.encode/decode` | local JSON codec | value/string |

Lua scripts are workflow bodies with `event` supplied by the runner. JavaScript
defines `workflow`. SQL statement identifiers and authorization come from the
trusted host, never the event. The VM crates do not grant SQL, HTTP, secret,
or broker privileges by themselves. Workflow bindings are separate from RESP
EVAL: transactional Redis scripts receive no external HTTP or secret access.

```typescript
interface Booking { appointment_id: string }
async function workflow(event: Booking) {
  const appointment = db.one("load_appointment", [event.appointment_id]);
  return db.transaction(tx => {
    tx.execute("mark_checked", [appointment.id]);
    return { status: "completed" };
  });
}
```

`bicdb-app-runtime::execute_script_workflow` supplies the shared capability
adapter. It rolls back open transactions after every invocation, including VM
interrupts, and rejects HTTP inside a transaction. SQL is matched to a declared
statement ID or its exact SQL text, then executed through existing parameter,
relation, tenant, role, mutation-grant and result checks. Declared SQL uses `$1`
parameters; the proposed example's `?` placeholder syntax is not implemented.
HTTP uses a declared policy and the configured egress provider. Production
egress checks destinations, DNS/private addresses, redirects, concurrency,
request/response sizes and deadlines. Secrets require an explicit plaintext
read declaration. Retry requests cannot run inside a transaction or be followed
by more host calls.

`execute_script_delivery` loads the pinned version, clamps execution to the
delivery lease and fresh actor deadline, then acknowledges completion or
durably schedules a retry using BicDB's broker. Settlement checks the exact
delivery attempt and unexpired lease, including reused consumer names. Errors
remain unacknowledged; the worker chooses retry/dead-letter classification.
The capability host uses current authority, ignoring serialized role snapshots.

The [TypeScript eligibility example](../examples/workflows/eligibility.ts) and
[Lua equivalent](../examples/workflows/eligibility.lua) exercise the EHR rule.
Their acceptance tests use real BicDB SQL tables and an insurance API test
provider: atomic five-table writes, validated external data, duplicate runs,
revision checks, retry/restart, rollback and hot activation of the revised rule.
Use [the ambient declarations](../examples/workflows/bicdb.d.ts) for editor support
and `tsc --noEmit --strict --lib ES2022` type checking. The SQL declaration and
tenant policy fixture is in `script_workflow_tests.rs`. These tests are not
evidence of a production EHR deployment or healthcare compliance.

`bicdb-workflow::publish_version` persists immutable Lua/JavaScript versions;
TypeScript is compiled before persistence. `activate_version` changes an active
pointer in an ordinary database transaction. `enqueue_on_commit` records an
event receipt and buffers publication into BicDB's durable broker in the same
transaction as the booking. Rollback discards both. Existing jobs retain their
pinned version across activation, restart and retry. Reusing an event ID with
different event data or a different principal is rejected.

These are trusted native embedding APIs. Management authorization belongs to
the caller; do not expose their storage collections or a direct publication
endpoint to untrusted clients. Use a WAL-backed database with fsync enabled.
The worker must revalidate the principal's current authority when executing,
including revocations; serialized role names are not an authorization grant.
Job receipt and inactive script retention require an operator policy.
Receipts bind the canonical job envelope and broker message ID. Generic broker
publishers cannot forge another tenant/principal or replay a copied envelope.
Receipt collections must remain inaccessible to untrusted management callers.
Jobs created with the earlier receipt prototype without `job_sha256` are
rejected; drain/requeue them through trusted booking data before upgrading.
