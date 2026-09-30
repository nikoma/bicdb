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

Hosts must roll back any open transaction when the runner fails, including
interrupts and out-of-memory errors, and reject network requests while a
transaction is open. They must persist retry requests and acknowledge the
delivery only after successful completion. Stored version activation, durable
jobs, capability integration and the EHR acceptance scenario remain in progress
in [the scripting roadmap](scripting-roadmap.md).
