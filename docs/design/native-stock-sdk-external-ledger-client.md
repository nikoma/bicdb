# Stock SDK client for a future independent qualification experiment

The original b440 source-time design below is retained as historical scope.
The current example-only external-sink successor is described at the end and
in [its design](native-colocated-probe-external-durable-sink.md). Neither source
availability nor offline checks establish Rust/SDK or physical qualification.

Current tooling includes a bounded offline verifier and cross-run read-only
reconciliation generator/checker. The external-sink source
`9c9d2ddcf83b8c913ab54dc6a67c4a688f77e36c` passed 61 focused Python controls,
including local file and loopback TLS operations. The Rust producer's compilation
is now covered by the all-target type-check checkpoint at source6de
`6de646beb603accf39358017d8e0c83c39c741b1` (1.0.520-beta candidate). This does
not build a retained optimized probe binary or execute its SDK/cluster workload.
The actual producer/collector experiment remains unexecuted, and later local
source additions do not inherit that type-check pass.

Later public source700 failed all-target/Core/Native test compilation before
Core/native cases; its separate application and component passes do not execute
this producer or collector. The source-only compilation correction has not run.
Statements below about the first leaf's missing verifier/generator describe
that historical b440 design;
they are not the current Python implementation inventory. See the
[current tooling overview](../../scripts/platform/native-sdk-ledger/README.md).

## Historical b440 source-time design

This source leaf is based on `4efcfc2cccc51307721777ad7d53a251e67bbbf3`
(1.0.512-beta candidate). It has not been compiled or run. The current native
automatic-relocation and cold-handoff qualification gates remain open. This
client is not authorization to provision Azure resources or admit production.

`crates/bicdb-core/examples/native_external_ledger.rs` prepares a bounded,
independent functional probe using one unchanged `ClusterEventClient`. It calls
the real `append` and typed `get_strong` methods and attaches the existing
passive `ClusterRpcEvidenceObserver`. It does not implement route discovery,
choose physical ranges/leaders, alter SDK retries, invoke management APIs or
fall back to legacy eventual reads. Its fixed plan supports at most 1,024
operations and 32 KiB values; it is not a sustained-capacity load generator.

The source must first compile, pass independent journal-verifier controls and
execute an isolated local smoke before any physical use. Those checks have not
run. The future server/controller path must separately support the operation
being tested. A useful diagnostic probe cannot make an unsupported native
automatic controller or cold restart safe.

## Inputs and identity

The future binary accepts four positional paths:

```text
native_external_ledger CONFIG_JSON TOPOLOGY_JSON PLAN_JSON NEW_OUTPUT_DIRECTORY
```

Configuration and topology use the actual public SDK types. The client uses a
test-only, authenticated member identity and refuses plaintext. Its independent
machine placement is separate from that member principal: archive both actual
client and server VM metadata. A member identity in a packet does not establish
the physical location of the client. Never copy production credentials.

The eventual build must set `BICDB_EXPERIMENT_SOURCE_SHA` to the exact clean
source commit. The client records that build claim, its actual executable hash,
crate version, debug-assertion mode, input hashes and initial topology. The
source claim must be checked against the build manifest; an environment string
does not prove source provenance. Archive the actual server binary/commit and
configuration independently. Version identity is not release qualification.

The current transport has no dedicated client-only principal on this path.
`authorize_cluster_data_caller` in `distribution_transport.rs` requires an APP
caller to be an Active metadata voter with its member certificate fingerprint;
typed reads require an Active registered member. A standalone CA-signed
controller identity can fetch bootstrap but cannot append. Physical use therefore
still needs an explicit test identity/placement arrangement. Do not silently add
a sixth voter or data-placement member, or copy a server's private key. An
explicit isolated test-member credential remains a privileged host identity,
not ordinary application authentication. Its quorum/placement effects and
actual key custody must be archived. No authentication change is introduced by
this probe; it does not establish unchanged PostgreSQL application access.

Example plan DATA:

```json
{
  "run_id": "019a80b8-6594-7000-8000-000000000001",
  "phase": "functional-before-expansion",
  "operations": [
    {
      "operation": "append",
      "logical_id": "write-1",
      "namespace": "isolated-qualification",
      "key": "fresh-record-1",
      "value": {"business": {"amount": 17, "nullable": null}}
    },
    {
      "operation": "strong_read",
      "logical_id": "read-1",
      "namespace": "isolated-qualification",
      "key": "fresh-record-1",
      "expected": {"business": {"amount": 17, "nullable": null}},
      "acknowledged_by": "write-1"
    }
  ]
}
```

An ACK reference must name an earlier exact append in that plan. At runtime the
read records whether that append actually returned an ACK. An expected value
following a failed write does not invent an acknowledgment. A read retains
`found` separately from `value`, so a stored JSON null cannot become successful
evidence for an absent record. An ACK-linked successful Strong absence is a
failure even if a later read recovers visibility.

The whole plan is validated before network dispatch, including the public SDK's
namespace/key identity validator. The actual canonical event record ID is
retained with each attempt. Duplicate logical IDs and duplicate append keys
are refused. There is no outer write retry after an SDK
error. Internal retries remain the SDK's own behavior and appear in RPC evidence.
The future orchestrator must retain every original plan and separately identify
any explicit later retry; it cannot quietly replay uncertain writes as new work.

## Complete attempt and reply evidence

The new output directory is private and must not already exist. Exact input
configuration, topology and plan bytes are retained as private, fsynced files;
their directory entries are synced before SDK dispatch. Every logical
attempt is written and fsynced before invoking the SDK. The journal preserves
the exact command/value, its hash, all actual SDK RPC fields and raw frames,
logical outcome, complete SDK error chain and raw latency samples. The observer
binds each RPC UUID to its original logical operation at `invoked`; later phases
use that retained mapping. Attribution is never inferred from packet routing.
The client invokes only one logical operation at a time.

An actual SDK append success is separately recorded as `sdk_ack_observed` and
fsynced. Its subsequent terminal record links the returned persistence receipt.
An SDK error remains `write_error_unresolved`, including a lost response after
possible commit. Raw database replies may show more than the SDK returned; an
offline verifier must report database-success replies separately from externally
recorded application ACKs. No timeout or error establishes rollback.

Each journal line includes a monotonic sequence and the SHA-256 of the preceding
raw line bytes excluding the newline. A successor is formed only after the prior
line's fsync returns. A visible trailing line alone does not prove its fsync
returned. Retain the independently observed process exit and captured stdout
completion receipt, validate their link to the exact final line, and distinguish
complete, incomplete and persistence-uncertain evidence. Hashes establish
internal integrity, not authenticity or a physical durability guarantee.

The new directory's parent entry, initial file and directory are fsynced.
Journal persistence failure fences
further appends. The SDK observer retains its existing 65,536-record / 1 GiB
limits; the outer journal has a 2 GiB ceiling. Exhaustion stops the plan. Where
the journal still accepts records, completion reports list unattempted
operations; after persistence fails, the original plan defines missing outcomes.
A failed observer is never replaced or reset. No record
is truncated to permit continuation. The future controller must monitor free
disk/RAM and terminate only its own client process at the agreed boundary.

Filesystem write/fsync can stall in the kernel. This source uses synchronous
file IO and does not establish a per-write wall-clock bound. Run it under an
independent process supervisor with an absolute deadline; capture the original
ledger and classify unfinished operations as uncertain on termination. An
external timeout is not evidence that a blocked write rolled back or that the
client met its SDK request deadline. Local default SDK deadlines remain unchanged.

## Interpretation and outstanding validation

Exit 0 means all planned operations returned the expected functional outcome;
exit 1 means a completed plan contains failures; exit 2 means incomplete/fenced
execution or a client/ledger failure. Every summary remains `not_qualified`.
Raw SDK duration includes synchronous passive-evidence IO but excludes later
ACK/terminal fsync. A separate logical duration includes attempt/ACK persistence
up to terminal append entry. Neither duration is a certified RTO.

Before this tool can supply physical evidence, implement and exercise its
independent bounded offline verifier: torn rows, altered hash links, missing RPC
phases, trailing unproven ACKs, wrong ACK references, null-versus-absence,
observer exhaustion and controller termination must fail closed while retaining
every attempt. A positive local fixture only tests this instrumentation.

Read-only reconciliation plans must cover every prior write, including failed,
uncertain and retried identities, with original expected hashes and source ACK
evidence. This first source leaf does not generate those plans or establish
cross-run reconciliation completeness. That remains a blocker for physical use.

A sustained four-versus-five-node comparison still requires a separate bounded
load driver, equal workload/latency/error budgets, stable completed convergence,
full attempt reconciliation, verified server/resource placement and repeated
fault runs. This sequential probe does not demonstrate aggregate capacity gain,
ordinary PostgreSQL transparency or fault-safe automatic rebalance.
# External-sink successor scope

The original local-only probe above remains unqualified. The separately approved
colocated trusted-member arrangement and example-only external collector are
specified in [the external-sink design](native-colocated-probe-external-durable-sink.md).
They avoid a new database principal; they do not turn this API into ordinary
PostgreSQL access. Preserve every known or possible SDK/server ACK, including
the SDK-Ok-before-explicit-ledger gap. Wrapper ACKs never narrow that corpus.
