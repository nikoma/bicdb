# Native bridge post-write correction: source scope

This candidate combines the reviewed native source with the actual main513
backend split and the documentation514 checkpoint. It depends on that
documentation delivery. Its own required checks must pass before merge; it
does not change the failed historical results below.

This is the broader previously unmerged native batch plus the narrow correction:
Registered/Joined image/history/row/base/catalogue receivers, typed writer
domains, remote NEW/PREPARE/native DECISION components, retained custody and
uncertainty, and native broker refusal. The [integration review](ha-recovery-integration-review-2026-11-13.md)
and clearly historical [512 snapshot](../releases/1.0.512-beta.md) retain those
separate boundaries. It is not merely a seven-file delivery.

PR201 tested native source with main511 version metadata. Its native job
132526673945 in run43621069383 finished330 passed/13 failed/one ignored.
Twelve cases reached the synchronous native bridge write, then refused at the
strict postcondition because the publisher still held its own Controlled
callback. The postcondition's CLOSED catalogue read requires quiescence. Its
old comment claiming otherwise was incorrect. This is the executed
[issue195](https://github.com/nikoma/bicdb/issues/195) boundary, separate from the
earlier503 pre-pin timeouts.

The correction releases only that completed callback after the ordinary
`in_paged_transaction` and actual receipt construction return. The original
Frozen, native journal, DB/service bindings and metadata are retained. The
unchanged complete postcondition runs with quiescence required, followed by
the same local scope, metadata, caller, admission and version checks. There is
no permissive gate overload, new transaction, replay, timer extension or
substituted Session. A new component control retains two genuine Controlled
permits and requires another callback to keep blocking after the first drops.

Five of the six new post-collection negative controls passed in PR201. The
Store replacement control failed in its diagnostic snapshot before invoking
the publisher. Its actual old slot had been revoked. The corrected test reads
only the retained custody UUID as DATA, records the strict lease-validation
error separately, then invokes the real publisher and preserves the exact
stale-slot, unchanged native counters and retained Frozen assertions. A
consumed custody reports absence; no raw permit or current authority is
returned by the diagnostic getter. The separately corrected issue196
foreign-service test passed in PR201.

Failure-only test diagnostics retain actual protected CURRENT/archive raw-byte
hashes, native xid/LSNs, gate state and clocks captured before forensic reads.
The fixture emits them after the synchronous call releases service/DB guards,
before its original assertion. They distinguish a new commit returned in this
call from reconciliation of an existing committed record. They retain no live
owner, guard, error or raw record. Coverage is the last synchronous publisher
call; caught retries may replace an unconsumed report, so this is not a complete
RPC ledger. The original API error is preserved.

The twelve historical traces left0–82ms after their physical postcheck.
Subsequent strict checks can still expose an actual timeout; this source
correction does not establish that all twelve cases pass or identify a CPU/I/O
cause. New source checks/results must name their exact head. The backend port
preserves main513's generic stage facade and moves the frozen local custody
changes into `local_backend`; its request/retry budgets are unchanged.

Automatic native placement, full remote handoff through Serving, cold recovery,
transparent distributed SQL and original transaction-effect replication remain
open. This candidate excludes the separate uncompiled future Joined-wire and
archive-lineage proposals. It produces no binary, published release, deployment
or physical HA qualification.
