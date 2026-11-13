# Public initial DATA: four real processes and one unchanged client

Classification: **whole CLI target passed on one CI host**. Source
`4b4059fe811d90dcf876507b92ef293c1ae4530d`,
[run43249496779](https://github.com/nikoma/bicdb/actions/runs/43249496779),
job131291102510: four passed, zero failed, zero ignored in45.81s. All-target
and isolated-consumer compilation also passed at that source.

The initial DATA case starts four actual command processes, uses the public
native lifecycle actions and the same externally constructed public client,
and acknowledges three fresh values. Strong reads return the complete nested
object, present JSON null and present empty object; a genuinely unwritten key
returns absence. Partial startup returns explicit unavailable with no write ACK
and no successful Strong absence. No client-selected range or manual root reopen
makes that result pass. The other target tests cover metadata-only registration,
subsequent joins and promoted metadata-voter process kill/same-root reopen in
SEPARATE fixtures. They are not a fifth-node join while this application runs.

The complete test output is retained verbatim in `tests.log.gz` (uncompressed
digest in `summary.json`). The21 emitted JSONL records are copied unchanged
from that output. Code fsyncs each record before echoing it, but this is stdout
custody, not an independent attestation of filesystem flush. Extra control/read
calls are not all journaled, so this is NOT the complete per-RPC physical
attempt/uncertainty/retry ledger required by the Azure gate.

The first record pins source4b, test package1.0.487-beta and observed executable
SHA-256 `1cbbd08d36e6522e905f43e1ad0f7709257e9e0ae63d2880ee0679a1bfffd2a9`
(748,650,376bytes), streamed before process startup. The binary itself is not
retained by this artifact and is not a published release or verified deployment.
The large original seven-file source/log ZIP13961100685 stays in restricted
custody; its digest and retained-output relationship are in `summary.json`.
The older8e test's null source and absent binary digest remain unchanged.

**No fifth DATA stage or automatic data move executes.** Native image/joint wire,
placement/controller cleanup/restart, general cold recovery, converged five-node
throughput, physical RPO/RTO/tails and production admission remain open. A
single-host process test is not independent VM or physical-host qualification.
