# Offline verification and external collection for the stock SDK ledger

Most recent completed successful compilation/parser evidence is source
`7e99c9c96970b78a44402c975627c64e001adffa` (1.0.520-beta candidate): its
normal all-target job passed, and
[Core job132883861981](https://github.com/nikoma/bicdb/actions/runs/43737698638/job/132883861981)
executed `native_external_ledger_receipts`: **7 passed, 0 failed, 0 ignored**.
The full Core raw-log SHA256 is
`7c507e92b1ef0a726573c496e89e6572990c20f67deeb320c65a3913f0a3319d`;
the target appears at original raw lines29651–29662. This is parser/schema
execution and compilation, not a linked optimized producer, actual SDK workload
or external-collector run. That source's separate native suite failed396/55/1.

Later public source `d3af39c107951802a612d31b42e70fd4bc90e71c` (same candidate
version) failed all-target/Core/Native test compilation on the new TLS control's
private response-validator call. No current Core/native or ledger-parser cases
ran. Its passing standalone Rails and PGwire results do not test this producer.
The reviewed successor `980b5983c927859c6218edc78e17b68d71eae2ac` is not compiled
or executed at this checkpoint. SQL and remaining-workspace jobs on d3 are still
running. See the [exact source-scoped findings](../../../docs/design/native-520-regression-findings.md).
No preceding pass is inherited, and no physical qualification gate changes.

This directory contains the format verifier, a read-only cross-run reconciliation
plan generator/checker, and external receipt collection/controller tooling.
The normal Core `native_external_ledger_receipts` integration target executed
all seven shared Rust parser controls at source
`2d379154a67eca6d6ed5f8f9b5dbe8d7a9c987bd` (1.0.516-beta): **7 passed, 0 failed**.
See the [source-pinned CI result and limits](../../../docs/design/native-ledger-2d379-failed-acceptance.md).
This supersedes the original checkpoint's uncompiled-parser status. It does
not execute the stock SDK workload or external collection against BicDB.
Source availability and parser tests do not establish a working end-to-end
SDK client, database durability, HA, consistency or scale-out. Later candidate
compilation and runtime results must retain their own source identities.

Earlier successful type-check checkpoint: source `6de646beb603accf39358017d8e0c83c39c741b1`
(1.0.520-beta candidate), tree `de0b68af8d158f39358604590769afb8237115f0`, passed
the normal [all-target CI job132814500830](https://github.com/nikoma/bicdb/actions/runs/43714999361/job/132814500830).
Its `cargo check --workspace --all-targets --locked` includes the automatically
discovered `native_external_ledger` example; Core defaults enable its real TLS
client module. This is **type-checking**, not a linked/optimized experiment
binary, a producer invocation or external-collector acceptance. The separate
Core job failed before its ledger integration target, so no new seven-parser
runtime result is inherited from this checkpoint. Core and Native failed overall.
Later public candidate source700
`700726e90f041815b132cda4f87ca92337f55baa` failed all-target/Core/Native
test compilation before Core/native case execution. Its separate production
helper, Rails, SQL and PGwire passes do not establish a new producer type-check,
linked probe or SDK/collector execution result. The minimal test-compilation
correction remains source-only at this checkpoint.
See the [source6de results and raw log identities](../../../docs/design/native-520-regression-findings.md).

The original producer source was
`b44064c2bf1b22712e5f30d2031aa5d58e907e44`, based on the 1.0.512-beta candidate
`4efcfc2cccc51307721777ad7d53a251e67bbbf3`. Its format and reconciliation
successors preserve those historical inputs. The external-sink successor is
`9c9d2ddcf83b8c913ab54dc6a67c4a688f77e36c`; it adds descriptor-bound collector
storage and strict receipt schemas. Its 61 focused Python controls passed,
including actual loopback TLS and local file operations. Earlier verifier and
reconciliation controls use synthetic journals/replies. None runs a BicDB SDK,
database process, physical fault or cluster expansion.

```sh
python3 scripts/platform/native-sdk-ledger/test_verify.py
python3 scripts/platform/native-sdk-ledger/verify.py \
  --journal /retained/run/journal.jsonl \
  --completion-witness /independently-captured/stdout.json --exit-status 0
```

`plan.json`, `config.json`, and `topology.json` must be the original archived
input bytes next to the journal. `--plan` can select another original plan. Its
hash must still match `run_started`. The completion witness is the JSON printed
by the producer, captured independently after the process returns. The verifier
compares its exact receipt and summary with the final row and checks the supplied
exit status. It does not authenticate that capture, a source-build claim, or a
host. Keep the build manifest and process-supervisor evidence separately.

Verifier exit codes concern **evidence completeness**, not business success:

- `0`: complete, internally consistent evidence with a linked completion witness.
  A fully recorded functional failure can still have verifier exit 0.
- `1`: internally valid prefix with missing evidence, an unfinished RPC, missing
  input archive, or unproven trailing persistence return.
- `2`: invalid input, broken chain/identity, contradictory outcome, or exceeded
  verifier bound. The report retains the valid prefix and labels observations
  after the first defect untrusted. It never resumes accepting a trusted chain.

All reports remain `not_qualified`. `complete_evidence` is not a claim that
acknowledged database writes survived a physical fault.

## What is checked

The verifier hashes each **exact raw UTF-8 line, excluding its newline**, including
the preceding-record link. It checks sequence, run identity, ordering, the actual
plan hash, archived configuration/topology hashes, and canonical event record IDs.
UUID spelling in the original plan is normalized only for identity comparison;
its original bytes remain the hash input. Omitted optional `acknowledged_by`
matches the producer's serialized null. Neither correction rewrites evidence.

Payload hashes use the exact structural JSON span of the journal command value.
They are not computed by Python reserialization, which can change numeric spelling,
map order, escaping, or precision. Type-aware comparisons distinguish true from 1
and present JSON null from absence. Arbitrary-precision decimals in report DATA
are explicitly rendered as `{"json_decimal":"…"}`; original JSON remains in the
input files. This tag is an output representation, never a replacement payload.

Every accepted logical attempt, canonical key/hash, SDK-observed ACK, terminal
failure, unfinished operation, unresolved write, and not-yet-observed plan entry
is reported separately. A linked terminal ACK receipt must name the actual ACK
row/hash and exact SDK reply. Any write error remains unresolved; no timeout,
raw reply, read, or verifier result is treated as rollback. The verifier does not
silently replay or resolve uncertain writes.

The RPC checks retain original SDK attempt UUID attribution, per-attempt phase
order, monotone dispatch/byte fields, request/response correlation, original APP
payload, typed Strong request mode/presence, and SDK reservation coverage. Global
reservation order can differ from sink persistence order for concurrent callbacks.
Missing reservations, missing finish phases, and successful logical outcomes
without a matching validated APP/Strong reply are evidence gaps. Raw database
`Appended` replies are reported separately from application ACKs; those raw frames
can subsequently fail identity, cancellation, or evidence validation. No HTTP
404 inference applies to this socket protocol. Original typed errors, response
bodies, and raw frames remain in the retained journal, referenced by record number.
RPC attempt counts include discovery and initial calls; the per-operation counts
do not pretend every transport exchange is an application retry.

An ACK row followed by a valid successor establishes that the producer's prior
ACK-ledger append/fsync returned: its successor can be formed only afterward.
A **visible trailing ACK** establishes SDK observation but cannot establish that
the ledger's fsync returned. It remains in `fsync_return_unproven_sdk_acks` and
`unresolved_writes`. A visible trailing completion row likewise requires the
independently captured completion/exit witness. This is a claim about the ledger
producer's execution, not power-loss durability of the database or filesystem.

## Bounds and incomplete captures

The input journal is limited to 2 GiB, the original plan to 40 MiB, and one line
to 64 MiB. `--max-line-bytes` may lower the line limit. The verifier refuses larger
records rather than dropping frames and claiming complete evidence. It allows
at most 70,000 physical rows (the producer has 65,536 SDK reservations plus at
most 3,072 logical records and two run records). SDK accounting checks retain the
65,536-record / 1 GiB limits and charge the original serialized evidence with its
maximum-width reservation field, as the producer's SDK does.

Full raw-frame byte arrays and decoded discovery topology bodies are discarded
from verifier RAM after bounded validation. The original file retains those
bytes. The report retains row references, bounded RPC metadata and type-aware
value comparison digests, not a second raw-frame copy. Actual peak RAM and runtime
at the maximum bounds have not been benchmarked; use an independent supervisor.

A torn or invalid row stops trusted-prefix interpretation. Remaining bounded
lines are scanned for **untrusted** logical attempts/ACKs/terminals and kind counts,
with an explicit 4,096-observation cap and omitted count. Invalid or oversized
untrusted identity fields are marked unavailable; their raw line hashes remain.
Unobserved plan entries must not be called definitely unattempted after corruption
or truncation. A byte/line/row bound can stop that scan; `journal_input` describes
bytes actually read and their hash, not an unread whole-file digest. Retain the
complete original file independently. Ledger IO failure can prevent a final
completion record; no successful final record is assumed.

## Experiment readiness: source d3

This section inspects candidate `d3af39c107951802a612d31b42e70fd4bc90e71c`;
it is source evidence, not a workload or physical result. The actual SDK, example,
external sink and bounded reconciliation remain distinct from the ordinary
[`cluster-events workload`](../../../crates/bicdb-cli/src/cluster_events_command.rs).
That basic command keeps an ACK-only list until after traffic, samples only
20 errors, emits successful-call percentiles rather than raw samples, and has
no RPC observer. Its deployment-default reads are not an explicit Strong
contract across all supported deployment modes. It fails the requested complete
attempt/uncertainty/retry-ledger requirement; capturing stdout cannot recover
unemitted failed keys or attempts.

The [stock producer](../../../crates/bicdb-core/examples/native_external_ledger/client.rs)
already persists each complete logical intent before SDK entry and explicitly
uses Strong reads. The actual
[transport observer](../../../crates/bicdb-core/src/distribution_transport/evidence.rs)
retains observed invocation/retry/discovery frames, original identities,
destination, dispatch facts and errors. Range/epoch/leader/topology evidence is
available through actual serialized requests/replies when reached; a pre-routing
failure can legitimately lack those fields. Keep them unavailable rather than
backfilling from a later topology. Complete per-replica watermarks, readiness
and restoration events require separate node/controller evidence; advertised
leader DATA is not independent proof of authority. SDK/server success, delivered wrapper ACK,
external receipt and uncertainty retain their separate meanings. The source
contracts and prior component controls pass within their recorded scope;
current actual SDK-to-collector execution remains **not tested**.

The [bounded reconciliation contract](RECONCILIATION.md) accepts at most
16 original sources, 8,192 targets, 32 KiB per raw value and 64 MiB total retained
expected-value spans per invocation. It refuses excess input rather than
returning a passing subset. A full 8 GiB corpus needs an additional pinned
campaign/full-corpus index and aggregate coverage over every bounded bundle;
that layer is not implemented here. Handwritten splitting or canary reads do
not prove complete coverage. This is a **failed source requirement for the full
experiment**, not an observed database loss or a reason to raise limits silently.

Raw `sdk_elapsed_ns` includes synchronous RPC evidence callbacks and their local
and external fsync work. It excludes earlier intent and later explicit
ACK/terminal appends. `client_elapsed_before_terminal_fsync_ns` has its own
broader scope and still excludes terminal fsync. These fields cannot yield
uninstrumented database latency by subtraction. The sequential producer is not
a sustained-throughput driver; no capacity or tail-latency result is established
by this source audit. Run phase labels are supplied input, not proof that native
relocation converged. Dedicated convergence-phase and restoration-lineage
provenance must be retained by the experiment; a topology generation alone
establishes neither. Current-artifact physical RPO/RTO, complete four-to-five-node convergence and
capacity gain are **not tested** here. The original Azure scale-out qualification
remains **failed**.

The next smallest **not-tested** boundary is a real child producer with normal
native TLS owners, the real external collector/controller, independently captured
completion/exit, and complete generated Strong reconciliation of its entire
small corpus. This is an end-to-end component prerequisite, not full-dataset or
physical qualification. Use the collector's existing actual-listener readiness
callback; an unauthenticated TCP readiness probe can trigger its fence path.
Read-only reconciliation runs can have a complete external receipt chain and
SDK evidence while `verify_external.py` returns 1 because its
`functional_evidence_complete` also requires nonempty writes. Preserve that
specific scope/exit and require actual child completion plus all-batch coverage;
never accept arbitrary verifier exit 1 as success. All tool reports retain
`not_qualified`.

## Remaining physical-use blockers

The stock SDK is a trusted member API, not an ordinary PostgreSQL application
principal. Current `authorize_cluster_data_caller` requires APP callers to be
active metadata voting members with their membership-bound TLS certificate;
typed reads require an active registered member. A new standalone CA-signed
client can fetch bootstrap but cannot perform APP. A dedicated non-placement
database principal is not implemented here. The explicit future arrangement
runs the trusted probe on an existing isolated member, retaining its member key
on that host, and sends evidence to a separate collector under distinct
instrumentation identities. Do not silently add a sixth data/voting member or
copy a server's private key to the driver. See the
[identity and custody contract](../../../docs/design/native-colocated-probe-external-durable-sink.md).
The arrangement has not been exercised with a real BicDB cluster.

`reconcile.py generate` and `coverage` implement bounded read-only plans and
coverage checking. Their [contract](RECONCILIATION.md) includes successful,
failed, uncertain and potentially unobserved writes. `verify_external.py`
checks collector/receipt linkage. These are implemented tooling paths, not
evidence that any real acknowledged value survived or that live reads covered
the original corpus. Actual reconciliation must still execute against the
source-pinned cluster and retain every original outcome.

There is no verified optimized probe binary, local SDK smoke result,
independent-machine result, server durability proof, sustained-throughput driver
or automatic controller qualification. The original failed Azure scale-out
result remains failed. These controls neither change its outcome nor clear the
production gate.
# External receipt tooling

The format-1 verifier and cross-run reconciler above retain their original
805 source semantics. New `collector.py`, `controller.py` and
`verify_external.py` add bounded functional instrumentation; their scope is
documented in `docs/design/native-colocated-probe-external-durable-sink.md`.
They do not establish SDK runtime, remote process provenance, physical HA or
throughput qualification. Never exclude a potential SDK/server success because
an explicit later wrapper ACK is missing.
