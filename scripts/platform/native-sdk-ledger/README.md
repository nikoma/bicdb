# Offline verifier for the stock-SDK experiment ledger

The producer is source commit `b44064c2bf1b22712e5f30d2031aa5d58e907e44`,
based on the 1.0.512-beta candidate
`4efcfc2cccc51307721777ad7d53a251e67bbbf3`. The producer has not been compiled
or run. This Python code verifies its format using synthetic fixtures; that is
instrumentation testing, not a BicDB durability, HA, consistency, or scaling test.
No network or database process is involved in these controls.

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

## Remaining physical-use blockers

The stock SDK is a trusted member API, not an ordinary PostgreSQL application
principal. Current `authorize_cluster_data_caller` requires APP callers to be
active metadata voting members with their membership-bound TLS certificate;
typed reads require an active registered member. A new standalone CA-signed client
can fetch bootstrap but cannot perform APP. A dedicated non-placement experiment
principal is not implemented here. Do not silently add a sixth data/voting member
or copy a server's private key to the driver. The experiment's identity/topology
decision remains explicit and unresolved.

There is no cross-run reconciliation-plan generator or proof that reads cover
every earlier successful, failed, uncertain, or explicitly retried write. There
is no optimized probe binary, local SDK smoke result, independent-machine result,
server durability proof, sustained-throughput driver, or automatic controller
qualification in this leaf. The original failed Azure scale-out result remains
failed. These controls neither change its outcome nor clear the production gate.
# External receipt tooling

The format-1 verifier and cross-run reconciler above retain their original
805 source semantics. New `collector.py`, `controller.py` and
`verify_external.py` add bounded functional instrumentation; their scope is
documented in `docs/design/native-colocated-probe-external-durable-sink.md`.
They do not establish SDK runtime, remote process provenance, physical HA or
throughput qualification. Never exclude a potential SDK/server success because
an explicit later wrapper ACK is missing.
