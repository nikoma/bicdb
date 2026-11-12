# Post-join writer-registry refusal observed at ca7 (2026-11-12)

Exact full-prototype source `ca7d481467c40cf6ac324ceb8c0c7e943d4d1cfc`, package
`1.0.489-beta`, passed the original **five CLI cases**. Its retained wire evidence
also demonstrates a remaining availability gate: after metadata enrollment of
the fifth node, the existing gateway's Strong read repeatedly received an
explicit writer-admission refusal and ended in its original two-second timeout.
The receiver case permits that refusal as a prerequisite-only outcome; this is
not healthy post-join continuity.

| Executed scope | Actual result | Run / job |
| --- | --- | --- |
| Public CLI target `native_metadata_operator_cli` | 5 passed, 0 failed; 57.38 s | [43275067911 / 131381248091](https://github.com/nikoma/bicdb/actions/runs/43275067911/job/131381248091) |
| Actual TLS observer controls | 9 passed, 0 failed, 924 filtered; 0.10 s | [43275067351 / 131381261215](https://github.com/nikoma/bicdb/actions/runs/43275067351/job/131381261215) |

Workspace all-target and isolated-consumer checks also passed. No local Cargo or
rerun was used. A version label alone does not identify the full prototype or
turn these results into a public release/deployment claim.

## The observed post-join request

The receiver case first ACKed and Strong-read the entire nested object through
the public client. One fifth `join-fresh --replica-receiver` process reached
owned metadata-voter/control/receiver status with `data_serving=false`; it was
not assigned a data range. Actual APP/Get/Scan refusal assertions passed.
The same gateway then tried its previously ACKed key:

- One original protocol business ID, eight distinct attempt challenges.
- **Eight actual n2 typed NativeEventGet responses**: `kind=unavailable`,
  code `native_read`, stage `WriterAdmission`, detail
  `native host writer enrollment: current voter registry changed`.
- Every response supplied the same route: generation 4, range 2, epoch 1,
  leader n2, retry-after 50 ms. Full source birth/incarnation/enforcement fields
  are retained in [the eight envelopes](observations/postjoin-typed-responses.json).
  They match the actual n2 birth in the observed generation-4 snapshot.
- Between retries, **34 successful Bootstrap snapshots** at generation 4:
  n0/n1/n2/n3 each supplied seven; n4 supplied six. There were **zero transport
  error records** during this logical call.
- The original public call ended with `canceling statement due to query timeout`.
  It never returned successful absence or a stale value.

These are actual request/response frames observed by the authenticated client,
not manufactured MQ/role or serving evidence. Snapshot data remains routing
DATA, not a fresh quorum grant. The before/after snapshots show exact unchanged
data-range descriptors and unchanged birth bindings for the original four nodes.
The fifth metadata voter was added. This run therefore records an explicit
admission fence while all five routing sources remain reachable; its timeout
must not be described as all nodes offline or a generic discovery failure.

The exact source guard is `WriterPolicyFacts::require_topology` in the bundled
`distribution_native_writer.rs`: it compares the current metadata-voter set with
the set captured by the installed writer policy. A mismatch returns the observed
error. This identifies the refusing boundary at **ca7**, not a tested repair.
A fresh-policy refresh must retain actual committed MQ, birth/open/custody,
writer identity and unchanged data-quorum checks. Clearing the guard, recreating
genesis or seeding recovered epochs is not an acceptance path.

## Other results and limits

The initial-four DATA case now produced three externally journaled ACKs for the
full object, JSON null and empty object. Five Strong successes (including the
final re-read) verified full present values and genuine absence; present null is
separately tagged. Its pending-startup operations correctly had no ACK/read
success. The fifth receiver case produced one ACK/full Strong read **before**
joining, and the post-join refusal above. **No fresh write was attempted after
that join.** Bounded healthy write/read continuity after registry addition remains
unproved and must be covered by a separate strict regression.

The earlier `3ee40c19...` cohort remains **4 passed / 1 failed**, with its null
append durable outcome unknown and no post-failure ACK reconciliation. This new
successful initial-four execution does not explain or reclassify that failure.
Its original evidence bundle is unchanged.

Both actual client journals have contiguous sink/per-observer sequence numbers,
matching logical intents/replies, complete case-finish markers and no interrupted
logical record. Their complete frame headers/JSON were verified offline:

| Journal | Logical calls | Physical attempts / phase records | Exact frames | Bytes / SHA-256 |
| --- | --- | --- | --- | --- |
| Initial DATA | 11 | 127 / 509 | 253 | 2,475,197 / `898235b865efb65f764372b246b367df4d732d87f00bf0e99b072fbe4998aa9c` |
| Receiver | 8 | 47 / 188 | 94 | 1,030,316 / `6bc04d45676adf19de3ec36966d237915ed9d72b12555e0eb3677dca330105b4` |

A complete trace may include an actual interrupted exchange with no response;
sequence completeness does not turn it into transport success. The initial
Bootstrap setup is explicitly logical-only (`bootstrap_raw_frame_covered=false`).
Host-internal traffic is outside these journals; they are not a global wire
ledger. The 34 later discovery snapshots are actual covered transport frames.
The 34 external-ledger stdout records comprise 21 initial-case and 13 receiver-case
records; all are extracted from already-fsynced source records.

There was no failed case, so **no DB failure-custody artifact or full DB/page/WAL/
range-catalogue archive was produced**. The copier's native failure path was not
exercised by this cohort. Source/helper checks and a COMPLETE manifest alone are
not native ACK reconciliation or power-loss qualification. No engine was opened
while curating this bundle. Image/joint wire/controller, a fifth assigned DATA
range, automatic relocation, VM/physical and Azure-cause claims remain outside
these results.

## Exact evidence

Full CLI raw-log SHA-256:
`e4ff1e1a48a483f1dd09682451a96abcd79203e79ff366dea8e4ce9538544ea6`.
Observer-controls raw-log SHA-256:
`05681185b092a96686c726bdcd85557dbc1d12fdfd9c45d8675a12c323522334`.

Artifact **13971542223**: 92,897,800-byte ZIP, SHA-256
`f3ead0b957e8c0405810bc7520ec7577aa54b9226af8776e78ef5c12dac59566`.
The original ZIP remains in restricted custody; only the two verified client
journals were extracted. This bundle retains compressed exact journal/log bytes,
focused parsed envelopes/topologies, source excerpts and checksummed receipts.
The actual streamed CLI executable was 750,240,064 bytes, SHA-256
`ab178716c609a07d60b501151d3c2773553e78564335b53fae716fcd7f41c544`.
No old binary or historical source is retagged.

```sh
python3 verify.py
python3 test_verify.py
python3 verify.py --repo /path/to/bicdb
python3 verify.py --private-zip /restricted/path/artifact13971542223.zip
```

Optional repository checking validates the bundled source excerpts against the
exact commit; it is not full source reconstruction or execution-authenticity
proof. The private ZIP check verifies the original archive and journal parity.
