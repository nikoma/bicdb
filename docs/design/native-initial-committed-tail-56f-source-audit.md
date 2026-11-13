# Initial native committed-tail isolation: exact 56f source audit

This is a **source-only audit and unexecuted test specification**, not a diagnosed cause of the CLI failure or an implemented recovery controller. No runtime code, deadline, retry, recovery guard or original assertion changes here.

Audited source: `56f9a3c999fba329c349d3e6dda664dcfb64e4c6` (source `1.0.489-beta`). The retained executed CLI evidence is [FAILED-CLI-56f.md](../evidence/native-writer-registry-first-aac-2026-11-12/FAILED-CLI-56f.md), docs commit `dce0f422a` / Root import `29bfd6c9a`.

## Observed boundary and limits

The initial public client obtained full-object and present-null ACKs and exact Strong values. The subsequent empty-object Append produced **no ACK; outcome remains unknown**. Actual requests 122 and 127 each sent all 286 framed bytes to n2/range2/epoch1, then timed out without a decoded application response. All four intervening Bootstrap snapshot RPCs succeeded at Cgen1. The final aggregate client error therefore does not establish that all hosts were down.

Stopped raw catalogues retain the same range2/index1 command `01a1d186-f4ba-7af1-82d0-d4d6e63d67d1`, checksum `951258d8fbde6ce3b563605e26dbc6478b96c47b933030832a8d12c7f4fdceb1`: n2 `quorum_committed`/resolved0; n0+n3 `applied`/resolved1; n1 is not a replica. These are raw catalogue observations. They do **not** prove n2's native prefix, native Pending/materialization receipt, row-WAL replay, or where its handler stopped. There is no demonstrated `{}`-type bug, rollback, permanent loss, all-host outage or Azure cause.

Driver verified the initial stopped copy: 76 files, 3,239,725 logical bytes, 32 directory entries; manifest SHA `0e0d660b11a1fff896d532a9fdf8db173a9559b8f3f96cf25d121e762610376b`. Private receipt `56f-cli-custody-verification.json`, SHA `0f2155d893c9d93af47d28afa448e1592b22dc1be3463f80f2220fe6592722fe`, is under `/root/.local/state/bicdb/ha-scaling-2026-11-11/private/native-registry-refresh-56f9a3c9`. No engine was reopened/replayed for this audit; those original roots remain untouched.

## Exact normal write sequence

The following paths and line numbers refer to the audited 56f revision.

| Boundary | Actual source | Consequence |
| --- | --- | --- |
| Authenticated APP dispatch | `distribution_transport.rs:2272`; `distribution_events.rs:557` | Actual same-open DB and leader range are checked, then a real transaction enters commit admission. Append calls do not receive the caller's cancellation token through this function signature. |
| Local decision and certificate | `distribution_range_consensus.rs:3394`, `:3694` | Real local Prepare/Commit and counted remote decision replies precede local CQ publication. CQ is irreversible; no abort may be inferred after it. |
| Owned local reservation retained | `distribution_range_consensus.rs:3710` | Later same-range writes/point reads cannot overtake this active command. |
| Actual NativePending | `distribution_range_consensus.rs:3735` | The full native intent is persisted before authenticated replica materialization instructions. |
| Remote native Apply | `distribution_range_consensus.rs:3771`; `distribution_native_apply.rs:22` | Each actual certificate-bound Apply is synchronous before admission returns. The shared ordinary native token is independently capped at 5000 ms. Missing replies are ignored as rollback evidence. |
| Admission ticket | `distribution_range_consensus.rs:3785`; `db/spatial_paged.rs:2733` | Pending ACK evidence is retained and the real ticket returned only after the remote Apply loop. The transaction then owns cleanup/recovery release. |
| Origin local WAL/rows | `db/spatial_paged.rs:2747` onward | Origin row WAL begins after the ticket. The existing injected post-admission/pre-WAL seam is before this boundary. |
| Native materialization/prefix and raw Applied | `distribution_range_consensus.rs:3802` | The actual borrowed-DB WAL receipt must validate; native Pending completion precedes the raw applied callback/ACK result. Errors remain unknown and invalidate recovered-epoch readiness. |
| Cleanup after rejected origin apply | `db.rs:9019`; `distribution_range_consensus.rs:3983` | Volatile ticket/range ownership and recovered-epoch admission are retired after transaction cleanup. Durable CQ/Pending are retained. This is refusal/custody, not replay. |

Consequently, local CQ plus Applied peers is reachable **before the origin admission ticket/local row WAL**, while the coordinator waits for remote Apply replies. It is also compatible with later local WAL/materialization/callback failure: the raw CQ alone cannot distinguish those cases. A fully successful local Applied checkpoint followed only by response loss would ordinarily leave raw Applied; the retained raw cut does not establish that simpler explanation either.

The client's original 2-second transport timeout is not proof that the APP worker stopped. The APP path does not pass that caller token into `execute_cluster_event`/`Transaction::commit`, while replica materialization uses its own bounded native token. A disconnected client can therefore coexist with legitimate work still running. This audit does not attribute the retained cut to that possibility without actual handler phase/exit evidence.

## What the next request does

| Entry point | Existing behavior at a genuinely unresolved native tail |
| --- | --- |
| Fresh Append | `admit_with_database` requires recovered epoch and a free actual range reservation, then plans against the real native prefix. It does not call a committed-tail recovery controller. Cleanup invalidates the recovered epoch; an active original worker instead retains the reservation. |
| Fresh Strong | `NativeStrongEventDispatcher::execute` obtains fresh HostMQ and separate leader metadata authority, then the real coordinator reserves the range. An active writer can exhaust that read's original reservation budget. Once available, `native_read_catalog.rs:61/:111/:209` rejects unresolved/ahead raw history or retained CQ; it cannot legitimately mint a clean absence from that local cut. There is no replay operation in this read protocol. |
| Generic `recover_local_leadership` | It calls `recover_leader_prefix` then `recover_leader_tail`. Native mode explicitly refuses a behind prefix (`:3037`) or unresolved native tail (`:3257`) and requires actual native recovery. It is **not** a working manual native replay shortcut at this cut. |
| Initial DATA opening/action | `distribution_range_consensus/initial_data.rs:28` is a genuine protected empty-genesis producer. Nonzero/history recovery cannot enter that R0-only path. Initial host startup/registry refresh does not schedule a native committed-tail replay; no supported DATA cold-reopen path is established here. |

The audited NativeHost/Strong/runtime call graph has no `recover_local_leadership` call. The pgwire operational loop separately calls it at `checkpoint_driver.rs:1666`; that loop cannot be silently assumed for the new NativeHost, and its generic native guards still apply.

There are two importantly different outcomes: the **original still-running worker** can eventually finish and make a subsequent read succeed without repair; after a genuine post-admission error and cleanup have returned, the preserved tail instead needs a distinct actual native recovery controller. A test must distinguish these cases before claiming automatic recovery. Current source supplies fail-closed refusal, not a bounded self-healing qualification.

## Existing genuine fault seam and smallest next test

The existing `BicDb::test_reject_next_native_after_admission` (`db/range_image_native.rs:771`, consumed at `db/spatial_paged.rs:2748`) rejects only after real admission/CQ/NativePending and before local row WAL. The actual enforced fixture already uses it in `native_strong_point_tests.rs:584`, `native_strong_point_raw_cq_or_pending_native_cannot_return_old_row_or_absence`. That test preserves a genuine old row and requires Strong unavailability. It does not use the public same-client APP path or prove self-recovery.

That injection is thread-local. Arming it on the test/client thread cannot be assumed to affect an actual TLS receiver worker. The smallest additive Core regression should use a narrowly targeted, one-shot **actual receiver-thread/per-actual-DB test hook** at this same boundary, not a fake quorum/ACK or fabricated native fact. No such new hook is implemented in this note.

1. Use actual initial native writers, owned metadata, pinned TLS and the public event client. Obtain a real old ACK and exact Strong value on a selected range; retain its full value/ID/digest ledger. Use the same client and original request timeouts throughout.
2. Arm only that actual origin/range/new event for the post-admission/pre-WAL error. Require real certificate-bound peer materialization and a real client error with no ACK. Prove the origin handler/transaction cleanup actually exited, rather than treating a sender timeout or brief drain as receiver exit. A missed prerequisite remains a failed test.
3. Inspect actual native point, Pending, raw command/state, UID/history/base and the old ACK through existing real observer APIs. Record the unknown event's exact command/checksum and both peer values; do not infer native state from raw counters.
4. Issue fresh public Strong reads after the original worker has exited. For the unknown key, only its **exact full value** or structured unavailability is acceptable while unresolved; `NotFound`/`None` is forbidden. For the old acknowledged key, preserve its exact durable value and likewise forbid false absence or a wrong value. Unavailability alone is a safety result, not recovery success.
5. Under a separate original bounded test deadline, require automatic recovery to finish, a fresh Strong of the committed unknown key to return the exact value, and the next distinct same-range Append to ACK followed by exact Strong/ledger reconciliation. No direct `recover_local_leadership`, forced cache/counter change, generic repair, manual phase clear, new genesis or synthetic certificate may supply that acceptance. If current source remains unavailable, retain the actual failing witness; do not mark the final recovery gate passed.

The expected missing producer is **committed native recovery**, not NoData abort: an existing certified command must be adopted/materialized under fresh actual metadata/data quorum and retained original obligations. Existing typed native historical/recovery APIs may supply the components, followed by real Ready/Complete/Activate and a legitimate coordinator admission transition. Their automatic same-host orchestration, original-request/cold ownership and bounded completion remain separate work; this audit does not authorize a generic native guard bypass.
