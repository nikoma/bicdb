# Separate 56f CLI runtime: 3 passed, 3 failed

Exact source `56f9a3c999fba329c349d3e6dda664dcfb64e4c6`,
[run43301082751](https://github.com/nikoma/bicdb/actions/runs/43301082751),
job131472177797: **3 passed / 3 failed / 108.68 s**. The production CLI compiled
and ran despite its separate workspace job failing the test-only scan calls.
No whole-cohort success or result for later DED3 is asserted.

Actual binary: 751,407,688 bytes, SHA-256
`466349b36e1d81e3cb64da77303070b11bf51cb85350d82f29c442dcb59e4d56`.
Source came from runtime `BICDB_TEST_SOURCE_SHA`; binary bytes were hashed before
startup. Raw log: 2,194,016 bytes, SHA-256
`c1534616ebf735894878a21f8e10fdf5ef432cbea72b2602cadec0cec39bc2f4`,
retained in `/tmp/bicdb-native-foundations-ci/56f-registry-cli-bounded-wait-first-job131472177797.log.gz`
and matching `-index.json`.

## Three distinct failures

1. Initial four DATA owners: full object ACK and exact Strong succeeded, then
   JSON null ACK and **present-null** Strong succeeded. Third `{}` Append failed
   at initial_data_tests.rs271 with `no event cluster member is reachable`.
   It was **not acknowledged; durable outcome remains unknown**. Its two actual
   requests to n2/range2/epoch1 (IDs122,127) each wrote all286 framed bytes, with
   `may_have_dispatched=true`, then timed out without a decoded application
   response. Between them, all four actual n0..n3 Bootstrap snapshot RPCs
   succeeded with Cgen1. Therefore that final aggregate error does not prove all
   nodes offline. There was also the earlier deliberate pending-readiness
   Append, separately classified no-user-ACK/unknown. The genuine absent-key
   final phase was not reached. The initial journal retains529 records ending
   at the logical unknown reply; the panic prevented its fixture-finished marker.
2. Automatic metadata join test: n4's `elect` command failed with **Registered
   catch-up owner changed/busy**, not the previous aac strict owned-member check.
   The exact hot catch-up source emits this at its post-factory `try_lock`,
   before proof revalidation or the requested election. Retained n4 error status
   was follower/term1/commit4. Later stopped JSONs show actual automatic n4
   Leader/term2/commit5, peers following n4. This is a command busy refusal,
   not proof that n4 cannot lead or a sixth-child exit.
3. Promoted cold test: its successful local status snapshot reported leader=null
   at promoted_cold_tests.rs273, violating the explicit retained-n0-leader
   prerequisite. The stopped JSONs later show all nodes term1/commit4, n0 leader
   and n4 follower with leader n0. That chronology does not establish why the
   earlier routing hint was null. No election/vote/cold assertion was reset or
   weakened, and no cold kill/reopen success follows from this failed setup.

The passing cases were initial metadata startup/refusal, registered replica
receiver prerequisite, and strict retained-registry continuity. Strict continuity
again passed in round1: old full ACK Strong + fresh ACK + fresh full Strong,
**1,583,451,549 ns <10 s**, all five child processes running at the gate,
no false absence/wrong value, fifth DATA serving=false, no DATA move. Its54-record
journal and the receiver42-record journal finished unfenced. This does not turn
this whole3/3 cohort green.

## Stopped custody and uncertainty

Private verified ZIPs remain under
`/root/.local/state/bicdb/ha-scaling-2026-11-11/private/native-registry-refresh-56f9a3c9`:

| Artifact | Bytes | SHA-256 |
| --- | ---: | --- |
| CLI13983331445 | 98,275,634 | `d25a2769dafb64023c0b5f32434b439483013bcf551510934b71c7c3e9c134f1` |
| DB13983441342 | 359,212 | `77ada1f4f7a6b1b8c41bfe1850ebd6c55ac170a8d1ef75cafaff52a0e9e937e6` |

All three stopped-copy and diagnostic manifests/files were hash/length verified:

| Failed fixture | Diagnostics | Stopped DB files / logical bytes / directory entries |
| --- | ---: | --- |
| initial DATA | 26 | 76 / 3,239,725 / 32 |
| metadata elect | 32 | 96 / 1,505,406 / 40 |
| promoted cold prerequisite | 36 | 96 / 1,464,285 / 40 |

The private `56f-cli-custody-verification.json` SHA-256 is
`0f2155d893c9d93af47d28afa448e1592b22dc1be3463f80f2220fe6592722fe`.
All manifest identities, exact child wait receipts, selected request/error/reply
records, full acknowledged object/null values and hashes are in
`failed-cli-56f.json`. No DBs or private keys are republished.

Offline **raw catalogue JSON** for the initial `{}` operation retains the same
range2/index1 command `01a1d186-f4ba-7af1-82d0-d4d6e63d67d1`, checksum
`951258d8fbde6ce3b563605e26dbc6478b96c47b933030832a8d12c7f4fdceb1`:
n2 quorum_committed/resolved0; n0+n3 applied/resolved1; n1 no range2 (not a
replica). This supports retaining the unknown classification. It is neither a
user ACK nor native-page replay/reconciliation. No engine was opened, original
DB fsynced, repaired or replayed. The passed receiver/strict fixtures' DB roots
are not these failed roots. Earlier aac, original1d and original3ee evidence
remain separate and unchanged. No injected fault, physical/VM qualification,
DATA relocation, Azure cause, release, deployment or main-publication claim.
