# First aac registry-refresh receipts, 2026-11-12

These are the first remote results for exact full-prototype source
`aac6192c2b034e1f2e4c88a16fd84bcd94e0bf6b`, package `1.0.489-beta`.
The strict metadata-registry-addition DATA check passed. The whole CLI target
failed a separate metadata command, and one Core regression failed an error-stage
assertion. Neither failure is waived. No result here applies to a later source.

| Run / runtime job | Exact scope | Actual result |
| --- | --- | --- |
| [43296887408](https://github.com/nikoma/bicdb/actions/runs/43296887408) / 131457525202 | `native_initial_data_registry_` | 4 passed, 1 failed, 940 filtered; 6.47 s |
| [43296897267](https://github.com/nikoma/bicdb/actions/runs/43296897267) / 131457557639 | `native_registered_learner_schema_` | 1 passed, 0 failed, 944 filtered; 2.68 s |
| [43296905362](https://github.com/nikoma/bicdb/actions/runs/43296905362) / 131457578487 | `native_metadata_operator_cli` | **5 passed, 1 failed**, 0 filtered; 65.89 s |

All three separate workspace/all-target jobs **131457463832**, **131457493463**,
and **131457521015** completed successfully, including their isolated consumer
compilation without Core defaults. These are compilation results, not additional
runtime test passes. Formatting/selector/licensing/dependency checks also passed.
No full Core, SQL, PGwire, Rails or workspace runtime suite is claimed here.

The actual `CARGO_BIN_EXE_bicdb` was streamed and hashed before process startup:
**751,406,136 bytes**, SHA-256
`35c3b103e1a54d6aee1c10ec9bbc597a995429ecc96879a44c1ebc14363361df`.
The source identity came from runtime `BICDB_TEST_SOURCE_SHA`; this is not a
published binary, release, verified main merge, deployment or physical run.

## What passed and what failed

The five registry controls covered actual publication ambiguity/fresh retry/cold
no-grant, original cancellation/retained-old-Store-slot refusal, old `None` policy
refusal, previously added member continuity, and writer replacement. The last
control failed at its first expected-error assertion: it expected
`original writer object`, but the actual fresh-MQ path reached the revoked old
listener and returned `native writer hook lifetime ended`. Its later kernel,
audit, full-row and cleanup assertions **were not executed** in this source.
Test-only successors `794fd5ad3f16e21e07c641bb9635910ffb6d274a` and
`90f36d9a3` separate the earlier ingress refusal from captured-proof kernel
rejection and inventory both generations; neither was executed by these runs.
The production kernel was not relaxed to force a particular error string.

The one learner-schema test passed actual sealed-birth schema-label publication,
exact retry, contradictory-label refusal and already-bound/missing-label refusal.
This is one test execution, not four independently counted passes.

The strict CLI case
`writer_registry_refresh_baseline::native_metadata_registry_addition_actual_cli_requires_healthy_existing_read_and_fresh_ack`
passed with the **same public gateway**, unchanged DATA assignments, ordinary
2 s API limits, and the explicit common 10 s receipt gate. In its first round:

- Existing acknowledged full value: Strong present, exact nested payload.
- Fresh Append: externally acknowledged; fresh Strong: exact full value.
- Triple receipt: **1,015,797,325 ns (1.015797325 s)**, below 10,000,000,000 ns.
- No false absence/wrong value; fifth remained DATA-unserved; no DATA move.
- All four original coordinator, registration, DB-open, Store-open and policy
  checksum bindings stayed equal. Each retained state reported one published
  registry extension. These diagnostics are observations, not linearizable
  readiness or a recheck of the current DB hook slot.

`events.json` preserves all **54** strict journal records, in original order:
32 physical-RPC phases, 10 logical intents, 10 logical replies, start and finish.
It replaces framed byte arrays with exact length/hash plus decoded frame, and
removes source configuration from Bootstrap replies while retaining topology
DATA. There were eight paired wire attempts: old Append/Strong, three deliberate
fifth receiver refusals, postjoin old Strong, fresh Append and fresh Strong.
The old key routed to n0/range4/epoch1; the fresh key to n2/range2/epoch1.
Postjoin Strong routes reported generation4 and current R1 for their respective
ranges; local visibility B is retained separately in each audit. Business IDs,
challenges, request IDs, source births, full values, typed errors, and distinct
first/final MQ context IDs are recorded. The expected receiver APP/Scan errors
and typed Strong `missing_dispatcher` are not healthy DATA-serving grants.

The three complete, unfenced actual-RPC journals have 504 / 42 / 54 records for
initial DATA / receiver prerequisite / strict continuity, respectively. Their
logical results record **3 / 1 / 2 user ACKs**, respectively, in separate fixtures.
The initial DATA fixture also records one
`NO_USER_ACK_DURABLE_OUTCOME_UNKNOWN` pending-readiness Append and one
`NO_SUCCESSFUL_READ_NO_ABSENCE`. Preserve those classifications: an error is
not rollback and a failed read is not absence. The strict fixture has no unknown
write reply; its two ACKs are the before/after operations. The receiver-only
case remains a prerequisite case even though its old-key read happened to pass.
No journal covers all internal host traffic. Direct Bootstrap setup explicitly
has `bootstrap_raw_frame_covered=false`; these are not complete global wire ledgers.

The CLI failure was the original
`native_metadata_operator_actual_cli_single_join_fresh_becomes_local_metadata_voter`:
**n4 command sequence1 `elect`, budget5000ms**, after owned-voter/control readiness,
**before spawning sixth n5**. It returned:

> cluster error: native host bootstrap: owned metadata member changed/busy

At this exact source, `learner_birth/metadata_transition.rs` emits that string
only when the second `metadata.try_lock()` fails, after factory validation and
before `proof.require_current`. Thus this requested command did not reach its
election. This is not evidence of identity mismatch, child exit, a sixth-node
constructor failure, or the cause of an older run's process exit. n4 stderr was
empty. Its error-status snapshot was follower/term1/commit4/Cgen4. All five
children were still running before cleanup, then actually SIGKILLed and waited.
The later stopped JSON files independently record n4 **Leader/term2/commit5**,
all peers following n4, Cgen4 unchanged. This is consistent with automatic
supervisor progress after the command refusal; it is offline JSON inspection,
**not engine replay or a fresh authority proof**.

## Custody and exact identities

Private archives are retained under
`/root/.local/state/bicdb/ha-scaling-2026-11-11/private/native-registry-refresh-aac6192c`.
No native DB, TLS private key, or 98 MB ZIP is republished in this receipt.

| Artifact | Bytes | SHA-256 |
| --- | ---: | --- |
| CLI 13981575889 | 98,197,632 | `6785428ed7c5fc35b8b2f32f824e20cfd875cf26067ac61754ef0bc8d76f714e` |
| Stopped DB 13981795545 | 98,639 | `9f692512b265231a9bd22234b12455fa3995ea8c2fad1e222448199458f800e9` |
| Registry Core 13981185241 | 40,868 | `8262c9907afd62a372e78e38cb42a88c0844440e1eae52f861f5f89b9e32a4d2` |
| Schema Core 13979952486 | 40,281 | `201f4df3ed579953a8bede2346560b35cf156dcbd36d2e2c4c92f1aaf6286347` |

The failed **metadata-only** fixture `/tmp/.tmpce5nHG` has 32 diagnostic files,
plus complete stopped custody of explicit db0..db4: **96 files, 1,505,406 logical
bytes, 40 directory entries**. Every file hash/length was verified, as were all
five actual child wait receipts. ZIPs omit empty directories; the manifest,
not physical ZIP directory entries, supplies that count. These roots do **not**
belong to any passed DATA ACK fixture; those DB roots were not retained.
No ACK reconciliation across fixtures, native-page replay, source power-loss
qualification, engine open/repair, or original DB fsync was performed.

Diagnostic manifest SHA-256:
`788acf765818f99b845b46df40f49642ae7d3e0c8bf79c77642811329277b1e5`.
Stopped DB manifest SHA-256:
`fdeca73b35545a3fe9bb60bdf5254b505959e752fc6b9dd6fa84f94d770f8bc0`.
Private verification receipt `aac-cli-terminal-custody-verification.json`:
`0ed8ee9d89394797deb836099b6e2de4747c620350e84d938d14c66bc8a46da7`.

Raw job logs/indexes remain in `/tmp/bicdb-native-foundations-ci`; exact names,
bytes and digests for all six runtime/compile jobs are in `receipt.json`.
Runtime raw SHA-256 values are registry
`ae5750db61755136a6aef1eae86d358c7b0789518d48f6b2b69bf9b9271ff07c`,
schema `a2739d0f5217e568591acb32ef937232ba032a9dfd21c1ac0bb1c8c4469dba34`,
and CLI `a622a72b78ccb49f9e9bf8027f064df1b90408b73280b275b4b4fc57e72eae6f`.

Run bounded offline checks from the repository root:

```sh
python3 docs/evidence/native-writer-registry-first-aac-2026-11-12/verify.py --tamper-tests
python3 docs/evidence/native-writer-registry-first-aac-2026-11-12/verify.py --private-dir /root/.local/state/bicdb/ha-scaling-2026-11-11/private/native-registry-refresh-aac6192c --tamper-tests
```

Both modes and four semantic tamper cases passed when frozen. The private mode
hashes all four ZIPs and checks actual strict frame parity, all diagnostics and
all stopped DB file hashes without engine access. Portable checks validate
receipt consistency, not independent execution authenticity or engine correctness.

The original strict **FAILED** source
`1d3976f9f8011f3ac7411494aa0dfb432763ce78` / run43282887768 /
CLI131408621456 remains immutable, separately preserved. This successful strict
follow-up neither edits that evidence nor turns this whole 5/1 cohort green.
No deliberate fault was injected in this strict follow-up. No Azure causal,
automatic DATA relocation, fifth DATA Serving, physical qualification, release,
deployment or main-publication claim follows from this receipt.
