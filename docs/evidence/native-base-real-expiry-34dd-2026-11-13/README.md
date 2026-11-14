# Real role-expiry baseline: native base abort cannot resume

This immutable packet retains **one failed baseline run**, [43502925624](https://github.com/nikoma/bicdb/actions/runs/43502925624), at exact source **`34dded55efef6ed989cdf7d141d5751d4f2a2391`**, unmerged runtime candidate **1.0.494-beta**. The two tests executed: **one passed, one failed, 18.06s**. Future fixes and their results are excluded. This duration is not RTO.

| Actual cut | First native publication | Fresh request on the same original A | Classification |
| --- | --- | --- | --- |
| Role expires after xid21 is issued, before the first native put | `QueryTimedOut`; actual same-open xid21 status **Aborted**, no visible base or attempt archive | Remains on xid21 and fails `Registered immutable native-base publication missing` | **Failed:** confirmed same-open native metadata resumption liveness defect. Original desired-resume assertion fails at `native_expiry_tests.rs:301`. |
| Role expires after actual native commit | `QueryTimedOut`; actual same-open xid21 **Committed**, exact base/archive/retained receipt agree, commit-return21 retained | Reconciles the **same xid21** and returns verified NativeBase R1 | **Passed:** actual committed publication survives lost request authority and reconciles with fresh authority. |

Both controls wait for the original Role's real clock deadline to expire while its parent request remains live. There is no fabricated cancellation, role retagging, TTL extension or reordered acquisition. In the before-put case, the cut waits 988,115µs; Role remaining time is −85µs while the original parent still has 3,501,858µs. After-commit waits 957,520µs; Role remaining time is −98µs and parent remaining time 3,501,802µs. Parent budget remains 5,000ms and the request order remains Role → OWN → Finished → actual host. Each retry acquires a genuine fresh request on the same original process/image attempt.

The failure reaches its intended cut, not merely a setup prerequisite. The runtime has already retained the exact native publication xid/receipt before the first put; the actual timeout aborts this metadata transaction. Fresh authority cannot move that retained Aborted publication forward. The selected full acknowledged event record and original range/history/rows, database-open/birth, namespace/schema, protected catalogue, slots and writer absence remain checked across before/first-error/retry observations. The test establishes a recovery-availability failure, not observed durable-data loss. The native metadata transaction is not the original logical row transaction, and no replacement logical row transaction is authorized by this packet.

This is distinct from the earlier original e1ec failed catalogue prerequisites. These actual-clock controls do **not** identify the cause of those earlier timeouts. Their original frozen results and separate raw-WAL interpretation remain unchanged.

The exact workflow selector is `native_registered_fifth_remote_native_base_actual_expiry_`. A separate isolated checkout of the pinned source can reproduce the unchanged acceptance with the same build/test concurrency:

```sh
CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 RUST_TEST_THREADS=4 \
BICDB_TEST_SOURCE_SHA=34dded55efef6ed989cdf7d141d5751d4f2a2391 \
BICDB_TEST_EVIDENCE_ROOT=/isolated/baseline-evidence \
cargo test -p bicdb-core --lib --locked \
  native_registered_fifth_remote_native_base_actual_expiry_ \
  -- --test-threads=4 --show-output
```

The pinned workflow also builds the `chaos_child` helper before listing/executing the tests. Its full commands, toolchain and source identities are retained in the two complete logs. The observed baseline includes the failed assertion; reproducing it must not remove the assertion or increase its deadlines.

## Exact accounting and evidence

Four source `ClusterEventClient.append` calls return API ACKs, with zero recorded source failures, uncertainties or separately recorded caller retries. Internal wire retries are not measured. The logical event collection is `items`; the full underlying records are stored in `__bic_cluster_events`. This is not a PostgreSQL table-write or general SQL guarantee. Each fixture verifies one full selected-range ACK record (177 serialized bytes) on the target; the other source range's ACK is not claimed copied.

Separately, two internal native-base requests initially return timeout errors. Their same-open native outcomes are then directly characterized: one Aborted, one Committed. Two fresh continuations produce one NativeBase return and one missing-publication error. These are native image metadata operations, not four additional APP commits. Their initial timeout uncertainty is distinct from the source APP ledger and is not silently converted into rollback. No four-commit failover RPO claim follows.

All six actual OLD close observations retain their original native contents. Source frame calls total36, counted only where `operation == source_image_frame`; ten seed/OLD journal entries are not counted as frames. The original Manifest and every subsequent real source frame remain in the journals. No frame is reissued to manufacture missing evidence.

Core job **132153142118** retains the failed acceptance and exact error. Workspace job **132153086320** passes all-target and independent workflow-consumer compile checks; those are compile checks, not a full workspace test suite. Both complete raw job streams are retained with raw/compressed SHA-256 hashes. The public projection is byte-identical to the original safe capture: **30 included /194 omitted members**, SHA-256 `3c378baa1d877254de40b8bb9f25ec7f4d3b4df0922893bddafe0b80e8bd41f8`. The private original artifact **14075351623** is identified by SHA-256 `a493a07ec6519949277d9813d105a7300025afc06059fcda8bf00ead074af573`.

Private custody contains **192 independently checked files** across the two fixtures. These are guarded **live-owner stable copies**, not stopped, atomic, power-loss or reopened snapshots. Only omission path/size/hash metadata is public; database and signing material remain private. The independent verification receipt binds both exact outcome JSON hashes. The default public verifier checks retained logical observations and custody identities; it does not pretend to inspect omitted bytes.

```sh
python3 docs/evidence/native-base-real-expiry-34dd-2026-11-13/verify.py --self-test
```

`--repo /path/to/bicdb` checks the exact source blob/SHA pins, including the unmodified failed assertion. Authorized custodians can add `--original /private/focused-core-34dd-real-role-expiry.zip` to reproduce all 192 file hashes directly from the pinned private archive. Neither mode opens an engine or performs recovery/checkpointing. Negative public checks reject falsely relabeling the abort-resume failure as a pass or changing the original A claim. Successful evidence verification preserves the runtime result **1 PASS /1 FAIL**.

## Open repair and qualification gates

The smallest separate repair must permit at most one new metadata-native attempt per genuinely fresh request **only after proven same-open abort**, healthy DB/page-WAL, absent current base and exact archive, unchanged original irreversible preimage, current original A/hook/birth/open/source/final/generation bindings and bounded retained abort history. Committed publications reconcile their existing xid. Unknown, InProgress, poisoned, failed-fsync, unresolved status or revoked authority must retain custody and refuse replacement. The original failed assertion must stay unchanged.

Those safeguards and their additional failure controls are **not tested by this baseline**. Required next tests are the unchanged real before-put acceptance, the committed same-xid control, actual post-first-put abort, real native fsync uncertainty, identity/generation revocation and abort-history exhaustion. They belong to a separate source/result packet. No repair is delivered here.

Catalogue adoption, NEW/decision/Open, Ready/Serving, full Strong reads, populated second-range movement, independent index/FTS/queue/audit parity, cold owner recovery, physical fault-safe controller convergence and sustained post-convergence capacity gain remain unqualified. The original failed Azure experiment and production admission gate remain unchanged.
