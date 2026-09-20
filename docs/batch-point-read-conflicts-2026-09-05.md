# Batch point reads and stale stock updates

The full audit of `hash-conjunction-balanced-r1` failed for stock warehouse 4,
item 75176. Exact commit counts and all financial checks passed, but that is
insufficient to qualify its 482,352.5 whole-run NOPM result. Expanded coverage
checked all 9,645,883 post-seed lines, 480,000 customer Delivery counters and
1,600,000 stock rows. One stock row held quantity/YTD/count/remote values
`38/30/5/0`; the committed order lines require `15/53/9/0`.

The retained WAL has valid checksums across all 39,559,115 frames and
19,107,928,500 bytes. Its relevant sequence is:

| Transaction | Commit sequence | Recorded stock before (quantity/YTD/count) | Recorded stock after |
| --- | ---: | --- | --- |
| 979036 | 969487 | 44/24/4 | 38/30/5 |
| 979095 | 969521 | 44/24/4 | 39/29/5 |

Both committed from the same old value. Transaction 979095 stamped its write
with snapshot 969504, which covers the other commit despite reading its old
preimage. Later live patches continue from 39/29/5, so the live write stream
lost the first transaction's quantity 6 and one count. Recovery applies the
first patch, skips the inconsistent second patch because the row appeared in
seed segments, and skips its successors. That explains the larger recovered
shortfall. The retained dataset is failed evidence, not repaired or qualified.

`Transaction::get_stored_by_pks` omitted the observed-version bookkeeping already
performed by single-point reads. A read-your-writes snapshot floor can extend
above the contiguous applied watermark; treating that floor as the row version
allows this stale write to commit. The new regression reproduces the lost update
on current main without the join experiment. Batch reads now record the actual
visible creation/deletion boundary under the same shard guard as the read,
then retain the earliest observation under one batch bookkeeping lock. Pending
write overlays remain applied after the underlying observations.

Validation: the regression failed before the change and passed afterward;
419 core unit tests and 105 core integration tests passed (3 and 1 ignored),
as did 281 SQL unit tests (3 ignored). All 25 focused cell-row, corrected-workload and routine-type integration
checks also passed. Optimized benchmark qualification is pending. There is no new
performance claim from this fix yet.

Evidence is under `docs/evidence/batch-point-read-conflicts-2026-09-05`.
The selected WAL excerpt includes later Delivery rewrites of the same order
lines, in addition to NewOrder patches and their commit records. Benchmark
runtime source was 0db4cf1bf650a863d0604263dfa6afd876369c12, binary SHA256
5253b88be525789c6dc04aabd6692e11390c811b7829e793bd0000a605c21859.
The original dataset and failed audit clone are retained on VM2; the original
is also being archived off the Spot VM to local persistent storage.
