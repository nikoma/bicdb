# Fresh standalone origin and standby snapshots: issue 198

This correction follows actual required-CI feedback at frozen source
`2c0193c37852546c51b99e2918c24ea17bb44e2b`, not a passing recovery experiment.
[PR194 Core job132489784034](https://github.com/nikoma/bicdb/actions/runs/43609026644/job/132489784034)
failed `external_attachments_require_a_snapshot_and_cannot_be_acknowledged_as_missing_blobs`
in `live_replication.rs:1311`. That target recorded 31 passed and one failed in
2.12s. The full lossless raw log has 1,720,542 bytes and SHA256
`a1b937ed99eca9f209636341ff52ee898093e3588a1578cb5f344c3b887351e0`.
The separate new `ha_qualified_data_epoch` target passed all 12 cases in 1.59s
at that original head. Those results do not validate this successor.

The attachment test created a genuinely fresh standalone database, committed a
row and attachment, exercised the real archive-hold attachment refusal and
released that hold, closed, then enabled HA. Its original attachment-write
refusal/no-sequence-advance and actual missing-blob frame-apply refusal reached
their existing assertions. Creating the recovery snapshot then returned
`unqualified legacy HA data epoch; validated snapshot bootstrap required`.
Stage/install, restored attachment content and final manifest assertions were
not reached. The original test body and every assertion remain unchanged.

## Genuine origin, publication and refusal boundaries

The original directory lock already excludes another opener. Before format,
catalogue or recovery writes, an actual directory containing only that lock is
now recognized as fresh data birth regardless of the initial replication
setting. Only that observed birth initializes qualified genesis in memory.
An existing HA file still supplies its actual state; a populated directory with
missing HA state still initializes Unknown. No row scan, filename absence on a
populated root or JSON edit grants history.

A fresh replication-enabled open persists the same genesis at open through the
existing reader/durability barrier. A fresh standalone open retains it in memory
without creating an HA file or sticky flag merely for ordinary use. Its first
real HA-state producer, such as the original attachment test's archive hold,
publishes that same state through `HaNode::with_file`. Thus later reopening the
actual published state preserves the known origin.

Snapshot creation holds its original commit exclusion, durably covers the
actual applied point and requires the existing election-position qualification.
Only after that succeeds does it publish the same actual HA state with a no-op
`with_file` before backup. This operation always repeats the sticky-reader flag
barrier and full state replacement; it does not use `persist_current`, whose
missing-file branch intentionally does nothing. A previously unqualified state
fails before publication or archive creation. A first state rename followed by
directory-sync failure retains the existing durability fence and returns before
creating the archive; visible bytes are not a successful fsync return.

A standalone handle that closes without any HA-state publication loses its
in-memory birth witness. Its later populated reopen remains Unknown and cannot
campaign or create a qualified standby snapshot without a genuine validated
bootstrap. Automatic upgrade of an all-legacy group remains blocked. This is
an explicit availability limit, not permission to bless a legacy directory.

## Source regressions and open gates

Three new file-backed controls use the actual public producers: a genuinely
fresh standalone row snapshot restored through real staging/open with full
record equality; unpublished standalone close/reopen refusal; and a genuine
first HA-state directory-sync failure that leaves no archive, fences the open,
and permits an ordinary verified reopen before later snapshot creation. They
have not been compiled or executed in this successor. The prior 12 controls
and the entire original attachment case remain unchanged.

This change does not resolve snapshot data-head/ACK-copy issue193, provide a
trusted all-legacy bootstrap source, support old-binary rollback during the
missing-root install window, or qualify bounded recovery, physical durability,
read consistency or production HA. The version and existing required-CI heads
remain unchanged until the owner integrates and validates the coherent batch.
