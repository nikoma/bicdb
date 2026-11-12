# Authorized SQL key updates: process, I/O and guest-memory recovery

Example source `3a8484bf16f1653d7e96292992b4900f706b7370` has the exact Core, Page,
SQL and protocol production trees and manifests of `0031d197`. All 38 process
cuts, eight actual EIO cases and ten actual KVM resets passed, and the independent
coverage/content verifier passed. The separate two-case VM smoke overlaps the
full matrix and is not added to these counts.

The actual SQL generated-key UPDATE uses a Native-issued actor-bound UPDATE
grant, two physical writes and one audit. Four current-source fixture ACKs
(two per storage mode) include an untouched row and Native vector, timestamp,
payload and unknown metadata. They are separate from the historical old482
upgrade fixtures. The expected new full record is derived by changing only the
intended key and base values. Original file manifests and ACK hashes remain
unchanged. Each recovered copy must match the complete old or new records,
raw SQL schema and mutation policy, and pass Native index verification.

Every attempt flushes a pre-commit Native transaction witness to the independent
controller. Recovery requires no audit for old state, or exactly one audit for
new state, with that actual transaction ID, original/replacement IDs, operation,
actor, grant, budget and idempotency key. Three opens must preserve the exact
rows and audit message, including its UUID, without duplicates. Writes without
a grant must still refuse without advancing the commit sequence.

Process cuts cover staged/uncommitted writes, Native log write/sync, before and
after broker publication, after commit and after external ACK; paged mode also
covers decision, durable log, page apply, marker and sync-publish boundaries.
Both ordinary abort and explicit page/directory volatile emulation ran.
Ten cases recovered old state and twenty-eight new. All eight one-shot or
persistent Native log EIO cases recovered old state; the actual fault fired,
no update ACK was emitted, and after removing the fault the failed handle still
refused a legitimate unprotected write with WriteFailed.

The ten VM cases use existing pre-I/O Native-log hooks in both modes and page-WAL
write/sync in paged mode, plus post-commit and post-ACK pauses. Only the owned
QEMU child was killed after the host fsynced its own witness. Two recover old
state and eight new; both external-ACK cases recover complete new state. Saved
crash-image hashes are preserved, and recovery runs on copies. This is actual
guest RAM loss with ext4/KVM and no network or volatile emulation; it is not
physical host power loss, RF3 HA or distributed capacity qualification.

The exact debug probe SHA256 is
`01e7e30a9ce469fbee01e2c407c3a81fde19302b232d297539388a2b3ed9ee1b`.
Reports, ACK snapshots, witnesses, serial logs and replay controllers are in
`source-scoped-results.tar.gz`, with per-member hashes in `archived-files.json`.
Database directories, ELF files, initramfs archives and raw images are excluded
from Git. Saved image paths/hashes remain in the case reports on S3.

The completed older broad run at `5aa92dce` passed 2,586 SQL tests and 491 protocol
tests (four and two ignored). A canonical Cargo-target parser recovers a protocol
summary interleaved with telemetry and excludes nested child-harness summaries.
It also corrects the older `6c16e66e` Core observer count from 1,699 to **1,698**
parent-harness passes: the earlier raw sum counted one child a second time.
The raw logs and previous counts are preserved; no test was removed or weakened.
All results remain scoped to their exact sources. The new `0031d197` broad run
is separate and was live at this checkpoint.

Core API delivery is isolated in PR165, source candidate 1.0.490-beta, over main489
without private SQL partition code. It has its own Core-only recovery matrix;
this private SQL composition's results are not substituted for its validation.
The full original application/HA/portable-origin campaign remains active.
