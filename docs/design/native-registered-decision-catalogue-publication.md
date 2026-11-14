# Registered DECISION catalogue publication

Source parent: `331c12a52fe3efa0d89021e33f8944c8acd4886a` (prototype
`1.0.502-beta`). This successor is an unqualified source candidate. Formatting
and source review do not establish executed runtime or physical results.

## Observed failure

Run `43555215776`, Core job `132326461560`, executed the unchanged 82-case
family at a workflow-only qualifier of that parent: 74 passed and eight failed.
The eight failures reached the target DECISION path and returned
`enforced native writer host policy is not installed`. The native producer had
its own original Registered exclusion and retained transaction outcome. Its
subsequent catalogue confirmation called generic `RangeWriteStore::persist`,
which requires an installed ordinary writer. A first Registered target cannot
have that writer before the later ALL-Open/release/FIRST sequence.

The two distinct local-term DECISION controls passed at the parent. Their
retained records establish the actual before-put and after-commit cuts, rather
than inferring cut reach from an arbitrary error. They are preserved separately
from the eight failures. Neither run establishes automatic operator relocation,
physical scale-out, or production qualification.

## Source change

Ordinary catalogue persistence and installed-writer checks remain strict. A
separate Registered sink retains the exact original and candidate catalogues,
encoded bounded envelope, actual target Arc/A, database/store open identities,
catalogue UID and actual committed native DECISION checksum before any catalogue
memory mutation or file replacement. The candidate changes the selected joint
DECISION fields using the actual existing typed publication admission. Other
range entries are preserved. Whole-catalogue matching conservatively refuses
unrelated concurrent catalogue changes; this is not a second-populated-range
progress guarantee.

The sink rechecks the original admission/hook/journal/frozen A, actual physical
base/final-image archive, native DECISION CURRENT and immutable archive, fresh
Role/OWN and actual current-MStore point. It records entering each bounded file
replacement, then retains the real API success before readback or fallible
postchecks. A failed replacement/readback/cancellation/version check leaves the
exact candidate and native transaction owned and catalogue durability uncertain.
It returns no vote. A visible renamed file is not treated as fsync proof.

A fresh same-prepared retry borrows the actual retained OLD and NEW owners.
All other real observations remain mandatory; the selected Registered owner is
allowed only to reach its existing exact candidate's re-fsync sink. That retry
cannot create a native transaction, reconstruct a NEW quorum, mint a vote, or
install a writer. It validates the same protected committed native record and
original A, independent fresh metadata context and nondecreasing native metadata
anchor. Only after real file+directory fsync, exact readback and full postchecks
may a short actual version guard clear the uncertainty. The guard performs only
memory/request checks and bounded flag publication, with no IO/callback.
Ordinary full quorum authorization then runs again before native vote counting.

A retained successful fsync/readback/version publication may be reused only
with exact current candidate bytes and original identity. Unknown results are
always re-fsynced. IO attempts are bounded to eight; exact originals and
outcomes are not discarded. Encoded catalogue bytes are capped at 64 MiB;
original/candidate clones incur additional bounded memory. This is not an
8 GiB image-memory or cold-recovery qualification.

## Additional controls, not yet execution evidence

Five genuine fifth-target fixture cases are added, preserving all original 82
case names, existing assertions and authority/SDK budgets:

- Before-memory caller cancellation: native commit remains owned; a fresh
  request publishes the same candidate with the same native xid/bytes.
- Actual atomic-rename IO error: no target vote, uncertainty retained, exact
  candidate re-fsynced on a fresh request.
- Actual readback-open error: a test callback temporarily renames the newly
  written file away. The same bytes are restored for forensic snapshots, then
  a fresh request re-fsyncs the retained candidate. This is a process test cut,
  not a power-loss or malicious-file-writer test.
- After-fsync caller cancellation: successful IO is recorded before the error;
  a fresh request retains the same native xid/bytes and re-fsyncs the candidate.
- After-fsync actual local metadata term mutation: the actual version point
  becomes stale and no target vote is counted. The known native/file outcomes
  stay owned. Recovery of a fresh quorum after that term transition is a
  separate liveness gate; the control does not pretend to elect a new leader.

The controls retain intent/result before assertions, native attempt and
catalogue outcome evidence separately, original source APP acknowledgments,
actual counted member IDs and physical snapshots. They verify unchanged user
rows/native prefix observations and the untouched range's catalogue entries.
They stop at DECISION publication and do not qualify later Serving, cold
restart, automatic controller progress or throughput. Independent range
progress under stalled physical IO remains open: the service/journal scopes
still cover synchronous file IO, and a finite Role deadline does not bound
kernel fsync.
