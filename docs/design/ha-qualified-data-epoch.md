# Qualified data epochs for native streaming HA

This is a source correction for [issue 190](https://github.com/nikoma/bicdb/issues/190),
against source `74f17b912cec7f2fe093d10dcf1618a43b35f454` (`1.0.511-beta`).
Implementation and regression sources are present; compilation and runtime
acceptance have **not** run for this change. This is separate from distributed
range relocation and from the failed Azure scale-out qualification.

## Source counterexample

The existing validator already rejects synchronous `N=5,K=1`; it requires
`K >= floor(N/2)` when K is nonzero. The issue persists with valid `N=5,K=2`:

1. Primary P acknowledges prefix 4096, durable on P/A/B; C/D remain at 0.
2. P fails. A wins epoch 1 using A/B/C. Epoch 1 begins at 4096.
3. C receives the first normal bounded batch (512 commits). Previously the
   heartbeat persisted `data_epoch=1` before application, even though C had
   only applied 512 after the batch. A then fails before C receives another.
4. C can advertise `(1,512)`, outranking B's `(0,4096)` in lexicographic voting.
   C/B/D can elect C. B's longer acknowledged history can subsequently be
   classified as divergent and replaced by snapshot.

This is a source-derived schedule, not an executed loss experiment. Election
silence timers do not exclude it: the second election occurs after those timers.
Table-verification health and stale-read watermarks do not establish catch-up
eligibility for this election.

## Correction and proof boundaries

`HaNodeFile.data_lineage` is a versioned node-local record of the qualified epoch
and its exact starting boundary. Fencing `epoch` and learned `epoch_starts` remain
separate. The new native handshake is **5**; version 4 is refused before vote or
Resume dispatch. Ordinary commit-frame encoding is unchanged.

A heartbeat only records authority and announced history. After its entire
batch applies, the recipient checks `actual_applied == sent_tip`, actual
transaction-log `written_seq` and applied watermark coverage, the same followed
primary/epoch, and the epoch's explicit starting boundary. It advances voting
history only if the boundary has been reached. Equality is sufficient: no new
term write is needed. Successful partial catch-up below the boundary preserves
the earlier qualified epoch across reopen. A trusted caller must use this
qualifier only on the authenticated connection whose qualified Resume the
primary accepted; it is not a certificate factory for arbitrary wire data.

Election, voting and promotion require the saved qualification and its exact
boundary to match the actual durable applied prefix. Real promotion and planned
switchover establish a new boundary from that prefix. The tuple comparison
remains lexicographic: a legitimately newer history may replace an older,
longer unacknowledged suffix. Announced boundaries need not be monotonic: an
abandoned epoch can start at 100 and a later elected branch at 90. The code does
not choose a data epoch by scanning gossip for the largest boundary below the
current sequence. A conflicting announcement cannot rewrite a boundary already
qualified for the local accepted history.

A snapshot source is checked under commit exclusion after flushing the exact
applied prefix. The archive carries its qualified lineage. Staging preserves the
target's original votes and highest fencing epoch; it does not invent missing
ballots. Structural source qualification is checked before the ready marker;
the restored database checks actual durable watermark coverage before primary
startup, election or eligible Resume. A newer local fencing epoch is preserved
and can still prevent following an older source. Bootstrap does not erase it.

## Related ballot and snapshot safety boundaries

A second source schedule lets an old primary stream after a recipient has
already voted for a successor: the learned primary epoch alone does not fence a
higher durable election term. The early announcement check now rejects an old
source against the saved election/vote term before application. The final
qualification check repeats that predicate under commit exclusion before ACK
eligibility. Thus a vote racing between early validation and application cannot
produce an old-primary ACK. If application wins first, a later shorter candidate
is compared against that actual applied/durable point and refused. Stale Ping
cannot renew primary contact, and stale-source switchover is refused. Learning a
ballot never manufactures a new data epoch or a committed primary.

An exact retained planned-switchover offer is a separate idempotent replay case:
its first durable drain itself reserves the target election term. Replay requires
the same original offer/ballot, source/config identities, source data epoch and
exact durable prefix, with no later term. A later term refuses replay and retains
the original offer. This exception cannot renew an old stream/Ping or bypass the
recipient's live authority checks.

Independent review also found a pre-existing staging race: a target can cast a
newer durable vote after `stage_standby_bootstrap` copied its HA file, then
restart into the older copied ballot. The companion fix retains SHA256 of the
exact original HA bytes in `BOOTSTRAP_READY_V2`. Immediately before any install
rename, actual original and staged DirectoryLocks exclude writers and the
installer rechecks that preimage. Changed state refuses installation and retains
both the original target and stage. A new explicit staging call can observe the
new ballot; the installer never silently merges or discards it. Recovery between
renames locks/validates the actual original `.replaced` inode; recovery after the
second rename validates it before completing the install. Marker reads are
bounded to 4KiB and original HA-state reads to 8MiB; larger state refuses.

This is fail-closed safety, **not bounded automatic recovery**. The whole HA-file
preimage can change for reasons other than voting. It does not prove the target's
applied-data head stayed unchanged: import progress can update WAL/replication
state without rewriting the HA file. General snapshot rollback/ACK-copy
redundancy, current Strong visibility and concurrent recovery remain acceptance
gates; this patch does not certify them.

The next deterministic **open** bootstrap/data-head gate is: stage a real source
snapshot at 100; import/durably apply the same-lineage frame 101 into the original
target (with actual ACK evidence if testing the managed host); verify its HA-file
preimage remains unchanged; drop the target and attempt installation. An
acknowledged copy must not silently fall back to 100. The current HA-preimage
check alone is insufficient to establish that result. This is a planned failure
schedule, not an executed regression or a newly passing acceptance test.

The normal CLI follower's `follow_once` has a single socket loop: SnapshotStart
returns into `receive_snapshot`, which accepts only snapshot chunks/end, stages,
then calls `restart`/`request_shutdown`. That same loop does not apply another
Commit after staging. Incoming peer workers can still vote (the preimage check
covers that race). This per-worker sequencing does not supply a general guard
for public `stage_standby_bootstrap` or other trusted engine import callers;
those require their own actual data-prefix/exclusion contract. It is not a
process-wide claim that no other mutation occurred.

Old511 runs bootstrap cleanup before checking format. Therefore the new marker
uses a different **filename**, not new JSON hidden under the old ready name.
The target reader fence is fsynced before staging starts. An old executable can
still delete the unrecognized staged work before refusing the target's format;
it cannot recognize that work as a ready snapshot. New code also refuses legacy
ready markers. During the existing two-rename missing-root crash window, however,
an old executable can delete staging and create a fresh root before seeing any
local flag; the original remains in `.replaced`. **Do not run or roll back an old
binary during an incomplete install.** The supported contract is a coordinated
all-version-5 upgrade. Atomic exchange/robust old-executable protection is a
separate design; no mixed-version rollback or power-loss qualification is claimed.

## Birth, old readers and upgrade limits

Genesis qualification requires a genuinely empty root observed under the actual
DirectoryLock, before format/catalog/recovery writes (only the held `store.lock`
may exist). It is durably published during replication-enabled fresh creation.
Missing HA state in an existing populated root is **unknown**, including a root
with sequence zero; it is never silently treated as a new member. An interrupted
fresh creation before qualification completes can therefore require a fresh root;
this change does not claim a general birth/recovery repair protocol.

`ha_qualified_data_epoch_v1` is a sticky local format-reader fence, excluded from
portable SQL/schema and row-stream reader features. Every HA-state mutation
unconditionally repeats its fsync barrier before writing the state. A visible
flag after an earlier failed directory sync is not accepted as proof of durability.
Routine metadata replacements retain the fence and force fsync. Old binaries
that do not recognize the flag refuse these stores; rollback is not an in-place
protocol downgrade.

Protocol-4 stores may already contain false data-epoch labels. A missing lineage
record refuses candidacy, votes, promotion, primary startup and eligible Resume.
It cannot be blessed by receiving another heartbeat. An upgraded standby with
its original voting state can request an actual snapshot from a genuinely
qualified source. A missing original voting-state file cannot be repaired by
manufacturing an empty ballot.
Accordingly, offline `ha reseed` no longer initializes a missing or non-HA target
root implicitly: first establish a genuinely fresh standby identity through its
normal native startup, or preserve the existing target's durable voting state.
An existing storage-mode bootstrap test now uses that actual source/target
initialization and snapshot producer; its engine/query assertions are unchanged.

**All-legacy group upgrade is blocked:** this change supplies no independently
validated recovery/bootstrap-source producer for a group in which every node
has unqualified protocol-4 history. Stop application writers and retain the
original stores/evidence. Do not edit JSON, discard ballots, relabel an existing
root as fresh, or assume a clean restart validates old history. A separate
reviewed recovery procedure with external acknowledged-commit reconciliation
and preserved voting identity is required before that deployment can upgrade.
This limitation is distinct from a fresh all-version-5 group. Mixed 4/5 groups
are unsupported; static membership and synchronous voter rules are unchanged.

`K=0` and timeout degradation remain explicitly asynchronous policies. Their
allowed RPO is not converted into a zero-acknowledged-loss guarantee by this fix.
Row fsync disabled likewise supplies no power-loss durability claim.

## Source attachment map

- `db.rs::open_inner` observes fresh birth under the real DirectoryLock;
  `db/ha_node.rs::HaNode::load` publishes only that genesis and preserves legacy
  unknown state. `HaNode::with_file` repeats the format barrier before changes.
- `db/ha_node.rs::{record_primary,qualify_replication_data_epoch}` separate learned
  authority from the actual durable applied boundary; `replication_resume_position`
  distinguishes explicit legacy absence from real errors.
- `db/ha_node.rs::{begin_candidacy,consider_vote,promote_by_election}` use the
  qualified tuple; `db/switchover.rs` uses the same exact prefix and term fences.
- `pgwire/high_availability.rs::{follow_once,serve_peer,run_election,start}` enforce
  protocol-5 tuple use, before-apply/final-ACK qualification and old-Ping refusal.
- `db/ha_bootstrap.rs::{stage_standby_bootstrap,finish_standby_bootstrap}` retain and
  compare exact target HA bytes through the versioned marker and locked recovery.
- `format.rs` recognizes the new **local** sticky refusal feature, forces durable
  replacement and deliberately excludes it from portable reader capabilities.

## Regression and acceptance scope

The added file-backed Core regressions use real durable commits, exported
frames, the importer, HA-file replacement, reopen and native voting to cover:

- Partial catch-up retaining the old tuple, full voter refusal, exact-boundary
  qualification and reopen, with legitimate newer-epoch ordering preserved.
- Nonmonotonic abandoned announcements, rather than a fake global monotonic rule.
- Persisted legacy mislabelling refusing election/heartbeat blessing, and an
  actual qualified snapshot replacing data while preserving fencing history.
- Missing HA state in a populated root refusing genesis.
- Actual injected HA-file directory-sync failure at qualification, health-fenced
  election before reopen, and saved-boundary validation after reopen.
- Real snapshot staging followed by a real later vote, exact install refusal
  preserving that ballot/stage, and fresh snapshot restaging/progress; versioned
  and legacy-ready admission boundaries; actual first-directory rename followed
  by the real new-version recovery entry (not a process-kill simulation).
  Public staging also refuses an already-installed pending new/legacy marker,
  rather than bypassing its recovery and deleting the original `.replaced` state.
- Higher durable ballots refusing old announcement/Ping eligibility and final
  qualification, including a real imported frame between early check and final
  refusal; a later shorter vote observes the actual applied prefix.
- Exact durable planned-offer replay, later-term refusal preserving that offer,
  and refusal to restage over an installed incomplete snapshot before recovery.

Existing vote assertions remain, but their setup now uses a genuine promotion
instead of gifting an epoch through `record_primary(empty_history)`. Existing
failed-metadata-write controls replace the initialized HA-file inode with the
same deliberate directory obstruction; their original refusal assertions remain.
The existing handshake test additionally checks refusal of actual version 4.
Format tests preserve exclusion from portable reader features and sticky fsync.
These regression sources are **not compiled or executed** yet.

The next real TLS/process acceptance must run normal five-node processes with
K=2 and retain an external attempt/ACK ledger. Establish the acknowledged prefix
on P/A/B, elect A, allow C only one bounded streamed batch below epoch 1's
boundary, then cut A before the next batch. Retain exact Heartbeat/ACK, durable
applied watermarks, data-lineage files and ballots. C must advertise its old
qualified epoch, B must refuse the shorter candidate, and no acknowledged
payload may disappear. Also exercise exact-boundary catch-up, legitimate old
unacknowledged-suffix replacement, snapshot, planned switchover, coordinated
protocol refusal and reopen at qualification/HA-file failure cuts. No such run,
power-loss test, RPO/RTO qualification or production election claim is supplied
by this source change.
