# Post-joint NEW-slot NoData: source audit and acceptance specification

Exact inspected source: `ba868e9ff6780e98d4d799f5cbd83bacf0fdb210`.
Source inspection/test specification only: no runtime edits, Cargo, executed
post-joint regression, Azure attribution or data-loss claim. The requirement is
bounded recovery of a NEW slot actually started AFTER completed joint closure;
old-domain unresolved tails and minority-promise adoption are different gates.

## Existing domain and permanent evidence

`start_native_abort_attempt` obtains actual `SourceRangeImageLineage`, checks
actual same-Store protected catalogue identity, native/raw resolved predecessor,
and protects the full command BEFORE Prepare. `NativeAbortDomain::from_source`
uses the effective `native_write_range()/native_write_members()`; a protected
joint bridge can therefore describe the NEW writer domain while the immutable
physical `base_range()`/base credentials remain OLD. Per-slot native journal
records preserve command/domain/predecessor H, StoreUUID/birth and monotone
never-Commit/ever-Commit/no-effect/promise facts. The mutable range mirror's
latest domain must never reinterpret historical slot domains.

The actual joint chain is distinct: old closed native quorum and old permanent
promises; actual source capture/history and target materialization; full OLD+NEW
decision and actual metadata commit; NEW prepared/native-publication quorum;
local native/catalogue activation; and actual owned freeze release/armed open.
`NativeJointServingDomain` loads its protected current record AND immutable
archive, binds actual local birth/base/materialized cut and exposes NEW members.
It is historical physical-domain evidence, not a replacement for fresh current
metadata/data authority. `NativeJointActivatedOpenGuard` additionally binds the
actual DB/store open and serving generation, and arms only after real release.

## Independent refusal seams (all must compose)

| Source / symbol | Current behavior / consequence |
| --- | --- |
| `distribution_range_consensus/native_abort.rs`: `NativeAbortDomain::require_recovery_source` | Compares NEW candidate to OLD physical base/members. An absent-slot recipient can fail before it can protect a fresh restrictive start |
| Same file: `preflight_native_abort_publication` | ANY permanent handoff promise refuses ordinary Abort publication; completed joint intentionally retains that promise |
| `db/range_image_native.rs`: `require_native_no_data_without_joint_domain` and `certified_prefix/no_data.rs::apply_native_abort_quorum` | Unconditional actual-joint veto BEFORE covered-index retry/WAL, including same-configuration/no-op joint |
| `certified_prefix.rs::validate_with_native_joint_domain` | Post-cut prefix must be Applied; NoData refuses. NoData producer currently sets `joint_decision_sha256=None` |
| `db/native_recovery_ready.rs::attest_native_recovery_materialization` | Ordinary recovery still requires physical-base voters/credentials equal current; changed-membership joint cannot pass merely by advancing R |
| `recovery_state.rs`, `native_read_catalog.rs`, `native_writer.rs`, `joint_handoff_state/activation.rs` | New recovery freeze invalidates the ORIGINAL joint open generation. Current helpers still require that old guard for handoff-promise exemption/read/write, rather than a new recovery activation that preserves joint ancestry |

Deleting only the unconditional NoData veto cannot qualify recovery. A canonical
Abort-only result would miss subsequent Ready/read/release/next-ACK refusals.
These are explicit source support boundaries, not newly executed defects.

## Smallest sound NEW-slot support

Introduce an opaque current-joint abort-domain producer from ACTUAL protected
joint bridge/decision plus same-service catalogue activation and same registered
range-catalogue StoreUUID. At writer start it must observe genuine current open
permission, exact effective NEW membership/credentials/span/schema/restore and
actual native predecessor. Require all earlier protected controls covered by
actual immutable native outcomes, with no unresolved old-domain slot above R.
Bind the resulting NEW slot immutably to the global joint decision/cut and local
native bridge/birth/StoreUUID. Preserve old slots and their original audits.
Do not retroactively stamp an old retained slot with today's joint hash.

During recovery, revalidate that protected slot and joint history as FACTS plus
independently fresh current NormalRecovery metadata role and actual CURRENT
NEW-data quorum. A new freeze deliberately makes old open permission stale:
never require or revive that old armed guard as recovery authority. Domain checks
must use the qualified effective NEW lineage, not an intended config or a
base-range substitution. An absent local slot can acquire a restrictive start
only through actual from-birth StoreUUID/inventory, complete predecessor and
qualified current joint-domain absence proof—not raw absence/row emptiness.

Existing real Core prepare-only cleanup must produce the origin no-effect intent;
actual protected never-Commit and permanent promises on a distinct NEW majority
must match the full candidate/predecessor. Any Commit/CQ/Applied obligation refuses
new Abort. Obtain separately fresh post-QQ metadata. Then one native page-WAL
transaction publishes canonical Aborted history, exact decision and prefix tied
to that joint decision/NEW quorum. Local visibility B comes from THAT node's
actual predecessor and remains unchanged; do not create an application transaction,
WAL receipt or R-to-B cast. Covered retry/reopen must revalidate the SAME native
decision and joint ancestry, not convert JSON audit into a new quorum.

The fresh current-domain proof must continue through LocalReady/closed ReadyQQ,
actual Serving/Completed/Activated and actual release. A NEW normal activation
may authorize the suffix above the immutable joint cut only by carrying actual
joint ancestry plus fresh current completion/current open generation. Never clear
permanent handoff promises, reset old epoch floors, re-arm stale old generations,
or use metadata epoch alone as that exemption. First scope should keep the exact
post-joint membership/epoch/leader; higher-epoch post-joint leader transfer is an
additional genuine control, not implied by same-config success. Any new persisted
binding needs an independently fail-closed reader/wire contract coordinated with
owners; old roots without it are not silently upgraded from matching numbers.

## Smallest genuine acceptance and current fixture obstruction

Tracked ba868 has only `native_joint_actual_old_closure_four_metadata_rf3_nonempty_complete_prefix_and_shared_pin`,
which explicitly stops BEFORE target/new quorum/activation/cleanup. `git grep`
finds `release_activated_new_member` only at its definition. Thus no existing
tracked test establishes complete joint closure. Do not reuse that old-closure
pass as this prerequisite or construct bridge/quorum DTOs to shorten it.

Build a reusable ACTUAL complete-joint fixture first: four protected native
metadata stores and genuine writer-enforced RF3 `{0,1,2}`; real ACK1 ledger;
actual committed learner plan, old closure/constraints, actual source capture +
full target data/history install/finalization; genuine old/new materialized
quorums and protected decision publication/metadata commit; actual NEW prepared
and native publication majorities; local catalogue activation and owned release
on NEW `{1,2,3}` including the real new leader. Refresh writer policies via actual
HostMQ. Retain old promises, decision/native/StoreUUID/H/UID/B evidence. This
component may use existing metadata-only target3; fresh fifth-node operator
joining remains a separate acceptance gate.

Then, with no old unresolved tail:

1. Admit a REAL ordinary next-slot transaction on the new actual leader. Make
   actual current peer services refuse Prepare AFTER protected writer starts
   (the existing test-only service cut), so real Core cleanup yields definite
   no-effect/no ACK. Assert all actual ever-Commit flags false, no application
   WAL/rows, protected new-domain starts and unchanged per-node native R/B.
2. Establish genuine fresh current MQ/fenced data QQ, actual origin candidate,
   durable promises/current NEW majority and post-QQ metadata; publish canonical
   Abort on actual recipients. Preserve the earliest baseline refusal at the
   real guard; a characterization-test pass is not recovery success.
3. Require actual NEW-domain native R=R+1 and H(Aborted), unchanged local B/UID,
   full old ACK payload/hash reconciliation, then real Ready/Complete/Activate,
   current read returning the old value/absence of failed value, and next real
   user ACK on the new leader/current data quorum. No fake admission callback,
   fabricated voter set, force-clear or cached recovered-epoch seed.
4. Negative/cut follow-ups: old-domain unresolved slot, different joint decision,
   stale/removed peer credentials, any ever-Commit, missing predecessor, wrong
   StoreUUID, no-op joint, and native publication-before-raw-mark cold retry.

Old-domain unresolved-tail adoption, minority promise A then committed B,
origin-unavailable no-effect, lagging-abort catch-up and automatic orchestration
remain OPEN. The 4,096 slots/range, 64 MiB permanent inventory and terminal-control
headroom/capacity gates remain OPEN; increasing bounds or evicting audits is not
this fix. A bounded complete-joint case would still not qualify sustained scale-out,
physical host failures, Strong end-to-end or the full M1 fault-safe milestone.

## Later complete-joint test source

`fd2510259` and `d8ef4bf04` add the proposed actual image-target/new-leader
fixture; `a96d4065` failed compilation at three new full-record comparisons.
Those assertions were corrected without removing checks in `dc6a13d4`; a
subsequent fixture correction provisions an empty generic planning cache from
the actual fresh HostMQ configuration rather than trying to auto-enroll an
already known native voter. No passing complete-joint result is established
by those source changes. All six post-joint NoData seams above remain open.
See [the revision-scoped status](../ha-milestone1-status-2026-11-12.md).
