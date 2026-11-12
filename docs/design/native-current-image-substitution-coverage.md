# Protected current-image source substitution

This is an implementation note for the private correction based on
`f1f7ebdf96b737c8f9c192b2e7831ee108abe6f6` and the subsequent source-only
review corrections to `ba868e9ff6780e98d4d799f5cbd83bacf0fdb210`. Actual
failed CI is distinguished below. No passing runtime regression, operator,
Strong read or physical qualification result is claimed here.
It supplements the historical [interrupted-image review](native-interrupted-current-image-resume-review-2026-11-11.md)
and [protected-proposal design](native-interrupted-image-proposal-protocol-2026-11-11.md).

## Actual failure and missing seam

Focused CI run `43177871485`, job `131046410372`, source
`f1d1398f738868accc20e3757a33752d5e62b726`, executed two replacement cases:
zero passed and two failed. Both reached the history kernel's explicit
current-quorum substitution refusal. Protected P1 and its old inactive history
were retained, the target was Installing/unknown, and genuine replacement P2
selected source n2 after source n0 became unavailable. The new source had
materialized the original uncertain command2 and verified its full history
against a fresh mixed data quorum. Command2 was not an additional application
ACK. The kernel still required a materialized TARGET native point to cover P1;
Installing correctly prevented that target from advertising such a point.

The full replacement/resume verifier was consumed while building the source
cut. Its live quorum authority and original cancellation/deadline were dropped;
only historical acceptance remained. A historical audit or matching hashes
cannot supply the missing current authority.

## Narrow producer and invariants

`distribution_range_image/source_cut.rs` now retains the consumed actual
`VerifiedCurrentImageReplacementHistory` or `VerifiedCurrentImageResumeHistory`.
Normal repair and committed-handoff cuts do not gain this capability. Every
capture validation for a retained verifier checks its original authority; no
fresh caller budget renews that authority.

`NativeHistoryTargetContext::with_source_substitution` uses that capability
only together with the actual target fence, current role, store/open, birth,
namespace UIDs, safety and complete catalogue obligations. The private producer
in `db/range_image_native/history_stage/substitution.rs` requires:

- An actual protected current proposal accepting this actual source/cut, plus
  its checksum-validated immutable ancestry (at most 128 records). The actual
  quorum's proposal and the prior stage's protected acceptance must both occur
  in that ancestry. Missing or deeper unsupported ancestry fails closed.
- Exact target identity/open/birth and current target-local role/session;
  preserved schema/restore/configuration checks remain in the existing fence.
- Same logical genesis and a source range index at least the prior accepted
  index. The accepted old H@R, stored received-prefix head, stored audited
  prefix head AND the prior stage's independently observed target-native
  point must occur in the ACTUAL source immutable generation under its
  existing exclusive capture pin. The old target point is required even if
  unmatched and fresh safety is now Installing/unknown. It is copied forward
  as an unverified compatibility obligation when fresh safety has no point;
  it does not become a newly advertised native floor.
- All inherited substitution points are preserved too. At most FOUR unique
  explicit points are supported. A larger union fails closed without pruning
  history or altering the old stage; continuation/compaction through a sound
  verified-chain summary remains an OPEN availability gate.
- Every remaining old stage command obligation is unioned with the actual
  current catalogue/native-pending obligations, rejecting conflicting hashes.
  The new full contiguous stream must discharge all of them before any row
  clear/import. No checksum list or history is truncated.

The no-capability path retains the existing materialized-native-point guard.
A substitution point is not put into `NativeRangeInstallSafety` or a native
floor. The private coverage type has no serde, clone or raw-parts constructor.

The actual old header, received cursor, audit progress and generation are
archived immutably in the SAME native page-WAL transaction that publishes the
new active stage. The new optional header binding records the old header hash,
protected ancestry and covered points; it is audit/binding data only. Staged
operations validate the ENTIRE immutable stage-archive chain, at most 128
records, and check covered points again in the full streamed history. Missing
immediate/nested archives and deeper unsupported chains fail closed. Repeated
substitution of an unchanged source scope also retains this binding; it cannot
drop inherited points merely by starting another attempt. Existing headers omit the new optional field, so
old None bytes/checksums remain unchanged. Each archive uses the existing
8 MiB bounded native-record codec; the full immutable history remains streamed
entry by entry and is never truncated to that receipt bound. Archives/generations
are retained without GC; total disk reclamation is not delivered by this patch.
An old reader that drops a present
binding fails the header checksum rather than silently using it.

## Source tests awaiting execution

Existing dead-source replacement, native-P2-orphan cold resume and durable-P2
cold resume retain their ACK1, original-command2, sparse/CQ2, native-unknown,
local-B-versus-R, namespace UID, old-intent and old-fence assertions. They now
also compare the EXACT archived old stage checksum against the actual header
observed immediately before beginning the replacement.

Controls run with a genuinely produced coverage token and reject changed
attempt, target-open and prefix bindings, the wrong actual source history
point, and the genuine source-local role used as the wrong target-local role.
These checks must leave the original stage commitment unchanged.

Two stopped/copy/reopen cases leave either only H0 received (no complete
stream/audit), or all entries received but only H0 audited. Both first assert
that row clear is refused, then exercise dead-source replacement and cold P2
resume through the full new source verifier. A third case stops before any
history is received: its ACTUAL old target Genesis R0 point is unmatched and
must survive Installing/unknown, protected replacement and cold P2 resume.
These are R0-target controls, not evidence for a nonempty target ACK R1.
Old audit completion is never invented.

The source-only test controls fault-project an ACTUAL existing archive read to
missing and require refusal, without removing/resetting stored records. They
also fault-project a nested archive if the actual chain has one. A separate
genuine multiple-substitution chain fixture remains unmeasured; conditional
nested checks are not a claim that such a schedule executed.

The code permits an explicitly covered higher accepted prefix, but a genuine
P1(R1) to P2(R2) application-history fixture remains an OPEN acceptance gate;
the current equal-cut R2 controls must not be cited as that evidence. All new
source/tests are uncompiled until separately recorded CI. Lower-R repair,
changed-membership/joint transitions, arbitrary source death, public operator
scale-out and independent-process/physical qualification remain separate gates.

## Executed feedback and remaining limits

Actual CI `43183211113`, job `131064003653`, source `ba868e9ff`, compiled the
all-target and isolated-consumer checks successfully, then ran six selected
cases: zero passed and six failed in the SHARED seed, before any substitution
assertion. They all stopped at `interrupted_normal_repair.rs:349` with
`controlled WAL/native callback is still active`. This was one production
ordering bug: `begin_native_history_stage` acquired its OWN counted mutation
lease before calling a target-safety preflight that correctly requires zero
controlled writers. No delayed callback or artificial drain was implicated.
The source correction keeps the existing journal/freeze preflight, acquires
the lease only for publication, retains it through actual durable page-WAL
completion, and drops it before post-publication quiescent checks. No guard or
original fixture assertion is relaxed. These corrections have not run Cargo.

The retained quorum/role deadline applies to FULL source-history verification,
capture and transfer. The small fixtures use a five-second authority; a
30-second capture budget does not extend it. Larger R/history or images may
not finish within that original window. No 8 GiB or long-history continuation
claim follows from these tiny cases. A safe checkpoint/reacquire protocol with
new actual quorum coverage is still needed; old quorum audits must never be
retagged or their deadlines renewed. Protected receipts, old stage history,
command obligations and refusals remain durable facts after expiry, while
fresh permission remains independently mandatory.

This capability is strictly **CurrentVoterRepair**. An unassigned/relocation
learner is not a current data voter and cannot obtain that role merely because
its catch-up source died. Learner SourceDiesDuringCatchup needs its own valid
never-exposed-learner abort/restart or fresh-current-voter source-coverage
protocol. That lane, post-joint NoData, cross-process transport/operator
control, and fault-safe physical 4→5 qualification remain OPEN.

## Nonzero target-prefix regression source

A further SOURCE-ONLY fixture based on full candidate
`a96d406596991057e6f4167b7185783f06f3c8fd` adds
`actual_installing_replacement_preserves_acknowledged_target_r1_under_source_r2_cold_resume`.
It has not run Cargo locally. Its schedule uses the existing real fsynced RF3
services and actual recovery/quorum producers, with simulated data delivery:

1. Deliver genuine ACK1 to the target while native delivery is enabled. Verify
   its actual native R1, common logical H1 and exact acknowledged record SHA.
   The client ledger still contains only ACK1. Then make the target unavailable
   for command2; source0 and source2 genuinely commit/certify command2, while
   source2's Apply2 reply is deliberately not delivered. The originating local
   post-admission fault leaves command2 Unknown, never a new application ACK.
2. Bring the target back and make source2 unavailable. Target raw1 is adjacent
   to command2, so use the EXISTING ordinary historical Certify2 path, not sparse
   certification. Verify its raw CQ2/full original command and native R1 remain
   separate. Source0 then uses genuine current-certification QQ to materialize
   command2 and establish native R2.
3. Produce protected P1 through full actual source0 history and target
   obligations. Stop before receiving any history. The target's actual stored
   native H1 commitment is therefore UNMATCHED. Installing hides native serving
   visibility but must preserve ACK1's physical record, UID, original CQ2 and
   protected preimage. Drop all owners and strictly reopen a copied tree.
4. Make source0 DATA delivery unavailable; commit the existing metadata E2/
   leader2 transition through actual metadata store election/replication. Real
   source2 + Installing-target observations select the existing command2 and
   certify it under the fresh scope. The actual Installing certification
   producer may now add its permanent full-body projection; tests do not inject
   or fabricate a sparse marker. The complete original ordered log is unchanged.
5. Source2 really replays/materializes command2, verifies its entire immutable
   history with a mixed current quorum and issues protected P2. Retain ACK1's
   target record across that handover; close all owners again and resume P2
   with a fresh actual quorum. Require the archived H1 point to equal the ACTUAL
   acknowledged H1 captured before P1, with matched=false and received count0.
   The new full stage/audit, image import, catalogue adoption and Prepared/Ready
   path must reconcile the exact ACK1 and preserve the original command2 hash,
   schema/UID/lower bounds and actual target MVCC B (greater than R2).

This needs no new write while source0 is frozen: ACK1 and uncertain command2
precede P1. RF3 is therefore sufficient for this nonzero TARGET-prefix test.
It does not establish P1 accepted-R1 to P2 accepted-R2 availability; that separate
schedule may need RF5 to retain an ordinary majority while the old source and
Installing target remain frozen. It also does not simulate an entire host0
failure: metadata stores remain reachable in the existing local test harness.
The default R0/sparse schedules retain their exact expected preimage0/raw0;
only this explicitly selected seed mode expects preimage1/rawCQ2. Birth-v1
component fixtures are not retroactively enrolled or qualified for Strong,
born-v2 writer policy, independent processes, operator scale-out or physical HA.

## Higher accepted-cut regression source

A separate unexecuted test,
`actual_installing_replacement_covers_higher_accepted_cut_after_cold_majority_ack2`,
uses three actual fsynced services and the existing recovery/metadata backend
paths. It changes no production guard or proof constructor. The test-only bus
forwards only readonly NativeReady/NativeCompletion observations to their REAL
Runtime receivers; it constructs no replies or quorum lists.

P1 accepts actual source R1/H1 after ACK1. Its full history is staged/audited and
the target receives the real row through P1's typed image grant, but no native
base, catalogue adoption or target Ready is published. Every source capture,
installer, data service, DB and Runtime owner is then dropped; weak DB checks
must drain before a copied tree is opened. The target's exact P1 stage, UID,
partial row and refusal remain unchanged.

Closing the capture does not release the durable Active recovery fence into
ordinary writes. On the new opens, with the target DATA path unavailable,
ordinary voters0+2 must genuinely fence, prepare, collect closed ReadyQQ,
persist CompletedQQ, protect/activate the serving decision and arm the owned
physical freeze LAST. Only then is the actual recovered coordinator installed
and allowed to ACK2; actual NativeApply on voter2 must establish R2/H2. The
client ledger now has TWO real ACKs. No write is performed while an old P1
capture/freeze remains live, and P1's nonce/permission is never revived.

All owners close again before a second copied strict open. Source0 DATA becomes
unavailable, and the existing actual metadata election/append path commits
E2/leader2. Source2 is ALREADY materially R2 from genuine ACK2, so this test does
not abuse `InstallingCommittedDecisionRound`: that producer correctly refuses
a historical certification for command2 beyond P1's accepted R1. Instead, the
fresh mixed Installing quorum and full replacement-history verifier check H1
membership inside the actual R2 immutable chain and every observed obligation.
The test explicitly rejects a forked H1 binding before beginning replacement.

P2 retains P1 as a protected ancestor. Its actual target stage archive must
report old acceptedR1/H1 and newR2/H2, with the exact old stage checksum. The
new full history/audit/image/adoption/PreparedReady path must reconcile BOTH
external ACK records, preserve target UID, use an actual new target MVCC bound
separate from R2, and remain frozen. Old P1 proof/state is not deleted or reset.

This is a SOURCE-ONLY regression until separately executed CI. The prior
R0/nonzero-target cohorts do not prove it. RF3 is sufficient for this particular
orderly-close schedule because actual ordinary0+2 recovery follows the end of
all P1 owners; RF5 may still be needed for a schedule that keeps the original
source capture frozen while an independent ordinary majority advances.
The test remains one-process component evidence: it is not source-host loss,
learner restart, new-target activation/Strong, operator or physical qualification.
