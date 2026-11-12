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
