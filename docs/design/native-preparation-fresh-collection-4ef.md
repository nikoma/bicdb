# Retained native preparations and a distinct fresh collection

This source batch is based on `4efcfc2cccc51307721777ad7d53a251e67bbbf3`
(1.0.512-beta candidate). It has not been compiled or executed. It does not
change metadata TTLs, original request budgets, admission requirements, or the
production gate.

The original required native job `132432945598`, run `43590137719`, failed with
307 passed, 23 failed and one ignored. Three failures reached
`hot_policy_tests.rs:743`, `record_candidate`, on n3. The retained
`NATIVE_PREPARATION_FAILURE` records show:

| Case suffix | Existing round already expired at current OWN entry | Existing round past deadline at count return | Current OWN remaining at count return | Current 5 s caller remaining |
| --- | ---: | ---: | ---: | ---: |
| `actual_original_a_through_first_policy` | 89,485 us | 1,084,477 us | 503,017 us | 4,005,000 us |
| `after_policy_readback_refuses_hook` | 107,671 us | 1,105,660 us | 500,021 us | 4,002,003 us |
| `early_all_released_admission_refuses` | 198,209 us | 1,229,071 us | 467,149 us | 3,969,130 us |

These are actual recorded clocks, not an I/O or scheduler profile. Positive
remaining budgets do not prove cancellation was absent. The original diagnostic
did not retain the earlier OWN's context identity or cancel bits. The token check
also checks the thread's query budget before cancellation/deadline, so the exact
sub-check that returned `QueryTimedOut` is not independently established. The
already-expired round deadline was nevertheless a mandatory refusal condition.
The later intended policy/term cuts were not reached in these three cases.

The source initializes the strict prepared round from its first actual committed
OWN deadline and only tightens that deadline for later observations. Native
preparation and CLOSED transitions for later members occur inside that window.
The current member's fresh OWN does not renew the earlier round. Simply ignoring
the aggregate deadline would admit expired observations. The existing fresh
preparation-phase API cannot repair this initial collection: it requires an
already completed actual prepared quorum.

## Source correction

`NativeJointPreparationCustody::retain` now wraps the genuine returned
`ActualServiceNativeJointPreparedEvidence` in one opaque `Arc`, retains its
original service association and Session ancestry, and grants nothing. The
native preparation, original State/A, native receipts, image/freeze lease and
one physical read pin remain the same objects. There is no decoder, parts
constructor, raw owner extractor, replacement preparation, or pin constructor.

The actual preparation parent/Session is sealed at each of the three genuine
service producers before native effects, then bound to the actual successfully
returned preparation checksum (including native xid), decision and DB-open.
`retain` accepts no caller-supplied Session. A canceled finite drive OWN remains
canceled; its original successfully prepared native fact can be revalidated
only through the distinct custody scope while that producer-sealed Session is
still live. Generic historical-request checks are unchanged. Source review
caught the first prototype's inability to resume this cancellation case; that
was a source blocker, not an executed runtime failure or qualified fix.

The actual driver retains per-member custody immediately after each successful
native preparation, before later callbacks or count errors. A retry uses that
same custody rather than repeating native preparation. The map is checked
against the complete original NEW participant set. A completed prepared quorum
is assigned before the map's extra references are cleared; unknown/failed
collection keeps the original preparations retained. The hot fixture follows
the same two-stage flow, while its existing strict candidate-negative controls
remain independent and keep their original assertions.

A distinct bounded collection obtains real recipient-specific OWN, then captures
the actual attached MStore point outside service/DB guards. It rereads the same
protected joint decision and returns an **uncounted** candidate over the retained
native preparation. The original record boundary alone can count it, using the
dedicated custody-scoped form of the strict fresh-phase validation: same decision and
native receipt, independently fresh context/nonregressing metadata, actual
CURRENT/archive, physical materialization/history/catalogue, original State/A,
birth/open, frozen lease and pin checks. Its actual fresh publication guard and
the aggregate/current/original-Session checks precede insertion. No DTO becomes
a role or a local quorum capability.

The original committed observation is preserved as historical evidence. The new
observation carries a separate fresh committed metadata object. Default strict
constructors still use the original observation and refuse its expired deadline.
The strict generic historical API also keeps its original cancellation refusal;
the custody scope independently checks the exact captured DB-open equality and
actual protected native/locked-scope identities, rather than renewing that request.
The aggregate deadline remains the earliest counted fresh OWN; it is not extended
after a count failure. Failure poisons/drops that request's round, while the caller's
opaque native custody remains. A later explicit collection must obtain actual
fresh authority again. It is not an automatic retry or reset of an uncertain
native publication.

Downstream metadata anchors and currentness checks select the distinct counted
fresh observation; original native preparation records remain unchanged. Pin
release reuses the existing fresh-phase validator, with original Session ancestry
still checked in later phases. It neither thaws the freeze nor grants Serving.
The release continuation carries opaque necessary lifetime checks for every
custody-backed producer, even when genuine producers use different Sessions.
Those checks retain only cancellation atomics/ancestor flags and the original
deadline, with no wait admission, State/A, pin, freeze, writer or lease reference.
Thus draining prepared owners does not discard a participant's cancellation
constraint or prevent unique release. This is generalized API lifetime
correctness, not a measured cause of the original three failures.
Existing driver `REQUEST=5 s`, `PHASE=4 s`, retry count and test network 1,500 ms
settings are unchanged. No old request, Role, OWN, or original round is renewed.

## New controls, not results

Five genuine fixture controls are added, all **NOT EXECUTED**:

- Create an actual strict round, let its actual issued deadline expire, prove its
  timeout refusal, then collect fresh observations over the same preparations.
  The unchanged remainder of the existing policy fixture must still pass.
- Cancel the original retained preparation Session and require refusal before a
  fresh metadata candidate can be admitted.
- Substitute another real service association into a genuine uncounted candidate
  and require the actual custody/count boundary to refuse it.
- Advance the actual local MStore term/version after the fresh candidate and
  require refusal of that stale captured publication point.
- Delete the actual preparation archive with the existing receipt-derived native
  fault helper and require the protected reader/count boundary to refuse it.

The controls capture Results before formatting/assertions, actual original/fresh
context IDs and clock/cancel observations, and protected preparation CURRENT and
archive before/after. New negative controls require their intended error rather
than accepting a timeout/busy outcome. A read-only cfg(test) memory probe survives
candidate consumption without exposing a cancelable token or authority. Native
preparation checksums, bytes and page xids must remain identical; the deliberate
archive deletion has its own actual fault xid and expected archive-only change.
The existing same-driver `AfterFirstNativePreparation` control keeps every
original assertion and adds actual post-cut proof that its finite OWN is
canceled while the producer-sealed Session remains live, with the same native
preparation context/hash. Its subsequent original full acceptance and no-new-xid
assertions remain required. It has not been executed on this source revision.

This is a same-process custody/authority correction, not crash-safe reconstruction
of the driver from disk. It has no cold-recovery, automatic controller, remote
global-decision, Joined additional-range Serving or physical scale-out result.
Original failed logs and classifications remain failed. The source prototype and
new APIs are not production qualification.
