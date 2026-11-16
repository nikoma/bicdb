# Retained v1 registry publication and the d3 CLI availability failure

This source unit starts at `2dcde82cf0de7ce0d87f5aa4264147c0f8e0b4d7`.
It corrects a confirmed source self-refusal and defines focused controls; it has
**not been compiled or run**. The preserved d3 runtime result remains a failure.
No future RegistryAfterAllocation, Serving, Source-retirement or controller
implementation is imported by this unit.

## Observed failure and causal limit

The complete d3 REST job `132945377712` at public
`d3af39c107951802a612d31b42e70fd4bc90e71c` executed 953 unique case rows:
950 passed, one failed and two ignored, with 96 summaries. The failure was
`writer_registry_refresh_baseline::native_metadata_registry_addition_actual_cli_requires_healthy_existing_read_and_fresh_ack`,
at `writer_registry_refresh_baseline.rs:463`, not a compiler error or the
historical AppHost port failure.

The original value was actually ACKed and Strong-read before adding the fifth
metadata voter. Afterwards the SAME four original coordinator, registration,
DB/Store-open and policy identities remained installed, but each reported
`publication_pending=true`, no verified registry cache and
`native writer policy publication unresolved`. Two original 2s read/append
rounds produced no healthy triple within the original 10s gate. Fresh appends
were explicitly NO_USER_ACK/UNKNOWN; no false absence or wrong value was
observed, and no DATA movement was attempted. Running child PIDs and published
dispatchers did not establish readiness.

The source regression is exact: `bafc3b86147290a5280a7a2a547579b823622d36`
introduced pending refusals in ordinary `require_range_id`, `require_range` and
`require_command`; these were absent at `7e99c9c96`. The v1 registry sink sets
that pending bit before its actual native transaction, then its final
`with_current_for_publication` calls `require_registry_refresh_origin` →
`require_original_database` → `require_range`, rejecting its own bit. A fresh
registry preparation reaches the same refusal. Ordinary APP/Strong must keep
those refusals; the maintenance identity purpose must not borrow APP admission.

The runtime artifacts do not record the first native registry helper return or
its exact first pending transition. Pending, CURRENT bytes or a committed page
status alone do **not** prove that original helper succeeded. The source
counterexample establishes that even a genuinely successful new publication
cannot pass that final chain. It does not prove the actual first failure was
a successful commit, nor establish a historical disk-loss cause.

## Actual owner and outcome contract

A private RAM attempt belongs to the SAME installed `NativeWriterHostState`.
It retains the original bounded v1 candidate before pending publication/IO and
records NotEntered, Entered, Known(actual returned XID) or Unknown. Known is
assigned only after the real `in_paged_transaction` returns `Ok(Some(xid))`
matching its actual issued candidate XID. Any error/None/mismatch retains
Unknown; the original error object returns unchanged. A first static error-kind
label is retained without invoking Display under journal/attempt guards.

`in_paged_transaction` returns success only after `paged.commit` returns success.
A sync or later commit error fences the actual DB and returns an error even if
page status or visible records look committed. Neither those bytes nor status
can upgrade Entered/Unknown. No replay occurs on those states. A pending bit
without this actual owner also refuses. The retained owner has no cold or
Deserialize constructor, and no generic health reset or READY permission.

The dedicated registry identity path retains all existing actual State,
coordinator, DB/root/open/birth, schema/hook, effective-range and physical
Serving-domain checks, excluding only the registry pending refusal. It creates
only the existing sealed registry preparation. All generic APP/Strong/command
methods remain byte-exact and strict. Actual service/Store slot, fresh original
MQ/OWN and parent checks remain mandatory before effects/publication.

Fresh Known continuation requires exact original candidate facts and
nonregressing fresh metadata anchors. It verifies actual CURRENT, every selected
ancestry/history record and the candidate, plus the actual committed status for
the retained Known XID. It performs no new native transaction. Reaffirming an
already published audit uses the original private verified cache matched to the
actual durable chain, never a record decoded into Known. Only successful cache
publication clears pending and retires the RAM attempt. A late error retains it.
A registry attempt and an allocation attempt exclude each other at their
factory origins and again under the same actual serializable admission gate;
another purpose cannot supersede it. Factory eligibility has a separate
attempt check; publication origin never reacquires the sink-held attempt lock.
The future RegistryAfterAllocation factory is absent from this baseline and
must carry the same v1 exclusion when that independent unit is later ported.

Durable v1 bytes, key/checksum/domain validation, native reader format and all
64-history/256KiB bounds remain unchanged. This introduces no new durable format
or permission from old records. Existing cold-open refusal remains strict.

## Source controls and preservation

The original CLI test, its 10s healthy-triple gate, all 2s SDK calls and all
assertions remain byte-exact. Existing registry tests retain actual fifth-voter
continuity, old/replaced owner and both generations' entire history inventories,
original cancellation/Store-slot refusal, late known readback/fresh retry and cold
no-grant assertions. Schema/member/domain checks and allocation controls remain.

Two additional normal-host component definitions use the original real RF3
INITIAL fixture and current private factory/sink. One cancels only the same
parent at the actual successful native API return when sampled parent/OWN are
both live, then requires original QueryCanceled, pending APP refusal, the SAME
Known XID/candidate, a distinct fresh no-native-replay continuation and real SDK
ACK/full Strong tail. The other injects actual `page_log.sync` failure, requires
one fired fault and original CommitOutcomeUnknown, retained Unknown, unchanged
XID/LSN under a fresh refusal and separate actual pending/health SDK denial.
The original installed hook remains present; presence is not readiness.

Callbacks store issued/returned-XID and sampled-live DATA before conditional
cancellation, without formatting/assertions/IO. Actual original Results/cuts are
reported outside guards before fallible projections, fresh requests or cleanup.
The unknown fresh request may lawfully stop at the actual DB-health refusal
before the private Unknown match; no bypass is introduced to force that match.
Transport, timeout and service-busy errors cannot satisfy the new expected
pending/health refusal checks. Real cleanup calls and their errors are retained;
action errors/panics keep precedence over cleanup failures.

These definitions are NOT EXECUTED. Additional adversarial actual untracked
pending, modified CURRENT/history/predecessor and cross-purpose overlap cuts
remain unexecuted. No fabricated pending setter or replacement owner is used to
claim them. Standalone formatting, source inverses and version checks do not
establish compilation, latency, the original CLI health gate, physical HA,
controller/cold/repeated relocation or full Milestone1 qualification.
