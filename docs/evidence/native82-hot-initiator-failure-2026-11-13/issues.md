# Reproducible findings and next smallest tests

These issues describe original runtime5a/Core tree0472 and qualifier88. All
original failed statuses, budgets and raw artifacts remain unchanged. The
checks below are proposed follow-ups, not results obtained by this packet.

## N82-1 — Incorrect initiator in eleven hot fixtures

**Confirmed fixture input defect; eleven original cases FAILED.** The real
collector/Manifest selected n2 (n3 for follower variant). The frozen shared hot
helper passes that donor's actual transport participant to the mutation Role
request. Recorded range leader n1 and copied metadata leader n0 differ. The
actual metadata server refuses the request at `actual_role(...).unwrap()`.

Source: `hot_policy_tests.rs:252–270`, helper `remote_source_tests.rs:283`, and
`distribution_consensus.rs:341`. Source copies and Git membership proofs are in
this packet. The named late hooks, NEW/decision, first policy and post-join
ordinary APP/Strong checks are **NOT TESTED** by those original cases. An outer
phase label does not prove that its intended operations started.

The smallest independent follow-up keeps the real donor's capture and OWN
unchanged and uses the actual range-leader transport participant for the
mutation initiator. Preserve the existing nonleader-negative production case.
Capture the real witness request/reply or error and actual transport identities
before assertions, with the same original budgets and all later application
assertions. First run one happy original-client ACK/full-Strong case, then the
original fault controls. A refusal before its requested seam is a prerequisite
failure, not proof of the advertised fault guarantee. Do not weaken the
leader/metadata-leader predicate or cast donor OWN into Role.

The original SDK observer does not contain these native witness RPCs. This
packet's actor relation is source-derived and correlated with actual collector,
Manifest and copied MStore data; it is not a wire capture or an executed
corrected-actor causal experiment. A later corrected run belongs to a separate
source-pinned corpus and must not replace this one.

## N82-2 — NoDomain metadata refusal matcher predates version fencing

**Original test FAILED; bounded actual refusal observed.** In the original
NoDomain point, `observe_remote_term(current+1)` runs on the real target MStore.
The subsequent current-metadata check refuses with the actual-version-dirty
message. The test expects the older owned-member-state message and panics before
its final state assertions/report.

Source: `target_no_domain_tests.rs:105–116` and the existing version guard in
`distribution_consensus/publication_version.rs`. Original earlier fence term1,
copied target term2 and raw error are retained. Exact gate generations and the
final NoDomain post-state report are unavailable; do not synthesize them from a
later fixture or claim an independently recovered database.

The smallest follow-up retains actual before/after MStore term/version evidence,
the exact primary error and target native/raw/row/UID state **before** assertions.
Require the actual term mutation to succeed. Match only the applicable typed
Cluster refusal at the owned-member or stronger version boundary; keep every
state/fault assertion and same request window. An arbitrary metadata/transport
error is insufficient. Run this control independently of the eleven actor
changes, then the unchanged full original cohort after a coherent fixture batch.

This is a local stale-metadata test; a genuine partition/new-leader fencing
scenario remains a separate gate.

## N82-3 — Qualification and evidence scope remain bounded

**Not tested:** preservation of every acknowledged record through physical
failure, correct Strong during a second populated-range movement, bounded whole
relocation convergence, operational node replacement, unchanged PostgreSQL
cross-range SQL, sustained five-node capacity gain and production admission.
The 70 passing library cases and 164 initial APP ACKs are useful partial
results. They do not establish those full guarantees.

The smallest next acceptance case is a completed actual first handoff with the
original fixed-gateway SDK's full Strong/new ACK, followed by the genuine
second populated-range receive while first-range APP/Strong continue. It needs
a real populated Joined admission producer; a blank FIRST receiver or local
proof object is not a substitute. Actual stalled engine/disk I/O is separate
from a transport hold after export. Keep physical four-to-five scale-out and
post-convergence throughput qualification isolated from this library test run.

The packet's omission inventory preserves hashes for the complete private
original. Public projection correctness and byte custody are not cold-replay
proof. Future witnesses/native-phase traces should be retained separately from
the application SDK journal, without adding authority from observational DATA.
