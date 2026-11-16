# Native runtime corrections after the failed 4f run

Prepared source checkpoint, **1.0.520-beta**, 2026-11-16 UTC. Delivered main
remains **1.0.519-beta / `587dd6e4775e454cc9e8224d5fed76e8795c6ff6`**.
The changes below have source reviews and standalone formatting/version checks.
They have **not compiled or run** at this checkpoint. Publishing this source
does not transfer the preceding candidate's passing tests to the correction.

## Actual preceding result

Source `4f33a8b016a999d43410ca8698270dc01fbeffd6`, tree
`4c87ba7caed1e5a9043eac854d7bdf15ac00b15d`, completed normal CI at checkout
`2f42e67a11c5d8f81d79e170edd0814d4a741d2d`: ten jobs passed, one failed,
four optional jobs skipped. Serial native failed **387/84/1**. The 67 FIRST
and 17 learner/hot-policy failures retain their actual reached cuts and errors.
Remaining-workspace passed **1,062/0/6**, including six native CLI cases.
The original fifth-metadata-voter availability regression passed with unchanged
healthy-old-read/fresh-ACK/full-readback requirements. Five database processes
ran on one CI runner; DATA assignments did not move.

The complete failed run remains read-only: 96 files, eleven full logs, eight
original ZIPs/28 members. Manifest SHA256:
`320733868a453f83a4aa3eb885c44126e438a12615a6e6482073016c6677d608`.
Independent complete-archive integrity receipt SHA256:
`3fcef4de7951cdd47f9ea857d8bb25b583c1cd68581582de0f1a6a61082ffa81`.
See [actual findings and reproducers](native-520-regression-findings.md).

## Source changes and preserved boundaries

| Change | Implementing source commits | Contract |
| --- | --- | --- |
| Finite control contention | `14b2f8ba1ed7e5b97be82e4ac0f77b9cb2cf64d3` | One original 900 ms metadata-maintenance window includes status. Committed publication waits on the same actual service only under an existing finite parent. Singleton cleanup initialization waits under the original finite opening request. No proposal/native commit replay, new timeout, swallowed startup error or changed unbounded-caller contract |
| Full metadata versus effective native allocation domain | `0986c006db96ce47f15a6014f51eab74a246fedd` plus corrected child `9f7b5c646f29734cc8ab895cc1673e414a676e6d` | The protected decision retains its full metadata descriptor and scope. A private actual-installed-State projection proves its relation to the effective native voters using the verified v2 AllocationEpoch audit. Ordinary/malformed mismatches remain lazy refusals, historical/observer predicates remain exact, and the three new hook/cache acquisitions are nonwaiting. No generic learner filtering or decoded-record permission |
| Bounded first-policy entry work | `43253fbe23ddfc18e11c42589b01e8b72eaa52da` | A single-use borrow carries the exact already-checked entry admission/caller into RAM-owner construction or replacement. Route/MQ acquisition observes both original clocks. Full external validation still runs after fresh HostMQ and before catalogue/native effects. Native IO, readback, protected publication and final owned-MStore checks remain |

The allocation correction's original local revision `48dd84d63` was rejected
for changing ordinary refusal ordering and adding blocking reads. Its failed
review remains retained. Corrected local source `ae31a910a` passed independent
source review; that is neither compilation nor runtime acceptance. The actual
comparison Result and any secondary forensic Result are now retained separately
until the test facade's guards unwind. Inherited health-error formatting remains.

The [allocation-domain contract](native-allocation-activation-domain-4f33.md)
and [first-policy entry contract](native-registered-first-policy-entry-scope-4f33.md)
describe the exact proof and lifetime limits. The ordinary Installed fast path,
original Known-XID read-only continuation and late-error retention remain.
Visible bytes or committed status cannot promote an API Unknown to Known.

## Validation still required

The original controls, assertions, 1,500 ms native fixture, five-second policy
requests, two-second SDK calls and ten-second CLI health gate remain. Fifteen
additional control definitions cover contention/initialization, actual
allocation comparison and real entry cancellation/expiry/metadata change.
These definitions have not executed at this checkpoint.

An additive test diagnostic retains the actual activation return before a later
fault assertion can panic. The preceding large-head case fired its expected
fault zero times and did not retain its original error in uploaded evidence.
Its cause remains **inconclusive**; this source batch does not reconstruct that
error or claim every one of the 84 failures is fixed.

Normal CI must compile and execute the final composed source, including every
original positive SDK/Strong tail and intended fault cut. A timeout, unrelated
refusal or unreached fault cannot pass an intended negative. No local Cargo,
Rust compiler/test, SDK or database execution is part of these source checks.

## Qualification remains open

This is protected virtual-range runtime work. Automatic native DATA placement,
populated-target admission, complete cold DATA restart/rebind, bounded repair
and source retirement/reclamation still need their own production integration
and executed evidence. The [operator audit](native-operator-four-to-five-gap-audit-2026-11-11.md)
retains those gaps. General transparent PostgreSQL SQL/joins/transactions remain
separate. The original Azure run stays failed with verified teardown.

No new VM, release, verified deployment, full 64-range/8 GiB external-ledger
campaign, physical RPO/RTO, fault-safe four-to-five convergence, sustained
post-convergence throughput gain or production admission is established here.
