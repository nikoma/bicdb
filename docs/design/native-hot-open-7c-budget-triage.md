# Current 7c local Open budget triage

This source correction is based on
`7c77418656a41ec6e25c7601319e061c328f48e1` (source 1.0.520-beta).
It has not been compiled or executed. It does not qualify the original failed
cases, their intended fault cuts, ACK/Strong tails, or the separate future
remote Serving leaf `0b792faeda8cf27735a37028dd59ac6ce8603e5e`.
No application timeout, role/OWN window, workflow, or original assertion changes.

## Observed scope

Actual normal Tests run 43704937146, Native job 132783896405, recorded
363 PASS / 45 FAIL / 1 ignored. This note covers only its 21 hot-policy failures.
The canonical full gzip is retained under
`/root/bicdb-ha-private-evidence/visibility520-pr207-7c7741865-run43704937146/`.
Raw SHA256 is `d64ff32826c1acd4a59aa4819be4ea0933a9ed5c8effa4b1ae6201d99aa83172`;
gzip SHA256 is `e8808425833f37cc6acacf5d161c627982daaf5f90d1091abb7fc5b4f5b46110`.
The sole collector owns that directory. This task reads it and writes separate
source/clock projections, never another raw download or canonical mutation.

Twenty cases fail at hot_policy_tests.rs:991 when the ORIGINAL first
`actual_catalogue_open` call returns QueryTimedOut. The large unrelated-head
case instead reaches its original `fault_fired == 1` assertion with zero fires;
its last service scope expires before entering the catalogue kernel wrapper.
The intended target activation fault/ACK/Strong tail is not established by an
earlier prerequisite success. Missing POST_WAL markers do not establish no
effect, rollback, lost ACK, or a CPU/IO root cause.

The last actual service invocation enters with 198–427ms of its original
bounded request remaining, and spans 263800–504668us. In the twenty ordinary
failures the local pre-kernel interval consumes roughly 252–350ms, followed by
a failing kernel invocation spanning 49859–251413us. These are wall intervals,
not CPU profiles or disjoint totals. Nested trace durations must not be added.
The current log lacks individual timing for several repeated owner/domain
validators; this correction adds those exact source boundaries.

For the original DATA ACK/full-Strong case, the final service starts with 414ms,
the kernel starts with 162ms, and the service ends after 500056us. Its final
foreign n3 current check spans 92978us and expires. This proves the observed
cut/time distribution, not why a particular filesystem or CPU operation costs
that amount. Last-boundary per-case projection is frozen separately in
`/tmp/bicdb-native-foundations-ci/7c-hot21-open-triage/last-actual-open-boundaries.json`
(SHA256 `6d1f5e98496ee12ff3b7ada5e5716672a5aba496104aacbbdec36e0ec5c1e946`).

## Narrow source correction

`recover_fresh_native_joint_activation` formerly performed
`original_phase_owner_for_activation` and
`original_writer_domain_for_activation` BEFORE checking whether any activation
existed. Those methods perform full original domain/currentness checks; the
new-publication path then repeats them. A None lookup grants no receipt,
activation, recovery permission or currentness proof.

The new early None branch requires the actual selected activation absent,
NO retained owned activation at all, NO persistence uncertainty, NO selected
activated-Open guard and NO selected image-install intent. Original live and
actual current-MStore binding checks still precede it. Existing/unknown/owned
states keep the entire original recovery path. No old owner can use equal
visible bytes or an absence result to clear uncertainty. The distinct new
publication path remains byte-exact, including full original owner/domain,
foreign participants, physical native/history/Frozen, catalogue, metadata,
Controlled mutation, actual owned IO and final readback/version checks.

The service also previously ran the SAME full writer-domain validation twice:
first through `actual.require_original_phase_owner`, then directly through
`actual.writer_domain.require_current`. Only read-only storage/catalogue checks
and the refsync method (which only refuses uncertainty) intervened under the
same service/DB scope. It now retains one full original-phase-owner validation
at the later boundary before either recovery or publication, with explicit exact
original prepared decision equality to preserve the admission-domain binding.
The existing actual owned-Unknown retry sink above this point keeps all its
own independent guards. No native write or resync is moved before authority.

This removes source-confirmed duplicate work; no saved time, failure-rate
improvement or sufficiency to fit 1500ms has been measured. Required subsequent
CI must run every original case and retain every failed initial API result.
There is no hidden fresh retry or failed-first-call-as-success rule.

## Discriminating real control

`native_registered_fifth_activation_actual_absence_then_cancel_refuses_before_io`
uses the existing genuine normal-host/actual native preparation fixture. The
selected-thread one-shot callback is invoked only at the actual no-state
recovery lookup above. It first proves the original admission live, records its
remaining original time, then cancels that SAME original admission token.
The untouched new-publication path must return exact QueryCanceled, with the
real catalogue rename fault not fired and full retained native/Frozen/catalogue
history unchanged. Earlier timeout/busy/foreign refusal is an unreached cut and
fails; no synthetic owner, status setter, native counter or new deadline exists.
The real service lock is dropped before invoking the control facade.

The definition has not been executed. Original whole test functions and the
new-publication body remain preserved. New test-only timing spans attribute
existing-state recovery lookup and full original owner/domain validation.
They are observations, never authority or injected success.

## Remaining work

A normal automatic CI run must compile this source and execute the original
21 cases, including actual fault fires and ACK/full-Strong tails. It must also
execute the new cancellation control. Further held-scope improvements require
new actual evidence and must preserve per-participant domain/currentness and
strict original custody. The current source does not claim the entire root
cause proven, any physical/RPO result, cold/controller continuation or completed
HA readiness. The separate 0b792 Serving leaf and parent bounded-native-recovery
work remain independent and need explicit source review/composition.
