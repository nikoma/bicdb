# First local Open: combine the same original owner/domain validation

This isolated source unit is based on public source `7e99c9c96970b78a44402c975627c64e001adffa`. It changes only the fresh local catalogue publication path. It does not include experimental remote source retirement/completion or Serving work. No Rust build or test has been run for this unit.

## Actual failed boundary

Native job132883862065/run43737698638 ran452 unique cases:396 passed,55 failed,1 ignored. Twenty hot cases returned `QueryTimedOut` at the first `actual_catalogue_open`; the large-head case failed its required directory-sync fault count with zero firings. All21 exhausted their actual admission lifetime before entering `open-final-native-scope-and-local-proof`. Twenty entered the mandatory final foreign-current sweep with87–222ms remaining and spent94,470–297,515us in that sweep. No first-policy stage trace was reached. This evidence does not establish that any preceding operation had no effects, or that the prior6de first-policy failures were fixed.

The fixture retains its original1500ms listener deadline. The existing4s Open round is clipped by the actual authenticated HostMQ context and the earliest member OWN; no duration or authority window changes here. The independent raw/source/clock receipt is `/tmp/bicdb-native-foundations-ci/7e99-native-driver-hot21-source-clock-review.json` (SHA256 `181eb0b4404875f40d875fa926978b765c0ebbc1ada17200019471aa263decbe`).

## Source lemma and guard scope

Before this change, fresh publication called `original_phase_owner_for_activation`, which calls `actual.require_original_phase_owner`. That full validator performs `writer_domain.require_current`, checks actual phase-owner equality, and checks original lifetime on both sides. Publication then called `original_writer_domain_for_activation`, which invokes that same full validator and separately invokes `domain.require_current` again: three full physical domain reads in the kernel, in addition to the independent service-entry check.

The new internal combined method calls the same full actual phase-owner validator once, obtains the same original opaque owner/root and domain, checks their actual identity, and retains request/lifetime checks. The fresh kernel consumes the tuple immediately under its existing same actual service mutex, DB read guard and mutable RangeWriteStore borrow. It does not cache it for another operation or promote returned identifiers into permission. The separate generic getters and recovery lookup remain byte-identical. No new scope is retained across foreign service locking.

This is a reduction of duplicate observations under the same existing caller scope, not an atomicity assertion over concurrent metadata, hook, health or filesystem changes. The full foreign-current sweep still runs before the journal. The original local locked native preparation/current/archive/provenance/history checks, exact Frozen/gate checks, controlled mutation, later full domain revalidation, original Role/OWN/metadata checks and owned catalogue sink remain at their existing boundaries. A change or expiry at those later boundaries must still refuse. The independent service-entry full check is unchanged and is not treated as a cached grant for the kernel.

The passive kernel trace now labels the single full check `open-original-owner-and-domain-full-validation`; the two legacy traces remain available for their unchanged callers. No stage result, original error, mutation retry or deadline is altered.

## Genuine component definitions and preservation

Two new normal native fixture definitions use actual mTLS metadata, original installed/coordinator owners, real preparation/publication and unchanged fixture windows:

- `native_joint_open_after_final_external_sweep_actual_cancel_refuses_before_io` uses the existing selected-thread `after_external` cut. It records only scalar reached/liveness observations under the actual scope, cancels the same live original admission, and requires exact `QueryCanceled`, unchanged actual catalogue bytes, unchanged complete native source-lineage points and continued CLOSED admission. Expiry or an earlier failure fails the required cut. Its result is reported after guards unwind, before assertions.
- `native_joint_open_same_local_original_domain_reenrollment_refuses_old_admission` obtains a real fresh HostMQ and normal sealed enrollment policy, then installs the same actual coordinator on the same actual local database/store outside service locks. It retains the real replacement guard and requires distinct actual registration generations, identical original database/store/coordinator identities, original old-owner liveness refusal, exact known native registration-denial class, unchanged catalogue bytes, unchanged native lineage and CLOSED admission. Original admission expiry is a failed prerequisite. This exercises the unchanged necessary original-lifetime refusal before the combined physical getter; it does not claim to reach that getter with an already invalid owner or to prove a physical-domain corruption case.

The old final-external foreign writer re-enrollment control and all other existing adversarial bodies are preserved exactly. The original Standard backup integrations, issue205 visibility controls, RF3 fixture windows, hot helper assertions/fault counts, native authority and owned recovery/sink bodies are unchanged. The new tests add no retries, manufactured owner/quorum, metadata/epoch setters, or assertion relaxation. These are same-process component controls, not physical or full SDK acceptance.

## Remaining limits

Standalone rustfmt and source/diff preservation checks are the only validations. The complete first Open has not been measured after this change; it is not known to fit1500ms, and the source optimization is not a claim that all21 failures or later ACK/Strong tails are resolved. Existing full acceptance and fault assertions remain required. Kernel IO, physical lock acquisition and scoped-thread joins are not made preemptible by this change. Controller, cold restart, source loss, repeated relocation and cleanup remain separate unfinished work.
