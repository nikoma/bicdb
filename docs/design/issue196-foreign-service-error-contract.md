# Issue 196: exact foreign-service refusal expectation

This test-only correction is a child of failed source
`503964ae93aa22f4c76b698a549b6ef857c834be`. It changes one error assertion;
production code, all other assertions, the real service substitution, native
preparation, original request/OWN budgets and the other three negative cases
remain unchanged. The corrected test has **not been compiled or executed**.

## Retained observation

Run `43608687107`, native job `132488757671`, ended **FAILED: 324 passed,
13 failed, one ignored**. Its canonical original log remains unchanged at
`ha512-503964-required-43608687107/job-132488757671.log.gz` in the private local
evidence archive. The gzip SHA-256 is
`3378daa6895c185fbca6eb1612c0a49aacd859cc9df6bb937f374bb9c1c01cc9`;
the decompressed original log SHA-256 is
`2a2c06352869a17cecb227cad273270a173c4fa0c65721c8e33889dfa303b82c`.

Original line 21057, `NATIVE_PREPARED_CANDIDATE_RESULT`, records:

```json
{"actual_error":"cluster error: native range image: native writer store/root/open changed","case":"TargetPreparedCandidateForeignService","chosen_actual_service_node":"n0","count_parent_cancel_requested_at_report":false,"count_refused":true,"error_truncated":false,"memory_observation_not_authority":true,"node_ids_truncated":false,"original_node":"n2","original_own_live_after_count":true,"original_own_live_before_count":true,"original_preparation_checksum":"2b885e846bc47bb5b749cfe8edd7378df5d926fdcc8c10b01134e505793326dd","past_original_deadline_us":0}
```

Lines 21059–21060 record the panic at
`hot_policy_tests/prepared_candidate_tests.rs:149`: the returned error did not
contain `cluster service belongs to another actual database/open`. The test
already required a returned error, checked the original request and process,
and passed its positive before/after deadline assertion before that panic.
The final uncanceled count-parent assertion follows the failed matcher and was
not reached; its false cancellation flag was recorded beforehand. These clock
and memory observations do not replace a current OWN proof.

## Source-pinned call order

All paths below are under `crates/bicdb-core/src/` at the failed source above.

1. The hot fixture obtains genuine local preparation before entering this
   negative helper (`distribution_native_host/provisional_listener/tests/
   learner_enrollment_tests/promotion_tests/registered_image_baseline/
   remote_source_tests/hot_policy_tests.rs:727`). The helper preserves that
   actual preparation and deliberately selects another actual service Arc.
2. `NativeJointPreparedServingCandidate::from_actual_service` grants nothing.
   `NativeJointPreparedServingRound::begin_candidate` checks its original
   bounded cancellation scope, then `record_candidate` transfers the private
   container directly into the original `record`/`record_inner` boundary
   (`distribution_range_handoff/joint/bridge.rs:245–313`).
3. `record_inner` calls `observation.require_current()` **before** membership
   accounting, publication guard and insertion (`:313`, insertion `:343`).
4. `ActualServiceNativeJointPreparedEvidence::require_current` opens the chosen
   service's actual database and validates its catalog, then validates the
   retained preparation's writer domain against that chosen database/Store
   (`distribution_data/native_joint_handoff/bridge.rs:153–159`). This precedes
   the later explicit preparation-node mismatch check (`:160`).
5. For this retained CLOSED writer domain, `NativeJointWriterDomain::
   require_current` preserves the real Store host-base check and then calls
   the retained domain's `require_lifetime` with the chosen root/open
   (`db/range_image_native/writer_enrollment/joint_domain.rs:96–97`).
   `NativeJointPhaseOwner::require_store` uses the genuine original writer.
6. `NativeWriterHostState::require_store` first validates that writer's
   lifetime, then refuses a different Store-open ID/root or absent enrollment
   fence (`db/range_image_native/writer_enrollment.rs:220–226`). The exact
   returned detail is uniquely defined there. The common `invalid` constructor
   produces `BicDbError::Cluster("native range image: …")`
   (`db/range_image_native.rs:190`). The marker does not distinguish individual
   disjuncts inside that combined store/root/open guard. The chosen Store's own
   host-base validation also invokes `require_store`; the marker identifies
   the rejecting guard, not a separately instrumented internal invocation.

Thus the retained result is the intended actual foreign Store/open refusal
before this candidate can count, not an unexpected successful admission or an
observed timeout. The old expected substring belongs to a different storage
policy/database check in `distribution_data/native_durability.rs:68`; it is not
the contract returned by the retained writer-domain binding guard here.

The correction demands the exact typed `BicDbError::Cluster` detail
`native range image: native writer store/root/open changed`. It accepts no
generic `is_err`, substring alternative, timeout or cancellation substitution.

This explains one failed **test expectation**. The original case and complete
13-failure run remain failed; no successor result or production qualification
is claimed. The other 12 failures and their activation clocks are outside this
correction. The smallest next check is this unchanged genuine foreign-service
case on the eventual coherent successor; no rerun is performed by this leaf.
