# Native512 hot decision issuance diagnostics

At source `1742776ab02957a053aded2ad4f665500d9325bf`, required native run
`43581326157` failed ten hot cases at `hot_policy_tests.rs:480`, the
`prepare_decision()` unwrap. Its combined recovery-role scope/expiry error did
not retain the failing member, failed substage, original Role/OWN deadlines or
cancellation state. It does not independently establish which predicate failed.
The later named faults were not reached in those cases.

This diagnostic successor preserves the original initial physical-current
sweep, every original member-issuance check and their order. Only after an
actual error returns does test code render at most 64 copied member clock
snapshots, with an explicit omitted count and 128-byte recipient limits. It
reports the exact failed issuance member when available, actual Role/OWN
request contexts, metadata anchors, original request cancellation state, and
remaining/expired deadline values at the actual call boundary. The primary
error is propagated unchanged. It retains no error, authority, guard or token
beyond the call, invokes no protocol callback, and produces no new success-path
output. Formatting runs after the original service/native validation guards
have returned. A failed initial aggregate physical check is identified as such;
it is not falsely attributed to a particular member.

Successful test paths add clock reads only at these boundaries. Non-test builds
compile out the new diagnostic getters/output. Request budgets, retry policy,
Session ownership, membership/physical predicates and all fixture assertions
are unchanged. Clocks can distinguish observed deadline exhaustion from a
scope refusal; they do not measure CPU, disk or scheduling causes.

The two separate failures at `hot_policy_tests.rs:736` had retained clocks:
the first OWN had expired by 14,036 and 36,884 microseconds when prepared-round
construction failed, while the request deadline remained positive. Those
measurements do not explain the ten decision-issuance errors or establish a
CPU/I/O cause. The prepared wrapper plus count boundary performs duplicate
physical checks; any separate optimization must preserve a complete genuine
current service/native/MStore check and original deadline before counting.

Formatting, diff and version checks are source checks. No compilation, tests,
CI retry, deadline increase or successful repair is claimed by this leaf.
Original 512 failures remain at their recorded source revision.
