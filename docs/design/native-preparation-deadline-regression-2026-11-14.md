# Native prepared-member collection: original timeout evidence

Required native job 132380250977, run 43572675143, tested source
`d8115fedf13b0bd7107cfddd2664f2c6ea2f261f` and completed with **273 passed,
47 failed and one ignored**. Eleven failures reached
`hot_policy_tests.rs:631`, where `round.record(observation).unwrap()` received
`QueryTimedOut`. The [complete original log and per-case receipt](../evidence/native510-required-terminal-components-2026-11-14/manifest.json)
remain unchanged. The other error-location groups are independent; their setup
corrections do not establish that these eleven timeouts are fixed.

## What the evidence establishes

One retained case shows four full `prepared-service-current` checks for its
first prepared member, each around 177 ms, followed by two for the next member.
The code performs these at the producer's final check, observation wrapper,
round `begin`, and round `record`. The observed checks consume the same bounded
metadata authority. They are not transaction latencies or measured recovery
times. Existing trace coverage does not establish how much earlier time was
spent acquiring OWN, capturing the attached metadata point or observing the
decision. A short remaining budget does not prove a configured timeout cap.

The round retains the earliest original observation deadline. A later member's
fresh OWN does not renew it. Returning a timeout when that deadline expires is
the correct safety behavior; the open question is whether the intended work can
finish inside its original budget. EOF and broken-pipe messages in the log do
not by themselves identify the cause of the timeout.

## Narrow correction and diagnostics

The correction removes only `begin(first).require_current()`, because `begin`
unconditionally calls the unchanged `record(first)` before returning anything.
`record` still performs full actual service/native validation, body/member/
history checks and the metadata publication guard before inserting a vote.
The observation constructor and every later publication sweep remain intact.
No permission or side effect exists between constructing the empty round and
that admission. An independent source review supports this redundancy removal.
It saves one check; it is **not a demonstrated fix for the eleven timeouts**.

The existing hot-policy fixture also records twelve in-memory `Instant` points
around OWN acquisition, metadata-point capture, decision observation, native
preparation, wrapper validation and round admission. Only a subsequent actual
error formats `NATIVE_PREPARATION_FAILURE`, before the original unwrap. It
reports the unchanged request, OWN and original round deadlines. If OWN itself
fails inside its existing helper, its original unwrap remains the failure; this
diagnostic does not claim to intercept that earlier error. Clock observations
can perturb timing slightly and are not an uninstrumented performance result.

No request duration, original caller, authority scope, retry, assertion or
publication sweep is extended or relaxed. The next acceptance is these same
eleven cases in the existing required native job after the coherent correction
batch. If they still fail, preserve the stage output and isolate the measured
expensive path. Source review and formatting have not executed that acceptance.
