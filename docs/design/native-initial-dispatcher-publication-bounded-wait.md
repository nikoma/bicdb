# INITIAL dispatcher publication under actual contention

Baseline composition: `b8f334a9ee6f67501ace93a55a83d2f4cdbb1c80`.
Classification: **source correction, not compiled or executed**. It does not
establish first-start, failover, relocation, Strong-read or physical acceptance.

## Reproduced failure and source boundary

Source6de CI job132814521759 failed
`native_first_actual_same_service_identity_wait_releases_to_original_own`
during setup with `service busy publishing Strong dispatcher`. The intended
later identity-wait control was not reached. The complete failed native cohort
and exact logs remain in the [regression findings](native-520-regression-findings.md).
This is distinct from the nineteen setup connection-reset cases and from the
later Registered policy timeouts; no common cause is claimed.

`RangeWriteCoordinator::validate_strong_event_dispatch_install` uses immediate
service and database acquisitions. That behavior is retained for existing
callers, including callers that may already hold other guards. The guard-free
`NativeInitialDataHost::advance` has an original finite caller but previously
used the same immediate dispatcher publication path.

## Correction

Only INITIAL `advance` opts into `publish_bounded_initial`. Its separate
validator requires a finite original parent and waits in five-millisecond
topology, service and database lock slices, checking the original cancellation
and deadline before and after acquisitions. Database contention releases the
service before another attempt. It keeps the actual runtime/service identity
and every original installed-hook/coordinator/range validation. Semantic
failures return immediately; no mutation, recovery or RPC is retried here.

The actual dispatcher binding lock is acquired within that same original
parent after validation. It is never held while acquiring the service, because
real read completion uses service-before-binding order. Publication remains
an ingress binding, not a read barrier or recovered-epoch grant. Each Strong
request still performs its full authority, quorum and snapshot checks.

The original immediate validator, `publish` and `prepare_publication` remain
unchanged. The INITIAL caller retains its existing post-publication cancellation
and admission checks and its existing first-publication cleanup behavior.
This is not a guarantee that kernel IO, hook validation or every other host
publication path completes within five seconds.

## Genuine component definitions and remaining tests

Three new native definitions use an already opened born-native INITIAL host,
its actual service mutex, original enrolled coordinator/guard, attached runtime
and actual dispatcher. A holder keeps the real service locked; a thread-local
test observer acts only after the actual bounded acquisition fails. The holder
is released or the original parent is cancelled at that real cut; the expiry
case leaves the service held until the original child deadline expires.
Cleanup releases and joins the holder before later assertions.

The controls require that the original one-shot validator still returns its
exact busy error. Successful bounded publication must return the actual new
binding generation. Cancellation and expiry must leave the existing binding
generation unchanged. All cases retain the original result before cleanup,
require unchanged native prefix and recovered-epoch markers, and read the full
previously acknowledged payload through the original SDK Strong path afterward.

These are actual already-published component definitions, **not executed
results** and not a first-publication fault schedule. Required next acceptance:

1. Compile and execute all three controls and the unchanged original FIRST
   identity-wait release case on the final composed source.
2. Hold the actual service at the first INITIAL publication boundary and prove
   cancellation/expiry leaves APP unpublished; independently hold topology,
   database and dispatcher binding at their real boundaries.
3. Exercise hook/Store replacement and host stop during waits and prove original
   identity failures cannot publish a replacement binding.
4. Run the original full ACK/Strong/native suite and physical relocation gates.

No application assertion, historical fixture, configured budget or original
failed-run evidence is weakened by this correction.
