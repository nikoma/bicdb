# Initial DATA open during a missing leader hint

The d74 native run's
`native_bootstrap_finite_original_service_wait_cancels` failed in normal host
setup with `current metadata leader unknown for DATA open`, before its intended
held-service cancellation cut. That scenario remains failed and its cancellation
guarantee was not exercised. The source was
`d74d363addb35b5b97308b1afcc1c2231e021d29`; the original full raw log SHA256 is
`0fb6aca4e8deb61f9d3fe1ff331d30041e19f4fefe7650bf9ff29f3d2e781f4f`.

`NativeMetadataControlHost::open_initial_data` previously read one actual
bootstrap snapshot and immediately refused a missing leader hint, although the
normal public opening action retains its original five-second deadline and
the separately owned metadata supervisor can still be electing a leader.
The observed error alone does not prove how long that election gap lasted.

The correction repeats only the actual owned bootstrap observation while its
leader hint is absent and the original finite parent remains live. Each
bootstrap call releases its service and metadata guards before the short wait.
Errors still propagate immediately; the request deadline is never replaced.
An unbounded internal caller retains the prior immediate missing-hint refusal.

A returned hint supplies routing DATA only. Existing independently fresh
metadata, native-owner, writer, range-recovery and serving checks still run in
the subsequent opening path. This neither elects a leader nor treats a hint as
readiness. No failed mutation or RPC is retried by the loop.

The next execution must retain the unchanged normal-host/bootstrap controls and
their original budgets and cancellation assertions. A bounded real election-gap
control must additionally establish absence before the wait, then actual leader
visibility or original cancellation/expiry without holding engine latches. The
correction is uncompiled and unexecuted; no startup RTO or HA qualification is
established by source inspection.

## Registered control validation contention

Another original FIRST scenario failed its setup with `registered control store
busy`. The existing registered-member constructor had already captured the real
service/DB open and a fresh OWN/member proof, but used a single `try_lock` for
its final metadata Store validation. It now waits in short slices bounded by
both that original OWN deadline and the original parent, outside service and DB
guards. It rechecks request liveness and runs the same exact Store/open/current
member validations after acquisition. It does not replace stale proofs or retry
failed persistence. A changed publication version still refuses.

The unchanged normal FIRST setup remains a regression gate. A deterministic
control must hold the actual Store, observe the live original wait, then release
or cancel/expire it while checking original-open and proof identity. Until that
control executes, the change remains source-only and supplies no availability
or recovery-time guarantee.
