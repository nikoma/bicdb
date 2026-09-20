# Adversarial review remediation — 2026-09-05

This tracks the first implementation pass against BIC-2026-01 through
BIC-2026-06, reviewed at `6709089`. It does not constitute a new independent
security audit or production-deployment verification.

| Finding | Implementation status | Remaining work |
| --- | --- | --- |
| BIC-2026-01 | Continuous expiry checks in admission status and HTTP admission; both serving binaries retain a process watchdog. | Deploy the change and exercise expiry with production streams, restart, key release, and fleet fencing. Embedded launchers must retain the watchdog or provide equivalent fencing. |
| BIC-2026-02 | General cluster startup, README, and container example explicitly state that database separation is not a Cell boundary. | Verify actual per-Cell process, mount, uid, network, and resource isolation in deployment. |
| BIC-2026-03 | Existing trust boundary retained. | Independently review signer workflows and actual attestation verification; run the negative KMS/IAM matrix with witnessed evidence. |
| BIC-2026-04 | Raw-pointer service leases remain open. | Replace unscoped pointer leases before adding queued or asynchronous dispatch. |
| BIC-2026-05 | Aggregate application HTTP resource accounting remains open. | Derive concurrency from a declared memory budget and load-test concurrent body, response, provider, and WASM allocations. |
| BIC-2026-06 | Authorization-path audit and mutation matrix remain open. | Cover every storage shortcut against SQL privileges, RLS, native policy, overlays, and cancellation. |

## Continuous admission contract

The first of the three signed expiries is an exclusive deadline. Expiry is
sticky for a running Cell, and monotonic elapsed time prevents a backwards wall
clock adjustment from extending its original lease. Startup clock skew does
not grant runtime grace. Renewal requires restarting with fresh evidence and
a new key lease.

The serving binaries check on a dedicated OS thread every 100 ms and exit with
status 78 on expiry. Abrupt termination bounds long-lived streams and background
work subject to OS scheduling; it uses normal encrypted-storage crash recovery.
No online signing-key revocation or transparency-log polling is introduced.
See [Phase 8 admission](cell-runtime-phase8.md) for embedding and fleet duties.

Regression coverage exercises each independent deadline, unchanged-runtime
status after fake-clock advancement, rollback after expiry, HTTP rejection
before body reading, and watchdog termination in a child process.

## Service-lease redesign constraint

`PluginServiceCall` is cloneable and contains database and transaction pointers
whose validity depends on synchronous dispatch. Removing `Copy` alone would not
encode that lifetime. A follow-up must make the invocation borrow scoped across
the dispatcher, or replace pointers with registry entries invalidated on scope
exit. Nested transaction propagation must retain a single mutable owner, and
compile-fail coverage should reject storing a call beyond its invocation.
Host-side memory safety is part of the process boundary even when the caller
runs inside WASM.
