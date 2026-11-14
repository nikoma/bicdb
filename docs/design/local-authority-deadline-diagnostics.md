# Local metadata authority deadline diagnostics

This is a test-only observation change to
`finished_local_authority_keeps_original_context_deadline`, based on source
`ea931e30db51a1aba63a9e156a251a94d06aaaff`. It preserves the original 100 ms
context, real synchronous follower probe and ACK, operation order, unwraps,
assertions and later deliberate expiry. It adds no retry, replacement authority,
production change or additional business deadline.

Fixed-size `Instant` observations record fixture, leader lock, begin, probe
construction, actual probe, leader reacquisition, ACK validation and finish.
Only an actual failed API triggers formatting and diagnostic output, immediately
before that result's original unwrap. Output errors are ignored so they cannot
replace the primary error. Successful calls perform clock observations without
diagnostic output or diagnostic allocation. These observations can perturb
execution slightly; they are not an uninstrumented performance measurement.

## Preserved historical failure

E3 source `e3ccc906ad961e6c83d29b07763fa67393731f3c`, run `43571466889`, Core
job `132376600724`, completed with **900 passed, 1 failed and 8 ignored**. The
failure was the first `record_read_authority_ack` unwrap, with
`metadata consensus: read authority context failed or expired`. The final
completed-authority assertions and deliberate-expiry assertions were not reached.
The full original Core log SHA-256 is
`612f9ced4da6979755615539954d36a09d304ad8f0798ccc4652a52ce119fb9d`.

The test, fixture and consensus implementation are byte-identical between E3
and parent `f69b242c9db3afe0d24e83520f94a853093d4b98`. That parent's run
`43571509341`, Core job `132376807658`, recorded this exact test as passed.
Different complete source and execution environments make this a historical
comparison, not a causal experiment.

The source path implies the original deadline was reached at first ACK
validation: the context starts with `failed=false`, and the only assignment
setting it true follows the error at that validation. The original failure
retained no per-stage timings. The real synchronous follower path performs file
I/O, with fsync disabled for this legacy-topology fixture. Neither slow I/O,
scheduler delay nor invariant-memo execution is established as the cause. The
coarse pressure artifact recorded no memory-guard interruption; it cannot resolve
a 100 ms window.

This leaf is observability only. Formatting and diff checks do not execute the
test, fix the historical failure, or qualify HA, durability or scaling.
