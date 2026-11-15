# Uncounted native preparation candidate

This source batch follows the diagnostic-only `522eaa6da` / `d137b099e`
successors to `1742776ab02957a053aded2ad4f665500d9325bf` (1.0.512-beta).
It is not an executed timeout fix or a qualification result.

The original hot preparation path produced genuine local native evidence,
validated that evidence again in its observation wrapper, then performed the
same complete current-service validation at `Round::record` before counting.
Two failed 512 traces measured 242–295 ms in the wrapper and 99–173 ms at the
following round boundary; OWN had expired at the latter failure. These are
wall-clock observations, not CPU, disk or scheduling attribution.

`NativeJointPreparedServingCandidate` holds the genuine opaque local evidence
and an explicitly untrusted service association. Its constructor performs no
validation and exposes no observation, grant, quorum, readiness or liveness
method. Only `begin_candidate` / `record_candidate` consume it privately and
pass it directly into the **unchanged** original `record` boundary. The full
actual service/native/attached-MStore checks, original deadline and caller,
member/body/history/duplicate comparisons and final publication-version guard
all still precede insertion. The strict existing observation constructor and
all later quorum/publication sweeps remain unchanged.

The three existing preparation aggregation callers use that candidate path.
The earliest original OWN/round deadline, 5-second request, Session ownership,
retry limits, original fault operations and APP/Strong assertions are preserved.
No expired authority is refreshed by this optimization. It does not fix or
explain the separate ten `prepare_decision` scope/expiry errors. Sequential
physical preparation of later members can still exhaust an earlier member's
window; the new diagnostics are needed before changing that phase contract.

Four new source-only controls reach an actual protected preparation and then
attempt counting with:

- A different real service from the same genuine cluster.
- The actual original OWN expired while its original request remains live.
- Its exact immutable native preparation archive deleted through real native
  metadata WAL while the current pointer survives.
- The actual count caller canceled, with process and original OWN still live.

Each records its real result before assertions. Non-expiry cases require OWN
still live, and exact intended identity/archive/cancel errors; a timeout or
unrelated busy refusal cannot masquerade as the intended negative. These
controls do not bypass existing prerequisite failures or increase budgets.
The destructive archive helper is `cfg(test)` DATA/fault injection only and
requires the actual original preparation/native database identity.

Direct formatting, diff and version checks are static checks. No Cargo build,
Rust tests, CI dispatch, deployment, physical throughput gain or RPO/RTO claim
has been performed for this batch. Original 512 failures remain failed.
