# Native c05 acceptance: failed

The **1.0.515-beta candidate** `c05d64fcdabe9991428f53baee1ea11e44166102`
(tree `b58128df941963a84456fb77cc2b3e3cb5341150`) failed its normal
[native multi-node job](https://github.com/nikoma/bicdb/actions/runs/43651053103/job/132619399565).
The actual GitHub checkout was `93d167cb72e44366dab5279aa333ccd6d9fc9d5f`,
with that exact tree. This is a failed acceptance result, not a release or
physical qualification. The job finished at 2026-11-15 00:06:24 UTC.

The result was **331 passed, 15 failed, one ignored**, with 941 filtered tests.
The test runner reported 3,234.00 seconds. This duration is not an RTO,
throughput result or controlled performance comparison.

| Evidence | Classification | What it establishes |
| --- | --- | --- |
| Whole native suite | Failed | The candidate must not merge as accepted HA |
| Four `remote_decision_tests` cases | Passed in this CI job | Actual donor provenance, pre-put refusal, commit-sync uncertainty and post-commit cancellation controls passed within their existing component scope |
| Twelve hot-policy cases | Failed | Each ended at `hot_policy_tests.rs:876` with `QueryTimedOut` during native bridge publication |
| Four of those twelve with `NATIVE_POST_WAL_FAILURE` | Failed API completion, useful partial evidence | The native commit returned, CURRENT and archive were readable, and the original OWN had expired by 1,202–12,262 microseconds at the recorded observation; all four reported `local_publication_scope` |
| Other eight publication failures | Failed completion; native effect inconclusive from instrumentation | The first publisher completed; second n3 publication completed its final foreign sweep and local preflight, then timed out before its bridge-WAL marker. An absent marker is not proof of rollback, no effect, or lost data |
| Foreign-service prepared-candidate control | Failed exact diagnostic assertion | It refused the actual foreign service while the original request was live, but returned `joint bridge preparation belongs to another service` instead of the original writer/store diagnostic |
| Missing-archive prepared-candidate control | Failed exact diagnostic assertion | It refused the actual missing archive while the original request was live, but returned the publication-scope diagnostic instead of the original preparation diagnostic |
| Retained-middle-history control | Failed prerequisite | The actual scope was reached, but its range index was one and the fault could not find an interior retained entry; the requested corruption did not execute |
| Physical expansion, full external-ledger reconciliation and sustained capacity gain | Not tested by this job | No five-machine or production claim follows |

The complete native log contains 13,569,007 uncompressed bytes, SHA-256
`d22dc1442d3aa2e85e3956b2210aece8a460eab15874e40d4fdff23babfe5714`.
The retained gzip is 1,948,877 bytes, SHA-256
`785b3077170d0f6f4b5a45a7aa856aefc0f43271dbc45e0bc89d057c1e77d08d`.
Those hashes identify the original evidence; later code does not rewrite it.
The source-pinned result is also recorded in
[issue 195](https://github.com/nikoma/bicdb/issues/195#issuecomment-6886440301).

## Separate follow-up and smallest next checks

The source follow-up preserves the original prepared-candidate assertions and
restores their necessary original protected-record/store refusals before the
full scoped validation. These comparisons grant no publication authority.

For fresh foreign participants, the follow-up keeps the original history and
fresh protected-decision checks, all metadata/open/decision comparisons, and
the original pin/Frozen guards. It uses one independently read local physical
point under **each participant's own journal**, with original phase-owner and
protected CLOSED audit checks on both sides. It does not reuse the publisher's
local scope for another participant. Every retained resolution remains checked.
This removes repeated physical sweeps from that path; it does not prove their
cost caused every timeout or that the follow-up meets the existing budget.

The middle-history prerequisite now issues two additional actual stock-SDK
commands before Close, with intent/return evidence and full Strong value
checks under the existing two-second request limit. It creates real interior
history rather than editing counters or manufacturing entries. The original
after-scope corruption and rejection assertions remain unchanged.

The smallest next checks are the original prepared-candidate controls, the
actual middle-history cut, and the existing full hot-policy path through SDK
ACK/Strong under its unchanged request/phase budgets, followed by the normal
native suite. **The follow-up is source only until that exact source executes
in CI.** No deadline increase, assertion weakening, new retry-as-rollback rule,
or physical success claim is part of it.
