# Second origin failure while an old replica remains completed/frozen

The separate required source test
`native_hot_peer_second_unknown_while_prior_completed_replica_debt_requires_full_recovery`
does not first finish every replica's previous recovery. It reuses actual
three-node TLS owners, the real first pre-WAL Unknown, actual post-Complete
reply loss and the borrowed debt-only scheduling hold. The source actually
acknowledges the next logical values and a fourth value, with full Strong reads.
Its current native head is read and must be at least four; repeated no-op/update
ACKs remain real log entries.

Before the second injected fault, the source and active peer must have that
complete actual head, matching histories, full records and original guards.
The other original peer must still be at **native2/NativeCompletedFrozen**, with
its current-open guard false and the third/fourth values absent. The actual
borrowed debt hold is not released to manufacture convergence first.

The second ordinary APP must reach a real pre-WAL Unknown, sequence None, actual
ticket/transaction/release and normal handler EXIT. Its original source must be
at head/Pending(head+1). The active peer must physically contain head+1 and the
full new value. The old frozen peer must **remain at two without that value**;
it is not incorrectly required to have already recovered the new command.

Only after those actual prerequisites does the test release its original
controller and debt scheduling holds. The original twenty-second recovery gate
requires committed-value readability, a genuine next APP ACK, full Strong reads
and every original replica at the complete actual source head/history with all
commands physically materialized. All previously acknowledged full values are
compared against their pre-fault records. No new owner, direct recovery call,
manual tick, role setter, or timeout extension is introduced.

This is an unexecuted required regression, not yet a reproduced red result.
The current implementation is expected to refuse new recovery while retained
old debt is nonempty. It also drives that old debt only after readiness is true,
so the actual second Unknown can prevent both forms of progress. Even a safe
refusal must fail this required availability/convergence test.

A repair needs either actual historical completed/activated evidence plus a
bounded known materialized prefix while the newer Pending remains protected,
or a new complete quorum protocol that includes all old obligations. A cached
phase, empty status, or blanket release of the old frozen peer cannot substitute.
Current-serving source getters and their Pending refusals remain unchanged.
