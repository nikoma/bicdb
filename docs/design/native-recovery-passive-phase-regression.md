# Separate passive continuation witness

The additional selector
`native_hot_peer_partial_debt_passive_native5_then_ack6_strong_within_original20s`
is a source-only regression proposal. It has not been built or executed in this
session. It does not replace
`native_hot_peer_second_unknown_while_prior_completed_replica_debt_requires_full_recovery`
or change the failed fourteen-case run at
`f641725b3574dfa19b8b5e5204b116cb7bb1f514`.

That run selected and executed fourteen tests: five passed, nine failed. Its
partial-debt case genuinely reached old target native2/CompletedFrozen, source
native4/Pending5 and surviving peer native5. The old debt then completed actual
target materialization through four and a fresh fifth recovery published. The
combined workload gate nevertheless failed: its last next-event call had no
external ACK and later returned its complete value from Strong; the old and
fifth Strong calls had timed out. The fixture stopped at 14.697 seconds because
it reserved eight seconds for another four calls. This does not measure a
hardware RTO or continuous unavailability for a full twenty seconds.

The new case reuses the same genuine first recovery, original writer/guard
objects, unfinished old completed2 debt, normal second pre-WAL Unknown, exact
native/pending command matrix and scheduling holds. Its original twenty-second
absolute clock starts before releasing those holds. The existing workers must
perform every actual completion, activation, suffix replay and new-session
operation. The fixture invokes no maintenance tick, recovery method, role
setter, source replacement, SDK route hint or readiness setter.

It first makes nonblocking observations of the original native materialization,
full retained command history, current original Store/open and identical full
records on all three members at the actual fifth command. It observes the real
publisher's existing scheduling hint without treating that hint as authority.
Only then does the same original client issue the ordinary Strong/APP calls.
The Strong requests must independently pass actual fresh authority/read-quorum
checks. A real next-event external ACK, exact Strong values and complete actual
sixth-command materialization on all original members remain required.

The client keeps its original two-second setting. The passive variant refuses
to start a call unless its two-second bound fits in the remaining original
absolute window; it also checks the same parent deadline and final receipt.
No deadline restarts, extension, cache-to-Ready conversion or inferred ACK is
introduced. All four calls and the final all-replica proof must succeed inside
that same twenty seconds. The original traffic-first cases retain their calls,
assertions and bounds.

A passing result would establish this narrower phase-progress witness under
its stated test workload. It would not establish the original overlapping
workload's availability gate, physical RPO/RTO, Azure scale-out, production
qualification or deployment. Every prior failed result remains independently
retained.
