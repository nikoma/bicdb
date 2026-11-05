# Transaction outcome and write survival receipts

The [native API guide](../../transaction-outcome-receipts.md) describes the
read-only outcome and write receipts. They preserve the origin transaction's
outcome and the first invalidating rollback prefix without retaining a database
pointer. Reusing a buffered-write position does not resurrect an older receipt.

[Validation results](validation-results.json) record 935 passing Rust tests and
42 matching Oracle reference comparisons through a development server candidate.
Three existing SQL microbenchmarks remain explicitly ignored. Selected tests,
workspace checking and reference comparisons are separate evidence; no GitHub
workflow was added or manually invoked.

The targeted core coverage includes late registration, nested rollback, reused
slots, abandonment, uniqueness rejection/retry, buffered and direct commits,
reopen and observation across threads. Five existing post-decision failure and
recovery scenarios now inspect receipts in embedded-memory and paged modes.
Buffered scenarios also construct their first observer after failure. A Linux
child-process test separately causes an actual short WAL append and verifies
that the durable watermark does not advance and the observer reports recovery
required through transaction/database drop.

The broad regression batch checks native isolation, snapshot recovery, sync
visibility, the full SQL unit suite, deferred constraints/triggers, transaction
system types/uniqueness, ordinary worker stacks, Oracle pgwire and general
PostgreSQL pgwire. The follow-up WAL test changes only test/guide files; engine
sources in the qualified candidate remain unchanged.

This is a prerequisite for execution-value provenance. Persistent Oracle LOB
identities, reads and mutation are not implemented by these APIs, and full
application and Oracle programme qualification remain open. The candidate was
built for local qualification; the accepted application deployment was unchanged.
