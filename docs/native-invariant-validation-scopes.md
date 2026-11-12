# Application mutation guards and existing-data admission

Automatic application guards and explicit admission serve different purposes.
The runtime attaches automatic mutation guards to ordinary and borrowed
transactions. A transaction with actual row writes retains the existing native
invariant validation, mutation owner receipts, conflict checks and global
invariant commit boundary. This decision uses surviving transaction writes,
not the HTTP method or a caller-provided flag.

A zero-write ordinary transaction does not rescan every historical row or take
the global invariant commit mutex merely because its application declares
guards. Actor and resource permissions, tenant policy, record visibility,
transaction isolation and serializable-read conflict validation remain active.
Native validators explicitly registered by a caller retain their old semantics.

`Transaction::register_commit_validator` explicitly requests existing-data
application invariant validation. That request is monotonic for the transaction:
automatic registration, a borrowed service invocation or a savepoint rollback
cannot clear it. Schema admission continues to use this explicit API. Historical
local guards are checked even when migration backfills write only an unrelated
collection; an unrelated write cannot turn admission into ordinary dependency
filtering. Immutable publication references retain their separate changed-subject
lifecycle rule, so an existing closed association is not reinterpreted as a new
reference.

`register_mutation_commit_validator` is the trusted host attachment path for
automatic guards. It grants no row, actor or mutation authority. Its transaction
state is private and is not selectable through JSON, SQL or the extension ABI.
It does not disable guards on a transaction that later acquires writes.

Qualification must use actual authenticated native execution: preserve a populated
financial store, compare individual and concurrent source reads at the unchanged
deadline, verify exact facts across restart/cold restoration, and exercise denied
writes, races, borrowed transactions and late rollback. Explicit admission must
still refuse invalid historical local rows both with zero backfills and with an
unrelated default-field backfill, preserving the old catalog and acknowledged
facts on refusal. Source review and faster reads alone do not establish that
security and recovery contract.

The motivating retained fixture has 63 tax calculations across 23 companies.
Eighteen source endpoints, including empty budget resources, each took roughly
seven seconds and serialized under concurrent load. Those are before-observations;
the proposed registration distinction still requires exact-binary qualification.
