# Constraint catalog validation cost

Cold constraint identifier validation previously constructed every table constraint
catalog row, resolving attributes and parsing CHECK expressions merely to check
identifier range and uniqueness. The validator now enumerates the same slots
and combines them with domain constraint identifiers. Successful-generation
memoization and the global collision fence remain in place. Catalog queries
still construct their requested rows normally.

The [local measurement](benchmark.json) uses a disposable copy of a 915-table
application database after 357 imported foreign keys. Three cold validation calls
changed from 2.729/1.848/1.849 seconds to 1.454/0.622/0.593 seconds. Median time
improved approximately 3 times; table constraint rows constructed per call fell
from 18,342 to zero. These calls exclude database open/recovery time. This is
one host and snapshot, not a production throughput claim or a completed migration.
The baseline used the previous production validator with the same test-only
counter and benchmark harness; the optimized source commit is recorded in JSON.

All 713 SQL library tests passed, with four ignored. The new manually invoked
snapshot benchmark is one of those ignored tests and passed separately. New
regressions compare the complete ordered identifier sequence with actual catalog
rows in both PostgreSQL and Oracle preview profiles, including primary, unique,
NULLS NOT DISTINCT, NOT NULL, CHECK, foreign-key, exclusion and domain constraints.
They also check invalid identifiers and deliberately aliased relation identifiers.

The migration tool inventories attributes once and retains only relations in the
selected namespace, avoiding repeated large IN predicates. A native integration
case checks exclusion of another namespace. Full application qualification,
legacy orphan inventory, source data equality and migration completion remain
separate gates.

The [native validation record](validation-results.json) reports 13 foreign-key
migration tests, seven BLOB migration tests, 20 deferred-constraint tests, eight
authorization tests, 181 PostgreSQL protocol tests and 48 Oracle-profile protocol
tests passed. The server build, workspace/all-target check and formatting check
also passed. The native migration cases include restart/resume and rejection of
changed data or declarations. No GitHub workflows were changed or dispatched.
The previously accepted application deployment remains separate from this build.
