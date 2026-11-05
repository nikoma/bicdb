# Predicate projections and existing row-lock ownership

Computed NULL/truth predicates in projections and ordering now use the row
expression evaluator. The record filter's ability to test a predicate does not
make that predicate a field projection. Aliases and parentheses retain the whole
expression, while ordinary field projections and WHERE predicates keep their
existing paths. Independent PostgreSQL and Oracle-preview regressions exercise
NULL, true, false and unknown values. Oracle uses `IS NULL` for the unknown
boolean state; `IS UNKNOWN` is a PostgreSQL spelling.

The core `Transaction::holds_record_lock` API checks the authoritative lock owner
and pending transaction state without acquiring a lock. Rollback-mark tests check
ownership before acquisition, retained/released lock suffixes, transfer to another
transaction and preservation of write locks. This is a prerequisite for APIs
which require a row to have been locked before mutation.

The unfinished Oracle LOB runtime is separate from this delivery. Persistent
locator mutation and complete application/programme acceptance remain open.

## Local resource recovery

An earlier validation was interrupted after the accepted application resource
guard stopped its process group when shared-host headroom fell below 8 GiB.
That partial validation is not counted as a completed pass. The accepted binary
and existing database were restored; all 77 selected workflow-state checks and
original routine/view fingerprints were preserved, and HTTP returned 200.
Build validation now starts with 18 GiB available host RAM and stops at 12 GiB,
leaving a margin above the accepted application's 8 GiB guard. These are the
local shared-host validation settings. No new application binary was deployed
by this checkpoint, and no workflow was added.

## Executed checks

[Validation results](validation-results.json) record two core rollback-mark
checks, 644 SQL library tests, and 26 projection, column-reference, locking and
ordinary-stack integration tests. All passed. Three existing library benchmarks
remain ignored and are listed explicitly in the results. Workspace/all-target
compilation passed. The passing batch observed at least 16.318 GiB available RAM.
This source checkpoint was validated independently of the unfinished locator
runtime prototype.
