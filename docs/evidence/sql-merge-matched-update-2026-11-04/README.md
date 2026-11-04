# Matched-update MERGE

A matched-update MERGE can now use the normal update executor, rather than
being rejected during Oracle-profile keyword lowering or statement dispatch.
The supported shape has one `WHEN MATCHED` UPDATE action, a table target and
a table or derived query source. PostgreSQL uses `WHEN MATCHED AND predicate`;
Oracle preview also supports its action-level `UPDATE ... WHERE predicate` and
qualified target assignments.

The source is materialized through the existing FROM executor. Target changes
use existing update authorization, constraints, row triggers and statement
rollback. MERGE has write admission, no result columns during prepared Describe,
and a `MERGE count` command tag. The transient UPDATE adapter disables routine
node-address memoization; temporary AST allocations must not become stored
routine identities. No persisted schema or routine format changes are needed.

Repeated target matches follow separate rules. PostgreSQL rejects a second
update, including one that writes the same values. Oracle allows preceding
assignments that leave the target unchanged, but a subsequent update after the
first actual change raises its stable-target error. The marker compares typed
column values with NULL-safe equality. Oracle also rejects assignments to target
columns referenced by ON, while preserving quoted column case and qualified
identities. Ordinary UPDATE FROM retains its existing first-match behavior.

## Scope

Unsupported action shapes are rejected before evaluating the source or making
changes. Multiple clauses, inserts, deletes, Oracle UPDATE DELETE WHERE, OUTPUT, RETURNING, top-level WITH,
Oracle ON subqueries and full MERGE conformance remain outstanding. Primary-key
moves retain existing UPDATE FROM restrictions. The current adapter requires a
table SELECT grant on the target; per-column MERGE read authorization and
Oracle SQL92_SECURITY variants are not qualified. Source reads use existing
query-executor authorization. Concurrency and broad Oracle trigger equivalence
require separate qualification.

Joined-row lookup now prefers preserved column identities before the existing
folded fallbacks. Binding folds unquoted identifiers while retaining quoted
column case. A shared regression also checks ordinary PostgreSQL UPDATE FROM
and unquoted-name folding. Broader identifier-error and alias rules remain
separate programme work.

All behavior is independent of application, schema and table names.

## Independent references

The [fixture](../../../examples/oracle/reference/merge_matched_update.py) uses
nonce tables and prepared PostgreSQL transport. It compares affected counts,
error categories and exact final rows independently against Oracle and
PostgreSQL. Cases include action filters, a derived self-join source, empty
sources, false/NULL matches, check failure rollback, Oracle ON-column rejection,
repeated no-op/changed matches, quoted column case and bound parameters. It does not exercise a
production application or establish complete migration compatibility.

| Prepared-command cases | Previous binary | Candidate |
| --- | ---: | ---: |
| Oracle 23.26.3.0.0 | 0/15 | 15/15 |
| PostgreSQL 18.4 | 0/15 | 15/15 |

The [baseline](baseline.json) and [candidate comparison](comparison.json)
retain independently satisfied source expectations, error details, final data
and binary/fixture hashes. Temporary objects and disposable containers were
removed. The [retained evaluation cases](retained-evaluation-comparison.json)
pass 19/19 Oracle and 20/20 PostgreSQL cases;
[typed boundaries](retained-typed-comparison.json) remain 16/16 in each profile,
and [RAW controls](retained-raw-comparison.json) remain 13/13 in each.

## Local validation

The [validation record](validation.json) identifies source and binary. All 612
SQL tests and 364 driver/protocol tests pass, with three existing benchmark
ignores and two existing external interoperability ignores. New tests cover
repeated matches, statement rollback, ordinary UPDATE FROM behavior, target and
source permission checks, quoted/scoped ON-column identity, temporary routine
adapter lifetime and protocol admission/Describe classification. Formatting and
whitespace checks pass. Builds use one Cargo job and memory guards requiring
14 GiB available to start while preserving an 8 GiB floor.

These local checks do not establish a release, application deployment or full
Oracle/PostgreSQL MERGE compatibility.
