# Procedure declarations and additional routine header dependencies

This evidence describes private source revisions, each still reporting
`1.0.465-beta`. It does not qualify the later versioned public source, a binary,
release, deployment, the full SQL/native transaction bridge, or application/HA
acceptance. Original application assertions and the 10-second timeout are unchanged.

## Procedure declaration correction

At tests-only source `ff40982a45bf58d001887d974126aedd3b95133d`, the
function control passed. The procedure control failed in its first embedded-memory
commit case: numeric argument OIDs and `pg_get_function_arguments` followed the
renamed relation, while `pg_get_functiondef` still declared `source` / `source[]`.
The failure stopped the remaining cells of that test.

The implementation uses the same fallible declaration rewrite during dependency
planning and publication. It supports both procedure AST parameters and the
existing PostgreSQL quoted-body procedure parser proxy. The stored SQL body value
is preserved, and header parsing/rendering failures propagate before publication.

Source `d1835c3fa7b96456372e6e85f2c9e3101d9785fc` corrected the types, but
the unchanged exact declaration assertion caught unwanted token-join spacing
around array brackets. The function control passed; later commands did not run.

Source `31a105ecb84eee935411524b7151695b1f97732a` uses canonical AST SQL
rendering and changes only the leading declaration kind from FUNCTION to PROCEDURE.
All five focused commands passed, totaling 77 test executions:

- Two routine rename tests and three standalone table row routine tests.
- Twenty rename library tests, including the expanded process crash matrix.
- Four existing Oracle procedure parser tests.
- Forty-one selected type, schema rename, read-only and statement-snapshot tests.
- Seven general SQL tests selected by the routine filter.

The process matrix completed 58 deliberate process exits and 116 reopen checks,
including a cut after the first dependent procedure row publication. It checks
procedure declarations, argument identities, committed versus uncommitted state,
full raw routine undo, and the existing row/ACK assertions. The common test
configuration retains `DbConfig::default()`'s `fsync: true`. These are process-cut
checks, not independent VM reset, power-loss, network-fault or multi-node HA proof.

The independent PostgreSQL 16.13 procedure control passed commit and rollback,
including declarations, calls and reused old names. Its temporary schemas were
removed and the protected application schema was not modified.

## Additional dependencies remain under validation

Tests-only source `fb8f5871458231e4fa9fdcf0b029091b806b326f` added routine
defaults containing a table row cast and a `RETURNS TABLE(item source)` function.
Both failed **before rename**: context-free default evaluation could not resolve
the table type, and `pg_get_function_result` returned `SETOF record`, losing the
output declaration. The later rename, decision, reuse and reopen cells were not
reached. The independent PostgreSQL 16.13 control passed both decisions and all
seven recorded probes, then removed its temporary schemas.

Candidate `10530586a99f79294f510e2160118ed95c1bae6d` did not compile because
it used a helper available only under `cfg(test)` from production code. No tests
ran at that source.

At `d135774385df30c702cff49db95049653b2b04d7`, both original dependency tests
passed through both storage modes, commit and rollback, name reuse and reopen.
The added permission test then exposed a fixture error: it had omitted EXECUTE
grants on the entry routines, so it refused the entry routine before reaching the
protected default function. At `24d2b16d9c083fc64701244d1afe371f0780766f`,
explicit entry grants and a stronger assertion naming the protected function
corrected that fixture. All four focused commands and 73 test executions passed.
The original dependency assertions were retained throughout.

Final production source `b2c044722519291a8c15c76d0d4849366776782b` keeps the
original shared frame constructor and Oracle default-binding paths. PostgreSQL
scalar, procedure and set-returning entry paths complete defaults as the caller
before SECURITY DEFINER entry. The evaluator excludes caller local variables,
slots and expression memos, but uses the current database type catalog. The
dependency planner and publisher share relocation of typed header defaults and
TABLE output columns, with full raw routine rows covered by undo.

All seven final commands passed: 76 focused test executions, 891 library tests
and 419 general SQL tests (1,386 executions including repeated focused subsets).
Four pre-existing benchmark tests remain ignored. The final full library run and
the focused rename group both include the 58-cut / 116-reopen process matrix.
These results qualify b2c04472's code, not a later source revision or all SQL
integration targets. The complete native/SQL admission bridge and independent
application, VM-reset and HA/fault acceptance remain open.

An independent PostgreSQL 16.13 permission control verified the denied default,
successful explicit argument and successful default after GRANT for all three
entry paths. It removed its temporary schema and role. Its script and receipt
are retained with the remaining controls.

Raw receipts, logs, command specifications and native controls are retained beside
this document. `sha256.json` covers those immutable evidence files. Earlier broad
SQL results apply to their recorded revisions; these focused checks do not replace
final integrated broad, application, VM or fault acceptance.
