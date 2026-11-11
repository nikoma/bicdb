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
ran at that source. A subsequent candidate fixes that compile error and evaluates
omitted PostgreSQL arguments before SECURITY DEFINER entry. Its outcome is not
established by this evidence set. Do not treat the proposed correction as qualified.

Raw receipts, logs, command specifications and native controls are retained beside
this document. `sha256.json` covers those immutable evidence files. Earlier broad
SQL results apply to their recorded revisions; these focused checks do not replace
final integrated broad, application, VM or fault acceptance.
