# Contextual LIKE ESCAPE and profile defaults — 2026-11-06

Oracle lowering now preserves `ESCAPE` where the expression parser identifies a
`LIKE` suffix. The nonreserved word still folds correctly as a column name,
qualified identifier and pattern operand. Literal and prepared patterns,
parenthesized expressions, nested CASE predicates, comments and Unicode escape
characters have independent PostgreSQL and Oracle-preview regressions.

`oracle-preview-v1` additionally disables PostgreSQL's implicit backslash escape
when Oracle source omits an ESCAPE clause. PostgreSQL retains its default;
explicit backslash escaping works in both profiles. This follows the
[documented Oracle pattern rules](https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Pattern-matching-Conditions.html).
The injected native execution sentinel is added after source empty-string
lowering. The narrow legacy HR profile receives the contextual grammar change;
it is not newly qualified for full pattern semantics.

The [exact source/artifact record](summary.json) reports 735 passing SQL tests
(four existing ignores), 183 protocol tests, 48 Oracle-HR tests, 44 authorization
and JSON integration tests, and 239 native-tool reports (235 passes, four
external-reference skips). The six new native escape tests also passed in a
focused run; they are part of the native-tool total, not extra distinct tests.
All checks ran locally on the recorded revision. A standalone parser control
caught an endpoint-index error before the engine validation batch; all final
engine checks passed on the corrected implementation.

The original zoom-provider application retest remains required. Explicit NULL
escape handling remains a known shared predicate compatibility gap. Bound
escape expressions, complete Oracle error-number parity, collation/NLS rules,
LIKEC/LIKE2/LIKE4, large-query parsing cost and full application behavior remain
open qualification work. No live Oracle comparison or production conformance
is asserted by this checkpoint.
