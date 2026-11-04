# Parenthesized query metadata

Named prepared queries beginning with parentheses could receive PostgreSQL
`NoData` during statement Describe, then return data rows during Execute.
PostgreSQL JDBC rejected those rows because it had no field structure.

Statement-kind recognition now looks through leading parentheses, whitespace
and comments. It describes the original SELECT or read-only WITH query without
rewriting its SQL, branch ordering or row limits. Mutating WITH queries retain
their existing classification guard. There are no application-specific runtime
conditions, storage changes or compiler-format changes.

The new driver regression fails on the unfixed native PostgreSQL profile with
zero described columns instead of four. With the fix, it passes in native and
Oracle-preview profiles: named statements are prepared against an empty table,
then reused for populated and empty results with numeric, timestamp and text
columns. A separate regression protects mutating-CTE classification.

[Independent comparisons](reference-comparisons.json), using the executable
[fixture](../../../examples/oracle/reference/parenthesized_queries.py), cover
single and nested parentheses, comments, and limited UNION ALL operands:

| Reference | Reference expectations | Target before | Target after |
| --- | --- | --- | --- |
| Oracle Free 23.26.3 | 12/12 | 6/12 | 12/12 |
| PostgreSQL 18.4 | 12/12 | 6/12 | 12/12 |

The six baseline failures per reference are statement descriptions. Row execution
already works for these cases. Oracle native cursor parsing and PostgreSQL wire
statement Describe supply the independent metadata observations. Nonce tables
are removed, with Oracle PURGE. Precision, scale, width metadata and complete
driver compatibility are not qualified by these fixtures.

[Validation](validation.json) records 324 passing Rust tests (122 library,
33 Oracle-driver integration, 169 protocol), 25 Python tests, and retained
Describe comparisons passing 9/9 against each reference. Builds use one job,
no incremental compilation or debug information, and monitored 14 GiB start /
8 GiB minimum available-memory gates. Cleanup reclaimed 1,117,533,024 bytes of
completed test executables; the remaining build cache was 7.5 GiB.

[Shipment observations](shipment-observation.json) are from the preceding
application binary, before this protocol fix was deployed. A real UI workflow
created shipment 500000 for order 50000. The shipment is Completed, two units are
delivered, reservations are cleared, stock decreases from 20 to 18, and exactly
one -2 movement exists. The UI result reports successful creation; its automated
result observer timed out and retained that observation rather than hiding it.

Accounting posting remains `E` in this snapshot: a cost-history query exposed
the missing field metadata. Posting recovery on the candidate, invoicing,
payments and the entire application remain unqualified here. No business
statuses, amounts or quantities were repaired through SQL.

The built candidate uses source commit
`40a94db5a12062e4cfa8b96a486e8679058074d0`, SHA-256
`b78991f08d526131bea683ec9cd1d814f499a941cd8d769953dfe98e301e1f7e`.
These are local qualification results, not a published release or production
deployment.
