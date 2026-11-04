# Typed builtins in set-operation operands

The Oracle builtin resolver only probed FROM bindings when the enclosing query
body was a plain SELECT. A later UNION operand could therefore leave a valid
numeric NVL inside SUM unlowered. Standalone execution worked, while the combined
query, stored view and prepared count could fail as unsupported NVL.

The resolver now tracks each SELECT operand's local scope. Metadata probes keep
the enclosing WITH clause, use that operand's bindings, and exclude the outer
set operation's ordering/pagination. Resolved nested queries also refresh the
active operand snapshot. The actual SQL tree gains no wrapper queries, and no
application, schema or table names select behavior. An independent parser gap
found during qualification now maps unquoted Oracle MINUS to native EXCEPT;
literals, quoted identifiers and default PostgreSQL aliases retain their tokens.

[Independent comparisons](reference-comparisons.json), using
[set_operand_builtins.py](../../../examples/oracle/reference/set_operand_builtins.py),
record fourteen explicit expectations on each source:

| Reference | Target before | First scope candidate | Final candidate |
| --- | --- | --- | --- |
| Oracle Free 23.26.3 | 2/14 | 13/14 | 14/14 |
| PostgreSQL 18.4 | 14/14 | Not run | 14/14 |

The first candidate's MINUS parser failure is retained rather than removing the
case. The suite covers joined aggregates, UNION/UNION ALL, intersection, set
difference, nested set operands, ordered pagination, stored views and counts,
CTE inputs, reused aliases with different types, logical DATE defaults and nested
scalar defaults. Oracle NVL's eager evaluation and PostgreSQL COALESCE's lazy
evaluation have independent expectations. Nonce views/tables are removed,
purging Oracle fixture tables.

[Retained comparisons](retained-comparisons.json) also pass scalar-subquery
7/7 and numeric-picture 120/120 cases independently against each source.
[Validation](validation.json) records 599 SQL unit tests, three existing ignored
tests, 35 driver tests, 25 Python tests and formatting. The new driver regression
reuses prepared value/count statements before and after reopening each profile's
database. Builds use one job, no incremental compilation or debug information,
and monitored 14 GiB start / 8 GiB minimum available-memory gates. Cleanup removed
completed test executables from both validation batches while retaining runnable
binaries and reusable dependencies.

The final binary uses source `c4da7cbde37770dc327cd1525329b1b6d91728b4`, SHA-256
`64a3be06c438851f3a59a55c396aedcfea812699299dc38e5686a2c5b1c26366`.
The first scope candidate uses source `38812a95d5b819140a15e240d299136f3829e115`,
SHA-256 `3264bf5d9d01879c71c036c4dfe87d54ea8942ba747a8e76d2be32557eb5ffca`.
No stored helper signature, compiler-language identity or durable metadata format
changed. These are local development builds, not published releases.

At this checkpoint the candidate has not been deployed to the application.
Arbitrary correlated/type-sensitive set queries, MINUS ALL, other set-operator
extensions, complete application coverage and the broader Oracle programme
remain unqualified. The original material-receipt selection screen remains a
separate application requalification gate.


## Subsequent application requalification

After the source reached verified main at `13fa542`, the old application and
database stopped gracefully; the database exited successfully. The candidate
opened a copy of that stopped database, reloaded 62 original routines and
verified all 160 original view definitions/type families. All
[16 read-only business-state gates](application-upgrade.json) matched the
preceding runtime: the paid invoice, allocated/reconciled receipt, bank balance
269.03, posted sales documents, completed purchase order and draft receipt were
preserved without SQL repairs.

The original prepared receipt-selection count now returns one eligible line,
quantity three. A fresh browser opens the same draft receipt and its original
[selection screen](receipt-selection.png) shows that row without the preceding
NVL or secondary negative-count errors. The standard action subsequently creates
one line, and the [material receipt checkpoint](../idempiere-material-receipt-2026-11-04/README.md)
completes/posts the receipt. This qualifies the selected local path; complete
application operation and the full Oracle programme remain open.
