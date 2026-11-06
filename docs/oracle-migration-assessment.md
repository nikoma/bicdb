# Offline Oracle source assessment

`bicdb oracle assess` inventories raw Oracle SQL and PL/SQL files without
connecting to a database, executing statements, or changing database state:

```sh
bicdb oracle assess schema.sql packages.sql --json > /tmp/oracle-inventory.json
bicdb oracle assess schema.sql packages.sql --fail-on-blockers
```

The target for this initial scanner is the experimental `oracle-hr-v1` profile.
The report identifies likely schema objects, qualified/quoted names, static
dependency leads, and known migration blockers or review requirements. Findings
include stable identifiers, severity, source file, line and column, and a concrete
next action. SQL literals and comments are not copied into diagnostics.

Detected constructs include package specifications/bodies, autonomous and dynamic
SQL, wrapped routines, hierarchical queries, ROWNUM, synonyms, triggers, MERGE,
multitable inserts, materialized views, supplied package calls, bulk PL/SQL,
Oracle DECODE/NVL2, session settings, index-organized storage, and LOBs. NUMBER,
VARCHAR2, DATE and partition clauses receive review findings because their
presence alone does not establish that a particular use is unsupported.

This is a **lexical inventory**, not a SQL parser, PL/SQL compiler, complete
dependency resolver or readiness certificate. It ignores keywords inside quoted
identifiers, strings and comments. Its remaining lexical matches are conservative
leads and can require manual disambiguation. For example, an analytic PARTITION
BY clause and physical table partitioning need different qualification. FROM
leads can include CTE names and other expressions; the scanner does not build a
dependency-ordered migration or resolve objects against source catalogs.

The input is UTF-8 SQL/PLSQL, not an executable SQL*Plus/SQLcl session. Common
client comment/output lines (REM, REMARK, PROMPT) are excluded at statement
boundaries outside literals, comments and procedural units. Recognized client
settings and include directives are also excluded and produce review findings.
This avoids treating a license comment containing “merge” as a MERGE statement.
Source line/column positions remain intact. Extract SQL from other client
commands before assessment, and include every referenced file. Substitution
variables, script includes and client commands are not executed or expanded. A
tokenization failure is reported as an incomplete scan. An input that tokenizes
is not thereby syntactically valid SQL.

Each file is limited to 16 MiB, an invocation to 64 MiB, and a source report to
20,000 inventory entries. Split larger exports into complete object-definition
files. Output truncation and lexical failure remain explicit; neither can be a
successful `--fail-on-blockers` check. Oversized/nonregular files, invalid UTF-8
and I/O failures cause command errors.

Without `--fail-on-blockers`, successful report generation exits zero even when
the report contains blockers. With that option, known blockers or incomplete
scans cause a nonzero exit after the report is printed. Neither exit status
certifies migration readiness: `migration_ready` is always false, and
`runtime_assessment_required` is always true in this version. A clean report
means only that no listed pattern was found.

Runtime traces, bind types, dynamically constructed SQL, source database
catalogs, dependencies, data reconciliation and the application's transaction,
security and operational requirements still need assessment. Wrapped code
requires inspectable source or a supported replacement. Driver/framework and
edition-specific dependencies must be identified separately. See the
[full compatibility programme](oracle-compatibility-programme.md) and
[live reference harness](../examples/oracle/reference/README.md).

Reports contain source paths and object names. Review those identifiers before
publishing an inventory of a private application. The tool does not print source
literals or tokenizer error text, which may contain sensitive source content.

## Review first-row selection and tied ordering

Inventory application calls that consume only the first row, including JDBC
`ResultSet.next()`, scalar query helpers, pagination and cursor loops that stop
early. An `ORDER BY` clause can still leave the selected row unspecified when
several eligible records have identical sort keys. The lexical scanner does not
detect this dependency or prove that ordering keys are unique.

For example, this query does not choose a particular identifier when multiple
active records have the same default flag:

```sql
SELECT choice_id FROM choices
WHERE is_active = 'Y'
ORDER BY is_default DESC;
```

If the business rule prefers the lowest identifier among equal flags, express
that rule explicitly with `ORDER BY is_default DESC, choice_id ASC`, using a
unique identifier. If selection depends on a customer's configured choice,
resolve that configuration first; an arbitrary identifier is not a substitute
for the business rule. Review null placement and collation as well.

Capture the complete eligible rows, bind values, effective role and query text
on both systems. Compare unordered results as multisets and compare ordered
results according to their declared keys. A different first row among tied
values is evidence of a portability dependency; it does not by itself prove a
sort, conversion or arithmetic defect. Oracle's
[ordering guidance](https://asktom.oracle.com/ords/f?p=100%3A11%3A%3A%3A%3A%3AP11_QUESTION_ID%3A9527481800346557177)
also requires deterministic keys for predictable row selection.

Keep a failing original application test and its source/target observations in
the acceptance record while investigating. Changing data defaults, adding an
undocumented engine tie-breaker, or repeatedly rerunning until a preferred row
appears does not qualify the original workload. Any approved application change
needs its own provenance and a separate acceptance run; it must not be reported
as an unchanged-application result.
