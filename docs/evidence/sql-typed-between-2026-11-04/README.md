# Typed BETWEEN boundaries

A datetime range could omit its exact lower boundary when an equivalent bound
had a different spelling. For example, a timestamp at midnight compared below
an unknown timestamp literal ending in `.0`, because BETWEEN compared their
transport strings without retaining operand types. Ordinary text with the same
contents must continue to use character comparison.

BETWEEN now uses the existing typed comparison operators for its lower and
upper comparisons. Scalar, row, slot-row, aggregate, virtual and procedural
paths retain expression provenance. Table predicates also retain their relation
types. Operand values are retained rather than guessed from date-looking text;
tuple handling and three-valued NULL conjunction remain separate existing rules.
There are no application/schema-specific branches or new persisted fields.

## Independent references

The [fixture](../../../examples/oracle/reference/typed_between.py) uses temporary
nonce objects. PostgreSQL cases use unknown ISO timestamp bounds; Oracle cases
use explicit datetime literals and do not qualify implicit NLS conversion.
Each source satisfies its own expectations, including their different volatile
procedural behavior.

| Prepared-query cases | Previous binary | Candidate |
| --- | ---: | ---: |
| Oracle 23.26.3.0.0 controls | 16/16 | 16/16 |
| PostgreSQL 18.4 | 4/16 | 14/16 |

[Baseline](baseline.json) and [comparison](comparison.json) retain each result,
source version, binary hash and fixture hash. The candidate fixes the ten
previously failing deterministic PostgreSQL boundary cases. These include row
predicates, negation, truncation, projected CASE, aggregates, joins, CTEs, scalar
subqueries and stored timestamp parameters. Character and NULL controls remain
independently checked. Nonce objects and disposable containers were removed.

Two PostgreSQL cases remain unqualified: `volatile_value_evaluation` and
`sequence_evaluation_count`. The actual PostgreSQL procedural expression reads
the sequence twice; Oracle reads it once. BicDB preserves its existing single
read in both profiles in this change. The PostgreSQL fixture deliberately
returns nonzero (`success: false`) while those differences remain. This is a
bounded boundary-comparison fix, not complete PostgreSQL or Oracle conformance.

## Subsequent application acceptance

The original application's previous Trial Balance request persisted 47 correct
rows, but its rendered range omitted 19 opening balances. Equivalent bounds
ending in `.0` or `.000000` reproduce that omission. The subsequent
[application checkpoint](../idempiere-trial-balance-2026-11-04/README.md) verifies
a clean runtime upgrade and a fresh HQ report/PDF containing all 47 rows and
19 opening balances, with 46 passing financial/reporting observations. This
qualifies the selected report, not the full application.

## Local validation

The [validation record](validation.json) identifies the source commit and binary:
601 SQL tests passed (three existing benchmark ignores), 363 driver/protocol
tests passed (two existing external interoperability ignores), and the final
focused Oracle fixture passed after its setup was refined. No production code
changed between the full suites and that test-only refinement. The
[retained RAW comparisons](retained-raw-comparison.json) pass 13/13 in each
profile. Formatting checks passed. The two native reference failures above
remain visible and are separate from the passing local Rust suites.

## Subsequent evaluation qualification

The [evaluation-context checkpoint](../sql-between-evaluation-2026-11-04/README.md)
closes both native volatile-evaluation differences above and retains all typed
boundary cases: 16/16 for Oracle and PostgreSQL independently. The earlier
14/16 result remains preserved here as historical evidence.
