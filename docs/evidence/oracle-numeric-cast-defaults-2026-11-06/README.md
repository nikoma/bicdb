# Oracle source numeric CAST defaults

A read-only Oracle control, fetched as exact decimal values, observes:

| Expression | Value |
| --- | ---: |
| `CAST(15.193 AS NUMBER)` | 15.193 |
| `CAST(15.193 AS NUMERIC)` | 15 |
| `CAST(15.193 AS DECIMAL)` | 15 |
| `ROUND(CAST(15.193 AS NUMBER),1.5)` | 15.2 |
| `ROUND(CAST(15.193 AS NUMERIC),1.5)` | 15 |
| `ROUND(CAST(15.193 AS DECIMAL),1.5)` | 15 |

The NUMERIC/DECIMAL source descriptors report precision 38 and scale 0. NUMBER
retains the unconstrained source descriptor. The control changed no source
application data and restored the reference container's original stopped state.
[source-control.json](source-control.json) records descriptors, values and the
query hash. This is source evidence; candidate validation and complete numeric
type compatibility are separate gates.

The [scaled ROUND contract](../../oracle-numeric-round.md) describes session
CAST handling and its current qualification limits. It must preserve PostgreSQL
cast semantics and the exact values of numeric bind parameters.
