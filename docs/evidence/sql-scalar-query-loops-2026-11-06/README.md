# Declared scalar query-loop targets

WalkNorth monitoring validation uses `FOR path IN SELECT unnest(...)` with a
declared `text` target, then uses `cfg ? path`. The interpreter previously bound
every query-loop target to a JSON record, producing `jsonb ? operator requires a
text right operand`. Numeric and JSON scalar targets were likewise incorrect.

Commit `066102b5dd15b766ff3c5f0e95169b1148cdf905`, based on current main
`75968281549894129ce023de91abbec6637add3e`, distinguishes declared scalar targets
from records using the routine's type metadata. Scalars receive the first result
column through the existing checked assignment path. Records retain their field
names, including single-column records. Registered scalar domains and type
modifiers are checked; composite types and implicit records retain record shape.

Actual local checks, executed on the isolated public/synthetic build host:

- `cargo fmt --all -- --check`: passed.
- Causal control: the six new tests on unfixed `e8a3656` source, with only the test
  module added: **one passed, five failed**. The unchanged record case passes;
  text/JSON/numeric targets and assignment constraints fail.
- `cargo --config profile.dev.debug=0 test -p bicdb-sql --lib routine_query_loop_ -- --test-threads=2`
  on `066102b`: **six passed**, no failures or ignored tests.
- `cargo --config profile.dev.debug=0 test -p bicdb-sql --lib -- --test-threads=2`
  on `066102b`: **748 passed, zero failed, four ignored**, 159.54 seconds. Ignored
  tests were not executed. This includes native PostgreSQL and versioned Oracle
  routine, cursor, type, policy, transaction and expression regressions.

The three retained logs are actual command output. Builds used one Cargo job,
disabled incremental compilation, and an active 12 GiB available-memory / 8 GiB
free-disk guard that stops only the task's process group if headroom is crossed.
Compiler warnings remain visible in the logs.

This proves the SQL interpreter correction, not EHR application acceptance or a
production deployment. The complete EHR HA workload and physical-host failover
qualification remain open. No clinical data or production credentials were used.

The target forms follow PostgreSQL's documented
[query-result loop semantics](https://www.postgresql.org/docs/18/plpgsql-control-structures.html#PLPGSQL-RECORDS-ITERATING).
