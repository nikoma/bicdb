# Nested exception-block reference contracts

These [six independent reference cases](reference-contracts.json) run on Oracle
Free 23.26.3.0.0 and PostgreSQL 18.4. They establish implementation requirements;
BicDB was not executed and remains unqualified for these contracts.

An update before a handled nested-block error remains visible in Oracle. In
PostgreSQL it is rolled back to the value at entry to that block. Both references
retain the changed local variable. A failing multirow INSERT leaves no partial
rows in either reference; Oracle retains the preceding successful UPDATE, while
PostgreSQL rolls it back with the block. An error raised inside a selected
handler propagates to the outer handler instead of selecting another handler
from the same block.

The [public runner](../../../examples/oracle/reference/nested_blocks.py) uses
fresh connections, unique synthetic tables and explicit expectations for each
backend. It verifies rollback after each case and drops its tables. The measured
PostgreSQL instance was disposable, used a cached image with 256 MiB memory and
one CPU, and was removed after validation. Source connection settings and
credentials are not published.

Set `ORACLE_CONTROL_PG_DSN` to a dedicated PostgreSQL reference connection and
provide a private Oracle configuration in the reference harness format:

```sh
python3 examples/oracle/reference/nested_blocks.py \
  --oracle-config oracle-reference.json \
  --output nested-block-reference-contracts.json
```

The [Oracle exception contract](https://docs.oracle.com/en/database/oracle/oracle-database/26/lnpls/unhandled-exceptions.html)
and [PostgreSQL block contract](https://www.postgresql.org/docs/18/plpgsql-control-structures.html)
explain why sharing one rollback policy would be incorrect.

BicDB acceptance depends on scoped block parsing and variables, nested handler
selection and propagation, condition/error mappings, and profile-specific undo:
statement rollback for the tested Oracle case versus block rollback for
PostgreSQL. Execution must carry the routine/block's profile identity; native
LANGUAGE plpgsql inside a preview database must retain PostgreSQL behavior.
The same synthetic scenarios must then be run through BicDB's PostgreSQL driver
with the appropriate expected results, including verification after rollback
and reopen. Packages, autonomous transactions and full exception semantics
require additional acceptance beyond these cases.
