# Exact text index lookups and numeric source casts

Exact text equality can use the leading field of an existing UPPER/LOWER
expression index. The probe uses the durable index key conversion, then retains
the original predicate as a residual filter. Case collisions and non-ASCII
strings therefore keep exact equality semantics. Both ordinary plans and cached
row-locator plans use the inference. Independent PostgreSQL and Oracle-preview
controls cover pending writes; PostgreSQL controls also check plan counters and
restart. Padded, collated, user-defined and converted column expressions retain
their existing fallback paths. This is not a latency or throughput benchmark.

Scaled Oracle ROUND now applies its precision policy to explicitly cast numeric
operands too. [Read-only Oracle controls](../oracle-numeric-cast-defaults-2026-11-06/README.md)
distinguish bare NUMBER from bare NUMERIC/DECIMAL source casts. The session
frontend applies the latter defaults before canonical lowering and bind
rendering, preserving exact numeric wire parameters. PostgreSQL's unconstrained
numeric casts remain separate controls. Formatting and anchored-type fixtures
use NUMBER for fractional Oracle input and NUMERIC for PostgreSQL; their value
assertions remain unchanged.

Validation source is `58c678fd76df45400a5bcc09616bd85a74dabcb9`. The server binary
was built from `9fd07c4db0e4e856b19d5c3ea9a3f1e4a7a375f4`. Exact source comparison
confirms that only `crates/bicdb-pgwire/tests/oracle_hr.rs` differs between those
revisions. Completed checks of unchanged code were reused with their original
revision recorded; the affected Oracle suite and previously unrun CLI build
were completed on the final revision.

| Validation | Result | Executed revision |
| --- | --- | --- |
| SQL library | 733 passed, four existing ignores | `9fd07c4` |
| Protocol | 183 passed | `9fd07c4` |
| Native migration/tools | 215 passed, four external references skipped | `9fd07c4` |
| Oracle wire suite | 48 passed | `58c678f` |
| Authorization/JSON integration | 44 passed | `58c678f` |
| CLI build and storage-copy command help | Passed | `58c678f` |

[summary.json](summary.json) includes focused results, binary hashes, exact
revision provenance and resource observations. Earlier failed fixture results
and interrupted resource attempts remain retained. No upstream application
source, assertions or bytecode were modified.

This batch does not rerun or qualify the full application. The original
vendor-invoice failure, attachment/heap-locking error, paged baseline migration,
remaining application tests, UI, security and recovery gates remain open.
Source NUMERIC/DECIMAL declaration defaults, extended PL/SQL casts and complete
numeric range/descriptor parity also need separate qualification. See the
[ROUND contract](../../oracle-numeric-round.md) for the current scope.
