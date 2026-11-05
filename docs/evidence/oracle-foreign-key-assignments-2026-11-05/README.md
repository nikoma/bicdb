# Oracle foreign-key assignment intent and durable enforcement contract

An Oracle-preview SQL UPDATE explicitly assigning a foreign-key column now
checks that relationship even when its typed key is unchanged. This matters for
legacy orphan rows under `ENABLE NOVALIDATE`: changing an unrelated column is
permitted, but explicitly assigning an unchanged invalid foreign key must fail
at the selected statement or transaction boundary. Composite and tuple targets
carry the same assignment intent. NULL references retain their existing
MATCH SIMPLE exemption. PostgreSQL retains its unchanged-key exemption and
parent-key reservation behavior.

The validator receives explicitly assigned column names from the parsed UPDATE,
including tuple targets. It does not infer application tables or names. Native
persistent BLOB append supplies no SQL assignment intent; it continues to detect
actual key changes from generated values through ordinary before/after checks.
The stored UPDATE optimization already excludes tables with foreign keys.

## Durable profile transition

Oracle-preview foreign-key schema saves advance the durable profile to format 8
with `oracle_foreign_key_assignments_v1`. This includes prior feature contracts
and creates prerequisite persistent BLOB ownership infrastructure. Earlier
feature saves accept the stronger marker without downgrading it.

Opening an existing Oracle-preview database through the server's profile
initialization upgrades its marker when foreign-key schema metadata is present.
This changes neither row values nor each constraint's validation state. Embedded
callers must perform the same `ensure_database_profile` initialization before
using older foreign-key metadata; UPDATE validation refuses an unfenced legacy
contract instead of silently applying the new rule.

Profile-aware servers that do not recognize format 8 reject its marker before
serving the database. The executable comparison checks this local startup
contract; it does not qualify mixed-version replicated deployments or rolling
cluster upgrades. Plan this as a storage-contract upgrade: retain a pre-upgrade backup if an older
binary rollback is required. Resetting only the marker would remove the writer
fence and is not a supported rollback. This does not upgrade the running accepted
application instance automatically.

## Reproducible checks

The [timing comparison](../../../examples/oracle/reference/foreign_key_timing.py)
requires independent expected outcomes on Oracle, PostgreSQL and BicDB. The
[writer-fence check](../../../examples/oracle/reference/foreign_key_writer_fence.py)
uses actual previous and current executables with a disposable database, then
cleans its owned state. Invocation is documented in the
[reference guide](../../../examples/oracle/reference/README.md).

This is a selected semantic and writer-compatibility checkpoint, not full Oracle
conformance, application qualification or production certification. Source
relationship migration, orphan inventory, concurrency/recovery qualification and
original business workflows with all relationships installed remain open.

## Recorded results

The [live reference results](reference-results.json) pass **17/17 on Oracle
23.26.3 and 17/17 on PostgreSQL 18.4**, each against independent expected
outcomes. PostgreSQL diagnostic SQLSTATEs match; Oracle error-number equivalence
is not claimed. Nonce objects, native comparison servers and the disposable
PostgreSQL container were cleaned up.

The [actual executable upgrade results](writer-fence-results.json) pass all six
gates, including native BLOB append on a legacy orphan, restart, repaired
references and old-server rejection after a subsequent BLOB schema save. The
disposable database was removed. Binary and fixture digests identify the tested
artifacts.

The [validation record](validation-results.json) includes 699 SQL library tests
passed with three ignored, all selected integration suites, 181 PostgreSQL and
48 Oracle-profile protocol tests, workspace/all-target checks and a successful
compatibility-server build. The negative regression failed on the previous
implementation because its COMMIT incorrectly succeeded; the PostgreSQL
control passed before and after the fix.
