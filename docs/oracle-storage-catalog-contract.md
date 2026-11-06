# Storage catalog compatibility contract

**Status: bounded native paged-store inventory and Oracle-preview dictionary
mapping implemented and locally tested. Full application and production
qualification remain open.** See the [storage-catalog evidence checkpoint](evidence/oracle-storage-catalog-2026-11-06/README.md) for
exact source commits, test groups and remaining gates.

## Meaning and storage mapping

Oracle's [`DBA_FREE_SPACE` reference](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/DBA_FREE_SPACE.html)
defines unallocated database extents. This is reusable database allocation,
rather than free space on the host filesystem. A file with no free extents
contributes no rows.

The native relation is `pg_catalog.bicdb_oracle_free_space`. Oracle-preview SQL
also resolves `DBA_FREE_SPACE` and `SYS.DBA_FREE_SPACE` to this inventory when no
corresponding local relation shadows the dictionary name. Local tables, views,
CTEs and quoted identifiers retain their ordinary resolution. In the default
PostgreSQL profile, unqualified application names remain ordinary relations.

| Column | Declared native type | Native meaning |
| --- | --- | --- |
| TABLESPACE_NAME | varchar | `BICDB_PAGED`, the current database's shared logical store |
| FILE_ID | numeric | `1`, identifying that logical store |
| BLOCK_ID | numeric | Actual first logical page of a reusable run |
| BYTES | numeric | Run pages multiplied by the actual store page size |
| BLOCKS | numeric | Number of reusable pages in the run |
| RELATIVE_FNO | numeric | `1`, the same logical file identity |

Segmented physical layouts remain one logical file, so a coalesced run can cross
physical segment boundaries. These identifiers are native virtual identities,
not Oracle data-file numbers, operating-system paths or Oracle storage formats.
The inventory excludes WAL, live-page slack, filesystem capacity and physical
tail bytes outside the published logical store. Clean checkpoint/restart can
truncate free tail pages or reuse holes for metadata: a later empty inventory
can therefore be correct even when an earlier vacuum produced free runs.

The relation requires `server_paged` storage. A memory-backed database reports an
unsupported-feature error; it does not manufacture an empty or estimated
inventory. Storage-mode migration is a separate offline copy operation whose
result needs application/data qualification before cutover. Existing application
baselines are not silently reinterpreted or migrated by this view.

## Authorization

Queries require a superuser or an explicit whole-view SELECT grant, directly,
through an inherited role, or through an administrator's PUBLIC grant. A role
name alone confers no monitoring authority. Provision the grant through the
native PostgreSQL administration interface, including on a preview connection:

```sql
CREATE ROLE storage_monitor NOLOGIN NOSUPERUSER;
GRANT SELECT ON "pg_catalog"."bicdb_oracle_free_space" TO storage_monitor;
GRANT storage_monitor TO app_login;
```

Replace `app_login` with the existing application role selected by the operator.
Only a superuser can grant or revoke this monitoring permission. It grants no
application-data access or maintenance authority. Native `VACUUM` retains its
superuser requirement through either SQL profile. Effective-role changes and
revocations apply to subsequent inventory queries. An empty predicate or a join
does not bypass the permission check. The relation is a view, not a callable
catalog function.

## Admission, cancellation and errors

The allocator holds its metadata lock while validating the free list and
reading checksummed page headers. It admits the complete inventory before reads;
there is no successful partial result. The SQL interface uses these session
settings:

| Setting | Default | Maximum |
| --- | ---: | ---: |
| bicdb.storage_inventory_max_pages | 8,192 | 65,536 |
| bicdb.storage_inventory_max_read_bytes | 67,108,864 | 268,435,456 |
| bicdb.storage_inventory_timeout_ms | 1,000 | 10,000 |

Each value must be a positive integer within its bound; it is validated when the
inventory is requested. Native `SET`/`RESET` control these session settings.
Both dotted names and their quoted identifier representations reset the same
setting. A configured default may be restored by RESET.

An inventory beyond the page or byte admission budget fails with SQLSTATE
`54000`, identifying both configured limits. Cancellation/deadline errors remain
`57014`. Cancellation is cooperative while waiting for the allocator and between
reads; it does not promise preemption of an individual blocking filesystem read.
Corrupt counts, bounds, cycles, page types or checksums fail without repair.
Unavailable storage, admission refusal, corruption and a successful empty
inventory remain different outcomes.

## Verification and remaining gates

The allocator controls cover fragmented/coalesced runs, actual segment
boundaries, allocation/reuse, reopen, corruption without repair, budget refusal
without reads, concurrent allocation, and cancellation including allocator-lock
waits. SQL controls separately cover both profiles, grants/revokes/effective
roles, unrelated-data denial, local-name resolution, prepared/empty descriptors,
actual free-page totals, fractional numeric aggregates, budget refusal,
configuration reset, allocation reuse and clean restart. Declared numeric
columns use the engine's exact numeric value carrier as well as numeric wire
descriptors. Labels and type families are checked independently of row counts.

A read-only Oracle 23.26.3 control captures the six source column descriptors and
source aggregates. This establishes the reference shape, not equality of native
and Oracle physical free-space values. Native `varchar` currently has an
unbounded descriptor; exact parity with source `VARCHAR2(30)` metadata is still
open. Broad type-family equality must not be described as exact descriptor
parity.

The full application must still run its unchanged monitoring job with explicit
monitoring access on a qualified paged baseline. A passing standalone query or
fixture is not application qualification. Broader catalog discovery, source
privilege mapping, security qualification, recovery and production behavior
remain separate gates. `DBA_DATA_FILES`, `DBA_TABLESPACES`, `USER_FREE_SPACE`,
extent allocation DDL, Oracle ASM/RAC storage and Oracle physical recovery
formats are not implemented by this interface.
