# Storage catalog compatibility contract

**Status: allocator inventory foundation implemented; Oracle storage views are
not implemented or qualified.** Running an application query without an error
will not by itself qualify its monitoring behavior.

## Required semantics

Oracle's [`DBA_FREE_SPACE` reference](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/DBA_FREE_SPACE.html)
describes unallocated extents within database tablespaces. Its columns identify
the tablespace, file, starting block, extent bytes, extent blocks and relative
file number. This measures reusable database allocation, rather than free bytes
on the host filesystem. Files with no free extents contribute no rows.

A general background monitoring workload uses aggregates over this view:

```sql
SELECT tablespace_name, COUNT(*) AS pieces,
       SUM(bytes)/1024/1024 AS free_mb,
       MAX(bytes)/1024 AS largest_kb,
       SUM(blocks) AS free_blocks
FROM dba_free_space
GROUP BY tablespace_name;
```

The view currently fails relation resolution in the preview. Do not disable
application jobs or provide fabricated constants/empty results to conceal this
gap. An empty inventory must mean the allocator actually has no reusable runs.

## Implemented allocator foundation

`bicdb_page::PageStore::free_page_extents(max_pages, max_read_bytes)` returns a
read-only snapshot of the shared store's reusable logical page runs. Its result
contains the page size, logical page count, free page count and sorted,
coalesced runs. Pages on the free list are available to the shared allocator;
they are not live heap/index pages.

The call holds the allocator metadata lock while validating membership and
reading headers. It checks both visit and logical-read-byte budgets before
allocating buffers or reading pages. `None` means the complete inventory was not
admitted; it is not a successful empty or partial result. Header reads include
whole-page checksum validation. Corrupt types, links, cycles, bounds and count
mismatches fail without repair. The call changes neither allocation authority
nor page/file contents.

This is an explicit inventory operation, not a replacement for scrape-safe
`paged_storage_snapshot()`. It excludes filesystem capacity, WAL, live-page
slack and physical tail bytes outside the published logical file. It applies to
monolithic and segmented logical layouts without changing their disk format.

Five focused controls cover budget refusal without reads, fragmented/coalesced
runs, reuse and reopen, corrupt chains/counts without repair, and concurrent
allocation/free. The complete page-manager test group passes: 33 tests, no
failures or ignored cases. This does not qualify any SQL catalog interface.

## Remaining implementation and acceptance gates

- Define a stable mapping from the native shared logical store to database-local
  tablespace/file/block identities. Document logical segmented-file identity and
  excluded storage areas; never count obsolete physical tail bytes as reusable.
- Expose the allocator inventory through the core layer with explicit admission
  and cancellation policy. Distinguish unavailable non-paged storage, deferred
  inventory, empty inventory and corruption. Do not hold a global allocator lock
  for an unbounded SQL operation.
- Implement Oracle view resolution, declared descriptors and row materialization.
  Check local relation shadowing, explicit system qualification, quoted names,
  prepared Describe/Execute and empty-result descriptors. Preserve ordinary
  PostgreSQL name resolution with independent controls.
- Require explicit monitoring authorization for the database-wide view. A
  dedicated grant must not confer superuser, data access or unrelated system
  administration. Check effective-role changes and unauthorized metadata access.
- Check bytes = blocks * actual page size, valid non-overlapping ranges,
  aggregate equality with the admitted inventory, allocation/reuse, vacuum,
  clean restart and segmented layouts. Never return partial rows as a complete
  successful view when budgets expire or corruption is found.
- Capture Oracle source descriptor/aggregate controls and independently test the
  native mapping. Document the supported profile contract and remaining physical
  storage differences without claiming Oracle storage-format equivalence.
- Rerun the unchanged application monitoring query and inspect both its output
  and background-job logs alongside original business assertions. Keep full
  application, production and source-security qualification open until their
  separate acceptance gates pass.

`DBA_DATA_FILES`, `DBA_TABLESPACES`, `USER_FREE_SPACE` and extent allocation DDL
need their own semantics and authorization. This foundation does not implement
those interfaces, Oracle ASM, RAC storage, or Oracle physical recovery formats.
