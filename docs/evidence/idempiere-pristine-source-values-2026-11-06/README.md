# Pristine saved-source data qualification

A fresh isolated BicDB database imported all **915 tables and 185,471 rows**
from the original saved Oracle export. All 915 table row multisets matched the
saved source, including duplicate multiplicities. Table creation, data import,
and verification each exited zero. The database stopped cleanly with exit zero.

[Validation](validation.json) identifies the exact engine, tool source, snapshot,
binary, report hashes, and resource observations. This uses the compatibility
example binary with the system allocator and `MALLOC_ARENA_MAX=2`; it does not
qualify the production CLI allocator or a production deployment. No live Oracle
connection was used during this run; independent saved-export source replay is
separate evidence.

This is a pristine data checkpoint. It does not yet include complete stored-code,
secondary-index, foreign-key, ownership, view-type, or application qualification.
Further schema assembly runs against a separate copy, preserving this checkpoint.
The earlier [89-test foundation batch](../idempiere-full-fk-foundation-2026-11-05/README.md)
used an exercised full-FK application database, not this new pristine database.
