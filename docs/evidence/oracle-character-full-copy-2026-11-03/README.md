# Full application-copy character reconciliation

Source-driven character reconciliation completed all 910 affected tables and
7,150 declarations on an isolated application copy. Every completed table's
before/after row multiset matched. The original source snapshot and running
database were preserved.

The migration trial used temporary RAM after earlier shared-disk exhaustion.
Its connection closed during the subsequent after-inventory pass. The resource
guard recorded no memory or disk threshold breach. The trial's server logs were
not retained, so the closure's cause is unknown; this remains an operational
qualification gap, rather than a resolved session-reliability issue.

After the trial process terminated, a new disk copy was verified byte-for-byte
against all 997 source database files. The independently qualified reader then
opened that copy and checked all 915 tables and 185,537 rows against the saved
before-inventory. It also checked all 7,150 logical character declarations. A
second process independently reopened the disk copy and repeated both complete
checks. Both passes matched. Reader logs were retained for diagnosis of any
repeat failure. The owned process-group peak was 2,182,946,816 bytes, with an
active minimum 8 GiB available-RAM guard.

This qualifies value-preserving reconciliation and process reopen of this
persistent copy. It does not establish host-crash durability, crash-atomic DDL,
Oracle implicit-commit behavior, general long-session reliability or source
application execution equivalence. Existing source/target row-count differences
are not erased by comparing the target's before and after inventories. National
types, defaults, constraints, catalogs and the remaining compatibility work
still need their own qualification.

The reader binary is the previously qualified SQL-package level-1 development
example, identified in validation.json. It is not a published release or a
deployed application upgrade. Routine acceptance remains 61/62; startup and full
business workflows remain unqualified.
