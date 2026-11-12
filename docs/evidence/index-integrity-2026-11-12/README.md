# Index integrity and full rollback retry

Implementation source `35e6260fe117947b8c5baea1febf606d91b050b4`, based on actual main482 `f43583da4c34209e810c5e6a8f5925831b2949a1`, passed **254 focused tests**. The runtime change is limited to SQL index drop helpers and retention of a refused RestoreIndex undo; no private Native/rename implementation is included.

DROP INDEX resolves the actual namespace and live search path, preserves quoted spelling, preflights every named relation and dependency before removing anything, and refuses direct removal of primary-key, unique and exclusion backings. Materialized-view descriptors and storage identities are included. The Oracle primary/unique diagnostic is independent of PostgreSQL's2BP01. A full ROLLBACK retains its Native index definition after a real pending-owner rebuild refusal; a later retry restores it after the blocker commits.

The batch includes seven new integration regressions, 41 existing SQL catalog/constraint/authorization tests, two version identity checks, one existing case-mapped index library test, 195 protocol, five online-backup, one catalog ACL and two read-only wire tests. The new rollback regression checks both stores, an independent acknowledged write and two clean reopens. These tests do not certify durable undo after process death or successful initial rebuild while another owner remains pending. Original application assertions and timeouts were not modified.

The real PostgreSQL16.13 control records five dependency refusals and preservation of the first free index in a refused multi-drop. Its new schema is rolled back; the protected source database and frozen export are untouched.

Earlier setup failures are retained separately from tests: the first bundle lacked a remote prerequisite before compilation; the next source failed compile because an adapter copied a reference where this public API owns a bool. Neither is a product test failure. The corrected source's actual254 positive results are recorded here with raw compressed logs and hashes.

This is pre-delivery evidence. A synchronized next-unused source version and exact final-head CI remain required before merge. DEFAULT partition execution, portable imported-origin rename recovery, generic standalone unique-index FK dependency tracking, full application acceptance, process/VM faults and HA qualification remain separate open work.
