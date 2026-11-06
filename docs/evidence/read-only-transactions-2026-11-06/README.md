# Enforce transaction read-only access

The EHR's native maintenance check deliberately attempts a modifying CTE inside
`BEGIN READ ONLY`. Before this repair, BicDB attempted the DELETE and reached a
foreign-key error instead of rejecting the write with SQLSTATE 25006.

Transaction access mode now survives SQL and pgwire transaction boundaries and
is enforced before writes, modifying CTEs, row locks, raw DDL, privilege changes,
procedural writes, streaming COPY inserts and nontransactional sequence changes.
Generic SET/reset operations cannot unset the mode. A called function cannot
commit its read-only caller and then write. Rollback restores normal pooled
connection behavior. No new session-default access-mode setting is introduced.

The six new SQL regressions and wire regression fail on the unpatched upstream
5e5fecc source. On the final repair, all six SQL regressions, 722 SQL unit tests
(four existing ignored), all 183 protocol tests and the wire regression pass.
The wire test also proves a separate connection can still write and that the
original connection can write after rollback.

The initial patch added an unnecessary `off` GUC to ordinary transactions. The
existing connection-memory test caught the resulting snapshot growth. Absence
already means read-write, so the final setter leaves ordinary BEGIN unchanged;
the unchanged memory test now passes. Both results are retained. Exported logs
omit Cargo artifact JSON and normalize trailing blank lines; raw log hashes,
source hashes and actual executable/library hashes are retained separately.
Completed test executables were hashed and retired under active resource guards.

Upstream main 4bb612e is incorporated; its Rust source is identical to the
5e5fecc qualification base, including the new timestamp-day indexes. This is a
source repair and synthetic qualification, not a production deployment. The
WalkNorth clinical-data cutover and recovery gates remain open.
