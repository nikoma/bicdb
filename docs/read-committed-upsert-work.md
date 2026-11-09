# READ COMMITTED upsert completion ledger

The ordinary primary repair #116 is merged as3d3c9005. The following coherent
successor remains isolated until source-current broad checks, independent review,
actual GitHub checks, a coordinated version bump and merge finish.

| Path | State and evidence |
|---|---|
| Autocommit declared-arbiter conflicts | Main #104 retries a fresh shared snapshot; unrelated unique violations remain23505. |
| Ordinary exact primary identity | Main #116: rival commit returns30, rollback10; asymmetric composite order preserves the other row. Shared sessions wait; exclusive sessions refuse busy rows without stranding owner COMMIT. |
| Signed/native authority | Successor focused checks pass: opaque transaction/row/backing/version capability, current policy projection, actual INSERT/UPDATE grant checks and exactly-once budgets/audit. INSERT-only fresh and DO NOTHING remain allowed; actual UPDATE remains denied. Exhausted protected WHEREfalse does not consume another budget. |
| CHAR/NUMERIC primary and secondary unique arbiters | Successor natural winner schedules pass in both storage modes. Native shared key announcements coordinate with exclusive SQL arbiters while preserving the existing concurrent native commit race. |
| NULL policies, deleted/moved keys | Successor covers NULLS DISTINCT nonblocking insertion, NULLS NOT DISTINCT waiting, and insertion of the new identity after winner delete/key move. |
| JSONB primary and secondary uniqueness | Successor canonical SQL reservation plus checked current-row arbitration passes reordered-object/decimal-scale schedules against actual PostgreSQL16 references. Physical lookup is followed by semantic candidate lookup on a miss. |
| Unrelated relation visibility | Successor returns31 using current conflicting row20, proposed10 and original auxiliary snapshot1; winner auxiliary99 remains committed. No whole-transaction snapshot refresh. |
| Pending DELETE followed by key reuse | Main #117 e64b2e96 activates original unchanged assertions in both modes; scaling target4/4 and independent PostgreSQL reference pass. No runtime fix claim. |
| Native fixed-snapshot writes | Explicit retry remains intentional; direct native secondary control still requires TransactionConflict and preserves committed winner20. |
| Legacy non-executable/unindexed arbiter shapes | Existing fallback remains outside the refreshed executable-index/canonical-JSON path. No concrete remaining recovered failure has been established for this category; do not infer universal arbiter coverage from the tested shapes. |

These checks establish specific SQL/native concurrency and authority behavior.
They do not establish integrated iDempiere acceptance or universal native SQL
canonicalization for callers bypassing SQL validation. PostgreSQL and Oracle
qualification remain separate. See the source-scoped evidence alongside the
reference schedules in docs/evidence/read-committed-arbiters-2026-11-09.
