# Private concurrent SQL rename development

This is an unmerged integration following the [local UUID consumer](../namespace-consumer/README.md). The inherited development version is not a reservation for another main merge. The results below cover their named sources, not current public binaries, original iDempiere acceptance, or production HA.

The producer reserves the affected relations, exact catalog records and journal backings before capturing its durable undo images. Its business transaction retains the reservation until a durable decision or complete undo. Recovery validates every remaining image, including its actual local UUID and runtime scope, before applying any repair. An unrelated native or SQL writer can continue; affected readers receive a retryable refusal.

The integration also retains the actual business owner during wire SAVEPOINT undo and releases durably decided scopes in native wire, interpreted procedure and compiled procedure commit paths. Foreign-key discovery admits the parent and actual referring descriptors, including cache hits, rather than observing every schema. No native application grant or compiled-routine declared-type guard was relaxed.

## Recorded results

| Source | Actual result |
| --- | --- |
| `5a7faea5` | Unchanged producer, four new concurrency regressions: **0 passed, 4 failed** at the first EmbeddedMemory case on the original global pending-owner/compaction gates. The later ServerPaged iterations were not reached. |
| `6ab82da9` | Initial scoped producer: **29 passed**: four native concurrency tests, 13 journal tests, three dependency tests and nine existing wire metadata tests. The four new concurrency tests iterate both storage modes. |
| `895c5036` | **23 passed, 4 failed**. All 14 journal tests passed, including the new complete-inventory and foreign-owner control in both stores. Nine existing wire tests passed; four real SQL concurrency tests failed because inbound-FK discovery observed the whole schema catalog. |
| `4885ceb8` | **55 passed, 5 failed**. Targeted FK discovery admitted real concurrent SQL commit, full rollback and autocommit. Failures exposed missing procedure finalization, missing wire savepoint owner transfer, and an embedding fixture that attempted identity migration after admitting owners. The Repeatable Read and Serializable matrices both stopped at their savepoint scenario. All 25 Core scope controls passed. |
| `1771cf4d` | **10 passed**, production source identical to `4885ceb8`: cold/warm UPDATE and DELETE controls admit unrelated writes, refuse reserved FK descriptors and retain the original `23503` denial after release; six existing reader controls also passed. |
| `7e546cdb` | **70 passed, 11 failed**. All six native/CALL/compiled-wrapper tests and 15 wire tests passed, including RC/RR/Serializable decision paths in both stores. Reader, FK-cache, journal and unrelated-writer/reopen controls passed. Eleven related integration cases stopped on missing trusted identity initialization before an explicit transaction; those failures are retained. |
| `79531c74` | Production source identical to `7e546cdb`; startup-only fixture changes: **all 24 related integration tests passed**. These cover schema/name reuse, nested parent/child renames, pending/read-locked composites, routine types/defaults, views, triggers, foreign keys and ACL preservation. |

The compiled-wrapper control executes real SQL and preserves an unrelated native ACK through the existing integer prototype's host wrapper. It does not prove live adaptive selection or promotion of typed routines.

## Trusted startup

An embedding host must perform catalog recovery and call `initialize_rename_journal_identities` while startup is quiescent, before admitting SQL or native application transactions. Existing PostgreSQL and Oracle profile initialization already calls this trusted initializer. It cannot be invoked through SQL or a GUC, and it refuses unresolved SQL intents or an incomplete migration with live owners. Missing/nil image identities still refuse recovery.

The follow-up embedding fixtures add this startup call before first database use. `embedding-startup-identity.json` records that removing only the 19 added calls restores every original byte in those seven fixtures, including all SQL, assertions, security denials and reopen checks. All 24 tests then passed on `79531c74`. This changes setup to follow the new initialization contract; it does not establish that a live legacy database can be upgraded without quiescence.

## Remaining gates

These component results do not complete all catalog readers or mutators, the remaining nested-DDL coverage, broad Core/SQL/wire regressions, original MTable/application acceptance, fresh crash/VM tests, live UI backup/restore or final combined-source delivery. Source471 application and crash evidence remains historical evidence for source471.

A donor's local UUID cannot authorize target-local replay. Authenticated logical-origin/backing-generation to actual-target binding, established in the Native creation/import durability domain, remains unimplemented. Replicated undecided-DDL promotion is therefore unqualified. The existing global recovery gate remains for startup or legacy journals without a retained runtime scope.

`manifest.json` hashes each specification, receipt, summary and complete compressed log. The verifier checks integrity and execution counts; it does not certify the implementation's correctness or replace any missing acceptance gate.
