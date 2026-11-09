# Unchanged iDempiere application controls

These runs establish specific functional outcomes and identify remaining performance work. They do not qualify all iDempiere tests or establish Oracle equivalence.

BicDb source: `fe9b678a46fd477f0c3877878921c85bcde9322b`. Both hosts used the identical standard release binary, SHA256 `977fec856d2d081efeae4c22b5cbd8c211f8ef2597e620773fe00b03be32ad7f`, with experimental features/gates off, `server_paged`, a 256MiB buffer pool, and the established `BICDB_SYNC_OUTBOX=off` setting. This source predates the subsequent comparison, namespace and durability integrations.

Each BicDb run used a fresh independently hash-verified clone of the same protected Oracle-profile baseline: 2,109 files, 1,018,230,665 bytes; manifest SHA256 `93d20eac5fbfd3875206c4a3848ea3be359478db18f7ddbf0bad3c6760b6dcfa`. The same existing public BLOB reconciliation ran before the JVM on both hosts. Protected baselines were never opened for test mutations.

The JVM remained on ny8. s3 used an SSH stdio bridge with `TCP_NODELAY` explicitly enabled on both accepted-client and outbound-server sockets. Measured warmed `SELECT 1 FROM DUAL` median roundtrip was 0.086ms locally and 0.934ms through that bridge. Server CPU and residual transport differences remain. Initial s3 repeats with the earlier proxy lacked accepted-socket `TCP_NODELAY`; their preserved results are transport-confounded and excluded here.

| Corrected run | ReportTest | AuditTraceContextTest | Only failure |
|---|---:|---:|---|
| ny8-r3 | 4/5 | 3/3 | ReportTest.testBackgroundJob |
| s3-r3 | 5/5 | 3/3 | none |
| s3-r4 | 4/5 | 3/3 | ReportTest.testBackgroundJob |
| ny8-r4 | 4/5 | 3/3 | ReportTest.testBackgroundJob |

All four runs had zero errors and skips; owned servers/JVMs stopped, with remote shutdown independently verified. Faster storage/host improved several report timings, but completion within the original job-wait assertion was not repeatable. Engine traces show roughly 22.7s total COMMIT time in ny8-r3 versus 3.23s in s3-r4; repeated metadata joins and subqueries still consume substantial time. The diagnostic file preserves normalized SQL, counts, totals, maxima, upstream templates and parameter binding definitions. Original runtime inline literals were redacted by the engine and cannot be reconstructed exactly from these logs.

## Native Oracle controls

Two separate physical hot clones of protected `FREEPDB1` ran on Oracle 23.26.3. Each clone independently matched all 915 table counts (185,471 rows), object/status metadata and column metadata before applying the same three missing original upstream Oracle migrations. Protected source metadata/counts were checked again after each application run and were unchanged. Each task-owned clone was closed afterward; its datafiles and evidence remain separate from the protected source.

The native runs used the original upstream Oracle-provider source, independently compiled unchanged, SHA256 `1813c6cd6f4596e8002dacc06ee7dccca0dd5cebe22a1634651d75a8c32d3d0b`. Class-load evidence confirms the real wrapped Oracle JDBC 23.26 driver. The BicDb SDK adapter and `-Dbicdb` flags were absent. Java sources match upstream commit `da0730aec42f3e9f5f671e81a70ff71e24d5e534`; packed test bytecode matches runtime classes and the BicDb runs' test jar SHA256 `261afbb15ba933880ddf8cb9bee9f5fa07f9dc21b205ba424f7036ba6e7679e7`. Assertions, original class selections and the 5400s runner budget were unchanged.

- TrxTest: 17/18 passed. `testReleaseSavepoint` failed with “releaseSavepoint should throw SQLException”.
- CalloutTest: 1/2 passed. `testAnnotatedCallout` failed because `CalloutInfoWindow` was not found. These are the same two assertions previously failing against BicDb and are now vendor-app reproduced.
- ReportTest: 5/5 passed; AuditTraceContextTest: 3/3 passed. Zero errors/skips. The remaining BicDb report timeout is therefore not dismissed as an upstream failure by this evidence.

| Whole ReportTest method | Native Oracle | BicDb ny8 repeats | BicDb s3 repeats |
|---|---:|---:|---:|
| Background job | 2.010s | 11.485–11.714s | 14.199–14.758s |
| Aging report | 4.039s | 36.061–38.387s | 26.364–27.844s |
| Create XML | 0.911s | 9.053–9.887s | 4.616–4.634s |
| Financial report | 1.755s | 15.875–15.908s | 10.578–11.529s |
| PDF filename | 0.369s | 3.349–3.902s | 1.690–2.315s |

These are complete method times including setup, not the internal 10s job-wait interval. The native timing is a product diagnostic rather than a strict engine benchmark: clone qualification counts and migrations warm Oracle's live cache; BicDb starts a fresh server and performs BLOB reconciliation. Cache sizes/configurations and JDBC providers differ. The full timing ratio cannot be attributed solely to the SQL engine or planner.

A first native harness startup accidentally selected an older template provider containing a BicDb transport branch. It ran no application tests, was stopped and retained as a harness failure. All native results above use the verified native-only provider and real driver.
