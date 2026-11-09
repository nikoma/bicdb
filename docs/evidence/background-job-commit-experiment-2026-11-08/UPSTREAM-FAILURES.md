# The two remaining upstream application failures (draft evidence)

Scope: the official qualification run on the release binary from main `f103a47` (2026-11-07,
`official-main-f103a47-qualification.json`). Upstream tree: `/tmp/bicdb-idempiere-reference-20261101`, iDempiere commit
`da0730aec42f3e9f5f671e81a70ff71e24d5e534`, read only. All paths below are relative to that tree. No pass rate is
claimed here.

---

## 1. `org.idempiere.test.base.TrxTest.testReleaseSavepoint` (batch remaining-09)

**JUnit failure** (run `upstream-diagnostic-pristine-routine-owner-remaining-09-release-i0tp3j6v`,
`surefire/TEST-org.idempiere.test.base.TrxTest.xml`), test time 0.014 s:

```
java.lang.AssertionError: releaseSavepoint should throw SQLException
	at org.idempiere.test.base.TrxTest.testReleaseSavepoint(TrxTest.java:615)
```

**Test** (`org.idempiere.test/src/org/idempiere/test/base/TrxTest.java`):
- 595: `Savepoint sp = realConn.setSavepoint("SP1");` This and the first `releaseSavepoint` call, on the real connection, pass.
- 605-606: `Connection mockConn = mock(Connection.class); doThrow(new SQLException("Simulated releaseSavepoint error")).when(mockConn).releaseSavepoint(any(Savepoint.class));`
- 609: `connField.set(trx, mockConn);` The mock goes into `Trx.m_connection` by reflection.
- 613-615: `trx.releaseSavepoint(spMock); fail("releaseSavepoint should throw SQLException");`

**Product code** (`org.adempiere.base/src/org/compiere/util/Trx.java`):
```
608	public synchronized void releaseSavepoint(Savepoint savepoint) throws SQLException
609	{
610		if (DB.isOracle())
611		{
612			// Note: As of Oracle Database 10g, releaseSavepoint and
613			// oracleReleaseSavepoint are not supported. ...
617			return;
618		}
```
- `DB.isOracle()` (`org.adempiere.base/src/org/compiere/util/DB.java:356-362`) returns `s_cc.isOracle()`.
- `CConnection.isOracle()` (`org.adempiere.base/src/org/compiere/db/CConnection.java:663-666`) is `Database.DB_ORACLE.equals(m_type)`. `Database.DB_ORACLE = "Oracle"` (`org/compiere/db/Database.java:41`).

**Why no database behaviour can change the outcome:**
- `m_type` is the client's configured database type. It is `Oracle` in this setup, which is the purpose of the Oracle profile.
- With that type, line 610 returns before `m_connection` is read, so the Mockito mock's `releaseSavepoint` is never called and nothing can throw.
- The exception the test expects can only come from the mock, an in-JVM object. No SQL statement, JDBC call or server response is involved between lines 609 and 615.
- The only way to pass is a non-Oracle `m_type`, which means not running the Oracle profile at all, or an upstream change to `Trx.releaseSavepoint` or to the test.
- The assertion encodes PostgreSQL behaviour. Ledger item 18 records the same.

**Reproduction:** reproduced 2026-11-08; see "Reproduction" below.

---

## 2. `org.idempiere.test.model.CalloutTest.testAnnotatedCallout` (batch remaining-11)

**JUnit failure** (run `upstream-diagnostic-pristine-routine-owner-remaining-11-release-utrgm1f6`,
`surefire/TEST-org.idempiere.test.model.CalloutTest.xml`), test time 0.006 s:

```
org.opentest4j.AssertionFailedError: org.adempiere.model.CalloutInfoWindow not found for AD_InfoWindow.AD_Table_ID ==> expected: <true> but was: <false>
```

**Test** (`org.idempiere.test/src/org/idempiere/test/model/CalloutTest.java:60-70`):
- `ColumnCalloutManager.findCallout("AD_InfoWindow", "AD_Table_ID")` is called.
- The test counts the results that are `instanceof CalloutInfoWindow` and asserts `found==1` (line 70). The message says "not found", but the assertion also fails when it finds two.

**Where the callouts come from:** `ColumnCalloutManager.findCallout` (`org.adempiere.base/src/org/adempiere/base/ColumnCalloutManager.java:66-`) asks every `IColumnCalloutFactory` OSGi service. None of them reads the database:
- `DefaultColumnCalloutFactory` (`.../base/DefaultColumnCalloutFactory.java:37-45`) uses the Equinox extension registry.
- `MappedColumnCalloutFactory` uses an in-memory map.
- The three annotation factories, all `immediate="true"` DS components (`OSGI-INF/*AnnotationBasedColumnCalloutFactory*.xml`), use a `tableNameMap` filled once, in `activate()`, by a ClassGraph class-file scan:
  - `DefaultAnnotationBasedColumnCalloutFactory`, bundle `org.adempiere.base`, packages `{"org.adempiere.model"}` (`.../base/DefaultAnnotationBasedColumnCalloutFactory.java:40-42`);
  - `AnnotationBasedColumnCalloutFactoryImpl`, bundle `org.adempiere.base.callout`, packages `{"org.adempiere.base.callout","org.adempiere.model"}` (`org.adempiere.base.callout/src/org/adempiere/base/callout/factory/AnnotationBasedColumnCalloutFactoryImpl.java:43-45`);
  - `ColumnCalloutFactoryImpl`, bundle `org.idempiere.tablepartition`.
- The scan itself is in `AnnotationBasedColumnCalloutFactory.activate` (`org.adempiere.base/src/org/adempiere/base/AnnotationBasedColumnCalloutFactory.java:150-211`): `new ClassGraph().overrideClassLoaders(bundleClassLoader)...acceptPackagesNonRecursive(getPackages())`, then `scanAsync(executor, ...)`.

**The race** (ClassGraph 4.8.147, the product's `plugins/io.github.classgraph.classgraph_4.8.147.jar`, sha256 `c585977b...` = Maven Central's; source from `classgraph-4.8.147-sources.jar`, `nonapi/io/github/classgraph/classloaderhandler/EquinoxClassLoaderHandler.java`):
```
49	    private static boolean alreadyReadSystemBundles;
...
205	        if (!alreadyReadSystemBundles) {
    	            ... modulesById.get(0L).getBundle().getBundleContext().getBundles() ...
    	                    classpathOrder.addClasspathEntry(location, classLoader, scanSpec, log);   // every bundle
243	            alreadyReadSystemBundles = true;
```
- The flag is a JVM-wide static and is not volatile. Whichever ClassGraph scan reaches line 205 first, or two scans that both get there before line 243, adds every installed bundle to its own classpath.
- If `AnnotationBasedColumnCalloutFactoryImpl` is one of those scans, it also finds `org.adempiere.model.CalloutInfoWindow`, which lives in bundle `org.adempiere.base` and is annotated `@Callout(tableName="AD_InfoWindow", columnName="AD_Table_ID")` (`org.adempiere.base/src/org/adempiere/model/CalloutInfoWindow.java:40-42`).
- `findCallout` then returns two `CalloutInfoWindow` instances, so `found==2`.

**The startup log shows which scan won.** The factory logs `tableNameMap.size()` (`AnnotationBasedColumnCalloutFactory.java:207`). From the upstream sources:

| Factory | Table names from its own bundle only | Plus every bundle (system bundles read) |
|---|---|---|
| `...Impl` | 10 (9 in `org.adempiere.base.callout` + `C_BPartner` from its own `org.adempiere.model.CalloutBPartnerQuickEntry`) | 12 (+ `AD_InfoWindow`, `AD_InfoColumn`) |
| `DefaultAnnotationBased...` | 2 (`AD_InfoWindow`, `AD_InfoColumn`) | 3 (+ `C_BPartner`) |

Every recorded CalloutTest run in this harness:

| Run | Impl | Default | testAnnotatedCallout |
|---|---|---|---|
| remaining-11-release (official) | 12 | 3 | fail |
| remaining-11-diag10 | 12 | 3 | fail |
| remaining-11-diag9 | 12 | **2** | fail |
| remaining-11-diag8 | 12 | 3 | fail |
| bgjob-a-upstream-pair-r1 (2026-11-08, `decb9d0`, CalloutTest without its batch) | 12 | 3 | fail |

- In all five runs `...Impl` read the system bundles: 12, which predicts `found==2` and the failure.
- `Default` varies between runs, 2 or 3. So more than one scan sometimes enters the unsynchronized block, which is the race.
- A run where `...Impl` logs 10 would pass. None was observed.

**Why no database behaviour can change the outcome:**
- The factories are filled at OSGi component activation by reading class files from bundle jars and directories. No SQL statement, JDBC call or server response takes part in the scan or in `findCallout`.
- Which scan wins is decided by JVM thread scheduling during startup.
- The outcome is fixed before the test method runs: the startup log line already predicts it.
- Fixing it needs an upstream change. Either ClassGraph synchronizes or scopes the system-bundle read, or `AnnotationBasedColumnCalloutFactoryImpl` stops accepting `org.adempiere.model`, or the test tolerates duplicates.

**Caveat:** this harness fails 5 of 5 runs. Whether upstream's own PostgreSQL CI hits the same race (it uses the same ClassGraph jar) was not verified here.

---

## Reproduction (cheap, variant A, disposable database)

`run_bgjob_rep.py a upstream-pair 1` ran the two whole original classes, `TrxTest` (18 tests) and `CalloutTest` (2 tests), in one JVM:
- binary: variant A release `oracle_server` (`decb9d0`, sha256 `34b3b120...`);
- database: a fresh disposable copy of `pristine-paged-current-schema-run.json`, removed after the run;
- upstream bundle, adapter and settings: the same as the official run;
- run record: `runs/bgjob-a-upstream-pair-r1.json`, base `upstream-bgjob-a-upstream-pair-r1-oxj5yzml`.

Result: 20 tests, exactly these 2 failures, with the same messages as the official run:
- `TrxTest.testReleaseSavepoint`, 0.018 s: `releaseSavepoint should throw SQLException`, at `TrxTest.java:615`.
- `CalloutTest.testAnnotatedCallout`, 0.004 s: `org.adempiere.model.CalloutInfoWindow not found for AD_InfoWindow.AD_Table_ID ==> expected: <true> but was: <false>`. The startup log again shows `AnnotationBasedColumnCalloutFactoryImpl loaded 12 classes`.

Both outcomes are independent of batch context and of the BicDB build (`f103a47` for the official run, `decb9d0` here).
