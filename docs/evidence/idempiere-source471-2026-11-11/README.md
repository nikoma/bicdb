# iDempiere application and UI evidence for source 471

These results belong to **1.0.471-beta**, public source `b0e2498842be7b01e355bb8da1cc249ff16be056`. The UI used the standard built CLI with SHA-256 `e23d7c1a078b7ce6722237f35cfa67a822727d9762c928c808f3ce2767c543c2`; the original full-suite collectors separately identify their exact server binary. This evidence is historical relative to the PR151 source that carries it. It does not qualify unrun application, HA, VM, or deployment gates on a later version.

## Complete original application suites

Both profiles completed all **1,464 original invocations, 155 classes and 17 batches**, with no skipped tests. Upstream source, bytecode, assertions and original 10-second waits were unchanged. PostgreSQL used its original PostgreSQL provider; Oracle-profile execution used the explicitly selected adapter/profile. Native Oracle controls used the original Oracle provider and actual Oracle JDBC without the adapter.

| Profile | Passed | Assertion failures | Errors | Comparison |
| --- | ---: | ---: | ---: | --- |
| Oracle profile | 1,462 | 2 | 0 | Both exact failing assertions reproduce on native Oracle: savepoint release and annotated callout. No additional original-suite failures. |
| PostgreSQL profile | 1,459 | 2 | 3 | Annotated callout matches the native PostgreSQL control. Three DB connection/isolation errors and the partition-related MTable assertion remain additional failures on this source. |

The original Report/Audit background-job and core callout-mapping methods passed in both profiles. A method's total duration includes setup/teardown and is not evidence that its entire runtime was below ten seconds; the original internal future-wait assertion itself passed.

The PostgreSQL comparison's aggregate runtime-health flag remains false because its DBTest batch failed the connection/provider verification when the three isolation connections could not be created. Its clean process shutdown and original provider-origin observations are preserved separately. Neither profile is relabeled as strict all-pass or complete application qualification. Later transaction-focused tests are not substituted for fresh application acceptance of a changed binary.

## Browser workflow and native first-use controls

The actual browser completed Sales Order **50000**, generated Invoice **100001**, completed Payment **400001**, allocated payment/discount, and displayed the order report. A fresh login retrieved the completed order. Separate read-only SQL connections verified completed/processed states, paid invoice, allocated payment, and balanced posting in accounting schemas **101** and **200000**. The rendered report screenshot shows the source marker, Patio Chair quantity one and **33.75 USD** total. An unauthenticated PDF-resource request returned HTTP 410; PDF download is not claimed.

The original four-session workload retained two distinct failures:

* Simultaneous first-use GardenUser login hit `AD_PREFERENCE_ATTRIBUTE`. The unchanged native Oracle UI reproduced that failure for the same user and preference key, with three ready sessions and one failed login.
* A later first-use lookup hit `AD_WLISTBOX_CUSTOM_NAME_IDX`, blocking the expected Hartford partner-location input. The same four-worker actions and assertions reproduced the blocking popup on native Oracle. The BicDB run completed **300 acknowledged saves** in three sessions; all 300 independently matched stored rows. The failed fourth session remains a failure. The native control completed 200 saves while two sessions reproduced the first-use customization failure.

Native controls used the existing disposable `BICDBAPPCTRL` clone on actual **Oracle 23.26.3.0.0**. Original UI, role-panel, preference-model and query bytecode matched the BicDB fixture. The native original Oracle driver was observed loading; the SDK adapter was absent. Protected `FREEPDB1` table counts, objects and columns matched before/after. Owned JVMs stopped and the clone returned to its original mounted state. Native JDBC vendor code/SQLState differ from the adapter's underlying PostgreSQL transport, and JDK patches/kernel versions differ; complete Oracle JDBC identity is not claimed.

A subsequent run observed a field's current value disagreeing with its markup while starting a new record. The next workload added waits for the installed ZK `zAu.processing()`/`zk.processing` queues to become idle. No timing trace was collected to prove which response produced the earlier field state. The recorded worker AST matches the original business actions and assertions after removing only these additional frontend-idle waits. No assertion was relaxed, no failed dialog was dismissed to obtain a pass, and no application/JDBC code was patched for this measurement.

## Four active sessions and actual process crash

With the frontend synchronization, all four sessions were active under user IDs **101/102**. At the cut request every worker had at least 20 durable client ACKs and one save was pending. Clients continued issuing work until the remote helper confirmed the actual signal; the workload was not quiesced in advance.

The helper verified the actual loaded executable digest, private database path, service cgroup, process birth, all-logs durability, disabled outbox and disabled experimental page flags. It sent **SIGKILL through a pidfd** to that owned database process. The original database exit was **-9**. This is process-crash evidence, not physical power-loss evidence.

The closed SQLite client ledger contains **84 attempts and 83 acknowledged saves**: 20/21/21/21 across the four workers. Separate recovery servers reopened the same database twice and exited cleanly. Independent reconciliation of their actual rows found:

* All **83 ACKed orders**, with no missing, duplicate or mismatched marker, document number, user identity or business value.
* No committed row for the one unacknowledged attempt in these captures.
* Identical rowsets across both reopens, with the earlier posted invoice and allocated payment intact.
* Rejection of five mutated-evidence controls: a missing ACKed row, changed document number, changed user, duplicate row and row outside the attempted-operation ledger.

Expected client failures after the injected crash remain in the original worker result. This establishes the stated four-session process-crash scenario on source 471. It does not claim a complete 400-save uninterrupted workload, a production HA qualification, a current-source acceptance pass, a current live-backup/restore pass, or campaign completion.

## Artifact custody

`manifest.json` lists all 42 input artifacts and their hashes; compressed files retain both raw and compressed hashes. Full original application audits, actual worker outcomes, native failure bodies, protected-source snapshots, bytecode comparisons, the closed client ACK database, raw two-reopen rows, cut/coordinator scripts and independent verifier are retained. Vendor JDBC jars and private connection properties are not included. These artifacts are evidence to inspect, not authority to enable an experimental feature or bypass a completion gate.
