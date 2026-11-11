# PostgreSQL transaction isolation evidence

This batch integrates writable Repeatable Read and PostgreSQL Serializable
dependency tracking with the current native snapshot admission boundary.
The original iDempiere DBTest failures on public source
`b0e2498842be7b01e355bb8da1cc249ff16be056` were `0A000` refusals of the
session isolation settings. These focused checks do not certify the complete
iDempiere application or change its original assertions or 10-second deadlines.

## Source boundaries

| Revision | Scope and observed result |
| --- | --- |
| `c40f74b6b4f7aa853d6588f95bf7a9b4331f78f6` | Earlier private RR/SSI implementation, before integration with current main. |
| `c097e1c5e78f1a39ef176ab80ca5a0eda1c3489e` | Integrated runtime on public 471, preserving native deferred snapshot admission, current routing deadlines and routine changes. All six focused commands passed, 57 test executions. Includes SET and AND CHAIN stale-write controls in both stores. |
| `2d29fb83b8986786f40d1ccfdd435b1c608c13ba` | Tests-only historical-index control on that runtime. Failed with 12 observed mismatches: equality, range and JSONB GIN probes lost old rows in writable/read-only Repeatable Read in both stores. Serializable cases passed. Full failing log retained. |
| `7f7d10d1252bddc533fe61d0b641c6912bb8242f` | Candidate repair: historical visibility and avoidance of current-only plans depend on fixed isolation, separately from SSI graph registration. PostgreSQL historical visibility passed in both stores. The separate Oracle fixture failed during connection startup because it omitted the required profile option; no Oracle historical assertions ran. The next test-only change declares that option. Broad validation was refused by its prerequisite after this failure; it did not run. |

The native PostgreSQL 16.13 controls preserve actual queries, plans and values.
The initial historical-index control passed 28 observations; the expanded
control adds primary-key MIN/MAX and passed 36. Each used its own disposable
schema, removed it afterwards and left application schemas unchanged.
Older native controls cover stale mutations, typed/partial uniqueness arbiters,
concurrent insertion outcomes, dependency cycles versus noncyclic transactions,
read-only deferrable waiting, index history, implicit transactions and COPY.

The recorded source revisions distinguish the earlier focused result from the
later discovered visibility defect. Passing a command parser or an SSI model
does not prove historical visibility for another isolation mode.

## Runtime boundaries

SSI graph validation occurs before distributed commit admission could persist
an irreversible quorum decision. The graph lock remains held through the
durable local decision and is released before publishing audit/broker outboxes.
Tentative edges are discarded on a failed predecision attempt. Existing
durability fencing and unknown-outcome handling remain in force.

PostgreSQL Serializable snapshot acquisition explicitly refuses standbys and
distributed commit authority configurations. This evidence establishes no
distributed SSI, physical multi-host qualification or production HA readiness.
The Oracle profile uses its separate snapshot-isolation contract.

`manifest.json` records the size and SHA-256 of each original input and receipt.
Build logs include the active host/cgroup memory monitor. Tests ran serially
under the shared source/build lock with one Cargo build job and incremental
compilation disabled; useful application runs were preserved.
