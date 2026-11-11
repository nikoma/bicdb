# Completed isolated Azure guest-power-off probe

Runtime **1.0.461-beta**, source `b05385d4b033edf0db03f3eccf4bdd86f7c8eddb`,
optimized binary SHA-256
`15ad8ad644c92fd9248020c19b1fad9d4f62e6dceef8d4bc43f1415a8ff4e028`.
This is a completed **single guest-power-off exploratory test**, not physical-host
power loss, capacity certification or production qualification. Longer traffic
trials and infrastructure teardown/billing remain separate artifacts.

## Fault, strict bounds and fresh writes

The controller selected range 21, whose leader was n4 and whose other RF3 voters
were n0/n1. The Azure helper requested `az vm stop --skip-shutdown`, observed
`PowerState/stopped`, then waited before the first start request. The strict
classifier uses actual final request boundaries, not the provisional recovery
marker or the later response from `az vm start`:

- Off request started: `1794374701206142566` ns UTC epoch.
- Guest-off confirmed: `1794374709319528766` ns.
- First guest-start request: `1794374859373869881` ns.
- Five calls started after confirmed-off and finished before that first start.
  First ACK: **97.494809666 s after confirmed off**, **105.608195866 s after the
  off request**. Fifth ACK: **148.556992324 s after confirmed off**.

All five are **fresh inserted writes**, not acknowledgments of already-present
idempotent values: sequences 20–24 map to unique fixture keys `probe-1361`,
`probe-1383`, `probe-1460`, `probe-1526`, `probe-1558`. Each has exactly one attempt
in the entire targeted ledger, that attempt is after confirmed off, its ACK is
before the first start request, and its response records `inserted: true`.
`fresh-off-window-writes.json` and the retained ledger support these checks.
The frozen v2 helper enumerates distinct keys and requests zero wrapper retries.
This controlled namespace does not make a claim about arbitrary outside writers.

Successful off-window calls cost approximately 11.55–12.11 seconds including
fresh-process/SSH transport, compared with roughly 1.9 seconds at baseline.
Three calls reached uncertain outcomes, including 30-second controller timeouts.
Those are not counted as fast successful operations or automatically classified
as rolled back. A later point-read observation found one whole record present
and two absent. Event reads are not a quorum-certified global snapshot; the
absence observations do not independently prove durable abort decisions.

**97.49 s is an observed first-response delay, not a certified universal RTO.**
It includes call scheduling, polling/provider confirmation, SSH startup, route
refresh and protocol work. Service capability may precede the first successful
observation. No physical host/device or entire storage failure domain was lost.

## Completed acknowledgment reconciliation

After the guest reboot, database open/repair and cluster reconvergence:

| Independently retained ledger group | Checked ACKs | Result |
|---|---:|---|
| Targeted range probe | 38 | Every expected payload verified |
| Earlier external controller ledger | 107 | Every expected payload verified |
| Earlier native workload ledgers | 1,281 | All five phase ledgers verified, zero unverified |
| Aborted v1 preflight attempt | 0 | No acknowledgment ledger or fault; not counted as a successful trial |

The targeted ledger contains 41 attempts, 38 ACKs and three uncertain entries.
The report records `complete: true`, `verification_passed: true` and remains
`qualification_status: not_qualified`. All earlier native ACKs were rechecked
here, unlike the earlier initial-smoke whole-process restart, which rechecked
only its external ledger. These are cluster-level preservation results. Replica
repair after the victim rejoined may contribute; they do not prove that the
victim alone retained every ACK before repair.

## Cold archive and boot identity

Before starting BicDB on the rebooted n4, the helper exported a database-file
archive, **1,363,864 bytes**, SHA-256
`45fd44506366d1302bec01d236221f306c7c6ca6e3cf099109bb7905a5d1f233`.
The controller copy had the same hash. This is
**after filesystem journal replay, before BicDB engine open; not a raw disk**.
No offline key-level reconciliation of this cold archive was executed. The
archive is retained privately because it contains test database/key material;
only its metadata is published. Later post-engine diagnostic exports from all
five nodes are separate archives and receipts, not substitutes for that cold cut.

Guest boot IDs before and after differ. `probe-report.json` publishes SHA-256
of the exact observed boot-ID strings rather than the raw identifiers. That
supports guest reboot identity in this controlled test, not independent physical
host placement, storage power-loss safety or a hypervisor attestation.

## Survivor and membership observations: timing matters

The four metadata-status snapshots classified all five members dead and all 64
ranges unavailable at the same committed generation 13094, commit 2096, term 28,
leader n2. Their metric timestamps and controller receipt-file mtimes fall between
confirmed off and first start. These stale/expired derived liveness views coexist
with later strict off-window ACKs; they are not proof that every survivor OS died.
Guest clock skew was not independently calibrated.

Files originally named `*-runtime-while-off` were actually captured **after the
first Azure start request**. The exports label them post-start-request continuity
snapshots and retain exact times. Between those earlier/later samples all four
owned BicDB processes had the same PID/start ticks and increasing CPU ticks.
Later captures showed port 9444 listeners matching those PIDs and no matching
kernel OOM log lines. This excludes a restart between those samples, not a
transient process/listener stall during the strict off interval. It is not a
continuous off-window OS/service monitor. See the separate
[diagnostic hypotheses](../../distributed-vm-fault-followup-2026-11-11.md): no
root cause or new engine repair is established here.

## Attempt and helper provenance

The first helper attempt was aborted before fault injection to correct its time
bound/restart fallback. Its original report, abort reason and exact source are
retained; there were no ACKs, ledger or ready-to-power-off marker. It is not a
passing fault trial. The v2 helper, event client/harness and infra helper hashes
were recorded before execution. Published exact v2/infra helper bytes match those
launch records. This is controller-recorded provenance, not a signed build or
hypervisor attestation. Helpers contain task-local private-inventory paths and
must not be run against another experiment's paths or resources.

`provenance.json` separates raw-original SHA-256 from each allowlisted normalized
or projected export SHA-256. Network endpoints, certificate identities, account
identities, credentials and raw boot IDs are omitted. Generated fixture IDs,
payloads, timing, error/outcome observations and numeric measurements are retained.
The targeted JSON-lines ledger is exported as a gzip JSON array. Raw database
archives, cloud inventory and keys remain private. A raw-original hash does not
authenticate rewritten sanitized bytes; the separate export digest identifies
those bytes relative to this publication.

Run `python3 docs/evidence/azure-vm-poweroff-2026-11-11/verify.py` to check export
hashes, fresh off-window classification and recorded reconciliation counts. It
does not rerun live reads, inspect the private cold archive's keys or establish
production qualification. This single test does not cover relocation-phase
faults, all partitions, repeated resets, arbitrary SQL, FTS/queue/audit recovery,
physical throughput scaling or generally bounded recovery time.
