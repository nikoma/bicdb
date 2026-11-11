# Isolated four-to-five-node scale-out experiment

Prepared against runtime source `c173fa783fb8323fe02c41fb7eda96f130bc29c3`
(**1.0.460-beta**). Original status: runner prepared, before infrastructure
authorization. **Subsequently executed on isolated Azure VMs at 1.0.461-beta**;
see the execution update below. The original plan/preflight is retained as such.
This is Track C, independent of distributed SQL design and durability qualification.

## Azure execution update — 2026-11-11

See the [consolidated actual-results report](azure-scale-out-results-2026-11-11.md)
for measured latency, failure, reconciliation and qualification boundaries.

The owner authorized the $25/eight-hour disposable experiment. Subscription
quota and region restrictions reduced allocation to five North Europe
`Standard_D2s_v3` VMs (2 vCPU/8 GiB each), dedicated uncached Premium data disks,
private cluster traffic and one restricted SSH entry. There was no sixth client VM
or verified five-host physical separation. No production service was changed.
The [infrastructure evidence](evidence/azure-infrastructure-2026-11-11/README.md)
records the actual configuration and bounded cleanup guard. Original candidate
prices and unavailable SKUs below are historical, not the deployed configuration.

The exact optimized binary is **1.0.461-beta**, source
`b05385d4b033edf0db03f3eccf4bdd86f7c8eddb`, SHA-256
`15ad8ad644c92fd9248020c19b1fad9d4f62e6dceef8d4bc43f1415a8ff4e028`.
[Build evidence](evidence/azure-release-artifact-2026-11-11/README.md) records
source, flags and bytes. RF3/64 ranges, `server_paged`, 256 MiB pools, fsync and
mTLS were observed. Native client traffic ran on n0; separate controller
per-attempt/per-ACK fsync probes invoked member clients through SSH. This is
exploratory functional/fault evidence, not the independent capacity client in
the stronger plan below. The corpus remained small and cache-resident.

- [Completed short growth/process-fault smoke](evidence/azure-scale-smoke-2026-11-11/README.md):
  node five received real records and leadership automatically. At the first
  recorded converged checkpoint, 38 relocations to n4 reported 537,266 snapshot bytes. All 1,281
  native ACKs and 107 external ACKs were later reconciled under their documented
  recovery scopes. Throughput was lower after expansion; no capacity gain.
- [Abrupt guest-poweroff probe](evidence/azure-vm-poweroff-2026-11-11/README.md):
  five fresh writes completed wholly between confirmed victim stop and the first
  restart request. First such ACK was 97.495 seconds after confirmed stop;
  this is an observed probe recovery interval, not certified service RTO. All
  38 new, 107 external and 1,281 native ACKs were verified after restart.
  Three uncertain calls and stale/all-dead status observations remain preserved.
- [Placement-input correction](distributed-placement-load-inputs-2026-11-11.md):
  automatic replica/leader count balancing is real, but live byte/QPS inputs
  remained zero. Measured byte/hotspot balancing is not established.
- A [separate longer failed trial](evidence/azure-long-growth-failure-2026-11-11/README.md)
  used 16 workers and 900-second target windows
  without extra append probes. Baseline completed: 7,592 successful operations,
  zero failures, 3,804 verified ACK writes over 907.00 seconds. Growth aborted
  after 927.50 seconds: 2,746 successful operations, 404 failures, 1,223 ACKs,
  and two ACKs unverified by the initial read pass. This verifier collapses
  transport/recovery errors, missing values and mismatches; that result alone
  does not prove lost commits. No steady-five, later fault or recovered phase
  ran. All five stopped databases/logs/ledgers were exported with hash parity
  before a bounded recovery/read diagnosis. Both originally unverified keys
  subsequently returned their exact expected values through all five member
  client contexts (10/10 matching reads). These are routed reads, not five
  independent physical copies. Policy convergence still failed its 300-second
  diagnostic bound, with active/failed relocations and degraded liveness.
  The initial failure remains a failed availability/verification trial.

The original qualification protocol below remains **unpassed**: no large fixed
working set, repeated matched read/write/mixed capacity windows, operation-aware
relocation fault matrix, physical-host power-loss claim, unchanged SQL business
application, or FTS/queue/audit qualification was delivered. Existing
`--require-independent-qualification` refusal is retained. No engine feature
was implemented or production admission enabled by these experiments.

## Original runner preparation

`scripts/platform/scale-cluster.py --sizes 4,5` now accepts a four-node baseline
and one-node admission under event traffic. It preserves RF3, fsync, authenticated
remote membership, binary SHA-256 verification, distinct OS machine-ID checks,
convergence checks, owned-PID-only crash/restart and all acknowledged-event
post-recovery rechecks. Existing 3/5/9 profiles remain valid. Reports explicitly
record client/ledger placement, physical-domain verification as false, and a
join/traffic/convergence/fault timeline. These facts prevent a shared-host run,
a successful join or a returned kill request from becoming physical qualification.

The [harness instructions](../scripts/platform/README.md) include the runnable
4→5 invocation. Fifteen Python platform contract tests pass, including the
new profile, malformed/unsafe profile refusal and early refusal of
`--require-independent-qualification`. Reports state `qualification_status=not_qualified`;
that flag refuses before touching a host or binary until the missing external
ledger and operation-aware injector are implemented. No cluster workload was run
merely to produce a nominal success: the available host runs production BicDB
services, had approximately 25 GiB available memory and fully used swap during
preflight, and has no identified isolated test hosts. Sharing that host would
not resolve the independent-machine question.

## Original read-only infrastructure preflight

The [sanitized preflight](evidence/scale-out-preflight-2026-11-11/preflight.json)
retains exact queried SKUs, public price API URLs, returned prices and blockers,
without subscription/account IDs, addresses, access tokens or credentials.

Observed 2026-11-11 02:36 UTC:

- Azure CLI is installed and its active account reports `Enabled`.
- `az vm list` returned an empty list: no existing disposable VMs were identified.
- `Standard_D4s_v5` and `Standard_D4as_v5` are listed in West Europe, but both
  report `NotAvailableForSubscription` restrictions in zones 1 and 3.
- Regional quota queries returned no rows; **quota is not verified**. An empty
  result is not proof of sufficient quota or of a zero quota.
- No machine inventory or current-source immutable experiment binary has been
  supplied to this track. Production binaries must not be repurposed as evidence
  of an unidentified current-source build.

These are concrete execution blockers. The operator must select an available
region/SKU, verify quota and authorize bounded disposable infrastructure before
provisioning. No experiment completion or physical RPO/RTO/throughput is claimed.

## Original reviewable Azure resource plan

Use a new, uniquely named resource group such as
`bicdb-scale-20261111-RUN`, containing only this experiment. Choose the actual
region after quota/SKU checks; West Europe below is a priced candidate, not an
available approved allocation.

| Resource | Quantity | Candidate configuration |
|---|---:|---|
| Data VM | 5 | Linux `Standard_D4s_v5`, 4 vCPU/16 GiB, no public IP; nodes 1–4 start first, node 5 remains empty until join |
| Independent driver/ledger VM | 1 | Linux `Standard_D2s_v5`, 2 vCPU/8 GiB, separate durable ledger volume/OS disk; no database replica |
| Data disks | 5 | 128 GiB Premium SSD LRS (P10), one dedicated disk per data VM; no host cache for measured database writes |
| OS disks | 6 | 64 GiB Premium SSD LRS (P6); driver ledger retained outside data VM failures |
| Network | 1 | New isolated VNet/subnet, no peering/VPN/production route or identity reuse |
| Access | bounded | SSH through the controller from an explicitly allowlisted administration address, private data-node SSH; cluster mTLS port 9444 limited to test subnet; no public SQL/data ports |

An existing isolated management route can replace a controller public IP.
Otherwise include its explicit public-IP and network costs in approval. Use a
prepared Ubuntu image containing Python 3 and required runtime libraries, or
price and configure explicit outbound provisioning connectivity; do not silently
assume private VMs have Internet access. Supply pinned test-only certificates
through the controller; never copy production private keys or grant applications
cluster-management credentials.

Each resource needs `purpose=bicdb-scale`, a unique `experiment_id`, `owner`, and
an absolute UTC `expires_at` no later than eight hours after allocation. **Tags
alone do not delete resources.** Before creating the group, arrange an independent
scheduled cleanup/watchdog that deletes only the exact group bearing the expected
experiment ID, even if the workload process dies. Do not let cleanup select groups
by broad name prefix. Preserve evidence externally before ordinary teardown.

Five OS instances do not establish five independent physical hosts. Azure
[availability sets](https://learn.microsoft.com/en-us/azure/virtual-machines/availability-set-overview)
have at most three fault domains. Record zone and provider placement evidence;
label a VM/reset result as such unless distinct physical hosts are independently
established. Dedicated-host placement would require a separately priced allocation.
Three-zone placement also cannot currently use the West Europe restricted SKUs
above without resolving their restrictions. Never infer a five-host power-loss
claim from five machine IDs or destroy physical cloud hardware for this test.

## Original cost estimate, before spending authorization

Microsoft's [retail pricing API](https://learn.microsoft.com/en-us/rest/api/cost-management/retail-prices/azure-retail-prices)
returned these West Europe USD Linux on-demand rates at preflight:

| Meter | Unit rate | Planned amount |
|---|---:|---:|
| D4s v5 | $0.230/hour | 5 VMs |
| D2s v5 | $0.115/hour | 1 VM |
| P6 LRS disk | $11.2272/month | 6 disks |
| P10 LRS disk | $21.6800/month | 5 disks |

Compute is **$1.265/hour**. Normalizing disk-months to 730 hours gives about
**$0.241/hour** for disks and **$1.506/hour**, or **$12.05 for eight hours**,
combined. Separate mount meters, if applicable, add approximately $0.013/hour
($0.11/eight hours). Network transfer, public IP, outbound connectivity, taxes,
account discounts and dedicated physical-host isolation are excluded. A planning
ceiling such as **$25 and eight hours** should include separately reviewed network
charges and an automatic teardown; it is not a guaranteed bill cap. Re-query
prices and restrictions immediately before allocation. No paid resource was
created during this preflight; the later owner-authorized allocation is recorded
in the execution update above.

## Execution contract and remaining runner work

1. Build and freeze the exact integrated source/version and optimized binary
   hash; record every node's actual hardware, storage cache/fsync behavior,
   schema, topology, certificates and limits. Configure RF3/64 ranges and the
   automatic controller. Do not run manual rebalance or supply application shard maps.
2. Use 256 MiB configured pools per data node, preload a fixed 8 GiB corpus,
   and record both configured-pool and OS-cache effects. Warm up and measure a
   four-node baseline for at least 15 minutes. Check disk, network and client
   saturation so shared bottlenecks cannot masquerade as an engine limit.
3. On the independent driver, record each attempted/acknowledged/uncertain
   operation to an independently durable ledger with stable IDs and payload
   hashes. The existing CLI workload fsyncs its ledger **at phase completion on
   the first data node**, then the harness copies it. That is useful functional
   evidence but is not this stronger per-acknowledgment external ledger contract.
   This driver remains to be implemented before a destructive qualification run.
4. Join/start empty node five during unchanged traffic. Capture data movement,
   durable destination bytes, per-node request load, leader/replica skew, CPU/RSS,
   pressure, disk/network I/O and raw workload samples. Membership alone fails.
   Verify fifth-node traffic and data/leader participation after convergence.
5. Run separate fresh trials for observed partial snapshot, catch-up, promotion
   and cleanup failures; retain before/after operation checkpoints. Use test-node
   process kill/VM reset/network blackhole and stale-writer probes. The existing
   runner's timed kill **after convergence** does not exercise those relocation
   boundaries; an operation-aware injector remains required. Keep one RF3 data
   failure at a time and do not stop production or shared host services.
6. After convergence, repeat unchanged load sweeps and ≥15-minute windows for
   read-heavy, write-heavy and mixed workloads; three independent 4→5 repeats
   include a clean control. Report goodput at equal latency/error budget,
   p50/p95/p99/max, failures and uncertain calls. A ≥10% goodput improvement is
   a proposed meaningful threshold, not a prediction or current result.
7. Reconcile every acknowledged payload/receipt after convergence and repeated
   recovery; uncertain commits are whole present or whole absent. Check fixed
   watermark data/index integrity, stale-owner fencing, RF3, no unfinished
   relocation and bounded skew. Measure fault→detection, leader recovery and full
   repair separately. Preserve failures rather than restarting until one passes.
8. Run unchanged PostgreSQL business transactions and cross-range joins through
   one endpoint separately. Current general distributed SQL/coordinator gaps are
   expected to fail this case. FTS, queues and audits need their own integrity
   cases; event append/read does not certify them.
9. Hash and export raw ledgers, logs, workload samples, fault timelines and
   topology outside the group. Verify that the exact disposable group and all its
   disks/NICs/IPs were removed, cancel its watchdog after confirmed deletion, and
   retain a sanitized resource-deletion receipt. No production resource is touched.

This small trial is independent of the unchanged 1/5/20 TB production gates.
Neither the new 4→5 CLI profile, passing Python tests, a cloud allocation nor a
future passing event workload constitutes physical scale certification.
