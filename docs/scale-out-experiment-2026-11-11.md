# Isolated four-to-five-node scale-out experiment

Prepared against runtime source `c173fa783fb8323fe02c41fb7eda96f130bc29c3`
(**1.0.460-beta**). Status: **runner prepared; physical experiment not executed**.
No VM, disk, network, key or production deployment was created or changed.
This is Track C, independent of distributed SQL design and durability qualification.

## What is ready

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

## Read-only infrastructure preflight

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

## Reviewable Azure resource plan

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

## Cost estimate, not a spending authorization

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
created by this work.

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
