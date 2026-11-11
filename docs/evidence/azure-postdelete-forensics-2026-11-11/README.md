# Post-delete Azure infrastructure forensics and gated reproduction preflight

The original five-VM experiment was deleted and its absence verified in commit
`0d857415`; this work does not resurrect those VMs. These additional queries were
performed **after deletion** on 2026-11-11. No new Azure resource was allocated by
this preflight. Original failed workload observations and archives remain intact.

## What could be recovered

The original Azure Activity Log returned 173 events. Raw events include identities,
resource IDs and request bodies, so they remain private. Parsing original
control-plane response bodies recovered creation records for all five VMs, ten
disks and five NICs. Public summaries omit the actual resource IDs and identities;
`private-artifact-digests.json` binds the retained private files by hash/size.

Original disk creation responses explicitly report:

| Original resource | Configured capacity | Reported IOPS | Reported MB/s |
|---|---:|---:|---:|
| Data disk, five instances |128 GiB |500 |100 |
| OS disk, five instances |64 GiB |240 |50 |

The original NIC responses report accelerated networking **false**. The recovered
VM response bodies name `Standard_D2s_v3`; placement fields such as zones,
availability set, dedicated host/group and platform fault domain were not returned
in those responses. Absence is recorded as absent, not invented as a physical-host
identity or independent failure domain. These are original creation-time API
responses recovered later, not time series of effective I/O limits or throttling
under the workload. Runtime changes would require their own evidence.

## What could not be recovered

The deleted-resource Azure Monitor endpoints returned errors for all five VM IDs
and all ten original disk IDs recovered from Activity Log. Both CLI discovery and
direct REST queries were retained; direct calls bypassed CLI resource-location
discovery and requested the original 03:52–06:52 UTC interval with valid basic
metric names. The initial CLI probes used an end time of 06:53 UTC, including
the deletion minute; neither path returned time-series data. `direct-summary.json` preserves sanitized exact
`ResourceGroupNotFound` errors. `summary.json` also retains CLI
`ResourceNotFound` responses. Raw commands/responses and exact IDs remain private.

Consequently original CPU, disk/network bandwidth, IOPS, latency, queue depth,
throttling and burst-credit series are **unavailable through the queried APIs**.
This is not evidence of zero utilization, zero throttling or zero latency, and it
does not prove that every possible Azure historical data store erased its data.
The first run did not retain those provider time series before deletion.

## Published limits queried after deletion

`sku-query-after-deletion.json` is the full SDK SKU response queried after the
original resources were deleted. `published-limits.json` binds public source URLs
and distinguishes supported/published ceilings from measured effective behavior.
The VM SKU advertises 3200 uncached IOPS and 48 MB/s; Microsoft's table also lists
4000 IOPS/200 MB/s temporary VM burst limits. Its expected maximum network bandwidth
is 1000 Mbps. These are guidance/ceilings, not observed throughput guarantees.

Microsoft lists P10 base 500 IOPS/100 MB/s and burst 3500 IOPS/170 MB/s for up to 30 minutes;
P6 base 240 IOPS/50 MB/s has the same published burst ceiling/duration. A fifteen-minute
window can be affected by VM/disk burst credits. Without original consumed-credit
and throttle samples, no causal performance explanation follows from these limits.
Read the [Dsv3 specification](https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/general-purpose/dsv3-series)
and [Premium SSD specification](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-types#premium-ssds).
Valid managed-disk metric names are documented in Microsoft's
[metric reference](https://github.com/MicrosoftDocs/azure-monitor-docs/blob/main/articles/azure-monitor/reference/supported-metrics/microsoft-compute-disks-metrics.md).

## Separate new reproduction gate

Read-only quota preflight reports regional and DSv3-family usage 0 of 10 cores in
North Europe. The SKU has zone 2/3 restrictions, not a regional restriction. A new
unique experiment identity is prepared privately, with no allocation yet.
Microsoft.Insights was registered with root authorization for standard read-only
platform metrics; no paid diagnostic destination, logging workspace or metric
agent was created.

A separate, exact-identity/tag/subscription-pinned persistent local cleanup guard
is active for **11:22:51 UTC**. It leaves time for confirmed deletion before the
original **11:37:51 UTC** deadline; the original eight-hour expiry is still
**11:52:51 UTC**. It does not restart an eight-hour window. The original deletion
receipt remains valid. The shared USD 25 envelope is unchanged. Current retail
planning rates put another 4.5 hours of the same five-VM/disk configuration at about
USD 3.38, or combined original plus new compute/disks at about USD 5.63, before
public IP, network, tax and account-specific adjustments. Actual bills are not
verified; `repro-preflight.json` is a planning estimate, not spending proof.

Provisioning helpers refuse unless root explicitly marks instrumentation GO and
at least 120 minutes remain before the early cleanup trigger (latest allocation GO **09:22:51 UTC**). Per-operation worker
telemetry must be validated before root authorizes allocation. Full private Azure
model/resource/disk/NIC/route/NSG/instance-view capture and available provider metric
definitions/series must then be verified live before starting the workload. Raw
private IDs must be retained and final metrics exported before deletion this time.
If required provider counters are not exposed, record that gap explicitly rather
than silently replacing them with guest counters. No engine fix is part of this
infrastructure task. A new allocation or a passing instrumentation check would
still not qualify production HA or distributed SQL.
