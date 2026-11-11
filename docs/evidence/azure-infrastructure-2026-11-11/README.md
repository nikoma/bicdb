# Azure exploratory experiment: actual infrastructure allocation

The user authorized up to USD 25 and eight hours for the isolated Azure run.
This evidence records **actual allocation and guest readiness**, not a passing
BicDB workload, a physical-host independence claim or production qualification.
Runtime source prepared by the parent track is recorded separately.

## Allocation and deviations from the priced candidate

Initial subscription provider registration was required. Compute/Network/Storage
are now Registered. Before registration, Compute usage queries returned an empty
array; that did not mean zero quota. After registration, both West Europe and
North Europe showed ten regional vCPUs, zero DSv5-family vCPUs, and ten DSv3-family
vCPUs. The proposed 22-vCPU six-VM candidate could not fit. No quota, subscription
policy or existing resource was changed to overcome that restriction.

West Europe then rejected the actual test VNet with `RequestDisallowedByAzure`:
“The selected region is currently not accepting new customers” and the
`https://aka.ms/locationineligible` reference. North Europe accepted the isolated
network. With parent authorization, the exploratory allocation became:

- Five North Europe `Standard_D2s_v3` VMs, each 2 vCPU/8 GiB, consuming all ten
  regional/family vCPUs. This is smaller than the original 4 vCPU data-node plan.
- One 64 GiB Premium LRS OS disk and one 128 GiB Premium LRS data disk per VM.
  Azure records data cache `None`. Guests mount the dedicated data volume at
  `/srv/bicdb-data`, ext4 `rw,noatime`; it is distinct from the root volume.
- A new private VNet/subnet with default outbound access disabled, no production
  peering, and a single public SSH IP on the first data node. SSH ingress is
  restricted to the external controller IPv4 `/32`; remaining nodes use that
  node as ProxyJump. SQL and data RPC ports have no Internet allowance.
- Existing external orchestration/controller rather than a sixth Azure driver VM.
  Workload/client placement and its resource confounds must be reported by the
  experiment runner. Allocation alone supplies no independent per-ack ledger.

All five Azure VM provision states are `Succeeded`; guest cloud-init, pinned SSH,
Python and dedicated-disk checks pass. Five distinct hashed OS machine IDs are
retained. SSH host keys were task-generated, injected into each guest at creation
and pinned locally, not accepted through first-connection trust. No production
keys/data were copied. `guest-bootstrap-attempt1.json` preserves the first probe:
node 4 returned SSH 255 before its Azure creation completed. The second probe
verified all five; the bootstrap readiness retry is not a database failure trial.

Distinct VM/OS identities do **not** prove distinct physical hosts, racks or
power-loss domains. No availability-set or dedicated-host independence claim is
made. The separate root run must record the exact executable, persisted
`server_paged` mode, pool, fsync, topology and workload outcomes before any data
safety conclusion is drawn.

## Budget and teardown

North Europe Linux on-demand retail API returned USD0.107 per D2s_v3 VM-hour,
USD0.535/hour total compute. The exact public query/result is in
`vm-retail-prices.json`. Premium disk pricing is estimated using the prior
West Europe P6/P10 reference; the attempted North Europe disk quote returned
HTTP429, so this evidence does not fabricate a regional quote or actual bill.
The reduced allocation remains well within the USD 25 planning envelope; final
billing, network/public-IP charges and taxes are not measured by this evidence.

Before VM creation, an independent persistent systemd timer/service was verified
active. It schedules deletion at **2026-11-11 11:37:51 UTC**, fifteen minutes
before the allocation's eight-hour expiry. Unit files live under
`/etc/systemd/system`, with private script/state under `/var/lib`, so a controller
reboot does not simply lose a transient `/run` timer or `/tmp` state. It pins the
subscription and full resource-group identity plus `purpose` and experiment tags;
it cannot delete by broad prefix. Each Azure CLI call has a 120-second timeout;
service timeout is 360 seconds, with unlimited failure restarts every 60 seconds until
the exact group is confirmed absent. Tags alone are not the cleanup mechanism.

Only after externally exporting experiment evidence and confirming resource-group
absence should the parent cancel/uninstall this task's timer/service. Resource
IDs, SSH/IP inventory, custom-data test keys and CLI authentication are private;
this bundle retains a group-identity SHA256 and sanitized facts, not credentials.
The final teardown receipt and actual workload results belong to the parent
experiment evidence. At this infrastructure checkpoint, resources are running.

For resource-provider registration behavior, see Microsoft's
[registration documentation](https://learn.microsoft.com/en-us/azure/azure-resource-manager/management/resource-providers-and-types).
Registration and allocation are setup operations; neither establishes an HA or
horizontal-throughput guarantee.
