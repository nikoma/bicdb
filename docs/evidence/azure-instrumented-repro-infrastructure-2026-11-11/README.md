# Instrumented reproduction: isolated Azure infrastructure

The second Azure run uses five new North Europe `Standard_D2s_v3` VMs
(2 vCPU/8 GiB each), pinned Ubuntu 24.04 image `24.04.202610250`, a new uniquely
tagged resource group and fresh dedicated 128 GiB Premium data disks. The original
group remains deleted. The original USD 25/eight-hour budget and expiry remain
in force. No availability zone, availability set or dedicated host placement was
requested. Physical host/storage-device independence is not established.

Operator GO required reviewed instrumentation and frozen baseline 1.0.463-beta
(`d49a391b`). Engine SHA-256 is
`840f0124f27a1a7695c4b2b0c72b05a262456802af65e063cff12242559e2f7d`;
worker SHA-256 is
`b8cc2607220e32f705a23fd6b7d4f41c22298cd1b0ec92cc8e83847783ef8d2c`.
See the adjacent current-main 463 build evidence. No engine repair was applied
to this baseline artifact.

All five VMs passed pinned-host-key SSH, completed initialization, unique guest
identity, dedicated data mount, and reversible data-file fsync/readback plus
directory-fsync health checks. Data filesystems are ext4 with `noatime`, distinct
from OS filesystems; data disk caching is `None`. These health checks establish
usable test resources, not survival of storage power loss.

The first initialization check failed on n1: cloud-init returned exit 2,
`degraded done`, with no fatal errors and a recoverable IMDS `reprovisiondata`
HTTP 404 warning at 08:49:59.876 UTC, before BicDB execution. Initial failure,
raw status, logs and journal were preserved. Acceptance explicitly requires done
status, no fatal/stage errors, WARNING-only recoverable entries, and only this
classified IMDS 404 warning. It does not ignore arbitrary exit 2. Mount and
fsync/readback checks passed on n1. The historical response body was not
established from warning output. This infrastructure warning is separate from
BicDB logical/runtime observations.

Private ARM captures include all resource models, all five VM models and instance
views, image version, disks, interfaces, NSGs, effective NSGs/routes, public IP,
VNet and regional usage. Published SKU limits and actual configured disk limits
remain distinct from observed performance. Missing placement fields do not prove
physical independence. Subscription/resource identities, IPs, keys and raw API
or cloud-init output remain private; their digests are retained here.

Standard Azure Monitor VM metrics are queried read-only at one-minute resolution:
CPU/network, data/OS disk IOPS, bandwidth, latency, queue depth, consumed limits
and burst-credit counters. Initial requests included transient resource-mapping
`NotFound` errors and empty series; neither means zero. Later samples and final
export must be evaluated with their request/result times. No Log Analytics,
paid diagnostic setting or guest metric agent was installed.

One public IP exposes SSH on the jump/data node, restricted to the controller's
admin IPv4 /32. Data/SQL endpoints are private. Implicit subnet outbound access
is disabled. Independent persistent cleanup pins subscription, full new group ID
and purpose/experiment tags, retries failures, and triggers at 11:22:51 UTC.
Application stop is 10:55 UTC, preserving export/deletion time. This snapshot
records allocation and health, not workload success, scale qualification or
teardown. Workload and deletion evidence follow separately.
