# Cluster event growth and recovery: functional evidence

On 2026-10-16, source commit
`69866e48c02f28affe2e763da8bec5f756384dea` completed a 3→5→9-process
growth, crash and recovery run. All **353 acknowledged event writes** from
all seven traffic phases verified after recovery. Placement converged after
each growth step and after the crashed process returned. Every admitted node
owned a range leader, with no unavailable, under-replicated or failed-relocation
ranges at those checkpoints.

This is a **shared-host functional simulation**, using an unoptimized debug
binary. It does not demonstrate horizontal throughput scaling, independent
failure domains, production latency or separate-machine certification. In
particular, the measured steady throughput did not improve with node count.
The recovered phase had a write p99 of about 14.7 seconds. These numbers are
development evidence, not a performance claim.

## Setup and reproduction

One Linux host supplied all server and client processes: AMD Ryzen 9 7900X
(12 cores, 24 threads), 65,424,196 KiB total memory, kernel
`6.8.0-139-generic`, ext4 on two Samsung SSD 980 PRO 2TB devices in Linux
RAID1. Resources and storage were shared with the development workspace.
Heavy builds and VM acceptance tests were stopped during the measured run.

The workload used RF=3, fsync enabled, 16 initial ranges, four client workers,
and 1,024-byte payloads (1,079-byte encoded JSON values). Loopback plaintext
RPC is allowed only for this local simulation. The per-call RPC deadline was
five seconds, and logical operations could include discovery and retries.
The harness declares 1 TiB node capacity; this small run does not measure it.

Build the exact source and keep the binary immutable during execution:

```sh
git checkout 69866e48c02f28affe2e763da8bec5f756384dea
cargo build --locked -p bicdb-cli -p bicdb-app-host
python3 scripts/platform/scale-cluster.py --local --sizes 3,5,9 \
  --ranges 16 --seconds 15 --failure-seconds 60 --concurrency 4 \
  --rpc-timeout-ms 5000 --max-operations 5000 --timeout 300 \
  --binary target/debug/bicdb --build-label debug \
  --source-commit 69866e48c02f28affe2e763da8bec5f756384dea \
  --output /tmp/new-cluster-functional-results
```

The measured binary SHA-256 was
`c33355e1c790b14a16a30a7dda3305092d2c0fd53a9f8e2e285ed75fb7c442b4`.
Build paths and toolchain details can change a rebuilt debug binary's digest.
The [sanitized measurement report](reports/cluster-events-functional-2026-10-16.json)
contains the original report digest, unchanged phase measurements, error
samples, per-node CPU/RSS/I/O summaries and placement checkpoints. Only machine
identifiers and random workload namespaces were removed.

## Measurements

Latency percentiles cover successful logical operations. Failed operations
remain separate; none are counted as fast successful requests. Traffic durations
include completion of in-flight operations and can exceed the requested window.
Verification runs outside those durations.

| Phase | Seconds | Successful operations | Failed operations | Acknowledged writes | Successful ops/s | Write p99 ms | Read p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| steady-3 | 15.81 | 101 | 0 | 53 | 6.39 | 1740.10 | 100.72 |
| growth-3-to-5 | 16.22 | 79 | 0 | 42 | 4.87 | 4104.06 | 313.93 |
| steady-5 | 17.16 | 41 | 0 | 23 | 2.39 | 6001.50 | 75.86 |
| growth-5-to-9 | 15.87 | 81 | 0 | 43 | 5.10 | 3535.71 | 612.66 |
| steady-9 | 15.69 | 53 | 0 | 29 | 3.38 | 6870.83 | 98.64 |
| failure-9 | 64.04 | 300 | 3 | 151 | 4.68 | 7717.49 | 177.02 |
| recovered-9 | 22.36 | 19 | 0 | 12 | 0.85 | 14703.77 | 10.20 |

The harness killed its ninth process with SIGKILL and restarted it about
20 seconds later. Three client operations reported connection refusal during
the failure window. Every acknowledged write in that phase verified, as did
every earlier and later phase's writes. This does not establish zero downtime
or prove the outcome of every failed operation.

Earlier development runs failed to converge or encountered client errors.
They exposed empty-schema snapshot export, controller catch-up authorization,
database/service lock inversion, metadata heartbeat delays, in-flight movement
accounting, immutable retry visibility and returning-node placement defects.
The implementation includes fixes and regression tests; this completed run
does not turn those earlier failures into successful measurements.

## Application evidence and remaining qualification

The [Field Notebook](../examples/platform/notebook/README.md) also passed a
real Chromium check: save while offline, reload from its service worker cache,
reconnect and upload once, then observe a second browser's note. Queue tests
exercise a lost acknowledgement and reconstruction of the client. Application
host acceptance tests cover namespace isolation and the binding through the
remote-worker protocol to a local Firecracker VM. Existing jailed and
remote-worker VM recovery acceptance tests passed separately from this cluster
workload; these did not use independent machines.

The [SSH harness](../scripts/platform/README.md) supports existing independent
machines, verifies distinct OS machine IDs, pins mutual TLS and records exact
binary digests. Separate-machine release-build trials, realistic data volumes,
repeated runs, partitions, power-loss testing and independent production usage
remain unmeasured. This workload exercises immutable application events;
arbitrary distributed SQL and atomic transactions spanning `env.DB` and
`env.EVENTS` remain outside its [transaction contract](application-cluster-events.md).
