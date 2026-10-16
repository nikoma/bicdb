# Cluster growth and failure measurements

`scale-cluster.py` uses the public CLI and the immutable cluster-event data path.
It bootstraps RF=3 with fsync enabled, joins nodes while append/read traffic is
running, waits for placement, measures steady traffic at 3, 5 and 9 nodes,
kills one of **its own** server processes, restarts it, and rechecks every
acknowledged write from every phase after recovery.

Build a binary first:

```sh
cargo build --release --locked -p bicdb-cli
```

## Existing separate machines

The script does not provision cloud resources. Supply nine existing Linux
machines reachable through preconfigured, host-key-verified SSH aliases. Each
needs Python 3, compatible system libraries for the supplied binary, free local
storage and TCP connectivity to the other nodes' data RPC addresses. The
controller needs Python 3, OpenSSL, SSH and SCP. No root login is required.

```json
{
  "format": 1,
  "hosts": [
    { "ssh": "bench-1", "address": "10.0.0.11:9444", "directory": "/tmp" },
    { "ssh": "bench-2", "address": "10.0.0.12:9444", "directory": "/tmp" },
    { "ssh": "bench-3", "address": "10.0.0.13:9444", "directory": "/tmp" },
    { "ssh": "bench-4", "address": "10.0.0.14:9444", "directory": "/tmp" },
    { "ssh": "bench-5", "address": "10.0.0.15:9444", "directory": "/tmp" },
    { "ssh": "bench-6", "address": "10.0.0.16:9444", "directory": "/tmp" },
    { "ssh": "bench-7", "address": "10.0.0.17:9444", "directory": "/tmp" },
    { "ssh": "bench-8", "address": "10.0.0.18:9444", "directory": "/tmp" },
    { "ssh": "bench-9", "address": "10.0.0.19:9444", "directory": "/tmp" }
  ]
}
```

These are example addresses. Run:

```sh
python3 scripts/platform/scale-cluster.py --inventory /path/to/hosts.json \
  --binary target/release/bicdb --build-label release \
  --source-commit "$(git rev-parse HEAD)" \
  --output /path/to/new-results --sizes 3,5,9 --ranges 64 \
  --seconds 120 --concurrency 32 --max-operations 1000000
```

The runner checks distinct machine IDs, creates a unique mode-0700 directory
on each host, transfers the exact binary and verifies its SHA-256, and creates
short-lived, per-node mTLS certificates. It uses existing membership admission;
no topology files are copied between live nodes. A successful join is followed
by promotion, bounded range movement and leader recovery. The declared
capacity is 1 TiB per node; this short run does not measure storage capacity.
Adjust the workload duration upward if growth outlasts the traffic window.

`--sizes 3` or `--sizes 3,5` permits a smaller experiment. A useful qualification
run uses a release build, repeated trials, substantially more data than this
default smoke workload, and machine/storage/network envelopes appropriate to
the intended deployment. This tool does not certify a multi-terabyte profile,
network partition tolerance, power-loss durability or independent-user adoption.
Distinct machine IDs identify different OS instances; they do not establish
distinct physical hosts, racks or availability zones. Record those separately.

## Shared-host functional simulation

```sh
python3 scripts/platform/scale-cluster.py --local --binary target/debug/bicdb \
  --build-label debug --output /tmp/new-local-results --sizes 3,5,9 \
  --ranges 16 --seconds 60 --concurrency 4
```

This always reports `shared_host_functional_simulation`. It uses loopback-only
plaintext RPC and separate processes/directories. It cannot establish
horizontal throughput scaling or independent failure domains. Never compare
these numbers to independent-machine database benchmarks.

The output directory must not exist. The script preserves its database and log
directories for diagnosis and stops only PIDs whose start time and exact data
directory match this run. It does not stop existing database services. Remove
the reported `bicdb-scale-RUN-nN` directories manually when no longer needed.
Use an immutable copy of the binary while running; do not relink its inode.

## Reading the evidence

`report.json` records classification, binary digest, the operator-supplied build
label and source commit, machine/kernel/memory information, exact workload parameters, throughput,
successful-operation p50/p95/p99/max latency, failed-operation counts and error
samples. Resource artifacts contain `/proc` CPU, RSS and physical I/O counters;
the report summarizes per-node peak RSS, sampled CPU percentage and I/O deltas.
Resource CPU percentage can exceed 100% for a multi-core process.
`--rpc-timeout-ms` sets the measured client's per-call I/O timeout (100–5000 ms,
default 1000). The emitted report records it. A heavily loaded shared-host debug
run may need 5000 ms; keep failed operations visible and do not interpret that
functional simulation as an isolated latency benchmark.

The traffic is roughly 50/50 append/read of a 1 KiB payload plus a JSON run
envelope; `encoded_value_bytes` records the exact encoded value size. Every worker
first obtains a successful append, so failures can shift the mix. A logical
operation can include route refresh/retry time. Percentiles cover successful
operations; failed counts must be shown alongside them. The operation cap can
end a phase before its requested time. Verification runs outside the measured
traffic interval and checks payloads, not just record counts. Saved ledgers
allow later rechecks with `bicdb cluster events verify ROOT LEDGER`.

Each growth phase has raw resource samples and topology snapshots. Check the
timestamps and duration to confirm that the traffic overlaps the relevant
membership/relocation transitions. The failure phase models one process crash;
the default 60-second window leaves the node down for about 20 seconds. Short
windows may restart it before dead-node detection and cannot demonstrate
automatic leader replacement. A recovered steady phase follows placement
convergence, and final verification rechecks all earlier acknowledged keys.
`--failure-seconds 60` keeps a longer failure window when using shorter
functional traffic windows for the other phases.

Raw topology/log artifacts can contain host addresses and paths. Publish a
sanitized report, exact commands, machine and storage specifications, error
counts and unmodified measurements. A local run or an unexecuted harness is
not a published independent-machine result.

Harness validation:

```sh
python3 -m unittest discover -s scripts/platform -p 'test_*.py'
node --test examples/platform/notebook/*.test.mjs
```
