# MicroVM application execution

The application platform can run Node application code in a jailed Firecracker
microVM. Set `"isolation": "microvm"` in the application manifest and provision
an operator-owned host allocation. The default `process` tier continues to run
trusted applications without requiring virtualization.

The guest has a Linux kernel, Node runtime, read-only root filesystem and
read-only application package. The application runs as guest UID 1000. Its
writable `/tmp` is ephemeral. A dedicated host UID/GID owns each jailed VMM.
Guest memory and virtual CPU counts bound the execution environment.

The database and broker remain trusted host services. The guest accesses only
its application's JSON API through a private listener and an execution-specific
capability. The supervisor rotates that capability when stopping or replacing
an execution and rechecks it after database admission. SQL batches carry a
cooperative execution deadline and use bounded query work and temporary space.
The host database service is part of this tier's trusted computing base.

## Host requirements

Use a Linux host with working KVM, TUN/TAP, IPv4 and IPv6 packet filtering,
`ip`, `iptables`, `ip6tables`, and `mkfs.ext4`. The microVM supervisor currently
runs as root to prepare networking and invoke jailer. Management credentials
therefore remain host-operator credentials; they are not delegated application
developer credentials.

Install a matching Firecracker/jailer release and a compatible guest kernel.
Follow the upstream [getting-started guide](https://github.com/firecracker-microvm/firecracker/blob/main/docs/getting-started.md)
and [jailer documentation](https://github.com/firecracker-microvm/firecracker/blob/main/docs/jailer.md).
Verify release checksums before installing executable assets. Keep the assets
and host configuration under operator control.

Build the read-only guest from a trusted Node.js 22+ binary and BusyBox:

```sh
python3 scripts/platform/build-guest.py \
  --node /usr/local/bin/node --busybox /usr/bin/busybox \
  --output /opt/bic-vm/rootfs.ext4
```

The builder copies the selected binaries and their shared-library dependencies.
It performs no network downloads or application build hooks and prints the
image digest and Node version. Linux builds require `ldd` and `mkfs.ext4`.

Choose an unused private `/30`, an unused private API port, and dedicated host
UID/GID values. An allocation reserves both addresses and the interface for this
platform; it must not overlap another workload's network. Generate the pinned
host configuration as root:

```sh
python3 scripts/platform/configure-microvm.py \
  --firecracker /opt/bic-vm/firecracker --jailer /opt/bic-vm/jailer \
  --kernel /opt/bic-vm/vmlinux --rootfs /opt/bic-vm/rootfs.ext4 \
  --application counter --host-ip 10.254.253.1 --guest-ip 10.254.253.2 \
  --api-port 8789 --uid 61001 --gid 61001 \
  --output /etc/bic-microvm.json
bic --data /var/lib/bic-platform serve --microvm-config /etc/bic-microvm.json
```

The configuration contains exact SHA-256 hashes for the VMM, jailer, kernel and
root image. It must be root-owned and not writable by other users. More
allocations can be added to its `allocations` map before starting the
supervisor; each needs a distinct `/30`, UID and GID.

The supervisor creates a platform-owned TAP interface and narrowly scoped
packet-filter rules. Guest traffic can reach its private API listener and
responses to host-initiated requests. Forwarding, other host ports and IPv6
traffic from the guest are blocked. General guest network egress is currently
unavailable. Application deployments cannot select host executables, devices,
images, network addresses or host users.

## Deploy and operate

Add the isolation field and choose at least 128 MiB of guest memory:

```json
{
  "isolation": "microvm",
  "resources": {
    "memory_mb": 256,
    "request_timeout_ms": 30000,
    "max_concurrent_requests": 64
  }
}
```

These fields supplement the ordinary application manifest. Then use the same
`bic deploy`, `bic list`, `bic jobs`, `bic stop` and `bic start` commands as the
process tier. An operator allocation requires microVM execution for that
application name; a package requesting process execution is rejected.

The memory setting is total guest RAM. Node old-space is bounded below that
ceiling to leave room for the guest kernel and runtime. The host allocation
sets 1–8 virtual CPUs. Host database memory, platform process overhead, artifact
storage and logs have separate capacity requirements. The ordinary unprivileged
[process-tier systemd example](examples/bic-platform.service) does not grant the
privileges required by the VM host.

Each runtime instance owns its upstream HTTP connection pool, so a replacement
guest cannot reuse sockets into its predecessor's TCP stack. Before allowing
jailer to drop privileges, a launch barrier durably records the process ID,
start time, boot ID and jail path. Startup fences any surviving VMM with that
exact identity before reusing its allocation. A graceful shutdown removes owned
network resources; after a forced shutdown, ownership records allow startup to
validate and recover them. Conflicting or unowned resources fail closed.

Application SQL state, request receipts, outbox and jobs survive VM replacement.
In-memory JavaScript state and guest temporary files do not. Background delivery
retains the platform's at-least-once contract and database idempotency receipts.
This implementation boots replacement guests; it does not restore live memory
snapshots or promise preservation of open client connections.

## Acceptance

The resource-dependent test is explicit:

```sh
BICDB_TEST_MICROVM_CONFIG=/etc/bic-microvm-test.json \
  cargo test -p bicdb-platform --test microvm -- --ignored --nocapture
```

Provide an allocation named `counter`. The test needs root, working KVM and the
pinned assets. It executes real jailed guests, checks guest/host identities and
host-file isolation, refuses an isolation downgrade, exercises SQL and
transactional jobs, kills the supervisor, verifies the previous VMM is fenced,
and checks state after replacement and stop/start. It also checks that a live
host HTTP service outside the scoped API port cannot be reached by the guest.
The default test suite reports this test as ignored when those operator
resources have not been supplied.

Host operators retain access to guest memory and database storage. Independent
security review, confidential-compute attestation and hardened Cell admission
are separate requirements. The VM test is implementation evidence, not a
production security attestation.
