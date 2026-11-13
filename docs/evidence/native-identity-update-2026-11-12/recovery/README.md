# Candidate 1.0.490-beta recovery qualification

All checks here execute the Core-only probe built from exact candidate
`6cafd400844f0dc11200afed52ab2f015009ecf2`. This is the actual versioned main490
candidate, without private SQL partition/generated-column code. Its Core method
and regression tests are identical to the preceding implementation, but these
results are separate executions against this candidate.

| Recovery scope | Passed | Failed |
| --- | ---: | ---: |
| Process cuts, with and without specified volatile emulation | 38 | 0 |
| Actual one-shot/persistent Native log EIO | 8 | 0 |
| Actual KVM guest RAM loss | 10 | 0 |

The independent verifier passed all 56 cases, checking exact coverage, source
and binary SHA, four seed ACKs and immutable fixture manifests, full Native row
hashes, index definitions, mutation policy, Native transaction witnesses and
exactly one matching audit for new state. Old state has no audit. Three reopens
must preserve full record hashes and exact audit values/UUID without duplicates;
ungranted writes still refuse without changing the commit sequence. The changed
row's Native vector, timestamp, payload and unknown metadata survive, and the
untouched acknowledged row stays byte-identical.

Process cuts cover staged writes, Native log write/sync, before/after broker
publication, after commit and after external ACK. Paged mode additionally covers
decision, durable log, page apply, marker and sync publication. Ten process cases
recover old state, twenty-eight new. All eight EIO cases recover old state, with
an actual fired fault, no update ACK and a later unprotected write refused with
WriteFailed after the fault is removed.

The VM controller uses an isolated initramfs/ext4 image, cached local QEMU/KVM,
one vCPU, 2 GiB RAM, no network, and the shared qualification lock. It fsyncs an
independent witness before SIGKILL of its exact owned QEMU child, preserves the
crashed image, and verifies a copy in a fresh guest. Cuts use existing pre-I/O
Native log hooks, page-WAL write/sync in paged mode, and post-commit/post-ACK
pauses. Two recover old state, eight new; both external-ACK cases recover new
state with the exact ACK-bound audit. Saved image hashes stay unchanged.
This is guest RAM loss, not physical host power failure, RF3 HA, distributed
capacity, application acceptance or deployment qualification.

Probe binary:
`/root/bicdb-durability-s3/qualification-artifacts-debug/6cafd400844f0dc11200afed52ab2f015009ecf2/native_identity_crash_probe`

SHA256: `3aa9f3cba6281699feb002d28641613a0cb8a0b9ed5804c6b5cc4323531f99e2`.
This is a debug qualification probe, not a production CLI or published release.
The source-pinned full Core suite, SQL authority regression, version identities
and GitHub CI are separate gates. The probe custody file is written only after
its explicit versioned build succeeds; passing recovery does not stand in for
those gates.

`source-scoped-results.tar.gz` stores reports, seed ACKs, raw stdout/stderr,
serial transcripts, external witnesses and replay controllers. Per-member size
and SHA256 are in `archived-files.json`; `summary.json` is the independent
verifier result. No database directories, binaries, initramfs or raw images are
in Git. Image paths/hashes remain in the source-bound case reports on S3.
