# Optimized artifact for the isolated Azure experiment

Built from clean source **1.0.461-beta**, commit
`b05385d4b033edf0db03f3eccf4bdd86f7c8eddb`, using Rust/Cargo **1.96.0**.
This is build and identity evidence, not a published release, completed Azure
experiment or production qualification. Later experiment results must separately
identify the actual deployed bytes and observed outcomes.

## Artifact identity

| Artifact | Bytes | SHA-256 |
|---|---:|---|
| Original release executable | 1,622,900,024 | `a43edd377e0748da944368f7226463513320b188f84ea1d7736efaf3a6b005e3` |
| Frozen experiment executable | 400,427,968 | `15ad8ad644c92fd9248020c19b1fad9d4f62e6dceef8d4bc43f1415a8ff4e028` |
| `CAPABILITIES.json` | See retained file | `7ed4ac50c1b0c257bce035c0f909364cebaa2005c5251a2b9efbc3d5cb8dd15a` |

The experiment copy was separately created, processed by `strip --strip-debug`,
then made mode 0555. It printed `bicdb 1.0.461-beta`. The original release
executable remains at `/tmp/bicdb-azure-artifact-target/release/bicdb`; the frozen
copy remains at
`/tmp/bicdb-azure-artifact-evidence/bicdb-1.0.461-beta-b05385d4`.
Neither executable is committed here. These are local retained artifact paths,
not public download links or a deployment inventory.

`artifact.json` contains the observed ELF header, dynamic-library requirements,
both executable identities and the capability-manifest admission result. Its
`deployed: false` describes **artifact creation before the experiment**. It must
not be used to deny or establish a later isolated VM test deployment. No production
deployment was verified by this build track.

## Build and resource scope

The exact command was:

```sh
CARGO_TARGET_DIR=/tmp/bicdb-azure-artifact-target \
CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 \
cargo build --release --locked -p bicdb-cli
```

Repository release settings were preserved: thin LTO, line-table debug information
and default CLI features, with no `--features` or `--no-default-features` change.
The clean detached source checkout was
`/tmp/bicdb-azure-artifact-source`. Source tree/configuration digests and an
allowlist of compiler/build environment settings appear in `build-context.json`.
The root environment was not dumped. Only allowlisted build settings were
retained; credentials and account information are excluded.

The build completed successfully in approximately **62 minutes 20 seconds**
(Cargo's duration; monitor interval includes a small completion delay). The monitor
sampled available memory, swap, pressure and disk every two seconds. Minimum
available RAM was **16,024,846,336 bytes (14.9 GiB)**, with **zero pauses**.
It could pause only its own process group below 10 GiB, resume above 12 GiB and
terminate its own build on prolonged memory pressure or insufficient disk.
Production services and other worktrees were not stopped or changed.

The current release target cache is approximately **7.5 GiB**, retaining the
original executable and reusable dependencies. No interrupted/superseded build
outputs required cleanup. Preserve this cache and the frozen copy while active
experiments or verification use them. `build_monitored.py` and
`finalize_artifact.py` are the exact task-owned helpers; they contain fixed local
paths and require fresh task-owned paths before an independent rerun. They are
not general commands that may overwrite another experiment's evidence.

## Real capability manifest check

The committed generator from the exact clean source generated the retained
`CAPABILITIES.json` against the frozen executable after stripping. The check used
its exact manifest digest, expected source revision and binary bytes. Requiring
`general_cross_range_transactions` in `distributed_ranges` at `qualified` level
returned **exit 2**, `allowed: false`. This is a correct refusal, not qualification.

The manifest keeps tested/qualified/deployed statuses at `not_established`. Its
source-build and target bindings remain explicitly operator assertions, not a
signed build attestation. A successful compile, matching version, ELF header or
release manifest does not prove physical durability, scaling, distributed SQL,
RPO/RTO, application latency or production suitability.

## Portable verification

Run:

```sh
python3 docs/evidence/azure-release-artifact-2026-11-11/verify.py
```

This verifies retained file hashes and internal identity/status consistency.
Supply `--binary PATH` to additionally hash a retained executable. Without it,
the verifier explicitly reports that executable bytes were not checked. Raw build
logs retain compiler warnings; no warning was removed to obtain the result.
There were no Rust tests or cluster workloads run by this build track.
