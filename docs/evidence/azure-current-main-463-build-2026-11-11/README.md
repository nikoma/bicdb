# Current-main 1.0.463-beta baseline artifact

An isolated, clean source worktree at `d49a391b3c6157f90f29e9f2f5214143c60f9f4b`
was built before the proposed instrumented Azure reproduction. The build completed
at 08:21:33 UTC on November 11, 2026, in 2,710.48 seconds. This artifact records
the user-requested current-main baseline; a subsequent experimental source repair
is a separate revision. No engine source was edited for this build.

- Version: `1.0.463-beta`.
- Command: `cargo build --release --locked -p bicdb-cli`, one build job,
  incremental compilation disabled, default CLI features.
- Compiler: Rust 1.96.0 (`ac68faa20`, LLVM 22.1.2); repository release profile
  uses optimization level 3, thin LTO, and line-tables-only debug information.
- Frozen executable: `/tmp/bicdb-azure-artifact-463-evidence/bicdb-1.0.463-beta-d49a391b`.
- Executable SHA-256: `840f0124f27a1a7695c4b2b0c72b05a262456802af65e063cff12242559e2f7d`.
- Frozen executable size: 400,333,360 bytes, after `strip --strip-debug`
  on a separate copy. The reusable build cache remains mutable.

The active two-second resource monitor recorded a minimum 14,585,372,672 bytes
(13.58 GiB) of available RAM, no resource pauses and no build termination. It
monitored available RAM, free swap, memory pressure and free disk space. Its
safety actions target only the owned build process group. The controller's swap
was already nearly full; that observation is retained rather than presented as
unused headroom.

The frozen executable passed the repository release smoke script: local SQL and
FTS after process restart, an ordinary `psql` read through pgwire, and CLI reopen
after server shutdown. These checks establish basic local executability only.
They do not test distributed routing, scale-out, crash recovery, production HA or
physical hardware durability. The generated artifact capability manifest rejects
qualified admission for general cross-range transactions.

A separate frozen wire worker has SHA-256
`b8cc2607220e32f705a23fd6b7d4f41c22298cd1b0ec92cc8e83847783ef8d2c`
and is statically linked to this baseline core library. The worker's event-policy
source and linked-core source have distinct identities in its own provenance.
The core, page and Lua `.rlib`/`.rmeta` inputs were copied and hashed independently
of the Cargo cache before concurrent source-fix tests were allowed to reuse the
cache. Their six immutable copies total 278,366,814 bytes. Older 1.0.461-beta
artifacts and original Azure evidence were preserved.

`prebuild-identity.json`, `result.json`, `artifact.json`,
`frozen-library-manifest.json`, `release-smoke.json` and `resource-summary.json`
provide the source, build, artifact and check identities. Larger retained raw
logs and resource samples are indexed by digest. This evidence does not assert a
published binary release, an Azure deployment, CI qualification, or production
qualification. No new Azure VM had been allocated when this artifact was frozen.
