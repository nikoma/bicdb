# Focused native-image foundation checks

This is a local source checkpoint, not HA qualification or a deployed feature. The tested core used release dependencies with `bicdb-core` optimization/debug disabled, one compiler job, incremental compilation off, and two test threads. An active process-group-only guard paused below 12 GiB available RAM, resumed above 14 GiB, and stopped at 8 GiB; it never paused or interrupted these runs.

The final frozen dirty-source manifest in `source-manifest.json` passed 12 image/native-genesis/binding/scanner/callback checks and 6 mutation-gate checks. The final compile/test run took 63.54 seconds; minimum observed available RAM was 19,058,610,176 bytes, memory full-pressure avg10 remained zero. `validation.json` retains exact commands, source hashes, test names/results, resource summaries and original raw-log SHA-256 values. The initial wrong-range fixture failure is preserved and distinguished from a runtime defect.

Reproduce the focused checks from the matching source:

```sh
CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 RUST_TEST_THREADS=2 cargo test --release --locked -p bicdb-core --config profile.release.package.bicdb-core.opt-level=0 --config profile.release.package.bicdb-core.debug=0 --lib range_image
CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 RUST_TEST_THREADS=2 cargo test --release --locked -p bicdb-core --config profile.release.package.bicdb-core.opt-level=0 --config profile.release.package.bicdb-core.debug=0 --lib distribution_range_mutation
```

The fresh native birth/genesis tests use actual fsync-enabled page storage, hidden native WAL records and clean reopen. They are local tests without network failures, power loss or replica-quorum experiments. The capture configuration-transition helper, certified suffix extension, target installation, final handoff and strong reads remain uncompleted dependencies. A new learner's empty ownership set establishes no source floor.

The manifest authenticates these retained files against accidental changes; its presence does not independently prove execution. No raw database, key material, cloud account or network identity is included. See [the implementation boundary](../../range-image-native-primitives.md).
