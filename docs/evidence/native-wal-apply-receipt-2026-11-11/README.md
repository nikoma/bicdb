# Actual local WAL receipt foundation

This local source checkpoint adds opaque actual-WAL evidence and bounded retention for a future native range-prefix producer. It advances no native range index, supplies no range quorum proof and implements no strong-read endpoint.

A receipt requires an actual committed transaction, the exact serialized final mutation hash, current database incarnation, and a real whole-file transaction-log `fdatasync` plus parent-directory sync under the writer gate. A written counter, a configured sync flag, an O_DSYNC append maximum, or a prior open's receipt is insufficient. Successful publication is tied to a private per-open writer nonce and log generation.

Native-mode commit obtains a bounded retention lease before admission/decision. The transaction and private receipt share one lease, so replay receipt ownership can keep the source WAL after the transaction drops. Central live log cuts defer while a lease exists; recovery rewrite separately refuses. A successful callback releases its transaction share. Retention emits an explicit continuous-window diagnosis and has a fixed admission cap. These in-memory pins are not crash-recovery proof; historical native-prefix recovery before startup log cuts remains an integration gate.

The additive receipt callback preserves legacy authority implementations. Ordinary legacy/buffered callbacks retain their prior semantics and do not acquire native proof. Actual sync failures after local commit report `CommitOutcomeUnknown`; the committed local transaction is not relabeled aborted. Durability health is checked before pin admission and cut deferral.

The final frozen source passed 10 WAL checks (9 new plus one existing journal regression), 12 native-image controls and 6 mutation-gate checks: 28 focused checks. The exact commands, source manifest, original raw-log digests, test names and resource summaries are retained here. The first attempt's two fixture errors are retained: it assumed the completed buffered API had not yet written its decision, and reopened a paged database using the memory default. Both were corrected to the current source contract. A guard-order concern found by source review was corrected with fenced-health negative assertions; a separate old-code red run was not executed.

The build used release externs, `bicdb-core` opt-level/debug zero, one compiler job, incremental compilation off, and two test threads. An active 12/14/8-GiB process-group-only memory guard never paused or stopped it. Final compile/test took 60.56 seconds; minimum available RAM was 17,765,388,288 bytes and full memory-pressure avg10 stayed zero.

Run the matching source with:

```sh
CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 RUST_TEST_THREADS=2 cargo test --release --locked -p bicdb-core --config profile.release.package.bicdb-core.opt-level=0 --config profile.release.package.bicdb-core.debug=0 --lib native_wal
```

`validation.json` lists the two additional focused control commands. The retained manifest checks file integrity; it does not independently attest test execution. No CI run, power-loss experiment, multi-host failover, production qualification, deployment or throughput gain is claimed.
