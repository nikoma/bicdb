# Original-writer DATA-policy diagnostic: first execution

Exact source `11f5b4b222f4bd84d1f3870dbd59da11ee8cd17c`, version
`1.0.489-beta`; [run 43314169894](https://github.com/nikoma/bicdb/actions/runs/43314169894),
Core job 131517781459. One selected test compiled and executed:
**0 passed, 1 failed, 0 ignored, 951 filtered; 2.12 seconds**.
The separate workspace all-target/consumer job 131517719477 was still running
when this first Core record was frozen; its eventual outcome is not asserted.

The earliest actual failure was the diagnostic `report_before_image` calling
`post_joint_no_data_tests::snapshot`, whose line 36 unwrapped
`current_native_range_source_lineage`. The actual error was:

> native range image: no trusted native anchor; image repair required

This is an earlier diagnostic prerequisite failure. The intended image-clear
writer-policy refusal, target adoption and global joint decision were **not
reached**. It is not evidence that an API guard can be relaxed, that the image
row-grant path passed, or that the eventual handoff succeeds.

Source `closed_policy_baseline_tests.rs::report_before_image` uses the shared
source snapshot for every original policy owner. Retained n3 factory JSON has
`initial_ranges: []`, and its range log is empty. The successor must record that
actual unavailable target point, rather than inventing R0. The original source,
failed assertions and this result remain unchanged.

The captured initial ACK ledger contains one full Record (`selected-ack-0`).
Retained raw catalogue JSON on n0/n1/n2 has range 1 last/resolved 1, permanent
promise through 1, and OLD-close native checksum pointers. Their global joint
decision fields are null; n3 has no image intent or range progress. These are
source/JSON observations from the captured cut, **not native page-WAL decode,
replay, temporal reconstruction or a newly verified ACK reconciliation**.

`raw/core-job131517781459.log.gz` contains the complete downloaded job log:
1,604,891 uncompressed bytes, SHA-256
`7c8e22f73ff6b93871f2b79deb11cf56158225cde5fa3268e97a5f53b82c1799`.
The raw compiler warnings and original failure are preserved. Source listing
reported exactly one selected test and compilation succeeded before execution.

The failure custody hook actually executed with status
`captured_observed_stable_with_live_engine_owners`. Artifact **13986884384** is
179,741 bytes, ZIP SHA-256
`fd1d98f6ad16d37b0feddb4017098ae931ca05dd8cf7c3ecc59262966e010467`.
Its manifest SHA-256 is
`edb1a871a28d8d786401a6159ee3435fc53272d5c217fa6acc70dc9a48e69024`.
All **75/75 manifest payloads**, totaling **2,515,198 bytes**, independently
matched their size and SHA-256, including hidden zero-length lock files.
The ZIP also contains the manifest and four workflow files (80 entries total).

The full ZIP is restricted custody outside this repository at
`/tmp/bicdb-native-foundations-ci/11f-policy-original-writer-baseline-artifact13986884384.zip`.
It contains private database state, including local signing material; it is not
published in this bundle. Downloaded copies and receipts were fsynced. No local
engine open, checkpoint, repair or replay was performed. Capture with engine
owners alive is not an atomic power-loss snapshot or recovery qualification.

This compact record preserves one failed diagnostic execution. It adds no
runtime fix, version/binary/release/deployment, automatic operator handoff,
transparent SQL, 8 GiB/online/fault or physical qualification claim. Source Git
objects are referenced exactly; this bundle does not reconstruct them.
