# Complete same-open handoff acceptance: focused CI pass

Classification: **passed for the exact unmerged engine-test candidate**.
Source `b1ba88c57d99d3601946bd02ef4aee1a4cf070b4` (inherited working version
1.0.487-beta), [run 43224600329](https://github.com/nikoma/bicdb/actions/runs/43224600329),
Core job 131204452860: one passed, zero failed, 888 filtered, 59.33 seconds.
All-target and isolated-consumer compilation also passed.

The genuine old/new membership fixture uses actual services, durable native
stores and TLS metadata quorums on one CI host. It completes the image and
all three native publications/openings, releases the three final replicas,
recovers the target's leadership and commits the next application write.
It checks exact original and new records, matching R2 history on all three,
Strong-read result and R2 audit, idempotent historical-release retry without
resetting R2, stale old-writer refusal, new-freeze/drop generation refusal and
original-caller cancellation. An actual drop-before-arm error is reconciled.
Every full-test invocation must return the typed `FullHandoffAckAndStrong`
outcome; early publication/open/adversarial branches cannot satisfy it.

This is same-open local opaque-object orchestration. It does **not** execute
operator-driven or wire-level handoff, automatic four-to-five expansion,
all-owner cold recovery, independent-host failure, physical scale-out or a
steady five-node throughput comparison. The local two-record ACK ledger is
not the complete external attempt ledger required by physical qualification.
Its debug test duration and injected waits are not RPO/RTO or latency results.
Later source changes require their own validation.

## Earlier apparent green result was a different case

Source `9f85f6eea4ccd17985c81b336026bdf70ff01626`, run 43222492743,
returned through the step-cancellation branch because root integration reused
mode 4. Preserve that green only as a cancellation/recovery control. It did
not execute full release, ACK2 or Strong assertions. The corrected source
uses disjoint mode 0 and an explicit terminal outcome assertion. Earlier
compile and timeout failures remain in the maturity status document.

## Immutable retained evidence

`ci-artifact.zip` is unchanged GitHub artifact 13947188827, 46,899 bytes;
SHA-256 `922f11d1354b07165f8d8b011a1c4ba5be1f926fd94e0568536a4048109f1010`.
It contains exactly the source revision, rustc identity, selected-test listing
and test output, with per-file hashes in `summary.json`. The test output
contains the explicit full-terminal marker once. There are no database images
or private TLS keys in this artifact. Raw complete job-log SHA-256 is
`002bc5275052a349f767337bc3a202354f736623ded5c4408a25f983a642bdd6`.
Custody and recorded assertions do not independently replay the engine or
qualify production.
