# RF3 actual metadata-point capture after PR203

Source checkpoint: `7b965c6854843b7a127a5575214deddee2bc2c36`.
The original required Native job `132560706302` in run `43632072415`
failed `native_writer_data_domain_actual_foreign_mstore_refuses_without_native_change`
at `writer_data_domain_fact_tests.rs:102`: an instantaneous MStore
`try_lock()` returned `None` before `observe_owned_native_metadata_member` and
before the deliberate foreign-MStore cut. The node ordinal and actual mutex
holder were not recorded. This observation does not establish a production
foreign-store admission defect, a CPU/IO cause, or a test of the later cut.
The complete original raw log SHA256 is
`e4d350e73723c7ab8da14bdca191b8aaa2ac3df72cf6e629a9a6ae1d10d5d903`.

The fixture now calls the existing production
`ClusterDataNodeService::capture_actual_joint_metadata_point` with the SAME
original service, OWN, business ID and parent. That helper captures the actual
attached MStore/Store slot outside service guards and acquires the original
owned member under its existing deadline. It issues no replacement OWN, Role,
native transaction or extended budget. The resulting point's
`require_native_point` invokes the original member's `require_database_point`
inside the existing journal scope, additionally retaining actual open, slot
and publication-version checks. It does not acquire that journal again.

All original assertions, including the exact foreign-MStore error matcher,
original policy checks and final native/data snapshots, are retained. A
failure-only diagnostic records the member ordinal and original OWN clocks.
Any immediate lock availability observed after producer return is labeled as
diagnostic-time data; it cannot identify the original holder.

Validation is standalone Rust formatting and source/diff checks only. No
Cargo build, Rust test, CI rerun, source-version assignment, push, merge or HA
qualification is implied. A deterministic controlled MStore-holder schedule
under the same original OWN remains the next useful execution control; it
must preserve the later foreign-MStore refusal and unchanged native snapshots.
