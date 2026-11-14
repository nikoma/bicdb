# Failed first ledger candidate: 3a

Source `3a2187401047e35023791a5d2786d0f4ea16b820`, 1.0.516-beta,
was tested by [run 43644544514](https://github.com/nikoma/bicdb/actions/runs/43644544514).
Actual CI checkout `33988fcc6859420931466c61c85d5a73b3d604fc` has the same
tree, `88b309b25ef830af15c97e4d657b0032082521cd`. This historical head failed;
a later source change cannot relabel it as a passing build or qualification.

| Evidence scope | Outcome |
| --- | --- |
| Workspace all-target compilation and Core | **Failed**: E0658 at `examples/native_external_ledger/client.rs:318`, calling `str::as_str` on an already borrowed `&str`. |
| Seven new Rust receipt/parser controls | **Not tested**: both failed jobs stopped at compilation and executed zero cases. Native library tests are a different target. |
| Native library acceptance | **Failed**: 319 passed, 27 failed, one ignored, 941 filtered, in 4,334.56 seconds; job132599291431 finished November14, 2026 at23:11:33UTC. |
| SQL, PGwire, remaining workspace and Rails | **Passed** on this exact head. Remaining-workspace finished at23:45:38UTC. These passes do not repair the compile/native failures. |
| Overall required run | **Failed**. |
| Source cause of different native timing versus 1fc | **Inconclusive**. Same347 actual case names; package versions and runner conditions differ. Counts alone establish no causal or capacity result. |
| SDK probe, independent-machine expansion, RPO/RTO and throughput gain | **Not tested** by this run. Production qualification remains incomplete. |

Twenty-four hot cases failed at n4 preparation (`hot_policy_tests.rs:756`),
before later collection/publication/Open/FIRST/application steps. Some earlier
CLOSED reply-loss, metadata and Registered DECISION helper assertions completed
before that prerequisite failed. Do not erase those partial results, or label
later unexecuted cuts as passed merely because a test name mentions them.
The RF3 foreign-MStore control and all five supervisor cases passed.

Three other native failures were remote-decision handling: the donor positive
timed out at line102; the postcommit case failed a busy custody getter at
line201 before its code2 assertion/retry; the preput case failed its intended-cut
assertion at line107 after an earlier refusal. No retained marker absence proves
rollback or loss. This run did not archive a native DB fault-state custody bundle.

The native raw log is13,798,973 bytes, SHA256
`7b98065d0dd3b0d4dfe53589b4de2fce41fd5abd10f31874d554c0661c9f0aeb`;
gzip SHA256
`75a14c8032dc5b16e9ee45e9cc90ea5dc1b949b3b655f580f3ec6df4eb7cfbcb`.
The frozen capture has11 complete gzip logs and eight original ZIPs, all listed
downloads present, with hashes, original API identities and ZIP member CRC/hash
verification. Canonical manifest SHA256:
`be8fd9671c94816a956188df7e4892b2adae48a0ea513b96728ca3c81fcd6ee2`;
complete-verification SHA256:
`25fc5fa11480c3d494ad13d3f6ecdc7dbb139a0d8bac55b09adfc58f35592015`.
Independent offline verification checked114 manifest files, all11 decompressed
logs and27 members of the eight ZIPs without opening a database or downloading
a duplicate corpus. Capture integrity is separate from test success.

The minimal compile correction is `acknowledged.insert(id)`. The distinct
successor also incorporates the newer native validation candidate c05; its
production Rust and workflows match that dependency, with separate source-version
metadata. Tooling bytes preserve the reviewed original corpus except for the
single Rust type correction. The unchanged61 focused Python controls do not
execute the SDK or a cluster.

The next deterministic software gate is all-target compilation followed by
actual execution of the seven receipt controls and the required regression
suites on the corrected source. Full native acceptance and physical scale-out
remain separate gates. No VM, binary release, deployment, throughput success or
production admission follows from this documentation or a green parser test.
