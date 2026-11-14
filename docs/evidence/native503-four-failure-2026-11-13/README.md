# Native 503 required serial suite: four failures retained

Run [43529104485](https://github.com/nikoma/bicdb/actions/runs/43529104485),
job [132242099377](https://github.com/nikoma/bicdb/actions/runs/43529104485/job/132242099377)
executed 213 selected tests: **208 passed, 4 failed, 1 ignored**. The ignored
test did not execute. Its 1197.50-second test duration is neither RTO nor a
performance qualification measurement. All four failures remain failed.

The run head is `c6a081a6026f4efd00d80f392c94b67f2f2f4e12`
(1.0.503-beta). The actual PR checkout logged by actions/checkout is
`de15f8f9836e0e93fb2e912ad230437bc975aa6a`, a different commit. Retained raw
Git commit objects verify that **both have complete tree
`a4712298110429f1c0d7ce63876b200e42a874b3`**. This packet does not assume the
run head and PR checkout are the same SHA. Fourteen source copies have Git
blob and tree membership proofs; comparison with main `629857f5e` verifies
that the HA failure paths are unchanged. That comparison does not establish
the cause of any failure.

| Case | Result and reached boundary | Intended assertion |
| --- | --- | --- |
| Normal Serving current-damage refusal | Failed fresh metadata quorum acquisition after recovery | Damage injection and refusal **not tested** |
| Finite open does not start hot worker | Failed shared initial CONFIG preparation | Hot-worker control **not tested** |
| Retained Store replacement/restoration | Failed shared initial CONFIG preparation | Actual slot ABA control **not tested** |
| Drop without stop and reopen | Current mode's first APP timed out; effect uncertain | Complete three-mode lifecycle **not qualified** |

The normal-Serving fixture reached three successful internal
`Transaction::commit` returns, including one selected record with a
source-inferred ledger fsync. These are **not public APP acknowledgments**.
The raw log does not contain the retained database or ledger contents.

In the lifecycle test, the failing mode returned no successful APP reply.
Its write effect is uncertain. The mode number is unrecorded; zero to two
previous modes may have completed and returned APP acknowledgments. The
packet cannot assign a precise total and does not call the entire test
zero-ACK or infer rollback from timeout. Broken-pipe/EOF messages lack linked
request identities and do not establish the cause.

The original [forensic report](forensics-original.json), lossless
[raw job log](raw-job.log.gz), authoritative [terminal job metadata](job-terminal.json),
[run observation](run-observation.json), [case boundaries](case-boundaries.json)
and [source identity proof](source-identity.json) remain separate. The run
observation was captured while the whole workflow was still in progress;
the terminal native job result is authoritative for these four failures.
No later whole-workflow outcome is inferred from that observation.

Run the portable checks from any directory:

```sh
python3 path/to/this/packet/verify.py
python3 path/to/this/packet/verify.py --self-test
```

A passing checker result validates the retained evidence; the native suite
still failed. The standard-library checker requires neither Git, network access, BicDB,
database opening nor the original absolute paths. It derives the test counts,
four failure sections and actual checkout from the raw log; validates source
blob membership through retained Git tree objects; verifies hashes and exact
section bytes; and detects deliberately mutated evidence. Git-export checking
is recorded separately at delivery. Integrity against this manifest and the
pinned repository commit is not independent origin authentication.

See [issues and smallest next tests](issues.md). Every cause remains
**inconclusive**; physical faults, RPO/RTO, post-convergence throughput,
production admission, cold recovery and data-loss reconciliation are **not
tested by this packet**. The historical failed Azure qualification is
unchanged. No engine fix, retry, deadline change or skipped assertion is
included.

The repository and upstream Actions logs are public under GitHub's access and
retention rules. A local restricted copy does not make upstream evidence
confidential. This packet contains no database/custody archive or recovered
ledger. Its literal private-key-block scan is limited to the retained raw
log and packaged files; absence of a recognized marker is not a guarantee
that every possible secret or credential format is absent. Public Git commit
signatures are not private signing keys.
