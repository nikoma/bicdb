# Baseline: retained serving record blocks a second compatible image

Exact unmerged candidate source:
`1978a0ea8eec099ab5285c7ff5d30118ae5c1825`, package `1.0.471-beta`.
This reproduces a candidate integration blocker. It does not attribute a defect
to GitHub main, Azure, physical storage, or a deployed system.

The corrected test compiled from the intended worktree and **failed: 0 passed,
1 failed**. Its first image, preparation, closed ready quorum and native serving
record succeeded. A second compatible image also completed actual content
verification, native publication, catalogue adoption and the Runtime's owned
freeze return. Its physical readiness producer then refused the still-retained
serving record because that record names the former physical base.

| Observed fact | Before image B | After image B |
| --- | --- | --- |
| Range/metadata epoch | 1 | 1 |
| Range index | 1 | 1 |
| Actual native birth | Same birth | Same birth |
| Logical history SHA | `53b21e309d37000df6302355b6e243ed22e933d32e4539a3f84b09e1eccefc52` | Same |
| Native receipt SHA | `d9c7be162aadf86dbff66e1bf3cbc6e6556b6c92a7afde8796bd8e98dc31fbe9` | `3a9b953e72111f4ee708b5db3f87eaf05210c613ccadd70357b82c1cc3317d18` |
| Serving record's physical-base SHA | Receipt A | Still receipt A |
| Serving record checksum | `c44bf0eb509856f55a7e7c6ac9b4d858002acb555a0b2e6a9ec2aebf0a5c6816` | Unchanged |
| Target namespace UUID | `1d3852da-c900-43ef-8aba-f6b53c916a6f` | Preserved |
| Recovery state | Fresh recovery nonce | Active; exact image-B refusal retained |

The exact final error was:

```text
native range image: recovered serving record does not bind the actual same-voter physical/native domain
```

The setup uses the real RF3 origin/service fixture with two initial ranges.
Selected-range R1 is a real acknowledged origin commit. Its behind current voter
is repaired using actual durable metadata witnesses, current-data-quorum fences,
complete source-history comparison and native image receipts. After image A
prepares, actual source and repaired-target preparations produce a genuine
closed native-ready quorum. The target then persists and reads back serving
record A while **still Prepared and fenced**. No activation or later user write
is invented. Old pins end; an independently fresh recovery nonce at the same
epoch fences the real receivers and authorizes image B. Every scope and receipt
comes through its actual producer; no supplied serving DTO or numeric floor is
used to manufacture authority.

Image B's complete native content verification passed, and its actual birth,
local namespace, R and H bindings passed through catalogue finalization. A
separate ordinary second-image MVCC read was not measured in this baseline.
The final error is an availability/refusal observation. It is not evidence of
irretrievable data loss or a physical RPO/RTO result.

Both current data-quorum members are actual local services (`n0`, `n1`), and the
metadata leader is separately `n2`. Metadata transport uses registered
in-process test identities. Compiling TLS support does not turn this witness
into an authenticated network or independent-machine experiment.

The earlier source `8d7e0d4c` also failed, at an earlier fixture precondition:
its closed quorum was collected for recipient `n0` and submitted to `n1`.
The correct recipient guard rejected it before serving record A was published.
That unsuccessful fixture and its logs remain separate. The only correction was
to acquire the actual fresh collector role for target `n1`, preserving actual
initiator `n0` and genuine voter observations. The earlier rejection is not
reported as a passing Rust test or as the stale-base defect.

Raw logs, resource samples, exact commands, source snapshots, both unsuccessful
results, before/after metadata and custody receipts are retained here. The
confirmed run used 6.23 seconds of test execution (75.07 seconds including
compilation), with no resource-monitor pause. These are test timings, not
performance or RTO measurements.

Actual database files remain in restricted private custody: one **post-B**
three-database bundle for the confirmed case, plus its before/after A/B metadata.
There is no independent cold database snapshot of image A from that same run.
The initial failed-recipient fixture has a separate retained bundle. The custody
manifest covers 118 files / 4,550,410 bytes with SHA-256
`d1e10ab900e82f49bf95f677ae143d98d9c4ffeaf71cd8fe716b54ffef2dbdf8`.
No binaries, Rlibs, global authentication caches or private keys are included.
The public evidence contains metadata/logs/hashes; it does not publish those
private database bundles. Its read-only verifier checks the retained evidence,
not a new native database reconciliation.

A later floor-preserving implementation and its GREEN results must retain their
own source identity. They do not change this baseline result. Neither result
establishes NativeActivated, a subsequent acknowledged write, StrongGet,
transparent SQL, physical failover or production qualification.

```sh
python3 scripts/verify-native-serving-image-baseline.py
python3 -m unittest discover -s scripts/tests -p 'test_native_serving_image_baseline.py'
```

To repeat at the matching baseline source (the expected result is RED):

```sh
CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 RUST_TEST_THREADS=2 \
  cargo test --release --locked -p bicdb-core --features tls-replication \
  --config profile.release.package.bicdb-core.opt-level=0 \
  --config profile.release.package.bicdb-core.debug=0 --lib \
  actual_prepared_serving_record_survives_a_second_current_voter_image_attempt \
  -- --nocapture
```
