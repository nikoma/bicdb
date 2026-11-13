# Failed CurrentJoint completion profile at fd85

This records **FAILED full recovery: 0 passed / 1 failed**, at source `fd85a24392abf8a273479f8637e040523c053109`, candidate source version `1.0.489-beta`. The [GitHub run](https://github.com/nikoma/bicdb/actions/runs/43278537440) built all targets and the isolated consumer successfully. Its selected Core test failed after 83.78 seconds; this is not a passing recovery or a deployment receipt.

The test genuinely completed joint release, target ACK2 and Strong R2, then all three CurrentJoint fences, canonical Abort3/native R3 and Prepare responses. Node n1 returned CompletedFrozen with 226 ms left in its original receiver budget. Node n2 returned from native-WAL and catalogue fsync before its original deadline, with 39 ms left after catalogue persistence, then expired during full binding refresh. Its completion response is uncertain. Node n3 completion, recovery activation, ACK4 and Strong R4 were not reached. The original request caps remain 4500 ms and 5 seconds; effective receiver budgets shown in the trace start near 4 seconds.

`github-core-job.log.gz` preserves the complete decompressed job bytes with SHA-256 `b3e5b12d1934d7e032987a8d1822da0a9b1a5938ec4aab96337aad517fd7a265`. Artifact test output, selected-test output, compiler identity and exact source identity are separate files. `inclusive-stage-counts.json` summarizes nested refresh instrumentation: **overlapping totals must not be added**. Instrumentation itself adds overhead. See the [measured table and borrow contract](../../design/native-current-joint-refresh-borrow-scope-contract.md).

Private artifact 13971054152 was checked against its manifest: 82 files, 6,100,203 bytes, ZIP SHA-256 `c33eaee85c576a437d6e52dd54c052f56752d3b9b16e9139e646ce95bece2b74`. Its manifest SHA-256 is `b74f2a0b1c1bc10722b6293c7c16d144596d33eb101cb14d1c6b027c0144a9df`. It records observed stable files while live engine owners existed; no engine was reopened and no atomic power-loss snapshot or replay was verified. Database files, certificates and private keys are intentionally absent here.

The source patch reconstructs 2,370 selected workspace build inputs (44,499,788 content bytes) from public main488 commit `d1d816afe0446b2a614c38004738d3d1c8bbc547`. Its explicit selectors include three JSON references used by compile-time includes. This is a selected build-input snapshot, not every repository document, a built binary or a claim of bit-identical compiler output. The receipt is pinned by the verifier; selected input paths, modes, Git blobs and SHA-256 digests are all checked.

Run from a repository containing the public base object:

```sh
python3 docs/evidence/native-current-joint-refresh-fd85-2026-11-12/verify.py --repo .
python3 docs/evidence/native-current-joint-refresh-fd85-2026-11-12/tamper_tests.py --repo .
```

Verification uses a temporary index and object directory with the repository's objects as read-only alternates. It changes no checkout or HEAD, opens no engine, compiles nothing and makes no network request. Raw-byte, source-patch, source-manifest, timing-summary, receipt and unexpected-file tampering must each fail independently. Public payload verification does not re-verify the withheld private artifact.

Earlier d633 and 99 cohorts remain distinct failures. Their post-failure custody snapshots establish durable effects by capture time, not fsync-before-client-timeout ordering. Only the later direct phase traces support the timing claims stated here. No physical HA, bounded RTO, new ACK after recovery, Azure causality or production qualification is established.
