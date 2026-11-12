# Complete fence obligations: copied actual 1025-ACK control

**21 distinct local component tests pass** at exact private revision
`d2c202202943f047618784994b929a49738cd627` (`1.0.471-beta`). This repairs the
first response-size bound from the [separate failed baseline](../native-recovery-order-2026-11-11/README.md).
It does not demonstrate full recovery, Native replica catch-up, new application
ACKs, StrongGet, CI, physical scale-out or production qualification.

The complete fenced-obligation bound is 2049; the closed-ready bound is 2048.
The producer deduplicates exact overlapping obligations, refuses conflicts and
refuses overflow. It does not truncate the map. The full encoded wire-envelope
control measured 2,283,918 bytes below the 4,194,304-byte native frame floor.
This is an encoding/boundary test, not a network capacity result. A retained
33d5f715 compiler error predates the successful corrected source; it is not
counted as a test pass.

The explicit external-artifact test first copied the preserved failed fixture
into a new owned temporary tree. Strict native and metadata loaders verified
actual birth, schema and persisted topology. The original/custody engines were
never opened. Actual new metadata quorum and same-session re-fsync fenced n0+n1
without synthesizing an authority from the stored scope request. The typed n0
response retained **all 1025 command hashes, indices 2 through 1026**, alongside
actual native point 1025 and pending 1026. The new bound therefore passed the
actual previously failed boundary, not merely a synthetic size fixture.

It then reached and preserved the next independent refusals:

- Export after raw 0 requires a snapshot because source compaction is through 1.
- Direct historical certification of 1026 on raw-0 n1 refuses a contiguous gap.
- Source NativeReady refuses its unresolved historical prefix/tail.

A fresh actual n1 history observation remains possible after the pure gap
refusal; no manual uncertainty reset or second re-fsync was used. All **1025
original ACK value digests reconcile after the cold copied reopen**. No new
application write was acknowledged in this diagnostic.

The native observations also expose an independent limitation. n0 has native
1025/pending1026; n1 has actual valid native genesis0; n2's native prefix is
**unknown/invalidated with factory-initialization proof unavailable**. Its raw
Applied labels are not a native floor. This observation is reported explicitly;
it is not changed to zero or inferred ready. The later two-origin generic-repair
characterization isolates that path separately. Neither finding identifies an
Azure cause or irretrievable data loss.

The exact 21 tests are: three complete-obligation/frame boundaries, one ready
boundary, this copied artifact case, nine loopback TLS controls, five state
controls, one retained-prefix preflight control and one entry-size control.
The raw summary lists every test and exact command/result. Earlier passing
suites are not counted as tests of this source.

Restricted durable custody contains the complete stopped diagnostic copy and
raw inputs/logs: `private/native-copied-obligations-d2c20220`, 98 files /
8,757,788 bytes, manifest SHA-256
`784e609625bdc9ca3140f9e2defc762f788cbf277865e6065314b74b021c3f52`.
Source/destination hashes match, files/directories were fsynced, and permissions
are 0600/0700. Generated database keys remain private; no global credentials,
binaries or compiler caches are included. A separate hash-only comparison
verified all 53 original fixture files, both original retained and custody
copies, against the original manifest. The original fixture has not been
reopened or altered.

`MANIFEST.json` hashes this public bundle. Revalidate its scope with:

```
python3 scripts/verify-native-complete-obligation-evidence.py
python3 -m unittest discover -s scripts/tests -p test_native_complete_obligation_evidence.py
```

The copied-state test is deliberately ignored by default because it needs the
retained external artifact. The recorded manual run explicitly sets
`BICDB_RETAINED_NATIVE_RECOVERY_ROOT` and passes `--ignored`; missing artifacts
are not counted as a pass. Its exact source and the copy-only loader are included.
The complete original is available only through restricted custody, not this
public bundle.
