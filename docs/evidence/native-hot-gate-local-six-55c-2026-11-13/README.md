# Local metadata publication gate: six hosted unit cases

Exact source **`55c50e07c0f36ee7314342b4c8653988f444a835`**, source version
**1.0.500-beta**, passed the selected six local gate unit tests in hosted run
[43523702442](https://github.com/nikoma/bicdb/actions/runs/43523702442).
This is **local gate unit coverage**, not full actual-MStore, NEW, APP, Strong,
controller, cluster or physical HA qualification.

| Evidence | Result |
| --- | --- |
| Core job `132224155429`, filter `metadata_publication_version_` | **6 selected / 6 executed / 6 passed / 0 failed** |
| Reported unit-test duration | 0.00 s; 1,144 other tests filtered out |
| Workspace job `132224127271` | All targets compiled; workflow consumer without Core defaults compiled |
| Full workflow | Success |
| Artifact `14083514559` | Complete original ZIP, 57,797 bytes, exactly four text files |
| Source merge, release binary or deployment | Not verified by this packet |

The four artifact members are `source-revision.txt`, `rustc.txt`,
`selected-tests.log` and `tests.log`. There is **no runtime custody**, database
root, generated fixture key or cluster report in this artifact. This packet
includes the original four-file artifact unchanged; it does not claim that a
projection removed anything. Text members and complete job logs were inspected
for literal private PEM markers. That bounded inspection is not a general
secret-scanner or production credential assertion. GitHub-hosted evidence is
subject to the public repository's Actions access/retention policy.

The artifact SHA-256 is:

```text
aa14896868d770919d5eb27aaffefa12b51e42d933677859107b39f962d7f38d
```

## Exactly what ran

The tests directly exercise the in-memory publication gate in
`crates/bicdb-core/src/distribution_consensus/publication_version.rs`:

| Test suffix after `metadata_publication_version_` | Exercised condition |
| --- | --- |
| `nested_outer_completion_and_noop_do_not_self_invalidate` | Nested invalidation stays unready until outer completion; a harmless completed persistence does not invalidate the current point. |
| `failed_persistence_stays_poisoned_after_harmless_persist` | An injected local gate persistence-failure state refuses later initialization/publication operations. |
| `changed_error_without_persist_cannot_be_cleared` | A changed failed operation cannot be cleared without valid persistence. |
| `short_guard_refuses_authority_change_before_assignment` | A held local publication guard prevents the change; an invalidated older point remains rejected. |
| `nested_failed_change_cannot_be_hidden_by_outer_persist` | An outer completion cannot conceal an inner failed change. |
| `original_open_owner_drop_revokes_retained_point` | Dropping the Rust gate owner revokes its retained point. |

The test helper calls gate bookkeeping methods such as `persisted()` directly.
These are not physical fsync failures, process death, disk recovery or complete
MetadataConsensusStore operations. In particular, the owner-drop test is a Rust
owner lifetime check; it is not a database crash or an operational failover.
The copied exact-source file and its hash permit inspection of that boundary.
No application acknowledgment reconciliation or runtime transaction occurred in
these six unit cases. Build success does not exercise unselected runtime paths.

The earlier source `d5fead60a42348edeaaab59bf28e603d685a9303`, run
43521850216, failed compilation with six errors before the selected units ran.
Its immutable packet is
[the original d5 compile evidence](../native-hot-gate-compile-d5f-2026-11-13/).
It remains a failed compile with **zero executed unit cases**. The successor
uses the existing sealed owner/fact accessors, actual Weak→Arc upgrade,
correct trait-object reference and missing Arc import; this passing run does
not relabel that earlier failure or qualify the wider native driver.

## Capture and verification

`evidence.json` pins run/job/artifact IDs, exact test names, source-file and
artifact/member hashes, scope and the earlier failed source. The packet also
retains GitHub run/artifact metadata, compressed complete Core and workspace
logs, and the source reference. The first read-only Core log download returned
`EOF`; that fetch failure is recorded separately. A direct job-log API download
then succeeded. No workflow was retried by this capture.

Verify offline with Python's standard library:

```sh
python3 -B /path/to/packet/verify.py
```

The verifier checks packet/member/archive/source hashes, the exact six selected
and executed tests, the source test definitions, successful Core steps and both
specific workspace/consumer steps. It does not build Rust, contact GitHub,
open a database or rerun the units. A tampered-artifact negative control checks
that this verifier refuses changed bytes; it is not an additional HA test.
`SHA256SUMS.json` covers packet files and `validation.json` records verifier
results. The original d5 packet and all earlier runtime evidence are unchanged.

No full release regression, actual NEW/decision/Serving path, application
commit, strong read, rebalance, cold recovery, physical RPO/RTO, throughput or
production gate was qualified here.
