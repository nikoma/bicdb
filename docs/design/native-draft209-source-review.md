# Draft209 source review: reuse boundaries, not qualification

Inspected draft: **`7bd1cabbfd255596c76a6903440926cb3fe6b9d0`**, PR209,
based on delivered main519 `587dd6e4775e454cc9e8224d5fed76e8795c6ff6`.
The draft is unversioned, unmerged and explicitly not ready for merge. This
review neither compiles nor executes it and does not import it into candidate520.
Later revisions need their own inspection.

The [machine-readable review](native-draft209-source-review.json) pins19 actual
source blobs and records a next deterministic test for every finding. The
classifications below apply to examined **source requirements**. Every runtime
result is **not tested by this review**. No loss, stale-read or Serving bypass
has been observed by this inspection.

| Source requirement | Classification | Evidence and limit |
| --- | --- | --- |
| Recipient independently collects authenticated ALL-prepared observations | **Passed**, examined mechanism only | `joint/bridge/remote_publication.rs` and `distribution_transport/native_handoff_step.rs`: opaque pinned frames, actual local preparation/writer and fresh metadata; separate remote backing. This is not simply decoded DATA constructing a local quorum. |
| Original native attempt survives a postcommit helper error | **Failed**, source custody gap | Both `registration/initial_data/old_step/bridge_step.rs` and `registration/registered_replica/remote_attempt/bridge_step.rs` return through `?` before `retain_custody`. The original lease survives, but that is not retention of the issued XID, candidate and exact API outcome. |
| Recovery proves ownership of the exact uncertain catalogue write | **Failed**, incompatible old route | `joint/bridge/publication/remote_open.rs` reaches the old generic `with_own_uncertainty_set_aside` path in `joint_handoff_state/activation.rs`. It must compose the same-owner candidate/IO-generation correction rather than overwrite it. |
| Preparation session follows original host-process lifetime | **Inconclusive** | `registration/bridge_step.rs` allocates an unparented session and cancels it on step destruction; existing admission checks do not, alone, establish concurrent-stop behavior. |
| Taking the Registered release handle permanently loses ownership | **Inconclusive; defect not established** | The handle contains an Arc; the original owner retains Frozen/activation and can return the same handle. A fallible continuation still needs interrupted-progress controls. |
| Remote controller reaches Serving and completion | **Failed**, incomplete implementation | `joined_data_handoff/remote_backend.rs` ends activation with an explicit unimplemented release error; Serving and metadata completion paths also refuse. |
| Node join automatically selects and moves native ranges | **Failed**, absent from examined path | `range_move.rs` admits one operator-selected range/target per host. The production caller found is `operator.rs`'s `MoveRange`; this is narrower than add-node automatic placement. |
| Arbitrary coordinator/member placement | **Failed**, bounded subset only | Controller requires metadata leader and range coordinator to coincide; backend refuses a coordinator remaining in final NEW because its local bridge steps are absent. |
| Restart preserves participant route-application barrier | **Inconclusive** | `registration/range_move_hosts.rs` retains pending route ACKs in RAM, initially empty. Postcommit interruption and reconstructed-host behavior need explicit tests; no routing safety violation is inferred. |
| Controller deadline proves bounded physical recovery | **Not tested** | A120s step token and500ms–30s backoff are implementation settings, not measured IO, convergence or RTO. |

The smallest preparation controls must cut the **actual** native return before
outer retention, separately exercise known commit and commit-sync Unknown,
retain the same original XID/candidate, and prove a fresh readonly resolution
does not issue another mutation. The catalogue control must introduce real
unrelated-range persistence uncertainty and reject publication without clearing
it. A root/UUID/checksum alone is not an owner or recovery grant.

Controller acceptance must first complete one real cross-process handoff with
ACK/Strong checks, then survive interruption after metadata commit and before
route-ACK retention. Automatic placement requires a separate node-join test
with **no operator range-selection call**. A worker repeatedly reporting an
unimplemented stage does not satisfy bounded convergence. Physical expansion,
throughput and production gates remain unchanged.
