# Metadata leader self-maintenance: actual baseline and correction

Classification: **baseline failed; fixed focused control passed**. This is
unmerged native-operator work, not a main487 or original Azure causal claim.
Both observations use actual owned metadata stores, genuine learner promotion,
TLS votes/current-term barrier and fresh OwnMQ. No supervisor/state setter or
fabricated maintenance target supplies the result.

| Source / run | Exact result | Actual observation |
| --- | --- | --- |
| `ec20e46ba3ee6334d00ccf0f77893538a7bb741a`, [43251524358](https://github.com/nikoma/bicdb/actions/runs/43251524358), job131298317193 | Failed0/1,2.34s | One automatic maintenance call changes the SAME n4 Store from Leader to Follower, while term2/commit5 and leader_id=n4 remain. Fresh OwnMQ then refuses the missing current-term authority. |
| `cd8e6e87ba48daebb15fe3cb875b63e1a6051d0b`, [43251543496](https://github.com/nikoma/bicdb/actions/runs/43251543496), job131298370673 | Passed1/0,2.80s | Automatic selection returns idle/no target; n4 stays Leader at term2/commit5 and the same original finite request obtains fresh OwnMQ before shutdown. |

The selector admitted its own elected, data-unassigned member; installed-member
verification then sent the leader's Append back to its own receiver. The
ordinary receiver correctly steps down when handling that Append. The fix
excludes the actual backend's own node from automatic selection and rejects
manual self catch-up/verification before network work. Remote verification uses
the peer-bound accepted-frontier request builder. Generic Raft receiver
semantics and all response identity checks remain unchanged.

Source paths: `distribution_native_host/provisional_listener/registration/maintenance.rs`
and `registration.rs` under `crates/bicdb-core/src`; exact control:
`native_metadata_promoted_leader_maintenance_never_appends_to_itself` in
`tests/learner_enrollment_tests/promotion_tests/self_maintenance_tests.rs`.
Run it on these exact sources with `cargo test -p bicdb-core --lib
native_metadata_promoted_leader_maintenance_never_appends_to_itself -- --show-output`.
A current main binary is not substituted for either source.

Earlier963 andbb8 submissions failed BEFORE maintenance because their fixture
client retained a pre-promotion native destination binding. They do not witness
self-demotion. The corrected fixture fetches the actual promoted member snapshot
over TLS as routing data, then independently obtains fresh OwnMQ; it never
sets a birth tuple or decodes a permission. Both corrected baseline and fixed
lanes retain this prerequisite.

The separate original CLI run8e failed3/1 when the sixth process exited code1
after factory creation with the same metadata-anchor error. Its stopped-store
state and source path are consistent with this reproduced mechanism; the CLI
failure did not retain an exact self-Append request trace. The corrected whole
CLI cohort later passed4/0 at4b, run43249496779. That corroborates this correction
without proving the cause of the older Azure failure or data movement.

The two original five-file ZIPs and extracted observations are unchanged. Hashes,
raw job-log digests and exact revisions are in `summary.json`. They retain
source/compiler/test output and the recorded status/MQ observations, not a full
engine replay, independent-host failure, application ACK ledger or production
qualification. Source merge, binary publication and deployment remain separate.
