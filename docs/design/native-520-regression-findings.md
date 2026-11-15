# Native 520 candidate regression findings

This record begins with candidate **1.0.520-beta / `326c363477477950f76bdb51fe6425cab85dc362`**, tree `66972d1997cc2c4626aa1a02ae94dd13d880329b`, and retains later exact-source checkpoints separately. Source326 is a failed CI candidate, not a qualified release. Its tested pull-request checkout was `44d50d087cd1e4fefdba93e3f539b1806f232f83`, with that same tree. Corrections retain their individual compilation and execution status below; an earlier passing result is not inherited by a successor. The original Azure qualification remains failed and its evidence is unchanged.

## Actual execution

### Source700: test-compilation failures; other jobs still running

Draft PR207 source **1.0.520-beta /
`700726e90f041815b132cda4f87ca92337f55baa`**, tree
`aacb16996f71880683459fc127482ef9ce4d3130`, follows the complete failed sourcebd09
run below. Normal [Tests43728210745](https://github.com/nikoma/bicdb/actions/runs/43728210745)
and [Rails43728210770](https://github.com/nikoma/bicdb/actions/runs/43728210770)
check out `939038eb35677874c941c2d1220cd0a7d6548b40` with the same tree.
This is an **incomplete execution checkpoint**, not a frozen whole-run result.

All-target132854681021, Native132854704823 and Core132854704889 failed test compilation with the
same eighteen diagnostics: seventeen missing `BTreeMap` references (eight E0433,
nine E0425) in four FIRST child test modules, and E0616 for private
`NativeRemoteLocalServing.release` access at `registration/joined_data.rs:423`.
All three complete logs contain zero executed test-case rows and zero runtime
summaries. Native's production chaos-helper build passed before its test build
failed; no native control or passive stage clock executed. The earlier three
library diagnostics are absent from these logs. Rails132854680904 passed its
build and application step: twenty runs, one hundred assertions, zero failures,
errors or skips, including process-kill queue/cache recovery, twenty-four reports
and twenty-four cable messages. PGwire132854704821 passed 511/0/2 across
twenty-eight summaries. Those component/application results are separate from
native HA. SQL and remaining-workspace were still running at this checkpoint.
Earlier revisions' passing results are not substituted for current results.

Complete all-target raw1,406,089 bytes SHA-256
`a28f1c7e66fbf60d9582b4fcf4958883a668e00f71b8899a093d2a153606fa36`;
Native raw1,370,551 bytes
`ba2b0397c287f6b4a4766c45b8ae53bbca74ca235ef561be325c6afb6a47a634`.
Core raw2,530,065 bytes
`8d29ecce91fb41c05d3a82248a9e1337b19c3f1f999cc7110ad83645c6dd59a3`;
PGwire raw1,505,387 bytes
`089d3952eac7fd42cf75db47a2251ad7b8eb6ca9ebd4b44a8acdeddde3a37c17`.
Rails original artifact14167656585 contains sixteen verified members; ZIP SHA-256
`a419978095b8b02c2c280c77d36448c00128395c624d918fa75fbcf5150e29a8`.
Its exercised CI debug binary SHA-256 is
`49107b5b027681de969ed77834881a6f2076dbc8c65fa84cf312485f67e6296c`;
that is not an optimized physical-qualification binary, release or deployment.
Independent root Core/PGwire complete-log and Rails artifact verification receipt:
`f867fe41c4f62a780aec9481fbb03710cf2ab9c585d634fe06e3df5c6174370e`.
The independent all-target eighteen-error classification receipt is
`c65a1869996c882dad275eccb53ea6153701dda5f68dfe44dbb925219e52f85a`;
the capture-side Native prerequisite classifier is
`c8887ade3e6e4f7ff48b9db5e94c69e0a18e323bff5872c0df739af531656265`.
The sole collector remains responsible for the original logs and artifacts;
no useful job is cancelled or restarted to publish a correction.

The reproducible issue is **failed test compilation**, not a demonstrated native
runtime failure. At the pinned source, the actual failing workflow command is
`cargo check --workspace --all-targets --locked`; the native command is
`cargo test --config 'profile.dev.package."*".opt-level=2' --config profile.dev.opt-level=1 -p bicdb-core --lib --locked -- distribution_native_host:: range_image_native::rf3_fixture:: --test-threads=1`.
These commands are recorded from remote CI, not rerun locally. The next smallest
deterministic check is compilation of the corrected Core test target, followed
by the unchanged native selectors and required broad suites.

Isolated source correction `c072c75ea802ea528c6985c55bb4407fd05c00f0`
adds the single parent `BTreeMap` import inherited through the four unchanged
`super::*` chains. It moves the diagnostic's same successor lookup and original
Joined-owner validation into the actual private owning module, returning only
the decision checksum string DATA. The release field stays private; original
assertions, deadlines, errors and diagnostic JSON values remain intact. This is
**source-reviewed; not compiled or executed**. Standalone formatting and diff
checks pass. Independent source-review receipt:
`94ef95d6a59b1d1622e2e911dc74fc09c04b7d8da98d9cb0af415eb0e439b897`.
Root whole-source inverse and actual Native-log review:
`bc7546d3c3d2ab01c82dc5b6cad9fa2de4e05ce3e9e5c6cf3e073d6af8fbba54`.
It establishes no HA, physical or production qualification and does not alter
the original failed Azure evidence.

### Sourcebd09: complete failed run; Native/Core execution not reached

Draft PR207 source **1.0.520-beta /
`bd09a77e8e72bb88e73c395b9629d96812061c30`**, tree
`c8b9d8d64a7593211cf3ebd1eaa7bddd45747909`, was published only after source6de
completed and its full archive was frozen. Normal
[Tests43726287929](https://github.com/nikoma/bicdb/actions/runs/43726287929) and
[Rails43726288005](https://github.com/nikoma/bicdb/actions/runs/43726288005)
check out actual merge `cb8c3b61bf70105f79af059f07b6a54704fa6ca6`, whose tree
matches the public source exactly.

All-target132848817942 and Rails132848817906 failed Core compilation on the same
three diagnostics: two E0425 unresolved `BicDb` types in
`joint/bridge/preparation_custody.rs:452,567`, and E0004 at
`db/native_recovery_ready.rs:1947` for the new `NativeJointServingPhase` and
`NativeJointLocalServingPolicy` purposes. Both complete logs contain **zero
test-case rows and zero runtime summaries**. The no-default consumer and Rails
application steps were not reached, not passed. SQL and PGwire have also failed
library compilation on those same three errors, with zero cases/summaries. Core and
Native subsequently failed the chaos-helper library build on the same three
errors, before test-only compilation or runtime: both contain zero executed
cases/summaries and their actual test steps were skipped. Remaining-workspace
failed later on the same E0004 purpose match after 508 actual passing component
cases across 34 summaries. It did not reach the complete workspace or offline
tooling acceptance step. All jobs are terminal; the overall candidate failed.
No earlier passing result is inherited.

All-target complete raw95,210 bytes, SHA-256
`820b96be94fac96483bc256dc83b9dec9a82ce235fece4bb0f47c24f93cd3d23`;
Rails raw108,757 bytes,
`58d95fb772cca5b3ec6ff295116a78727337c755b1c794bd69790a14ad9aa56c`.
Independent root complete-log compiler/result-scope receipt:
`3b93957ad58c70cb15089f17410ef00a77b28cc0dd7308c25fca0e60cde7854a`.
Native raw158,473 bytes SHA-256
`efd16c221c341dc70b2a499baf3b85e0e23b9e9b902891eba340c42f82180164`;
Core raw158,940 bytes
`c73885f57fafc73d1421eaf1486056a9dd6d77e29cd6a5c5ea7db2b8fd746367`.
Independent complete-log prerequisite classifiers respectively:
`5a2a7eb6ab05ccd8b58f861fe9312d7a95ed031ee9339903caefdcdc5b20a917`,
`efd0809531ff318680355331462ef6182d66d47a3312f73b94f68df5c7452b0a`.
The new controls and stage clocks remain unassessed; helper build failure is
not a Native runtime failure at any intended fault cut.
Remaining-workspace raw2,543,803 bytes SHA-256
`85b3bd3445c6802b0dc9d22c8eff340cdab23306f9fdf6b45a306226b2ecb5a8`;
root partial-runtime/cut receipt
`108eb5e13154ddcd614bebecf3d70ba09ce2560c04ba73e6d1e5469e8404729c`.
The full failed-job classifier SHA-256 is
`4e4da738eeba1c6d8e73c4b4b64e3376b7bed2436aff1849fb4f5a2acc11fe8b`.
Its six zero-case compilation failures remain separate from those real partial
component passes: Lua five, Page499 and Script four; Cell/AppHost/CLI and the
offline acceptance step were not reached. The sole collector exited naturally;
the complete archive is frozen: 90 premanifest / 93 total files, eleven full
logs and six original ZIPs / six members. Manifest SHA-256
`7a8a4d589be1a09312e6d404d9a6af71a35968c2880d37066fc8a62afe0ceb2b`;
capture verification
`dcc41dbed64e642ff88eede95629608e69bed0722ab7af00d8802f6cce3768f1`.
All actual job/artifact identities, raw/stored hashes, ZIP CRCs and member
hashes were verified, and a wrong expected manifest hash was rejected. No
useful job was cancelled or restarted to publish a correction.

Local corrective source `c62f0fb4c` qualifies both parameters as `crate::BicDb`
and explicitly refuses both narrow serving purposes in the generic recovery
readiness consumer before Frozen validation and new source-pin/readiness
construction. The caller's Frozen input already exists; this is not a global
no-effect claim. It assigns no
generic recovery owner to either purpose. This is a **source correction only**:
standalone formatting and diff checks passed; it has not compiled or executed.
This compile defect supplies no runtime proof of an authority bypass or of HA
acceptance. The original failed source and all original assertions remain intact.

### Source6de: Core and Native failed; compilation, SQL, PGwire and Rails passed

Current draft PR207 source is **1.0.520-beta /
`6de646beb603accf39358017d8e0c83c39c741b1`**, tree
`de0b68af8d158f39358604590769afb8237115f0`. Normal
[Tests43714999361](https://github.com/nikoma/bicdb/actions/runs/43714999361) and
[Rails43714999384](https://github.com/nikoma/bicdb/actions/runs/43714999384)
checked out `7714bd37b9cfe87658e3baadd1f906a187a06b58` with that identical tree.
Workspace/all-target and no-default consumer compilation passed. PGwire completed
511 passed / 0 failed / 2 ignored across 28 summaries. Rails completed 20 runs /
100 assertions, zero failures/errors/skips, including actual process-kill queue
and cache recovery, 24 completed reports and 24 cable messages. Core has failed
at the legacy primary-record-only format-preservation assertion described below.
SQL completed 2,682/0/4, Native failed 375/37/1 and remaining-workspace passed
1,062/0/6. All required jobs are terminal; the overall Tests run failed. No overall
pass or inherited Native acceptance is claimed.

This successor composes the two backup-vector expectations, all-worker INITIAL
failure evidence, duplicate-validation reduction and same-OWN FIRST identity
wait described below. Compilation does not establish that the four new control
definitions or original 45 failed native cases passed. The original failed Azure
qualification and all physical/production gates remain unchanged.

Complete raw log hashes: all-targets 2,781,766 bytes,
`16e875783c19e0ff58ef486753c8548823bfbf9ae96ea7c819d71cc94e054b66`;
PGwire 1,320,138 bytes,
`40c26cd3ab5fb44e10bd46cc84375e608cb3a31ad2ae4ae29ce927ae16e80345`;
Rails 1,184,666 bytes,
`5fee47ff0fff5b4da39044378d73dcb2e1c1877e0641e2691d30c9700797212d`.
Independent complete-log receipts respectively:
`9a5bb9b2ba34088f04bc4fc8bb3d864b18f7f104396d9d520c90841633995c16`,
`332574d8c7116d551187e1bda23a92294e9ec565828409767cb08f3405f445e4`,
`74bf19aebe1ae949eaeb1ce5b817fee943a1b4a14f139670d1e407d71c56c104`.
Rails built and exercised a CI debug binary with SHA-256
`64e18a818ebade9ebfbca810f4932bd132bf827ff4a1dfffbb9279483dda9245`.
This is not a retained optimized physical-qualification binary or published
release/deployment. The sole collector exited normally after every job became
terminal. The complete archive is frozen: 98 premanifest files, 101 total files,
11 complete raw-log gzips and eight original artifact ZIPs / 28 members.
Canonical manifest SHA-256:
`48e180b4f4c6e48e061837fd00fb3634887aeba467b7b954d9f49dc01c401b5c`;
complete-capture verification:
`fe16354e66e73842e13417d3a329338d7939d6766fb224c82720ee20643680cf`;
independent root full-archive rehash/inventory/ZIP verification:
`1e8b772a079138d6c392ba918e683c8242d676751466d09f105ea21473a4c8e2`.
Every non-skipped job and original artifact ID is covered. No useful run was
cancelled or restarted, and these immutable originals receive no later writes.

The Core job executed 1,300 passing / one failing / 13 ignored case-result rows
across 38 summaries; these are executed rows, not 1,300 independent trials.
Library975/0/8 and visibility8, Standard3, bootstrap11 and rekey4 controls passed.
The original full encrypted backup/verify/restore case now passed at raw24404;
the 40-definition backup inventory has 32 actual passes and eight unexecuted
definitions. This is component coverage, not a physical backup certificate.

The failure is
`index_disabled_primary_handle_preserves_legacy_predicates_and_refuses_mutations`
at `crates/bicdb-core/tests/decimal_predicate_generations.rs:118`. Its actual
main482 legacy fixture retains its verified original hashes. Opening with
`with_secondary_indexes(false)` reads the acknowledged row, refuses the
duplicate mutation as primary-record-only, preserves duplicate absence and
leaves `indexes.json` byte-identical; those assertions passed before FORMAT
equality failed. The actual FORMAT differs only by adding
`physical_backup_gate_v1`. Classification: **failed legacy export metadata
preservation; implementation boundary under investigation**. No acknowledged-row
loss, broken uniqueness or physical-recovery result follows from that failure.
The original fixture and byte-equality assertion remain intact; they are not
silently adjusted to accept an upgrade by a primary-only export handle.

Complete Core raw log: 2,516,374 bytes, SHA-256
`d6c409afc703404fc1bded0b8d1266022fbfcfcfa887d2b5f2eda9322a9b7117`;
gzip SHA-256 `013d775639fab566d50f435149f7497928a5dd17ce258b0711733f7b33cba67b`.
Independent complete-log verification receipt SHA-256:
`0f3d65f50ac123aa2f69947c7ca12751deaef9d3773659ebc3b55d8918a430bd`;
original assertion/cut classifier SHA-256:
`4babc8214096f5504a0e7c166bb81666b2e4a8990978f259968fe7974df03578`.
Later targets not reached by this failed Core job receive no passing coverage.

The isolated [primary-only enrollment correction](primary-only-export-physical-format-enrollment.md)
(`4829efc186c58951aba87d9a946255baa209939b`, descriptor child
`058586f90026ea44ba510ac8183cfbc8fb33d4ed`) leaves new Core FORMAT enrollment
off for the mutation-fenced primary-only profile. Existing enrollment still
requires its actual inode and unchanged-file/parent durability barriers; normal
indexed enrollment and actual WAL/Page recovery remain unchanged. Three genuine
controls are defined for exact legacy bytes/live-backup refusal, missing enrolled
inode and existing-enrollment sync failures. Classification: **source correction;
not compiled or executed**. Original test/fixture bytes remain intact. Root and
independent source-review receipts respectively:
`3dc3f6a23cd53cc7f302e275f3e4ff2614f6463f946e072a00f775036bbb6436`,
`12d78f3206e9ef4aa3d1fcaf27aa6bc80e2b70ea72a06796ba0f6fab963ffae3`.
The failed source6de result is not reclassified by this untested successor.

Source6de SQL completed 282 summaries and 2,682 passing / zero failing / four
ignored case-result rows. Raw log: 295,903,295 bytes, SHA-256
`8591bf8f284cbb0ee3dd03ca0858154104d465e3385373860f4d7019b881414c`;
gzip SHA-256 `f18c2b482c81846d34744e39172ab82741c37818c8068fa9ed650bfab9ccdf0f`.
Independent complete raw-hash, checkout, summary and case verification receipt:
`baad4882d9065bde1168575947729dc0f605c3145e85329a07a53d90be8c59fd`.
The real known-committed error/DDL cleanup control passed at raw12418 and the
SQLSTATE mapping control at11875. This is component acceptance on source6de;
it does not test the later primary-only correction or remote Serving composition,
establish general distributed SQL, or override the failed Core job.

Source6de Native job132814521759 completed **375 passed / 37 failed / one
ignored**, 413 unique case-result rows, in 3,033.13 seconds. This is a failed CI
cohort, not a physical failover measurement. Complete raw log: 103,678,832 bytes,
SHA-256 `aec6a2b77679171285c9210039b7359df71339c604f0f189d1bef588bb27754d`;
gzip SHA-256 `3ab11e1e4093f1b3a5d9689b21f342bae840e0bc41a5733565e7bfb87c15215b`.
Independent complete raw-hash, summary and unique-case receipt SHA-256:
`4352596d7ac6d5c3d1369e5eca8501bf7b9cc5055c8e53a3673dcadf4a70f06a`.

| Actual boundary | Classification | Result and next evidence needed |
| --- | --- | --- |
| FIRST setup prerequisites | **Failed** | Twenty cases stop during setup: nineteen connection resets and one `service busy publishing Strong dispatcher`. These do not execute their intended later faults. Preserve the new all-worker returns and identify the first failing actor before changing timeouts. |
| FIRST over-512 exercise | **Failed** | Initial SDK work is reached, then the original full Strong RPC times out before the intended relocation exercise. Its budget and assertions remain intact; the cause is not established. |
| Hot Registered continuation | **Failed** | Fourteen cases reach actual Open then fail the first Registered policy helper; one fails a fresh same-candidate policy retry; one large unrelated-catalogue case reaches survivor OLD-observation retirement before failing. These are distinct cuts, not sixteen Open failures. The later source-only serving corrections have not executed. |
| Same-service identity wait expiry | **Passed, component CI scope** | The actual held-service/original-parent expiry control passes. It stops before ALL-OLD effects and establishes no full relocation acceptance. |
| Same-service identity wait release/cancellation | **Inconclusive for intended control** | Their cases fail setup: release on dispatcher publication contention and cancellation on connection reset. Intended held-lock outcomes are not tested by those failed executions. |
| Stale-gateway/catch-up and independent follower Apply | **Passed, component CI scope** | The original stale gateway recovers the full acknowledged payload during the real CQ gap/transfer (raw17424); follower Apply finishes before another range's global gap closes (17492). These do not execute the failed full host lifecycle. |
| Remote decision and final-frame metadata contention | **Passed, component CI scope** | All four remote-decision controls pass (22955–22970), including actual commit-sync Unknown with retained XID and postcommit cancellation reconciliation. All three final-frame metadata wait/release/cancel/expiry controls pass (23015–23025). Their original authority and attempt boundaries remain separate from the first genesis metadata read. |
| Integrated bounded recovery, automatic convergence and physical expansion | **Not tested** | New source composition is outside source6de. No physical loss, RPO/RTO, routing or capacity gate is cleared. |

The counts alone do not prove an improvement over source7c: source6de defines
four additional controls, and the reached failure cuts differ. Per-case cause
classification remains in progress. No error/no-effect, lost-ACK or stale-route
cause is inferred from a timeout or a missing diagnostic marker.

The complete 37-failure case/cut classifier SHA-256 is
`8524f2d09dc06b6030f91ff9a90e54ffa8b685b76088758d60c3990fe61c992a`.
Root independently matched all 413 actual names/lines/outcomes, all 37 failed
names, six exact source-file hashes and the cut category totals; receipt
`e0eba12c30ce0b26d3e791df37fdcdeec82bc446692c96d2ec01e3397e45fce6`.
All 409 source7c names remain: 361 pass in both, 33 fail in both, twelve change
from fail to pass, two change from pass to fail and one remains ignored. Of the
four additional definitions, two pass and two fail setup. These differing-source
outcomes are neither a causal comparison nor a scaling measurement.

The nineteen setup-reset cases retain 47 `INITIAL_NATIVE_READ_FAILURE`
events at `first_metadata`: 46 connection resets and one connection refusal.
Their original five-second parents still have 4,944,640–4,958,254 microseconds
remaining and are not cancelled. Original-parent expiry therefore does not
explain these immediate failures. Source inspection distinguishes the public
genesis `MetadataReadAcquire` path from enrollment's existing opt-in
`HostMetadataReadAcquire` socket recovery. Existing listener-transition controls
select the latter; their passes do not cover the former. A listener replacement
is a source susceptibility, **not a proven historical server-side reset cause**.
The next smallest deterministic control must drive the real genesis request
into the actual old listener backlog, perform shutdown/rebind on that same
metadata host, and require either a fresh authenticated read under the original
finite parent or the original cancel/expiry refusal before readiness publication.
Semantic, identity and mutation errors must remain terminal; increasing setup
budgets or serializing the fixture would not prove that recovery contract.

Source6de remaining-workspace job132814521575 passed **1,062 / zero / six**
case-result rows across 129 Rust summaries. Separate Python groups passed
47 platform, three Oracle HR and 34 Oracle reference controls; two notebook
Node controls passed. Complete raw log: 148,746,194 bytes, SHA-256
`6f43dcb5eff8acfc649f56e97ac3fb9e8e7c6e3baa96c46681b09f787da5181b`;
gzip SHA-256 `0e5fe3135641f773e166ea27ca85b6bbe46fae38a0dabdf0f729fb34cedbb30a`.
Root complete raw/summary/case verification receipt:
`52dc8d00019b86153e16c30903470a5876bfe86f6b2834575abbf631afc02c81`.
These component and tooling passes do not repair failed Core/Native acceptance,
execute the later startup corrections or qualify physical HA.

### Completed source7c: Core and Native failed; workspace and application components passed

Candidate **1.0.520-beta / `7c77418656a41ec6e25c7601319e061c328f48e1`**,
tree `df63298f61b0a4cdd56f0caabc694d888c38dbda`, was checked out by CI as
`25b2a1454450bd591364ce9d026a8469ab03c2fe` with the identical tree.
This is a completed failed execution checkpoint of [Tests43704937146](https://github.com/nikoma/bicdb/actions/runs/43704937146),
not a qualified release. The complete archive is frozen and independently verified.

| Scope | Classification | Result and limit |
| --- | --- | --- |
| Workspace/all-target and no-default Core consumer | **Passed compilation** | Test channel type correction compiles. Compilation does not execute HA cases. |
| Core | **Failed** | Library 975 passed / 0 failed / 8 ignored. All eight visibility, three Standard, eleven bootstrap and four new rekey controls passed. Later backup target: 9 passed / 1 failed; 30 additional backup cases were not executed. |
| PGwire | **Passed, CI scope** | 511 passed / 0 failed / 2 ignored; Oracle HR50, embedded HA11, verification3 and protocol195 are separate component scopes. |
| Rails | **Passed, application CI scope** | 20 runs / 100 assertions, zero failures/errors/skips, including process-kill recovery. |
| SQL | **Passed, CI scope** | 2,682 passed / 0 failed / 4 ignored across 282 summaries; actual committed-error/DDL cleanup and SQLSTATE controls passed. This is not distributed SQL or Native timed-Unknown acceptance. |
| Native multi-node | **Failed** | 363 passed / 45 failed / 1 ignored; 409 unique outcomes. Exact failure cuts and separate passed components are retained below. |
| Remaining workspace | **Passed, CI scope** | 1,062 passed / 0 failed / 6 ignored across 129 summaries and 1,068 unique cases. Original Cell key rotation and CLI/application component controls passed. This does not qualify distributed movement or close the cross-call source gaps. |
| Independent physical expansion | **Not tested** | Original Azure qualification remains failed; no stable five-node throughput gain or production admission. |

The backup failure is `encrypted_full_backup_verifies_and_restores_records`
at `crates/bicdb-core/tests/backup.rs:235`: actual capabilities include
`physical_backup_gate_v1` alongside `format_metadata_v2`, while the assertion
expects only the latter. Backup creation returned successfully; later plaintext,
verification and restore assertions were **not reached**, so this is not an
observed restore failure or data-loss result. Source correction
`2e4ddcbe882632613d89b8415f2d33f9f71a9f66` changes only the two exact expected
capability vectors. It is **not tested** and remains local while useful CI jobs
finish. Historical input fixtures and all restore/payload assertions are retained.

Complete Core raw log: 2,433,064 bytes, SHA-256
`fea8f65fa0a030ae47609fd9da94e2ffa00ae2f2f922dd881eca134c220c96d7`.
Complete SQL raw log: 295,655,118 bytes, SHA-256
`742b900d47cd4d90c1b1649df51a302da61cc5e065ad24c7704567e24835b5c1`.
Independent full-log/checkout/case/summary receipt SHA-256:
`9eab149b2dcf9c05431732937167488bdba3ee9a1b73662d68deb97e5ec47a94`.

The remaining-workspace raw log is 148,670,739 bytes, SHA-256
`f933b3483fbf5fb787b4a17727535bf0c400f2f92c3c9034e4d4f4ff50e251e9`;
gzip SHA-256 `c05b2c9566a77d8403100193f28a7ebdc9fb3962647dbdc5f2cccd80f4cd5081`.
Independent complete raw-hash, checkout, case and summary receipt SHA-256:
`577502e55151eaea63fd08a4d1472f68db7d2f5db01001b9d584d26123d79554`.
Original Cell signed key rotation passed at raw307786, and the original schedule
identity/SQL control passed at283067. These execute the source7c corrections;
they do not inherit success for the later local backup, diagnostic, Open-budget
or FIRST contention changes, which remain uncompiled and unexecuted.

Passing same-attempt controls do not close the documented cross-call bootstrap
association or Prepared-rekey source-head gaps.
The [deferred final-tail design](bootstrap-deferred-final-tail-7c.md) specifies
both ordinary absent/existing-target association and latest closed-target
finalization. It is **not implemented or tested**; native final acknowledged-prefix
coverage remains an explicit missing producer. The separate
[root-local fence refinement](bootstrap-root-local-fence-refinement.md) specifies
shared IO exclusion, ancestor Page admission and pending-marker/FI ordering;
it is **design only**, with runtime wiring and crash qualification outstanding.

### Completed source7c Native job: exact failure boundaries

[Native132783896405](https://github.com/nikoma/bicdb/actions/runs/43704937146/job/132783896405)
compiled and ran **363 passed / 45 failed / 1 ignored**, with 983 filtered out.
This is a failed acceptance result. Its complete raw log is 42,835,265 bytes,
SHA-256 `d64ff32826c1acd4a59aa4819be4ea0933a9ed5c8effa4b1ae6201d99aa83172`;
gzip 3,594,128 bytes, SHA-256
`e8808425833f37cc6acacf5d161c627982daaf5f90d1091abb7fc5b4f5b46110`.
Independent full-log/case inventory receipt:
`c7883e3929c3f6027a078b885bf2a7edad409be14cb150de661095d2d0575172`.
The complete archive contains 96 premanifest files, 99 total, 11 full gzip logs
and eight original ZIPs/28 members, with no missing or partial captures. Manifest
SHA-256: `d2da4de6a06ae9b6fd8663c64e90e5b8cbd0ad1f8fea411a6d20462baf6b7967`;
independent full-archive verification receipt SHA-256:
`39a3508a61ea7ea7017cae52ffaa44a001d9b2744d27537c6940d3785002881b`.
Verification covers every stored/raw hash, exact source/tree inventories,
original ZIP API digests/sizes, CRC and member hashes. The verifier passed its
positive control and rejected a wrong manifest digest. The sole collector
exited naturally; no job was cancelled or restarted. Archive integrity preserves
the failed result and does not qualify the database or untested successors.

| Actual failed scope | Classification | Observed cut and limit |
| --- | --- | --- |
| 21 FIRST startup cases | **Failed prerequisite; later cuts not tested** | Normal parallel INITIAL DATA opening returned ConnectionReset/ConnectionRefused. Intended later FIRST/fault assertions were not reached. Exact inner read/socket cause remains inconclusive. |
| Cross retained-discovery exclusion | **Partial pass, later failed** | Opposing FIRST refusal and before/after positions passed; later original SDK APPEND failed with a range-policy refresh refusal. |
| Over512 | **Partial pass, later failed before allocation** | 45 APP acknowledgments and 44 full Strong reads passed; the next Strong call hit its original two-second RPC deadline. The over512 transfer/fault cut was not reached. This is not observed successful absence or proven data loss. |
| Catalogue rename-unknown | **Partial pass, later failed** | Two APP/full-Strong pairs and allocation/enrollment prerequisites passed; ALL_OLD_close failed `joint metadata service identity busy` before the catalogue fault. |
| 20 hot catalogue Open cases | **Failed** | `hot_policy_tests.rs:991`: first `actual_catalogue_open` returned QueryTimedOut. Release/policy and their later selected APP/Strong tails were not reached. Trace intervals do not establish CPU/IO root cause or absence of effects. |
| Large unrelated-head activation | **Failed, intended fault not tested** | `activation_owned_tests.rs:127` expected fault-fired1, observed0. Intended owned-IO/Unknown/refsync assertions were not reached. |

Separate current-source component passes include typed native-local follower
apply (raw17516), combined stale-gateway/catch-up-gap transfer recovery17448,
source-frame controls21135/21138/21141 and post-scope archive/middle-history
controls20953/20956. They do not qualify the failed cases' unreached tails or a
physical/operator expansion. All400 earlier326 names and all349 main519 names
are present, but changed helpers/budgets preclude causal or performance inference
from matching names alone.

The comprehensive 45-case/source-cut classifier SHA-256 is
`f88af999ada1689060b5bce34fc3134dae5eec78b0225c70c48d8c76ddde9311`.
Direct stderr events precede buffered failure footers; the separately retained
serial-case projection SHA-256 is
`72cb76e227e01c8adec6fc1b18a83b3d57a3fcb77a10a9cfecb9356ae30664ac`.

The [INITIAL diagnostic correction](native-initial-open-failure-evidence.md)
retains every real worker result and read failure boundary without retries or
budget changes; it does **not** fix the socket failures. Separate source
`fe8618729236c63446830254f76a9a560e87ff4a` removes redundant Open validation and
adds a genuine cancellation control, retaining original1500ms windows and full
publication/owned-IO checks. It is **not compiled or executed**; saved time and
sufficiency to resolve the 21 hot failures remain unproven. No failed case is
reclassified as passing by these untested source changes.

The separate [FIRST same-OWN identity wait](native-first-same-own-metadata-identity-wait.md),
source `90259379d227d66fad18adde38b0748b2c6fe6ca`, addresses the observed immediate
service-identity contention refusal. Only the guard-free FIRST coordinator uses
bounded acquisition of the same service mutex, under its original request and
OWN deadlines; the original one-shot API and subsequent mutation/intent/RPC
paths remain unchanged. Three genuine held-service controls are defined for
release, cancellation and expiry. Classification: **source change; not compiled
or tested**. The historical holder remains unidentified, and the original
catalogue-rename acceptance is still failed at the earlier cut. This is neither
a mutation retry nor proof of bounded end-to-end relocation.

Independent full-source review receipt SHA-256:
`4a73eaa9d56ac436408b82b9f6f8d6c043b762c0241a4047a77a6fac7aebafd6`.
That review checks the protected metadata-capture suffix and the FIRST
coordinator's unchanged mutation paths; it does not execute the controls.

### Historical pre-CI review of the source7c corrections

The pending batch composes test-channel correction
`781b4e8029f0e8c747fee69252c94a9f04992103` and Cell gate-lifecycle correction
`118bb5ecd7fe0e2fe720cd42b163f4522e5a6d22` (reviewed leaf
`ff791fd2883a3f5b44b238727c1fbfaca9430e94`). The
[Cell correction contract](cell-rotation-physical-gate-lifecycle.md) retains
actual closed root/stage claims, a new stage gate before FORMAT copying, and
strict retry/activation checks. It adds four Core and one Page control while
preserving original tests. Formatting, source pins and independent source
review passed; compilation and every old/new runtime control are **not tested**
on this batch. Earlier component passes below do not qualify it.

### Completed head2ce: application passes, HA compilation and Cell rotation failures

This is a **completed, failed CI checkpoint**, not a qualified release.
Source **`2ce4c48202d4d1bb1ef759b08a41dc36f431af6d`**, tree
`f4d72164c2a2a906857b179d44df216d4457b9f8`, was the public PR207 head at that checkpoint. Actual
checkout `e1ef338cb41d51d4d9dfbc8660502e4afbc3f366` has that identical tree.
[Tests43696050976](https://github.com/nikoma/bicdb/actions/runs/43696050976)
failed; Rails completed successfully. All logs and original artifacts are
frozen and independently verified. No useful job was cancelled or restarted.

| Scope | Classification | Exact result and limit |
| --- | --- | --- |
| All-target, Core and Native | **Failed compilation** | Core test `storage.rs:2910` has E0282 for an unconstrained `mpsc::channel()` payload. Core/Native helper builds passed, but library-test compilation failed with zero actual Core/Native cases. The nondefault-consumer check was skipped. |
| PGwire | **Passed, CI scope** | 511 passed / 0 failed / 2 ignored across target summaries. Separate Oracle HR: 50/0; embedded HA: 11/0; verification: 3/0. These do not exercise the complete distributed handoff or physical expansion. |
| Rails | **Passed, application CI scope** | [Rails43696050973](https://github.com/nikoma/bicdb/actions/runs/43696050973): 20 runs, 100 assertions, zero failures/errors/skips; real queue/cache process-kill recovery, 24 completed reports and 24 cable messages. This does not qualify distributed HA. |
| SQL | **Passed, CI scope** | 2,682 passed / 0 failed / 4 ignored across 282 summaries. The actual committed-finalization error/DDL cleanup and SQLSTATE mapping controls passed. This is not timed visibility-Unknown or distributed SQL acceptance. |
| Remaining workspace | **Failed** | 851 passed / 1 failed / 1 ignored across 61 summaries and 853 unique case names. Cell signed key rotation failed at `bicdb-cell/src/lib.rs:7589` with `Database(Backup("enrolled physical backup gate is missing"))`; Cell target 39/1/0. Activated/reopen/canary tails and later CLI/AppHost targets were not reached. |
| Page components | **Passed, CI scope** | Checksum/no-repair at raw1396, paired segmented/monolithic gated profiles2649, tracked relayout3855, retained claims4350/4351 and moved-claim4512. Page unit346/0 plus integration152/0. This does not execute Core's backup/bootstrap or HA controls. |
| Physical expansion and release qualification | **Not tested** | No new Azure experiment, stable five-node throughput comparison or production admission. |

The completed raw PGwire log is 1,323,097 bytes, SHA-256
`6ad1a62b5f5256fbbf176f7e04061ab74cec6592889a835f2c88f0a923d3f099`;
Rails is 1,185,534 bytes, SHA-256
`3978cc0707f57abac7640839e1b0b20f9bce7c88bb941d94167f40246a4a691c`.
SQL is 295,664,542 bytes, SHA-256
`59c5698e2bb6ea488349669edee4ec3488d9f9dcd70eebb6c39e1abcf8976bf6`;
its complete gzip is 35,058,855 bytes, SHA-256
`29d2eb64d5706bb9f71f54b9cfd3a2670645e489e2ca460706887cfd4cbc3d74`.
Independent raw-hash/case/summary receipt SHA-256:
`3aa6a420f16ac32430ab6c0e02d0369af98865008be70e684e2b316dc431ee54`.
The committed-error case passed at raw line12410; SQLSTATE mapping at11867.
The former uses real public transaction/model APIs and checks the original
`BicDbError::Memory` containing the synchronous indexing DimensionMismatch
detail, detached committed transaction/DDL undo, preserved row/DDL/GUC after
later ROLLBACK and a fresh SQL session, and healthy recovery with a failed
derived job. It does not invoke the separate Core timed-Unknown cut or establish
HA/failover behavior. Core's eight visibility controls remain unexecuted here.
Remaining-workspace raw log is 39,194,032 bytes, SHA-256
`2c5178564320d017b562fb54dad65516f9c7b20edc31214c45577d470dccb535`;
gzip SHA-256 `a6fd268fc6662bac8629ae8cca9b5b1558844b0f41a9e4fb25c57eafd6b7329f`.
Independent full-log/case/cut receipt SHA-256:
`5065ceb6c7b6a55740e36d475e4ffd9496df20f5526280c3c628a1de6735aa74`.
The failed Cell unwrap/error is at raw307795–307796; the raw error does not name
the internal rotation path or checkpoint. Source inspection finds that rotation
deliberately omits the source gate while copying enrolled FORMAT into a fresh
stage; later guarded stage writes then require a gate that was not initialized.
This is an integration defect repaired at source through actual stage ownership
and a new stable gate; the mandatory check remains. The composed correction
above remains uncompiled/unexecuted. Core visibility/Standard/bootstrap
and the later CLI/application-port acceptance remain **not tested** here.

The complete archive contains 89 premanifest files, 92 total, 11 complete gzip
logs and seven original ZIPs/24 members, with no missing or partial captures.
Manifest SHA-256:
`eaf355e7d77c1b41813a16547d4bc1848be9bf0fe7085a6e65939aa68b1b36e5`;
independent full-archive verification receipt SHA-256:
`74af72b27b7d31c39c3d9f4e67dbef702d7b79ef67ee4d8566c2afe6c794893b`.
Verification covers exact source/tree/API inventories, every stored/raw hash,
original ZIP API digest/size, CRC and member hashes. The original collector
exited naturally; no replacement collector was started. Archive integrity does
not qualify database behavior or the local successor fixes.

Local correction **`781b4e8029f0e8c747fee69252c94a9f04992103`** specifies
`mpsc::channel::<()>()` in the failed test. Only dropping the sender releases the
existing receive; no payload is sent. The actual thread-held shared guard,
timeouts, ordering and assertions remain unchanged. This correction has source
review and standalone formatting only; it has not been compiled or executed.
The smallest next check is compilation of the actual Core test target followed
by the unchanged backup exclusion control and required broad suites. The
original FIRST/Strong/ACK, cold recovery and controller gates remain open.

An independent source review also confirms the existing
[cross-call bootstrap association gap](standard-live-backup-physical-gate.md):
an originally absent target can be created and acknowledged by a normal opener,
survive the first install's no-replace refusal, then be claimed and replaced by
a distinct finish call after that owner closes. The original live-owner race
control stops before this fresh call. Classification: **failed source safety
review; runtime reproduction not tested**. The next deterministic regression
must retain both real acknowledged records and the first error, close the real
owner, invoke a distinct finish and require target/stage preservation plus safe
subsequent recovery. Original-target and renamed-predecessor association must
survive restart; a RAM flag or newly acquired DirectoryLock does not supply it.
This is not an established cause of the historical Azure visibility failures.

The separate [Prepared key-rotation source-head gap](cell-key-rotation-prepared-source-head-gap.md)
also remains open: per-call closed ownership does not prevent ordinary source
writes between preparation and a later resume. The staged digest alone cannot
prove inclusion of those writes. Its deterministic regression is unexecuted;
the retained retired ciphertext distinguishes active-view rollback risk from
durable erasure. The physical-gate repair does not close this boundary.

### Earlier head0a: Core guard compilation and Page profile assertion failures

Source **`0a04ccc06db33a04d254a5e00776248d7ea1772a`**, tree `06a13e5accafa331271049a9ae44f1bf3628661a`, failed [Tests 43694564959](https://github.com/nikoma/bicdb/actions/runs/43694564959) and [Rails 43694565004](https://github.com/nikoma/bicdb/actions/runs/43694565004). Actual checkout `8295fbbb5e0c724ddbfe2d6325c30805b847a568` has the identical tree. Six jobs failed Core `E0277` at `paged_collection.rs:494`: the facade promises `Result<impl Drop>`, but the new owning Page guard has field cleanup without an explicit `Drop` implementation. Core and Native stopped in projection-helper compilation with zero actual cases; SQL, PGwire/Oracle, Rails and nondefault runtime acceptance were not reached.

Remaining-workspace reached **384 case-result lines: 383 passed, 1 failed**, over 11 target summaries. Page unit tests passed **346/0**, including the new `physical_gate_profile_requires_actual_page_checksum_and_never_repairs_it` at raw line1358. This is an executed checksum/read-only component result. The later `extent_segments` target failed **6/1** at its original `segmented_superblock_uses_v2_magic_and_legacy_stays_v1` assertion137: actual `BICDBPG6`, expected `BICDBPG2`. The paired monolithic assertion142 was not reached. Later retained-claim, CLI and application-port acceptance was not reached. Do not classify this workspace result as another compile failure or infer a data-loss/transaction outcome from the format assertion.

The complete archive is frozen: 86 premanifest files, 89 total, 11 full gzip logs and six original ZIPs/six members. Manifest SHA-256: `8584a08b5c072b0c2b5f82490e18e02949c3973f5553d2da40960a56b1f10d3b`; independent full-archive receipt SHA-256: `7536d815a45611a893c724dd03a3a29f1f2ebafaecc42c7ade97a2953449a7d7`; independent seven-job raw execution classification SHA-256: `95a8a1e5aca4120836e7518e1e2ac89f4955e90138d9bbc00b2c2b07cee11a27`. Workspace raw SHA-256: `d2d755c06f12a42d2bbcfb6c5bc1dafa9da3978e24d313aad36c1af250de4902`, 750,532 bytes. Native raw SHA-256: `952d8b089c42d2fb54bcdc54026df439559d1e35b7a799a6c7ba55915f5ff73e`, 157,798 bytes. The collector exited naturally after complete capture; no jobs were restarted or cancelled.

Source correction `5e292cf7c1a58d92e72b3cb1523ef7356e9c43ef` preserves the facade's opaque public API and adds explicit `Drop` to its real owning guard. Its original writer/physical fields and automatic release order remain unchanged. The paired [profile test correction](page-gated-profile-test-correction.md), unit `e8acc74054aa69c5e43eb395bae199fb03d7ddba`, requires exact mandatory gated profiles5/6/7/8 while retaining original layout/relayout/reopen assertions and historical input fixtures. The tracked-output mismatch was found by source inspection after the actual first failure; it was not executed in this run. Those successors are uncompiled/unexecuted at this checkpoint. Source-byte comparisons with the historical accepted set are not execution of an old binary. Complete exact-source CI and original HA acceptance remain required.

### Later composed head8bc: compilation failed before HA acceptance

Composed source **`8bc2df66361a3d4307b2943439a8a9d2b4fa0b92`**, tree `ce2dcceb3f8c0fddb34ce64dfc7566018c848637`, failed [Tests43693587297](https://github.com/nikoma/bicdb/actions/runs/43693587297) and [Rails43693587277](https://github.com/nikoma/bicdb/actions/runs/43693587277). Actual checkout `6b96f5507e0619bef1c7284282885dd2372eb9f2` has that exact tree. Seven jobs failed the same production Page `E0308`: `manager.rs:2841` passed `decode_superblock(path, &bytes, size)` while its actual signature at line2748 requires bytes, path, size. This is an executed compiler failure, not failed HA assertions or unavailable CI.

Core and Native failed while building their projection helper, with **zero actual case or summary lines** and their test steps skipped. PGwire/Oracle, SQL, Rails runtime and the nondefault consumer check were not reached. Remaining-workspace retained five earlier successful Lua runtime/workflow cases before hitting the same Page compile error; CLI election, original app-port and new port-owner diagnostics were not reached. Older326 passes below cannot be inherited by changed8bc source.

The complete8bc archive is frozen: 86 premanifest files, 89 total, 11 full gzip logs, six original ZIPs/six members, no missing/partial captures. Manifest SHA-256: `e2db464029f39830458c18f96199a4be8f4f130d7f68c5623efbc12fb64bf18f`; independent full-archive verification receipt SHA-256: `0f0b0020eb55fcbb7d72f5cb96050c60da37825632277977cbecd613e40db575`. Raw Core log: 109,705 bytes, SHA-256 `1a008f0b229143caeb218b50e179f2632de1c916432450eca81e28f0a804df0a`; Native: 110,193 bytes, SHA-256 `d684e10250e6ee87e75296c26ea9330cc1f04ebb3a64bbc0d7b1ba801285ab05`. The collector exited naturally; no useful job was cancelled or restarted.

Local successor **`327a8935f15dc3a3deba018ed1722a5321a17680`** corrects the argument order. Source review also found a separate missing precondition: the shared decoder assumes a page whose checksum/trailer was already verified. The reader-fence helper now calls the actual page verifier before decoding. A genuine fsynced Page file under actual EX is checked, damaged in its body while preserving profile/size, required to return `ChecksumMismatch` without repair, restored and reopened. This corruption control is **not executed**, and the missing check is a source finding rather than observed corruption. The successor has formatting/preservation review only; compilation and all original/new acceptance remain pending.

### Earlier source326: executed runtime results

[Tests run 43682851011](https://github.com/nikoma/bicdb/actions/runs/43682851011) passed all-target and nondefault-consumer compilation, Core, SQL and PGwire/Oracle checks. [Rails run 43682850892](https://github.com/nikoma/bicdb/actions/runs/43682850892) passed its actual application checks. These are separate scopes.

[Native job 132717026027](https://github.com/nikoma/bicdb/actions/runs/43682851011/job/132717026027) failed: **372 passed, 27 failed, 1 ignored**, with 956 filtered out, in 2,334.11 seconds. All 400 individual result lines and the complete raw log were independently verified. The raw log is 35,508,424 bytes, SHA-256 `3deb1461fa4fb269c54e3a6adacafa91623e8f6c69ba221fae7777ca06e38046`; its retained gzip is 3,239,240 bytes, SHA-256 `4d1f868fec4fc19118f3b236d34d75572ba261359a58ab97c2e4fa2a7ad2f4f2`. Independent result receipt SHA-256: `9a214208afd00ddf37d714e1f6a17428d99408d41bd5d97f0db0405238c34942`.

The [remaining-workspace job 132717026000](https://github.com/nikoma/bicdb/actions/runs/43682851011/job/132717026000) also failed, independently of the native suite. It failed `native_metadata_operator_actual_cli_single_join_fresh_becomes_local_metadata_voter` at `crates/bicdb-cli/tests/native_metadata_operator_cli.rs:76`: `operator elect failed: Error: native metadata action refused: "cluster error: native host bootstrap: owned learner metadata status busy"`. The CLI target finished 5 passed / 1 failed. Its actual schedule control had passed earlier; the main519 application-state port-bind test was not reached and receives no candidate326 passing coverage.

The complete workspace raw log is 91,375,658 bytes, SHA-256 `71db759ba79963909dbc6f6edcd3fbf41d23271f6f60d0512329e1b79e2c05da`; retained gzip SHA-256 `118ddf7b4e851d0725c303d11fb3c6a105264a24ac2e8764e791fcb0c144457a`. Independent raw-log/failure-cut receipt SHA-256: `94b12a7d243b250b5c361bab6afaa3a131c4e783df41413b2df9eb908ac1c0d7`.

All jobs have terminated. The complete candidate archive is frozen: 98 premanifest files, 101 total files, 11 full gzip logs and eight original artifact ZIPs containing 61 members, with no missing or partial captures. Manifest SHA-256: `1d92548c8b49f6089eb32d3b03260a35f4721cab4aaeb34a05aee8e80828891e`; independent full-archive verification receipt SHA-256: `952619a06d69e84ce753c44576c2e27f4ff3f1638d1d8998f12938a705c97658`. Verification covers stored and decompressed hashes, exact API source/job/artifact inventories, ZIP API digests/sizes, CRC and member hashes. The collector exited naturally; no useful job was cancelled or restarted. Archive integrity does not qualify database behavior.

| Finding | Classification | What the evidence establishes |
|---|---|---|
| Entire native suite | **Failed** | Twenty-seven actual test failures; this candidate cannot pass its merge gate. |
| Remaining workspace | **Failed** | The CLI join test failed on a busy owned-learner metadata status read. Later application-state tests were not reached. |
| Real follower apply with another range's global visibility gap | **Passed, component scope** | `native_local_rf3_real_follower_apply_completes_before_other_range_global_gap_closes` passed, raw line 17254. This does not qualify the complete handoff or physical deployment. |
| Partial-setup cleanup phase check | **Failed test; source correction untested** | Actual JSON phase was `stopped`; the assertion expected `Stopped`. The existing enum has `serde(rename_all = "snake_case")`. The control did not establish its complete two-cut cleanup result. |
| Two surviving-leader hot controls | **Failed tests; downstream guarantees not tested in these invocations** | Their first genuine recovery call passed `recovered >= 1`. An immediately repeated recovery returned zero; the second assertion failed before the applying probe and subsequent acknowledged-payload/Strong checks. |
| Other FIRST/cross-protocol failures | **Failed tests; causes under investigation** | Actual errors include connection reset/refusal during initial-data opening, changed range configuration, changed source metadata, absent original native decision and changed metadata acquisition authority. An expected injected fault return must be distinguished from the eventual unexpected test failure. |
| Metadata-owner catalogue certification | **Failed path with a source authority mismatch** | The metadata owner asks for a range-leader-only metadata observation to construct its collection plan. A metadata leader distinct from the range leader cannot pass that request's authority check. This requires a separate read-only certification scope, not a weaker range-leader predicate. |
| Large unrelated-catalogue control | **Failed test with passed earlier components; timeout cause inconclusive** | The real directory-sync fault fired once; fresh recovery, same native/Frozen ownership and retained-intent size above the former 8 MiB aggregate cap passed. Large-entry preservation verification then passed. The test later ended with `QueryTimedOut` at `hot_policy_tests/data_tests.rs:105` during DATA-policy installation. This is not complete capacity/DATA-availability qualification, evidence of acknowledged-data loss or proof of a particular performance bottleneck. |
| Physical expansion, cold recovery and sustained capacity gain | **Not tested by this run** | These are in-process CI controls; they do not replace the independent four-to-five-node experiment. |

## Reproducible test corrections

### Follow-up source status at the source326 checkpoint

At this historical checkpoint the public candidate was `326c363477477950f76bdb51fe6425cab85dc362`, with the failed execution above. The following corrections were separate source units without compilation or runtime results at that checkpoint. Later exact-source execution is recorded above; it does not establish unexecuted acceptance cuts:

| Source unit | Source status | Remaining execution gate |
|---|---|---|
| Owned-learner status acquisition, `6c29102c617564fb3a87e73abc8959b03c7b1798`, composed locally as `e4edfe0790f2d699a58d3649a3d240f6fe183b34` | Bounded wait on the same actual metadata mutex, using the existing caller/OWN window. Release, cancellation and expiry controls defined. The original election is not repeated. | Compile and execute those controls and the unchanged failing CLI test. Cold status uses the helper but has no separate executed held-lock control. |
| Initial listener transition, `a179fce0f8b5304848af5ed4f50b2c8f1c1c57ea` | Locally composed source unit. Only the pre-enrollment read retries privately identified socket failures within the original action deadline and actual service open/slot. Accepted authenticated worker drain and an unaccepted backlog socket have distinct controls. | Compile, reach both actual socket cuts, then pass unchanged full initial-data/FIRST acceptance. The component fixture retains later genesis errors and does not assert full DATA readiness. |
| Backup/bootstrap ownership, `38d15efbb815e673859ebfd8b67a450acdba322d` plus `d6760a938ceb6983abcc28196239864a7764ad13` and `c81a26981ddc86c2215040e11041d3c91a7cad76` | Locally composed source units. Retained physical custody covers selected writers and both restore codecs' HA/READY tails. Namespace publication uses actual held owners and atomic no-replace renames; an interrupted predecessor remains until parent sync. | Compile and execute genuine contention, retained-child, both-codec interruption, namespace/preimage cuts and original backup/bootstrap regressions. Cross-call target intent and replaced-tree logical association remain open cold-protocol blockers. Windows held-handle rename and old pre-format bootstrap remain unqualified. |
| Final source-frame metadata acquisition, `62313619ddd7fa2625067e169c95218e65cd5e06`; controls `72740ea03f02370920dc8fb7d5510741b30fea93` plus `048be25abc9ef7679e31b4c78e1dc9fe23b94dc0` | Locally composed correction and three genuine final-frame held-lock control definitions. The child signals the actual server result only after releasing the session mutex, avoiding a false diagnostic-busy race. | Compile and execute real same-session release, caller cancellation with OWN live, and original earliest-scope expiry; retain separate wire/server outcomes and the full unchanged frame/payload assertions. Independent OWN-only expiry is not established by the coupled expiry definition. |
| Metadata-owner catalogue certification, `8ea6251fab97b913efdf807d30223ce8810135b4` | Locally composed source unit. Distinct read-only actual metadata-owner scope; strict range-leader/native-role factories preserved. Separate TLS guard and new-control expiry/guard-lifetime corrections are retained. | Compile and execute distinct-owner, foreign-owner, cancellation, coupled-expiry, exact ancestry and metadata-version controls plus full FIRST acceptance. Independent OWN-only expiry and bounded progress under unrelated metadata churn are not established. |

Independent source review is partial and does not substitute for these gates. No replacement CI candidate, new binary, merged correction or physical experiment is established by these source units.

The cleanup failure is in `crates/bicdb-core/src/distribution_native_host/provisional_listener/tests/first_owned_tests.rs`, test `native_first_partial_normal_setup_error_and_panic_stop_returned_supervisors`. Raw lines 21756–21759 show the assertion and its two values. `distribution_native_host/operator.rs` declares the serialized phase convention; `operator/first_acceptance.rs::test_first_stop_state` serializes the actual enum. Local correction `378e62bb9acb2af4e21ae96e1f6a9e8594336c0f` changes only the expected string to `stopped`. Actual stop success, absent supervisor and stopped control-listener assertions remain required.

The two hot failures are `native_registered_fifth_remote_hot_data_follower_binds_without_fake_leader_ready` and `native_registered_fifth_remote_hot_surviving_leader_actual_pending_callback_rejects_foreign_open`. Both execute `hot_policy_tests/data_tests.rs::exercise`. At the tested source it called the same surviving leader's `recover_initial_data_leadership` twice, with no intervening epoch change. `distribution_range_consensus.rs::recover_local_leadership` selects only epochs absent from `recovered_epochs` and counts newly completed recoveries. Thus zero from the second call does not establish failure of the first recovery. Raw failure lines 89543–89544 and 117867–117868 precede the applying probe and original-payload read loop.

Local correction `b1f84e02e8deebef5c54f4e35c3207510effe9d8` invokes that real recovery once through the existing operation ledger, retains its actual `(leader, recovered)` result and requires both original positive-count assertions. It retains the original five-second recovery budget, follower-not-leader checks, foreign-open probe and complete subsequent ACK/Strong/write assertions. No production authority or readiness is synthesized. Passing source checks do not establish passing execution.

The smallest next controls are the existing exact cleanup test and the two exact hot tests on the corrected source. They must reach their intended fault/read/write cuts and complete all original assertions. A missing cut remains a failing or inconclusive prerequisite. Do not extend their deadlines, skip their assertions or treat a safe refusal as successful recovery.

For the initial-data connection failures, the next deterministic test should hold a real peer at the control-to-DATA listener transition while another host performs its original finite `OpenInitialData` action. Preserve the actual transport error, retained owner and original deadline; then prove bounded continuation with that owner after the listener resumes. The current logs alone do not identify the causal interleaving. For changed-metadata failures, independently advance unrelated metadata and selected range membership at explicit barriers; distinguish legitimate invalidation from unnecessary failure under unrelated progress before changing any freshness rule.

The FIRST failure inventory is 17 setup failures before the requested exercise, one cross-protocol exercise failure, five other exercise failures and one cleanup serialization mismatch. At raw line 20448 a genuine OLD-recipient native-decision request returned service contention; its subsequent known read at line 20451 failed because original D was absent. That sequence must be distinguished from a retained unknown attempt. The three passing FIRST cases were lost-Close read-only resolution, native-decision reply-loss read-only resolution and server-cancelled queued-request handling. These individual controls do not turn the failed suite into a passing handoff qualification.

The exact source of `source metadata changed before frame` is another important distinction: `distribution_range_image/remote_source.rs` returned that text when its final metadata `try_lock()` failed, before `proof.require_current` could check freshness. The matching runtime error therefore establishes mutex contention at that point, rather than an observed topology change. Local source correction `62313619ddd7fa2625067e169c95218e65cd5e06` waits on that same metadata owner within the minimum original caller, OWN and source-session windows. It rechecks cancellation/expiry after acquisition and retains the unchanged physical/native freshness predicates. It also applies those original-window checks to the earlier metadata acquisition in that same observation. Formatting passed; actual held-lock release/cancellation/expiry controls and compilation remain pending. This is not a blanket retry of source frames or native mutations.

The errors at raw lines 20124 and 20728 occurred in `actual_metadata_owner_independent_dual_durability_certification`. At the tested source, `distribution_native_host/provisional_listener/registration/first_catalogue_certification.rs:123` calls `acquire_metadata_read_observation` with the metadata owner's node ID and the selected capture range, then starts `NativeJointOldRound` to obtain a plan. `distribution_consensus.rs::validate_acquire_requester` requires that requester to be the range's leader. The FIRST fixture deliberately selects a range whose leader differs from the metadata leader. The certification path therefore applies the wrong authority requirement for that supported role separation. This is distinct from a busy mutex and must not receive a generic transport retry.

The correction needs a distinct read-only catalogue-certification scope produced from the actual metadata owner's HostOWN and protected metadata context. It must compare the complete pending manifest, birth/pin/schema/restore identities and original OLD-close ancestry, independently collect actual recipient native/catalogue durability, and retain fresh metadata-owner validation before proposing the candidate. It must not convert a HostOWN observation into a recovery role, local OLD quorum or native mutation plan. The composed [source contract](metadata-owner-catalogue-certification-scope.md), final unit `8ea6251fab97b913efdf807d30223ce8810135b4`, defines that path and genuine refusal controls while preserving the existing distinct-leader tests. Neither fixture co-location nor a weaker `validate_acquire_requester` is used. Compilation and execution remain **not tested**. Its original protected metadata version deliberately fails closed on unrelated changes; this is not bounded-convergence qualification.

## Follow-up bootstrap source gap

Review of the unexecuted `38d15efbb815e673859ebfd8b67a450acdba322d` unit confirmed that optional ownership captured for an absent old/replaced tree did not authorize later mutation of a newly appearing tree. `finish_standby_bootstrap` could subsequently use fresh path-presence checks to rename/remove it; its completion helper could also remove the current replaced path without an actual retained owner. A normal Core opener which passed its bootstrap preflight before staging appeared can create that root in the intervening window. An optional absent conventional Page child has a related standalone-Page-open race. These are confirmed source gaps; a runtime failure or historical data-loss cause has **not been tested**.

The corrective source units `d6760a938ceb6983abcc28196239864a7764ad13` and `c81a26981ddc86c2215040e11041d3c91a7cad76` retain actual owner/FD identity across moves, use atomic no-replace publication, refuse newly present unowned trees and reserve the conventional child through a genuine DirectoryLock. The separate moved-path observation preserves the strict writer-transfer validator and opens no extra Unix `store.lock` descriptor. Genuine normal Core/Page controls are defined at the preflight, custody and completion boundaries. They retain full locally committed payloads; they are not HA/SDK ACK or physical evidence. A repeated second-rename/parent-sync fault control also requires the original predecessor to remain until the durability cut. All these definitions remain **not tested**.

These changes close only the examined same-attempt source gaps. They do not persist the original target intent across calls: after the initial no-clobber error and a competing owner's later close, a fresh installer may claim that newly existing root while READY remains. A physical claim also does not prove the logical association of a replaced tree after a crash. Those explicit cold/recovery protocol blockers remain open; passing the new controls cannot establish their closure. See the [physical-gate contract](standard-live-backup-physical-gate.md).

## Separate main519 workspace failure

Main **1.0.519-beta / `587dd6e4775e454cc9e8224d5fed76e8795c6ff6`** has a failed post-merge [Tests run 43677527718](https://github.com/nikoma/bicdb/actions/runs/43677527718). Its [Native job 132700843200](https://github.com/nikoma/bicdb/actions/runs/43677527718/job/132700843200) completed **348 passed, 0 failed, 1 ignored**, with 944 filtered out, in 2,183.25 seconds. The entire log and all 349 unique case results have now been independently reviewed and reconciled. Remaining-workspace job `132700843144` failed `unmigrated_legacy_jobs_are_rejected_before_any_state_listener_starts` at `crates/bicdb-app-host/tests/replicated_state.rs:313`, on the final replicated reopen: `Address already in use (os error 98)`.

All 349 main519 Native case names also ran in candidate326. Candidate326 added 51 cases: 25 passed and 26 failed. Of its 27 failures, 26 therefore have neither a test definition nor an actual execution in main519. Only `native_registered_fifth_remote_hot_data_follower_binds_without_fake_leader_ready` existed and passed in main519. That wrapper retained its name/body, but its candidate exercise added the second positive recovery-count assertion which failed before the original downstream probe/read checks. The common hot fixture budget also differs between the revisions (main519 4,000 ms; candidate326 1,500 ms). A matching name is not equivalent source/cut coverage; these facts do not by themselves identify a runtime regression or attribute a failure to that budget. The candidate's failed acceptance gate remains failed.

The separately frozen main519 Native scope contains 11 payload files, one manifest and one verification receipt; it does not claim to archive the other jobs or artifacts. Its raw Native log is 13,543,529 bytes, SHA-256 `da06ccedd111fc3d420c9ffb4f70691702f9ee1d7a31d2872af0dd9ce0305f10`; retained gzip SHA-256 `0aebda0d45fff90cb09f1530418ccd43a838997c2f646651bd60ffc3342f55ec`. Manifest SHA-256: `7eefde57b7e1023328b6b0ffbe107793f80319323d439444e54faeda42d42bfe`. The complete 27-case comparison matrix SHA-256 is `bff00b6060c94094de4bb6dfbb68ce0feb68c8d0d1319a4340a3f62331b4c832`; independent full-log/archive/case comparison receipt SHA-256 is `c99505e1ce75810637baf4eacd850dee47fe11ff3da4f0b925afb4c220c9e28a`. This is executed CI evidence, not physical or whole-release qualification.

The earlier no-listener bind assertion at line 306 and standalone migration/queued-job assertion at lines 310–311 had succeeded. The final prepare, consume and sync-policy assertions after line 313 were not reached. The test's port helper releases its reservation listeners before reuse; a port collision is plausible, but the competing owner was not recorded and the cause remains **inconclusive**. Do not relabel this as a schedule failure, unavailable CI or a demonstrated replication defect. The candidate326 test source is byte-identical and its actual result must be recorded separately.

The complete main519 workspace raw log is 134,690,258 bytes, SHA-256 `a080beeb01ecf60d3f360b44f48125a129dc4a131fd03039ef80336ee6358311`; retained gzip SHA-256 `59bed3b33e4c6e99e48b90b1d57c56f5e64a674afeeec59bc80a2ddd163770a0`. The smallest deterministic diagnostic is to introduce a known competing port owner at an explicit reservation/reopen barrier and capture both the bind owner and exact startup stage, while retaining the original migration, no-listener, binding and queued-job assertions. Merely rerunning until green would not explain the failure.

A new, uncompiled control is defined in `crates/bicdb-app-host/tests/explicit_port_owner/mod.rs`. It retains a genuine competing listener acquired after the actual standalone job migration, requires the exact typed `PgWireError::Io(AddrInUse)` from the original failed replicated open, and records the persisted replica binding. That binding is an important possible startup effect: its initializer runs before HA listener binding. The control then releases only its own listener and requires a distinct open with the exact same replica configuration, followed by the original job-consumption and sync-policy checks. It preserves the old error and does not replay a transaction or claim that the failed startup was effect-free. The original failing test body remains unchanged. This definition is **not tested**; even a passing control would establish this known-owner scenario, not identify the unrecorded owner in the historical main519 run.

The next smallest CLI status control should hold the actual learner metadata mutex during a finite status/election action, release it and require completion with the same original caller. Separate real cancellation and expiry cuts must return their exact errors without starting or repeating an election. A generic busy-string retry would not establish this contract.

Source tracing places this busy status refresh after `elect_metadata_control` returned successfully. The CLI's `operator elect failed` message therefore does not prove that no election happened. A correction must wait for the original status read within its original caller/OWN window and preserve the actual election result; it must not repeat the election to make the CLI command appear successful.

No new binary, deployment, Azure allocation, production admission, RPO/RTO result or post-convergence throughput measurement follows from these findings.
