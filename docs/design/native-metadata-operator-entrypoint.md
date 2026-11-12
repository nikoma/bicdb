# Explicit native metadata-only operator entry point

This source batch is based on private full-prototype `0bad7543d6d703b8765424d72a4efec0e81d2839`.
It is separate from isolated read delivery and registration169. No local Cargo,
CLI execution, VM action or qualification is claimed by this note.

## Supported operator surface

`bicdb cluster native-metadata serve-fresh ROOT --plan COMMON.json --node-id ID
--address ADDR --cluster-tls-cert LOCAL.crt --cluster-tls-key LOCAL.key
--cluster-tls-ca LOCAL-ca.crt --command-file PRIVATE/control.json
--status-file PRIVATE/status.json` retains one real public Core
`NativeMetadataHost`. It opens **metadata-only** listeners, not pgwire, SQL,
application data, Scan, Strong or a generic database. The root must not exist.
Existing native/legacy roots refuse rather than being retrofitted, deleted,
reset or passed to generic `MetadataConsensusStore::open`.

The common plan is version1 configuration DATA: DistributionConfig, initial
members, collection names/modes, and a common created_at_ms. All participants
must use identical data. There are no proof, stored-birth or readiness DTO
constructors in the public facade. Its private native plan enforces the existing
fresh all-voter/from-birth order, actual enforced factory, schema/genesis and
protected metadata initialization. A plan cannot revive a used voter root.

Every process takes its own explicit address/TLS arguments and local root.
Address and actual leaf SHA must match that process's planned member; real TLS
configuration is parsed before factory creation. The common source template's
transport/TLS paths are ignored for authority and IO and replaced with the
local network before use. No source private-key bytes are fetched or installed.
The CLI process test deliberately puts nonexistent seed-VM-only paths in the
common template while supplying actual per-process TLS files separately.

Operator command/status files must be distinct, outside the native root, and
initially absent. Their canonical parent is required to be caller-owned and
not writable by group/other on Unix. Writes use a same-directory private temp
file, fsync, rename and parent fsync. These are local trusted-operator files,
not an application-client authentication protocol. Explicit local TLS cluster
node credentials retain the existing CFT cluster-CA assumption.

## Actual finite actions and retained owner

`bicdb cluster native-metadata step --command-file ... --status-file ...
--sequence N --action ACTION` submits a bounded local command and waits for its
actual action result. Actions are `prepare`, `launch`, `elect`, `enable-control`,
`heartbeat`, `status`, and `stop`. Each action is capped by its original caller
budget, at most five seconds; a timed-out command does not report success or
remove any durable proposal. Status is diagnostic data, never a row/vote grant.
Data-serving remains explicitly false in every reply.

Drive all four real processes through prepare, then all four through launch,
then elect a candidate using actual remote votes/current-term barrier, then
individually enable-control. This explicit phase schedule preserves the actual
all-C preparation requirement. It is not an automatic election/bootstrap
scheduler, nor may an operator skip all-C using a command value.

The public Core facade exposes no raw DB, service, coordinator, metadata store,
factory or permit handles. It retains the actual launch immediately before
fallible listener creation, and the control-transition owner before drain/bind.
Existing same-service/RangeWriteStore-open/DB-open/protected StoreUUID checks
remain mandatory. New control listeners are immutable; old provisional and
initial metadata listeners do not broaden in place or through Weak expiry.
Cancellation/bind errors during the existing control transition retain its
actual owners. Explicit stop uses actual listener stop/drain; process lifetime
or Ctrl-C performs bounded cleanup before dropping the owner.

The serve loop retains finite periodic metadata heartbeat maintenance while
actual status reports a leader. It does not install data writer policies,
perform range activation, run automatic elections, schedule placement or turn
failed heartbeats into an accepted CONFIG. The all-member initial setup remains
operator-coordinated. Native factory/Prepared/cold restart cuts beyond this
fresh-only facade are unsupported and require separately proven strict loaders.
A partial fresh factory is never silently removed/recreated on retry.

## Actual authenticated registration command

`bicdb cluster native-metadata register --cluster-id C --seed-node-id SEED
--seed-address ADDR --node-id SELF --address SELF-ADDR --incarnation N
--capacity-bytes BYTES --cluster-tls-cert SELF.crt --cluster-tls-key SELF.key
--cluster-tls-ca LOCAL-ca.crt` uses the existing authenticated bootstrap client.
It follows committed metadata leader data and submits only its own exact TLS
learner identity. The native receiver/controller supplies actual independently
fresh own-leader MQ, bounded CONFIG queue, protected persistence, actual old-
voter commit and final fresh MQ. The CLI does not propose topology or count
queue acceptance as joining. Conflicting current identity refuses; existing
birth/role/placement is not erased or demoted on retry.

Success is named `metadata_identity_committed`, with actual metadata role,
birth-bound state, generation and `data_serving:false`. The command creates no
data directory, fifth native factory or metadata backend. Bootstrap templates
remain DATA; this command never passes their TLS paths to a joiner's transport
or uses generic member-directory provisioning. Registration as an unbound
nonvoting learner is not birth binding, a vote, backend catch-up, promotion or
data membership.

## Regression source and remaining gates

`native_metadata_operator_actual_cli_four_process_startup_registration_and_data_refusal`
starts four real `CARGO_BIN_EXE_bicdb` child processes through the actual public
CLI/consuming Core facade, sends real CLI step commands, registers the fifth
identity through the actual CLI/mTLS entry point, and asserts committed learner
identity without birth/backend. It rejects reuse of a live root and verifies
Scan remains refused by all metadata-only receivers. There are no direct test
metadata proposals/private host operations or recorded/synthetic acknowledgments
in this path. Failed process roots/logs are retained privately by the test owner.
The preexisting registration component result does not validate this new CLI
source automatically.

Required remote validation starts with Core/CLI all-target compilation and the
isolated no-default/workflow consumer, then
`cargo test -p bicdb-cli --test native_metadata_operator_cli --locked -- --nocapture`,
plus the existing native registration/startup/owner/metadata-ACK controls on the
same frozen source. Local formatting, diff and version checks are not test runs.

Still open: a released native data/SQL Serve mode, full Init/Join host policy
installation, actual fifth factory/schema export and birth binding/backend,
metadata catch-up/promotion/controller automation, automatic leadership/abort
recovery, cold all-owner host startup, joint-data serving/handoff/cleanup and
physical four-to-five operation. Generic pgwire/CLI paths are not used as
substitutes. No application ACK, data4-to5, independent-VM availability, SQL
compatibility, throughput, Azure causality or full Milestone1 claim follows.

## Manual remote CLI selection

The tests workflow accepts optional `cli_target=native_metadata_operator_cli`
with an empty `core_filter`. That explicit manual selection runs the serial
90-minute focused CLI job and skips the broad matrix only for that dispatch.
Ordinary PR/push and empty-selector dispatch retain the full matrix; workspace
all-target compilation and the isolated workflow-consumer check stay independent.
Target names pass a bounded alphanumeric/underscore/hyphen check and enter the
shell through a quoted environment variable, never expression interpolation.
The job lists actual tests and refuses zero selection, preserves Cargo pipeline
failure, and refuses a zero-executed/all-ignored success.

Its artifact includes the exact tracked-source archive/revision, Rust toolchain,
full list/build/test output and target name, always uploaded on success/failure.
Only the dedicated evidence subtree is uploaded. It never opts generated
private TLS/database fixture roots into that subtree. Adding this workflow is
not evidence of a CLI execution; a dispatched run must supply the actual result.
