# Native operator CLI failure diagnostics

This source-only change adds failure custody to
`crates/bicdb-cli/tests/native_metadata_operator_cli.rs`. It makes no runtime,
enrollment, metadata, schema-authority or data-serving change. No local Cargo
run is attributed to this change.

The original exact a4aaec61a68cb7c6a7468bd3ffa4de0568c03a4b CLI run
43212556847/job 131163595260 failed 1 passed/2 failed in 15.75s. The sixth-process
case observed node 5 exiting at line 819; its stderr/status stayed in the
runner's temporary directory and were not archived. Its original cause is
unavailable. The other failure compared JSON Null with Voter even though
`ClusterNode.metadata_role` deliberately omits default Voter during serialization;
Root is correcting that assertion through typed topology decoding separately.

## Failure-only allowlist

With `BICDB_TEST_CLI_EVIDENCE_ROOT` set, `Processes::Drop` first kills any still
running owned child and actually waits for every child, including an already
exited child. It then captures only on panic. The manifest records PID, actual
pre-kill observation, whether cleanup sent kill, actual wait status/code/signal
or error, and the original recorded spawn program/arguments. A deliberately
killed/restarted fifth process has a separately recorded prior exit.

The exact allowlist covers node stdout/stderr/status/control commands, the
common plan, explicit saved killed-state JSON, and named native learner/seed,
topology and metadata-consensus JSON. Missing and refused paths are listed.
There is no recursive database-tree or TLS-directory copy. Symlinks, binary
payloads and private-key PEM markers are refused. Invocation arguments contain
credential PATHS only; key/certificate file contents are excluded. Bounds are
160 paths, 8MiB/file and 64MiB total. Files and directories/manifest are fsynced;
the manifest includes copied file sizes/SHA256 and optional actual checkout
`BICDB_TEST_SOURCE_SHA`, distinct from non-authoritative workflow SHA.

This captures stopped-process diagnostics, not an engine replay image. It opens
no database and invokes no checkpoint, repair, enrollment or state fabrication.
Absent environment or successful tests create no custody files. The hook remains
unexecuted until an actual future failure exercises it. Artifact upload must
include its output directory; a kept temporary path alone is not custody.

## Source-backed schema-export boundary

At the original source, `distribution_native_host/operator_learner.rs` obtains an
actual own HostMQ, selects an actual range's DATA leader and requests schema.
`provisional_listener/registration/schema_export.rs::export_learner_schema`
uses `bootstrap.metadata_leader_id`, not self, for its fresh own MQ. Bootstrap
reads the actual owned metadata store's current role/leader hint. The exporter
strictly requires local committed topology to match the actual fresh MQ topology
before export and again after an independently fresh post-export MQ; actual
birth/restore, schema and namespace identities are also revalidated.

A plausible unconfirmed schedule is that new metadata leader 4 commits node 5's
registration, while original data leader 0 has not yet installed that committed
configuration. Schema export can correctly refuse at the current-CONFIG check
(or earlier authentication/leader discovery), and the one-shot fresh constructor
propagates the error to process exit. This is NOT an observed cause without the
missing child stderr. No such guards are relaxed here.

If future custody confirms a transient pre-root race, any retry must remain
bounded by the original caller, rediscover genuine current leader/configuration
and repeat actual own MQ/schema validation. It must distinguish permanent
identity/schema refusal. Once a native root exists, a fresh factory retry must
not erase or recreate it; strict existing-root recovery is a separate path.
No transparent operator 4→5 data handoff or physical qualification follows from
these diagnostic/source observations.
