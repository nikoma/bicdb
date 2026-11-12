# Public-client CLI attempt evidence

The two existing DATA/receiver cases now create one mode-0600, bounded durable
journal before their first public EventClient operation. Every actual EventClient
and explicit receiver-probe transport is configured with the frozen Core observer
before its first RPC/refresh. Existing original assertions, keys, two-second
client deadlines and retry policy are unchanged. This is diagnostic fixture
wiring, not a workload/recovery fix or a new success claim.

The sink serializes complete JSONL records under its own mutex, fsyncs before
returning, and holds no engine guard. Core releases its quota lock before sink
callbacks. Both layers cap at 4,096 records / 64 MiB; no payload/error truncation
is allowed. Sink failure/cap fences subsequent observed dispatch. Filesystem I/O
can itself be uncancellable; it consumes the original Core budget and never
renews it. There is no hard OS-I/O bound or guarantee that instrumentation has
zero scheduling cost.

Each serial public operation has a distinct **diagnostic** UUID. It is set
before invoking the API and remains active until all synchronous RPCs/retries
return and its public outcome has been fsynced. It is never passed into protocol
fields or used as membership/quorum/ACK authority. Existing external ACK/attempt
records gain that ID for linkage. Raw evidence retains Core's actual physical
attempt ID, request ID, observer-local reservation sequence, wall/monotonic phase
times, destination/generation/address, exact length-header+JSON frame, bytes
written, dispatch uncertainty and complete error chain. Each observer instance
has a separate ID, while the sink adds one persisted completion sequence.

A public append ACK is recorded as USER_ACK only from the actual returned result.
Errors are NO_USER_ACK_DURABLE_OUTCOME_UNKNOWN; even a pre-dispatch failure is not
a global rollback proof. Internal dispatch evidence is retained separately.
Strong success uses tagged present/absent records so present JSON null remains
distinct from absence. Failed reads cannot become successful absence. A context
abandoned by panic records an interrupted/partial logical call where possible.
Only after all original assertions pass does the sink emit a durable case-finish
marker. A missing marker, partial JSONL tail or evidence fence cannot qualify
trace continuity.

The actual initial `TcpClusterBootstrapClient` call is preserved. It has no
frozen raw-observer builder. Its exact logical request/reply/error/timestamps are
recorded with `bootstrap_raw_frame_covered=false`; no raw-frame coverage is
claimed. Host internal metadata/replica traffic, CLI command-file step processes
and application execution inside those hosts are also outside this client
journal. This is **not a complete global wire ledger**. No synthetic topology,
MQ, authority, vote or bootstrap replacement is introduced to fill those gaps.

Focused CLI CI retains journals on both success and failure under:

```sh
BICDB_TEST_CLI_RPC_EVIDENCE_ROOT="$RUNNER_TEMP/native-cli-evidence/rpc-journals"
```

Without that environment, journals stay under the test's own fixture root.
They contain actual test application frames and errors; keep them private.
They contain no TLS handshake/key-file bytes. A journal path is printed to the
retained stdout/stderr; matching source, test name and fixture-root metadata are
in its first record. The already-fsynced external ledger remains separate.

The same controlled manual cohort may explicitly set workflow input
`cli_failure_db_custody=true` with
`cli_target=native_metadata_operator_cli`. That enables the separately bounded
post-ALL-waits copier and a separate exact-subtree artifact. The default is false;
normal PR/push/broad checks retain their selection semantics. Source/TLS fixture
roots and private quarantine siblings are never recursively uploaded. See
[DATABASE_CUSTODY.md](DATABASE_CUSTODY.md). A standalone COMPLETE manifest is not
proof: require successful actual helper exit, matching final receipt/checksums,
no fence/partial diagnostic and the declared source/owner scope. A copied DB
still needs separate isolated reconciliation; copying is not replay or recovery.

Original failed source `3ee40c19b44affa0a481a66b278ded3af817e910` and its raw
4-pass/1-fail evidence are unchanged. Receiver-ready remains DATA-serving-false;
a post-join timeout is not healthy ACK/Strong continuity. Native image/joint
wire/controller and eventual bounded healthy service remain open requirements.
