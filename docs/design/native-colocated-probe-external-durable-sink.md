# Colocated native SDK probe with an external durable sink

This is functional test instrumentation. It adds no database principal, routing
rule, quorum member, deployment, production qualification or throughput result.
The source inherits 1.0.515-beta. Rust compilation and real BicDB execution must
be recorded separately from Python format, local persistence and loopback TLS
controls.

## Identity and placement contract

`ClusterEventClient` is a trusted host API. APP callers must be Active metadata
voters with the membership-bound certificate; typed reads require an Active
member. A separate CA-signed controller principal cannot issue APP requests.
See `distribution_transport.rs::authorize_cluster_data_caller`.

Run the probe only as an explicitly authorized host process on an existing
isolated test member. Its database membership key stays on that member's VM.
The collector/controller use separately provisioned instrumentation TLS
identities; neither receives a database private key. The example rejects the
same resolved key path and the same certificate for member and ledger TLS.
Separate key generation and complete custody remain independently checked run
facts; path/certificate inequality alone cannot prove distinct private keys.
Do not add a sixth voter, copy a server key to the controller, use plaintext or
broaden authorization to make a test pass.

Colocation shares the member's failure domain and consumes its CPU/network.
Record these effects. A producer killed with its host leaves an incomplete run.
Reconcile through a new read-only run on a surviving legitimate member under
that member's identity. This is native Event SDK access, not ordinary PostgreSQL
or an unprivileged application's authentication contract.

## Actual ordering and the acknowledged-write corpus

The optional fifth probe argument selects the external sink. Four-argument
local-only operation remains available for offline development and explicitly
reports that physical use is blocked. Existing format-1 JSON rows remain
unchanged; external metadata is additional `run_started` DATA and receipt files.

Before constructing the SDK client, the probe sends exact `config.json`,
`topology.json`, `plan.json` and `ledger-config.json` bytes and hashes to the
collector. Inputs are structured JSON, with an explicit key-free-input review
attestation and a PEM-private-key accident check. This is not a blanket secret
scanner or permission to archive key files. TLS files themselves remain on the
appropriate host. The actual executable hash and compile-time source claim are
recorded; independent build/source verification is still required.

Each journal append serializes one exact row, preserves its SHA256 chain, fsyncs
the local supplemental copy, sends the exact bytes plus newline to the external
collector, validates the returned hash/identity/end offset, and fsyncs the local
receipt transcript before returning. The collector writes to its own local
durable storage, fsyncs the complete row, then writes and fsyncs its receipt
journal before sending the receipt. No NFS/SSHFS or network-filesystem durability
assumption is used. The collector opens the real directory using `O_PATH` where
available, binds its `fstat` device/inode and `/proc/self/fdinfo` kernel mount ID,
and requires one matching `/proc/self/mountinfo` record. Its major/minor,
filesystem root/path relationship and freshly opened mountpoint must match
the actual handle. Ambiguous, unsupported, network and memory filesystems
refuse. The same retained directory handle is then used for `openat` creation
and directory fsync; later namespace/path replacement cannot retarget those
writes. These are kernel filesystem/mount observations, not backing-device,
cache, flush or physical-host qualification.

The SDK observer waits for this receipt at every original SDK phase, including
before dispatch and before response decoding. Its fallible callback can fence
the original observed SDK; it cannot supply or alter replies. The explicit
`sdk_ack_observed` row comes **after** the unchanged SDK method returns `Ok`.
A later wrapper ACK may be delivered only after that row is externally durable.
Keep these boundaries separate:

| Observation | What it establishes |
| --- | --- |
| Raw `Appended` frame | Possible server success; later validation/API completion may fail |
| Validated transport result | Actual transport validation; sibling cleanup/deadline can still reject SDK return |
| `sdk_ack_observed` | Probe observed actual SDK `Ok`; record delivery/durability remains separately linked |
| Valid external row receipt | Collector completed that exact row's fsync path before its reply |
| Independent wrapper-delivery witness | Additional evidence that the external recipient observed the wrapper result |

Never redefine the goal's ACK set as wrapper ACKs alone. Reconciliation covers
every known SDK success, correlated possible-success reply, attempted write,
uncertainty and retry. If gaps prevent proving an operation unattempted, retain
every original planned write as a possible reconciliation target. Missing
`sdk_ack_observed` does not prove the SDK never returned `Ok`. A missing known
ACK value fails the relevant check; missing possible-success data or incomplete
ACK coverage fails or leaves qualification inconclusive. Zero wrapper ACKs
cannot turn those gaps into a successful no-loss result.

## Failure, bounds and supervision

The collector uses pinned mutual TLS and one bounded request per connection.
The producer pins the collector's leaf DER hash in addition to normal CA/name
validation. The collector pins distinct producer and controller leaf hashes.
Both Python helpers decode only the first certificate from a bounded 1 MiB PEM
bundle, matching Rust's leaf selection and the actual TLS peer certificate.
They do not hash an entire concatenated certificate chain. A malformed first
certificate is refused rather than skipped for a later certificate.
The controller cannot impersonate producer row submission and the producer
cannot submit independent exit/fault witnesses.

The sink's original absolute IO window includes attribution/journal mutex wait,
framing, connect, TLS handshake, writes, receipt reads and local receipt fsync.
Partial progress never renews it. Numeric socket addresses avoid DNS dependence.
The production SDK's request timeout, routing and retries are unchanged; sink
work consumes that budget. The public observer receives no SDK deadline, so the
sink also has its own finite bound. SDK checks still reject a late success.

Local persistence errors, quotas, partial frames, wrong identity, conflicting
sequence, failed fsync, receipt loss or invalid receipt poison that collector
run and/or fence the producer. No automatic DB replay, sequence reset or fresh
producer is used. A collector may have persisted a row whose receipt was lost;
read-only reconciliation may discover it, but cannot invent historical receipt
delivery. Restart never appends to an existing collector output directory.
Preserve its exact uncertain tail rather than repairing/truncating it.

File fsync and scheduling are not preemptible by a socket timeout. Independently
supervise owned producer and collector processes with an absolute experiment
deadline. If they stall, retain incomplete evidence and stop qualification.
Do not assert bounded kernel IO or RTO from the configured socket deadline.

## Independent completion and fault evidence

`controller.py` runs a bounded actual child command and durably records launch,
stdout/stderr capture, exit status and receipt links independently. It uses the
controller's separate instrumentation certificate. Output exceeding the bound,
missing exit, timeout or ambiguous receipt remains incomplete. `--purpose fault`
records the actual fault-command/result boundary, with remote effect explicitly
unconfirmed.

An SSH/provider command's local exit does not automatically prove remote
producer exit, power loss or a completed VM operation. The physical runner must
independently establish those facts, bind the actual process/binary/run and
capture provider/process status. This helper is not that runner. A visible
`run_completed` row alone never proves the producer's append returned. An exact
stdout receipt plus independently established producer exit is still required.
Collector failure diagnostics preserve the actual failing stage, bounded error
chain and original request identity DATA on stderr. A failed collector disk
cannot certify its own diagnostic persistence; capture that stream independently.
If a journal/attribution failure occurs after an SDK result, the probe's returned
error retains that original SDK observation instead of masking it with a later
generic journal-fenced error. Missing stderr remains an evidence gap, never
proof that an SDK success did not happen.

`verify_external.py` checks an independently pinned immutable snapshot index,
complete original hashes, exact collector/producer receipts, durable end offsets,
controller witness bytes and the original verifier's consumed bytes/plan. It
never repairs input. Its `functional_evidence_complete` describes these bounded
instrumentation observations; it does not certify remote process provenance,
reconciled database values, HA, physical durability or production admission.
The separate 805 cross-run reconciler must check exact values, preserving all
possible-ACK targets. Its existing 63 synthetic controls remain historical
offline evidence, not SDK or physical tests.

The snapshot index has strict fields: `format`, run/producer/session UUIDs,
`source_sha`, `binary_sha256`, `member_node_id`, member/collector/ledger-client/
controller certificate SHA256s, `files`, and `completion`. Each `files` entry
has exactly `bytes` and `sha256`. Include these exact allowlisted originals:
`collector-manifest.json`, `collector-receipts.jsonl`,
`controller-witnesses.jsonl`, `storage-contract.json`, `journal.jsonl`,
`producer-receipts.jsonl`, and the four original JSON inputs. The producer's
original `remote-receipts.jsonl` is copied byte-for-byte to the snapshot name
`producer-receipts.jsonl`; retain its source/hash provenance. Never include TLS
key paths' contents or a wholesale VM/database directory. Pin exact source and
destination hashes when exporting a stopped/immutable snapshot.

When independently captured completion is available, include exact
`completion-witness.json` bytes in `files`; `completion` has exactly
`controller_witness_sequence`, `command_sha256` and actual `exit_status`.
Otherwise set `completion` to null and retain the incomplete classification.
The checker matches those exact bytes against authenticated controller output;
it does not promote a local SSH/provider command's exit into remote-process or
fault-effect proof. Every actually consumed stream and JSON input is rehashed
against the pinned index, including inputs also consumed by the old verifier.
Hash provenance and truthful identities remain independent run facts.

## Four-to-five discovery preflight remains a real gate

Keep the producer's initial configuration, seeds and topology at four members
and retain their hashes. Do not insert the fifth member in advance, edit input
files, recreate the SDK instance or provide range/leader hints during expansion.
Existing CA trust or a prospective signed certificate is distinct from actual
membership admission.

Source inspection of 515 shows that the SDK and transport share the same
topology `Arc`. `refresh_members_until` obtains metadata snapshots from known
members; `install_topology` validates same cluster/image domain, monotonic
generation and equal-generation equality. Every dispatch reads its destination
address and membership certificate fingerprints from the current shared
topology and verifies the actual TLS peer. These paths can discover a legitimate
new member without a static peer whitelist. They are not execution evidence.

The actual experiment must retain discovery frames demonstrating the newly
admitted fifth member, its real address/certificate and subsequent APP/Strong
dispatches through the unchanged client. If that fails, report a client/operator
integration gate. The instrumentation must not work around it. A certificate
or topology-generation change alone does not prove data/leadership movement.
Retain whether discovery used the committed-snapshot path or the existing
legacy topology fallback. Neither response is permission to bypass actual
server readiness or per-request Strong barriers. A fifth node requiring a new
CA outside the unchanged client's initial trust is an unresolved integration
gate, not permission to rewrite the client's trust during the experiment.

## Historical a846 validation

The following result belongs to frozen `a846ea208241e14eaf021da410125e57d95b5874`.
Its receipt remains unchanged. It did not exercise multi-certificate chain
interop or bind the mount preflight to a retained directory descriptor. The
successor adds separate controls for those boundaries; source availability
alone is not their execution result.

The new `test_external` cohort executed 35 controls: 35 passed, none skipped or
failed, in 4.750 seconds under a configured 512 MiB address-space cap. These
include actual local file/fsync paths with explicit write/fsync fault injection,
synthetic SDK-row/receipt/plan/timing controls, one real loopback mutual-TLS
collector exchange using ephemeral test certificates, and actual bounded local
child capture/timeout/output-limit controls. They do not run BicDB or the Rust
adapter. Filesystem-type refusal tests inject mount facts; they are not physical
storage-failure experiments.

The four original Python source/test files are byte-identical to 805. Its
separately recorded 63 passing synthetic controls are reused, not rerun or
relabelled as tests of a compiled 515 probe. Rust formatting/parse checks are
separate from compilation. No Rust build, real SDK request, VM provisioning,
physical failure, production deployment, scale-out or throughput trial was
performed for this batch. Both Rust compilation and genuine SDK/collector
end-to-end execution remain gates before physical use.

## Descriptor and PEM-chain successor validation

The successor's focused aggregate executed 53 controls: the 35 existing
instrumentation controls plus 18 new descriptor/mount/PEM-chain controls.
All 53 passed, none skipped or failed, in 5.676 seconds under a configured
512 MiB address-space cap. The unchanged 805 files and their historical
63-control result are reused separately.

The new controls observe real opened directory devices, inodes and kernel
mount IDs, test contradictory/ambiguous mount observations, refuse a genuine
other-device memory filesystem, and demonstrate that actual path replacement
cannot redirect subsequent collector input writes from its retained handle.
No mount operations are performed. Contradictory mount observations are
injections around actual descriptors, not physical disk or namespace faults.
A real loopback mTLS exchange uses multi-certificate PEM bundles on both
peers and validates the original single-leaf pins; a wrong peer pin refuses.
The same control also verifies file/descriptor cleanup after a genuine
occupied-port startup refusal.

The first aggregate is retained as failed: 52 executed, 14 passed, 7 assertion
failures and 31 errors. Its new parser incorrectly rejected legitimate
unrelated `nsfs` roots such as `net:[...]` before reaching the selected storage
preflight. Those are prerequisite failures, not physical storage failures;
even its passing refusal checks are not proof that their intended selected
mount cut was reached. The correction keeps unrelated kernel DATA intact
and applies strict root/device/mount-ID checks to the uniquely selected actual
directory only. It adds an explicit unrelated-namespace control. That failed
source/log and a846's earlier 35-control receipt remain unchanged.

The external checker now requires the stronger descriptor-bound storage
profile. Older path-only a846 storage facts remain historical DATA and cannot
substitute for that profile. Format-1 SDK journal bytes and the original
known/possible-ACK reconciliation corpus are unchanged. Rust examples remain
uncompiled; no real SDK, database, VM, node expansion or physical fault was
executed by these Python controls.

## Receipt-schema successor controls

The subsequent source review found an online fencing gap. Defaulted Rust
`Option` fields accepted an omitted first-row `previous_record_sha256` as the
same value as its required explicit null, and accepted forbidden row fields
on start/input receipts when those fields were null. The offline checker would
reject these later; that does not satisfy immediate invalid-receipt fencing.
The controller also accepted incomplete witness receipts without a global
receipt sequence. These are instrumentation contract gaps, not evidence of
database data loss or false physical qualification.

The example now uses a shared strict `receipt.rs` decoder. Derived type and
duplicate-field checks run first, followed by the exact action-specific raw
field set. First-row null remains explicit and mandatory; required non-null
values cannot disappear as defaulted options. The original raw receipt bytes
are retained without reserialization. Decoder errors follow the existing
sticky sink-failure path before observer success. The controller likewise
checks the exact witness schema, strict request-value types and monotonic
integer receipt/witness counters before proceeding. Its absolute IO window
and failure fence are unchanged.

Seven Rust parser controls are shared with the example through the normal
Core integration target `native_external_ledger_receipts`. The unchanged
required Core command runs `cargo test -p bicdb-core` with two unrelated native
module skip prefixes; these integration test names match neither prefix. No
workflow or manifest change is needed. All-target compilation alone is not
their execution result: these new Rust controls are source-listed, uncompiled
and unexecuted until an actual target result is retained.

Eight additional Python witness-schema/fencing controls cover this layer.
One uses a synthetic endpoint to exercise the actual controller
method's sticky refusal; another proves that framing consumes the original
window and cannot be followed by a connection after expiry. The controller's
window now begins before encoding/hash work, and never renews for partial IO.
These are not TLS or database tests. Both counters use the unsigned 64-bit
wire domain, matching the Rust decoder; the collector's existing operational
70,500-receipt and 256-witness quotas are unchanged. Oversized and upper-bound
schema tests do not demonstrate that the collector can produce that many
records.

Frozen `9217354fab89b1e8065c7bbf59c57cee875c14f4` executed a coherent 60-control
Python aggregate (the previous 53 plus its first seven witness controls): all
60 passed, none skipped or failed, in 5.599 seconds with the same configured
512 MiB cap. Its raw result is retained separately. Source review subsequently
identified the missing explicit `serde_json::json` macro import in the shared
Rust tests; that import is corrected without removing any of the seven Rust
tests. This was a source-identified compile blocker, not an actual completed
Rust compiler/test result. The descendant adds the eighth Python counter-bound
control and independently executes the coherent 61-control Python aggregate:
61 passed, none skipped or failed, in 5.764 seconds with the same configured
512 MiB cap. Its log pins every actually tested Python source hash. These results
do not compile or execute any Rust parser control or real SDK/database request.

The earlier bf3 53-control pass belongs to that exact previous source. The
original 805 files/63-control result and all prior failed and passing receipts
remain unchanged. No Rust build, SDK execution, VM, physical fault or CI action
is performed by these Python/source checks.

## Commands and evidence boundaries

Use a new output directory for every process/run. Configure the collector and
controller with their own existing local-storage paths and separate TLS files.
Do not put private-key contents or credentials in command arguments. The new
configuration schemas are strict; sample field names are in `external.rs` and
`controller.py::WitnessClient`.

```text
native_external_ledger config.json topology.json plan.json NEW_LOCAL_OUTPUT ledger-config.json
python3 collector.py --help
python3 controller.py --config controller-config.json --output NEW_CONTROLLER_OUTPUT -- ACTUAL_COMMAND ARGS
python3 verify_external.py --index independently-pinned-snapshot-index.json --snapshot IMMUTABLE_SNAPSHOT
```

The collector has start/input/row/witness actions. Requests carry a bounded JSON
header (u32 big-endian length), raw payload (u64 big-endian length), and exact
payload hash. Responses are bounded u32-length JSON receipts. A row payload is
the original format-1 line including its newline; `record_sha256` excludes that
newline, while `payload_sha256` includes it. Offsets count complete line bytes.
Collector receipt sequence can include interleaved independent witnesses;
producer receipts must be an exact increasing subsequence. SDK reservation
order and sink arrival order are separate fields, never silently reordered.

Sync per-phase remote fsync can dominate this workload. Use it for correctness
and fault reconciliation. It cannot establish sustained aggregate throughput
or a four-to-five capacity gain. No physical infrastructure is provisioned by
these tools, and no physical qualification result is claimed.
