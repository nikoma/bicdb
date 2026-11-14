# Cross-run read-only reconciliation evidence

This tooling extends source `678abb8c4`'s offline verifier. It generates plans for
the stock probe from `b44064c2` without modifying Rust, the SDK, routing, credentials,
or server state. Tests use synthetic journals and reply frames: no BicDB process,
SDK invocation, network, native transaction, or physical fault is executed.

```sh
python3 scripts/platform/native-sdk-ledger/test_verify.py
python3 scripts/platform/native-sdk-ledger/test_reconcile.py
python3 scripts/platform/native-sdk-ledger/reconcile.py generate \
  --index retained/input-index.json --output NEW_PLAN_BUNDLE
python3 scripts/platform/native-sdk-ledger/reconcile.py coverage \
  --index retained/input-index.json --bundle NEW_PLAN_BUNDLE \
  --results retained/read-result-index.json
```

Neither command contacts a database. Generated plans contain only `strong_read`,
with namespace/key/expected value and no physical range, node, leader, route,
append, or replay. The stock SDK retains all routing and retry decisions. The
operator must separately arrange a lawful isolated member principal; this tool
does not add a voting member or borrow a server private key.

## Pin original inputs

The index is bounded JSON with these exact fields (replace explanatory hashes
with actual hashes and paths). Relative paths resolve against the index file.

```json
{
  "format": 1,
  "cluster_id": "isolated-experiment",
  "bootstrap_id": "019a80b8-6594-7000-8000-000000000099",
  "read_client": {
    "identity": {
      "build_source_claim": "40-hex-exact-client-source-commit",
      "client_binary_sha256": "64-hex-actual-client-binary-hash"
    },
    "directory": "read-client-inputs",
    "config_sha256": "64-hex-original-config-hash",
    "topology_sha256": "64-hex-original-topology-hash"
  },
  "sources": [
    {
      "directory": "original-write-run",
      "identity": {
        "build_source_claim": "40-hex-exact-client-source-commit",
        "client_binary_sha256": "64-hex-actual-client-binary-hash"
      },
      "plan_sha256": "64-hex-original-plan-hash",
      "journal_sha256": "64-hex-whole-original-journal-hash",
      "completion_witness": "original-write-run/captured-stdout.json",
      "completion_witness_sha256": "64-hex-captured-stdout-hash",
      "exit_status": 0
    }
  ]
}
```

Every original run directory retains exact `plan.json`, `config.json`,
`topology.json`, and `journal.jsonl` bytes. The read-client directory needs the
two intended input files. An incomplete source has all three completion fields
set to null. A missing linked witness does not become evidence of a successful
exit. Source/binary identity is checked against the run header when available;
a missing valid header is reported unknown and prevents complete evidence.
Actual build provenance is still external: matching a recorded hash claim does
not authenticate a build. Missing/different cluster or bootstrap bindings are
refused, rather than silently mixing database histories. Unknown restore lineage
always remains an external gate. Original archives must be immutable while
inspected; whole-file hashes and prefix chains are checked across the reads,
but this is not an adversarial filesystem snapshot protocol.

The ledger verifier's own reported `bytes_read` and `read_bytes_sha256` must
also equal the independently pinned whole-journal size/hash, for both original
sources and read results. A different or incomplete consumed-byte report is
refused even if another before/after file hash matches. This closes a reader
binding gap; it does not turn ordinary filesystem reads into an atomic snapshot.

When a valid run header is available, the verifier's checked `plan_sha256` must
also match the independently pinned source plan or the exact generated read-batch
plan. A different reported plan is refused even when the journal-byte binding
matches. A missing or rejected header remains incomplete evidence: original
planned writes stay possible targets, and its observations cannot establish
complete reconciliation. These checks still require immutable input archives.
The two forged reported-plan controls are source additions; they have not been
executed as part of this correction. No Rust client or SDK runtime was exercised.

The generated manifest retains every original write identity as
`original run UUID / logical ID`, its key, original-plan raw-value hash, chosen
expected-span hash and origin, actual attempt/ACK/terminal raw-record hashes,
SDK attempt identities and phase references, failed/unfinished/unresolved states,
and confirmed-unattempted status. Source journal/witness hashes pin the complete
original error, frame, and latency evidence; the manifest does not duplicate
those potentially large/private bytes. A later successful retry preserves the
earlier uncertain identity. It never relabels the earlier operation committed
or rolled back.

Only a complete, witnessed, internally consistent source summary can establish
that a planned write was unattempted and exclude it from the read target set.
For truncated, corrupt, missing-completion, or otherwise incomplete evidence,
**every original planned write** remains a possible target, including writes
unseen in the trusted prefix. Corrupt-tail observations and their explicit
omission counters remain in the manifest. A cap or source failure cannot silently
turn unknown work into definitely unattempted work.

## Exact values and batches

For trusted attempts, the expected span is the exact JSON value serialized into
the attempt record. Otherwise it is the exact original plan value span. Raw
UTF-8 spans, including arbitrary-precision number spellings, are inserted
verbatim into read plans. Python does not reserialize or round their values.
Original-plan and observed-attempt span hashes remain separate, since the Rust
producer can canonicalize object ordering or escapes when it parses the plan.

Deduplication requires identical namespace, key, and **exact expected-span bytes**.
All source identities stay attached to that target. Different payloads for the
same immutable key generate separate targets and explicit conflicting-history
findings. Even structurally equal values with different lexical encodings stay
separate and inconclusive; the tool never silently chooses a representative.
Type-aware comparison digests classify these differences, never replace raw
expected values or claim cryptographic protocol authority.

Plans have at most 1,024 operations each. Their run UUIDs, logical IDs, target
assignments, and raw plan hashes are deterministic from the pinned index. The
manifest covers **all** batches, with no physical placement hints. Each read has
`acknowledged_by:null`: a write in another run cannot become a fictitious earlier
append in the read-only plan. Cross-run ACK linkage stays in the manifest and
coverage checker.

Generation derives all targets, batches, conflicts, and bounds before creating
the output directory. It refuses an existing directory. Files and output/parent
directories are fsynced; a failed write can leave an incomplete bundle, which
coverage refuses. The manifest is written after the plans. Its visible bytes alone
do not prove the generator's final fsync returned; retain process-supervisor
evidence separately.

## Verify every read result

Retain each stock-probe result directory and its independently captured stdout
and exit status. The result index has this shape:

```json
{
  "format": 1,
  "batches": [
    {
      "index": 0,
      "directory": "read-run-0001",
      "journal_sha256": "64-hex-whole-read-journal-hash",
      "completion_witness": "read-run-0001/captured-stdout.json",
      "completion_witness_sha256": "64-hex-captured-stdout-hash",
      "exit_status": 0
    }
  ]
}
```

Coverage independently rederives the entire expected bundle from the original
source index and refuses changed, omitted, or extra bundle files. Each result
must use the exact generated plan, intended read source/binary identity,
configuration/topology hashes, and cluster/bootstrap. Missing batches remain
explicitly not verified. Duplicate/unknown batches and subset/different plans
are refused. No TTL extension, automatic rerun, or replacement source is used.

The stock-ledger verifier checks actual RPC identity/phase/reservation coverage,
typed Strong presence, complete outcomes, and linked completion witnesses for
each batch. An incomplete batch cannot lend its passing subset a whole-batch
success. Its observed terminal outcomes remain visible separately. Reports list
every required, verified-present, Strong-observed-absent, failed, and not-verified
target, plus original confirmed-unattempted and unterminated source identities.

Absence after any trusted-prefix SDK ACK is a failure, with the stronger
ACK-ledger-fsync linkage recorded separately. A present JSON null is an exact
value; an absent record is not. An unacknowledged write's Strong absence is an
observation at that read, **not rollback proof**: its original outcome remains
unresolved even when all read coverage is complete. Payload mismatch, conflicting
histories, incomplete source evidence, and incomplete read batches prevent a
complete reconciliation-evidence result.

Exit 0 means complete tooling evidence under these rules; exit 1 means a generated
or checked incomplete/conflicting result; exit 2 means an input, binding, bound,
or filesystem error. These statuses never certify physical durability, zero RPO,
bounded RTO, correct server read barriers, successful rebalance, or capacity gain.
Every manifest/report remains `not_qualified`, keeps source write uncertainty,
and reports restore/build/server/principal gates separately.

A successful reconciliation gate requires a nonempty original-write corpus
and at least one eligible read target. Read-only source histories and histories
whose writes are all confirmed unattempted produce explicit `no_target_reason`
results and exit 1, rather than vacuous success. `has_visible_source_ack_sample`
and `has_linked_source_ack_sample` report trusted-prefix ACK evidence separately;
an empty ACK sample cannot imply demonstrated acknowledged-write durability.
An uncertain-only write corpus is still useful and may have complete read
observations. Its zero-ACK flags and unresolved original outcomes remain explicit.

## Bounds and limitations

Each invocation permits at most 16 original sources, 2 GiB aggregate original
journals, 64 MiB aggregate original plans, 8,192 targets, 32 KiB per raw lexical
value, 64 MiB total retained target spans, and 128 MiB total generated output.
Read-result journals have a separate 2 GiB aggregate limit. Individual source
ledger verification retains its existing 64 MiB line / 70,000-row limits. Original
plans may include large whitespace; if an unseen value's exact span exceeds
32 KiB, generation refuses the entire output rather than normalizing it or
omitting its target. Parser depth and Python integer-literal limits likewise
fail closed; arbitrary numeric exponents and decimal precision are preserved
within accepted parser bounds.

Inputs are scanned in bounded passes for hashes, ledger validation and selected
raw spans. Peak RAM/runtime at maximum bounds have not been measured; use an
independent supervisor and preserve its original outcome. Budget exhaustion
produces no complete subset. No physical experiment is authorized or performed
by this tooling. The failed Azure scale-out qualification stays failed.
