# Optional exact client RPC evidence

This source-only diagnostic addition is based on
`2a7a587fcfd392e851dc42eccc8762837b0c7fb6`. No runtime result, original CLI failure
cause, HA qualification or lost-ACK reconciliation is inferred from it.

The public opt-in builders on `ClusterEventClient` and
`TcpClusterRelocationTransport` accept an
`Arc<dyn ClusterRpcEvidenceObserver>` and `ClusterRpcEvidenceLimits`.
`record(&ClusterRpcEvidence) -> std::io::Result<()>` must persist the COMPLETE
record durably before returning success. A fixture can use compact JSONL with
`serde_json::to_writer`, newline, then `File::sync_all`. Keep this journal private:
exact application values/keys, topology and response bodies are intentionally
retained. No private TLS key file is read or copied by the observer.

Default `None` uses the existing send/receive, route selection, retries, peer
checks and deadlines. The observer supplies no network reply, topology, role,
certificate, decision or read/write permission. A new `RpcEvidenceFailure`
means the opt-in journal failed; it is not a fabricated DB commit identity and
does not use `CommitOutcomeUnknown`'s database incarnation/sequence fields.

## Actual records, not reconstructed explanations

Each invocation has a distinct diagnostic attempt UUID. Every phase carries
wall-clock Unix nanoseconds and monotonic elapsed nanoseconds from that attempt.
An observer-wide journal sequence identifies reservation order; concurrent sink
completion order may differ. Once the existing
transport allocates its actual frame request ID, the record includes that ID
and the address/topology generation taken from the SAME locked route snapshot
used for dispatch. Request and response `raw_frame` contain exact framed JSON
bytes, including the four-byte big-endian header. Metadata discovery failures
therefore remain visible even if the Event client later falls back, retries or
returns a generic no-member-reachable error. No response/error is truncated or
replaced with a digest. Oversized evidence refuses rather than silently pruning.

Phases distinguish:

- `invoked`: before cancellation/route failure; no invented frame ID.
- `request_frame_before_dispatch`: full request durably recorded before connect
  or frame write. The observed path sends these same bytes.
- `response_frame_before_decode`: full bounded response before JSON decoding,
  identity validation or final cancellation/deadline override. This is captured
  data, NOT a claim that the response is valid/authenticated protocol authority.
- `partial_or_rejected_response_frame`: all consumed partial bytes or the exact
  rejected length header, plus the available error chain. An oversized declared
  body is not read past the existing frame bound.
- `exchange_error_before_cancellation_override`: the actual IO/TLS/decode error
  even when a later original-token check returns cancellation instead.
- `validated_transport_result_ok/error`: the original transport's result before
  the final diagnostic IO/return-boundary check. This is not a business ACK.
- `return_boundary_error_after_evidence`: original cancellation/deadline expires
  while persisting evidence; a late successful response is not returned as ACK.

`bytes_written` counts framed plaintext accepted by the actual writer, not TCP
ciphertext bytes, server execution or durable commits. `may_have_dispatched` is
set before the first frame write: it deliberately treats write failures as
possibly dispatched. Response frames and complete available Rust error chains
remain distinct; decoding an error envelope does not replace the raw envelope.

## Failure, bounds and lifetime

Limits default to 4096 records/64 MiB of compact JSONL (with conservative
reservation for the largest sequence-number field) and accept at most65536
records/1GiB. Each full record is counted before invoking the sink. The current
network frame bound still applies. Neither row values nor history are truncated
for diagnostics. The external sink must enforce its own file/format quota if
its representation differs from compact JSONL.

Any callback/quota or actual same-thread sink reentry failure permanently fences
further dispatch on that observed transport. A new observer cannot rearm it.
Before any frame write, `RpcEvidenceFailure.may_have_dispatched=false` refuses
without dispatch; after possible write it is true and the actual operation must
be reconciled independently. An error after actual journal sync is still an
error. The client does not count a success when response/terminal persistence
fails. Other already concurrent operations may already have dispatched; this is
not atomic cancellation of remote IO and never claims rollback.

Observer IO consumes the original caller/network deadline, captured before
logging begins. It cannot renew that deadline. Quota reservation waits are
cancellable under that same window; the reservation lock is released BEFORE
sink IO. Normal parallel callbacks are supported. Each Send+Sync sink must write
complete records atomically and enforce its own IO bounds/order; same-thread
reentry into the same sink is refused. EventClient owns no engine guard here.
The public transport adapter itself acquires no engine guard for callbacks, but
arbitrary transport callers must ensure they hold none when opting in: this is
not a universal public-transport lock guarantee. Sink IO is trusted host code;
a blocking filesystem callback cannot be interrupted by this adapter. This is not a hard-IO deadline guarantee. Original
cancellation is checked after diagnostic IO; completed durable remote work is
not erased by cancellation or a failed reply journal.

## Required execution

Nine new `actual_rpc_evidence_` controls use a genuine legacy mTLS receiver and
fsynced private JSONL. They cover exact discovery frames/error followed by real
fallback, exact APP key/value and stale-epoch refusal, pre-dispatch cancellation and genuine response-before-original-cancellation,
pre-wire persistence refusal with no application mutation, post-response actual
journal sync then error/uncertainty, no observer rearm, and record/byte quota
refusal without truncation. A real concurrent two-RPC rendezvous requires both
callbacks to enter together and both actual TLS Pings to succeed; a separate
same-thread reentry control must refuse before dispatch. They are source assertions, not passing results.
They do not establish born-v2 DATA, HA, deployment or receiver relocation.

Run these controls and existing Event route/discovery/deadline controls before
integrating the observer into the external CLI fixture. The fixture must bind
its logical attempt/reply ledger to these actual per-RPC records and preserve
the original failed CLI cohort separately. Public projections may redact values
only as a separate artifact; retain the exact private originals and hashes.
