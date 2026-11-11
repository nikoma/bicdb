# Metadata read-authority foundation

This candidate adds a fresh, request-scoped **metadata-only** read-index proof.
It does not implement `StrongGet`, certify a range's visible image, or change
legacy event `Get` semantics. Strong row reads remain unavailable until the
custom range acknowledgement/recovery, reconfiguration fencing, certified
application floor, namespace/schema/reader-capability image and final semantic
authority checks are proved and integrated.

The source base is `d49a391b3c6157f90f29e9f2f5214143c60f9f4b`,
**1.0.463-beta**, plus the source hashes in [summary.json](summary.json).
Tests ran before a source version bump; they do not qualify the frozen463
production binary. The core candidate used opt0/debug0 with retained optimized
dependency externs, one Cargo job, incremental compilation disabled and an active
12 GiB available-memory guard. No resource interruption occurred. The focused
build took58.01s with at least24.36GB available; the same test binary then passed
the distribution regression selection in3.07s with at least26.50GB available.

| Check | Result |
| --- | --- |
| Fresh authority cases, including actual loopback wire round | 7 passed |
| `bicdb-core --lib distribution_` selection, same binary | 132 passed |
| General strong point reads, physical fault/scale qualification | Not implemented / not tested here |

The new round requires a **committed entry from the leader's current term**, a
fresh UUID context and application request identity, a finite monotonic context
budget, and fresh responses from both the old and prospective voter majorities.
Voter identities bind configured incarnations and active/pending certificate
pins. Only opaque transport-correlated ACK objects count; historical
`match_index` never supplies a read vote. The normal durable Append receiver
supplies the metadata-applied floor. Higher terms, changed voter identities,
wrong rounds, insufficient applied indexes and expiration refuse authority.
Transport still validates its existing TLS/SAN/envelope/source/destination/
cluster/request/protocol boundaries. Local loopback plaintext is used only by
the retained wire fixture, not evidence of mTLS qualification.

`finish_read_authority` consumes the context and returns the **latest committed
metadata topology/restore activation** with readIndex/appliedIndex. That result
is neither a range-read permit nor a renewable lease. A context deadline is a
validity bound, not proof that blocking network calls finish within that bound.
The participant incarnation is the transport's expected member identity, not a
new cryptographic process-incarnation attestation. Existing member/fencing
assumptions remain.

The added wire variant is explicit and fail-closed on unsupported peers. There
is no fallback to an ordinary Append/weak read to manufacture read authority.
The legacy MetadataAppend payload shape remains unchanged and is checked by the
wire test. Read ACKs do not commit pending metadata proposals.

The six store negatives/positives cover an uncommitted current-term anchor,
healthy fresh quorum versus cached historical votes, wrong-round replay,
higher-term stale leader, one-sided joint quorums, and expiry/member identity.
The transport case executes one fresh loopback round and asserts unchanged
legacy Append serialization. Full retained test output and command receipts are
adjacent; [summary.json](summary.json) binds the three candidate source files.

The proof follows the current-term anchor, fresh-quorum and applied-readIndex
requirements described in the primary [Raft paper](https://raft.github.io/raft.pdf)
and [Ongaro dissertation, section6.4](https://raw.githubusercontent.com/ongardie/dissertation/master/online.pdf).
BicDB's separate range-write protocol must be proved on its own terms; calling
the metadata round Raft-shaped does not establish range safety or availability.
In particular, the separate [RF3 ACK-certificate cut](../range-certificate-ack-cut-2026-11-11/README.md)
already exposes an acknowledged-write recovery-availability failure that this
metadata foundation does not repair.
