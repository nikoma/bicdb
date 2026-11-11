# Explicit strong point reads: P0 design and proof obligations

Design baseline: **1.0.463-beta**, source
`d49a391b3c6157f90f29e9f2f5214143c60f9f4b`. This document proposes an additive
contract. No strong-read implementation or qualification is delivered here.
The original 1.0.461 Azure failure and its unresolved causes remain unchanged.

## Contract and primary proof boundary

Keep legacy `Get`'s documented local-applied/eventual behavior. Add an explicit
`StrongGet`/`strong_get` operation and matching client method. A successful strong
point read must be linearizable with successful writes to that range: its result
reflects a state between invocation and response, including every earlier completed
write. A healthy, caught-up quorum must actually return values; permanent refusal
does not satisfy this contract. Cross-range SQL/snapshots/transactions are outside
this point-read change.

The primary Raft argument requires a committed current-term anchor, a fresh
majority confirmation of leadership, and application through the captured read
index. Joint configurations require their respective majorities. See
[Ongaro and Ousterhout, §§6 and 8](https://raft.github.io/raft.pdf) and
[Ongaro dissertation, §6.4, pp.72–73](https://raw.githubusercontent.com/ongardie/dissertation/master/online.pdf).
The latter explicitly separates the committed read index from the applied state
machine. Timing-based lease shortcuts require additional clock assumptions.

These references justify the **shape of a metadata read-index proof**. They do
not prove BicDB's custom range-write protocol is Raft, or transfer Raft's Leader
Completeness property to range storage. A BicDB implementation must separately
establish the metadata invariants and the custom range decision/relocation
invariants below. No inherited production qualification is claimed.

## Existing mechanisms and missing APIs

| Source | Existing mechanism | Reusable fact / missing proof |
|---|---|---|
| `distribution_events.rs::ClusterEventRequest`, `ClusterEventOperation`, client `request` | Range/epoch routing, bounded retry/discovery, immutable key/payload | No consistency selector, minimum commit token or strong-read operation. Legacy Get must not silently acquire a new guarantee. |
| `distribution_transport.rs::ApplicationEvent` dispatch | mTLS member identity, schema compatibility, published epoch and named-leader check | Published cached topology is not fresh leadership authority. No Get quorum barrier is executed. |
| `distribution_consensus.rs::MetadataConsensusStore::become_leader` | Persists current-term `leadership_barrier` entry | A merely appended barrier is insufficient; read API must prove that current-term anchor committed. |
| Metadata `append_request_from`, `handle_append_request`, `record_append_response` | Terms, log identity, durable responses and joint old/new commit quorums | Can transport fresh authority checks, but no exported per-read context/read-index completion API exists. Cached `match_index` is not a fresh read acknowledgment. |
| `checkpoint_driver.rs::replicate_metadata_round` | Parallel authenticated Append RPC rounds | Host-private helper; its returned voter set alone is not a strong-read certificate. Need post-round role/term/configuration validation and both voter sets when joint. |
| `distribution_range_consensus.rs::RangeWriteCoordinator` | Per-range reservation, cached `recovered_epochs`, fresh prefix/tail recovery | Cached epoch is not fresh authority or an applied floor. Recovery must run from fresh evidence for the read, not skip because a boolean was previously set. |
| `recover_leader_prefix` / `recover_leader_tail` | Checksummed histories, certified decisions versus provisional/aborted entries, ambiguity/snapshot refusal | Candidate reuse for a custom range committed-floor proof. Requires explicit evidence output and reconfiguration proof; a maximum `last_index` is not sufficient. |
| `distribution_data.rs::apply_range_write_leader_recovery`, `resolve_range_write_leader_tail` | Applies certified mutations before marking resolved/Applied; rejects bad identities/epochs | Candidate reuse for local application, subject to atomic snapshot/base-image and durability invariants. |
| `db/access_mutation.rs::get_unchecked`, `paged_collection.rs`, page MVCC | Local committed snapshot and durability-health checks | Local committed state does not establish cluster freshness or a quorum-certified floor. |
| Backup/anti-entropy fences | Durable bounded write fencing for their own protocols | Do not relabel as read-index or leadership leases. They have different authority/expiry semantics. |

All paths above are relative to `crates/bicdb-core/src`, except the host helper in
`crates/bicdb-pgwire/src`. These are implementation facts, not already established
strong-read APIs.

## Three separate readiness indicators

| Indicator | Required evidence | What it must not imply |
|---|---|---|
| Replica ready | Verified installed range image/base snapshot, namespace/schema identity, non-fenced storage health, and local applied/resolved prefix tied to that image | Replica membership, a transport connection, snapshot byte count, or a high `last_index` alone is insufficient. It grants no leadership. |
| Leader ready | Exact range authority/configuration epoch and node incarnation; recovered unambiguous certified prefix/tail applied locally; transition/publication fences satisfied | A historical `recovered_epochs[E]` flag is not proof that the node is still leader or ready for a future strong read. |
| Strong-read ready | A **request-scoped** fresh authority context, quorum-certified required floor, locally visible snapshot covering that floor, and revalidated configuration/health | No reusable permanent `strong_read_ready=true` bit, cached heartbeat lease, or observer watermark makes later reads safe. |

Expose evidence/reason fields with these indicators: range ID, authority epoch,
range-configuration digest, node incarnation, required certified floor, local
applied floor, verified snapshot/base identity, schema digest, fresh metadata term
and read-index/context, quorum membership digests, and failure reason. An operator
status can report last verification and its age; it must not manufacture a live
read permit from an old status sample.

**Do not equate range-log command epoch with authority epoch.** Recovery replays
old commands without rewriting their checksums/epochs. A caught-up node at metadata
epoch E2 can retain `RangeWriteProgress.current_epoch=E1` until a new E2 command.
The original cold 16/13 and 29/27 epoch differences therefore do not themselves
prove unreadiness. Record validated authority/recovery epoch separately from the
highest retained command epoch and from the applied floor.

## Smallest candidate protocol with explicit dependencies

The initial implementation should use fresh quorum exchanges, not wall-clock
leases. It may be expensive; performance optimization cannot remove proof steps.
This is a proposed composition, contingent on the proof obligations below.

1. Resolve the logical key and enter a request-scoped range reservation. Do not
   hold an outer database read lock across network calls or block metadata
   heartbeats behind storage work. Bind a new server-generated read context.
2. Obtain a metadata authority context from the metadata leader. Require a
   committed current-term anchor and a **new** authenticated quorum round for
   this context. Verify role/term after the round; reject higher terms, stale
   replies, wrong identities, and incomplete joint old/new quorums. Capture the
   committed range configuration, schema/restore identity and node incarnation.
3. Under the validated range configuration, establish a fresh certified range
   prefix/tail floor using the custom decision protocol. Resolve or fail on
   ambiguous provisional history; recover a verified snapshot if required.
   Do not convert a `Prepared`/merely `Committed` entry, maximum `last_index`, or
   aggregate progress counter into a committed visibility promise.
4. Apply all certified decisions required by that floor, retain abort decisions,
   and pin a local MVCC snapshot/base image covering it. Recheck storage health,
   namespace/schema identity and snapshot installation completeness. Range
   relocation/repair application must share the guard or expose a generation
   fence so an image cannot change underneath the proof.
5. Before returning the value, obtain a final fresh metadata authority check and
   require the relevant range authority/configuration/restore/schema identity
   to remain the same. Abort/retry if it changed. A heartbeat-only global topology
   generation increment need not invalidate the same range configuration; use a
   semantic configuration digest and retain generations for diagnostics.
6. Return the value from the pinned snapshot and its diagnostic proof metadata.
   If a deadline expires or any proof is missing, return an explicit unavailable/
   retryable result. Never fall back to eventual Get or present timeout as an
   absent value. Release the range reservation on all paths.

Fresh metadata Append RPCs can be reused, with unique transport identities and a
new host/core read-context API that never substitutes historical `match_index`
for this round's acknowledgments. The metadata read-index/current-term test must
also handle compaction and in-flight membership changes. Existing helpers do not
currently expose all this evidence; copying their return type is insufficient.

The custom range recovery functions need to return a validated required floor
and local application evidence, rather than only `Ok(())`/cached epoch. They must
be freshly invoked under the read guard even if the epoch was recovered earlier.
This does not require pretending each range is a Raft group or immediately
replacing its consensus implementation.

### Proof obligations that block an implementation claim

- **Metadata authority:** establish the Raft-style current-term/read-index argument
  for BicDB's actual metadata store, including joint voter configurations and
  persistence failures. A follower's committed snapshot by itself is not a fresh
  read-authority certificate.
- **Custom range decisions:** demonstrate that a read quorum plus recovery cannot
  omit any earlier completed acknowledged command, including a durable certificate
  surviving on only an intersecting voter, unresolved tails, compacted histories,
  crash/restart and node replacement. Preserve existing fail-closed ambiguity rules.
- **Range membership changes:** prove old/new decision-quorum intersection or an
  equivalent durable authority fence across replica relocation/leader transfer.
  Metadata joint quorum does not automatically prove range joint quorum. A fresh
  read barrier cannot compensate for a write protocol that lets an obsolete range
  configuration acknowledge divergent writes. This is an explicit test/proof
  dependency, not a newly proven defect or an Azure root-cause assertion.
- **Application floor:** prove local Applied/resolved progress is coupled to the
  installed physical image and the snapshot used by Get. A command record without
  visible row state is insufficient. Do not rewrite old command epochs just to
  match new topology epochs.
- **Linearization and health:** prove the range guard and two authority checks
  place the pinned snapshot inside the read interval while retaining all prior
  completed writes. Restore, schema transition, incarnation change and durability
  fencing invalidate permits. No permits survive process restart or are reused
  for a later request.
- **Liveness:** with a healthy authoritative caught-up quorum and stable range
  configuration, the positive case must return the actual expected value within
  a declared finite budget. Parallel bounded quorum work must not wait for every
  unavailable voter; timeout/error is not a substitute for the positive test.

Until these obligations pass actual-source tests, this remains a design candidate,
not a finished proof that the composed implementation is linearizable.

## Additive wire and compatibility boundary

Use a new mandatory operation variant (`strong_get`) or new versioned request;
do not add an optional `consistency` field that an old server could silently
ignore while executing legacy Get. Old binaries must reject the explicit strong
operation, and a new client must report unsupported capability without downgrade.
Preserve legacy Get's serialization and contract.

Add capability negotiation/manifest identity for explicit strong point reads,
separate implemented, CI-tested, physically tested, qualified and deployed states.
Initial mixed-version rollout keeps strong admission closed until the required
metadata authority service, range leader and quorum support the negotiated
protocol. Membership changes cannot admit an incompatible voter into a strong
configuration. Protocol additions require cross-version refusal tests; an API
being present is not evidence of qualification.

The smallest new surface is a strong point-read operation/client method, a typed
metadata read-authority context, and a typed range floor/snapshot permit. Existing
Append transport and certified-history transfer can be reused where their
invariants are demonstrated. No weaker acknowledgment, fence, fsync, or automatic
stale-read fallback is permitted.

## Deterministic characterization and required regression matrix

| Case | Required strong outcome | Why a boolean or index shortcut fails |
|---|---|---|
| Previously recovered leader isolated in a minority; a higher authority completes a write | Reject/redirect; never a successful obsolete value/absence | Cached recovered epoch and named-leader local view remain true. |
| New named leader before applying an existing certified command | Wait within budget or unavailable, then exact value after catch-up | Topology ownership is ahead of local application. |
| Healthy caught-up quorum, with old-command epoch below current authority epoch | **Return exact value** | Requiring equality of retained command epoch and topology epoch creates unnecessary permanent refusal. |
| Current-term metadata anchor appended but uncommitted | No strong success | Election or log presence alone is insufficient. |
| Joint membership; only one side's majority responds | No strong success | One majority is not a joint quorum. |
| Provisional last index exceeds certified/applied floor | Resolve/fail; never expose provisional state | `last_index` is not commit/application. |
| Certified floor exceeds local applied floor; or snapshot base is incomplete | No value until image/application proof closes the gap | A checksummed catalog is not row visibility. |
| Authority/leader/schema/restore changes between initial proof and return | Retry under a new context or unavailable | A permit is scoped to a specific configuration/image. |
| Heartbeat generations change while range authority is stable | Healthy strong reads still complete | Requiring global-generation equality can starve reads during normal operations. |
| No strong protocol support on an otherwise healthy cluster | Explicit unsupported; healthy strong positive remains red | Refusing every read is safe refusal, not implementation of the goal. |

Actual-core characterization fixtures should label scripted topology changes,
mocked transport partitions, loopback networking and fsync=false explicitly.
They can expose legacy Get's missing strong guarantee; they cannot qualify HA,
establish physical RPO/RTO, or identify the original Azure failure's cause.
