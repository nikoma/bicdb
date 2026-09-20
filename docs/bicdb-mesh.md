# BicDB Mesh — local-first peer-to-peer replication

A BicDB database doesn't have to live somewhere. It can live everywhere the
application does. BicDB Mesh is the campaign to make that literal: every
database syncs with authorized peers over whatever moves bytes — LAN, BLE,
USB stick, shared file, cloud. The cloud is just one peer. Connectivity is
merely a transport.

Reference workload: an intermittently connected field application where intake,
review, and operator
devices collaboratively complete patient encounters with no server and no
Internet, converging over proximity links.

## Layering

| Layer | Status | Where |
|---|---|---|
| Event replication (origin envelopes, UUID dedupe, checksummed bundles, record reconciliation, encrypted bundles) | shipped pre-Mesh | `bicdb-core/src/sync_mesh.rs`, `db.rs` |
| Per-origin version vectors + delta exchange + transitive relay | **Phase 0 — this document** | `SyncVector`, `BicDb::sync_vector`, `BicDb::export_sync_bundle_delta` |
| Duplex mesh session over any reliable byte pipe (strict ping-pong framing) | shipped | `bicdb-sync/src/mesh.rs` |
| Transports (`SyncEndpoint`: file today; LAN/mDNS, BLE, cloud later) | file endpoint shipped | `bicdb-sync` |
| Causal conflict ordering (write contexts, dominance-first resolution, surfaced concurrency) | **shipped** | `record_audit_events_for_commit`, `reconcile_record_audit_events`, `BicDb::record_conflict` |
| Timing evidence (session offset estimation, replicated clock observations, corrected-age tiebreak, monotonic write clocks) | **shipped** | `FrameTiming`, `CLOCK_OBSERVATION_STREAM`, `corrected_order_winner` |
| LAN transport (multicast/unicast beacons, auto-sync loop, per-peer status) | **shipped** | `bicdb-sync::LanMesh`, `BicDb::mesh_peer_status` |
| Signed frames v1 (ed25519 origin signatures, TOFU key pinning, strict-import mode) | **shipped** | `DbConfig::with_mesh_signing`, `EventEnvelope.signature`, `BicDb::pin_node_key` |
| Identity & authorization (camp/device cert chain, QR join, role-scoped publish filters) | designed, not built | Phase 2 |
| Per-field conflict strategy declarations | designed, not built | Phase 2+ |

## Phase 0: the replication core

### Version vectors

`SyncVector` maps each authoring node to the highest origin sequence held.
Origin sequence = the event's offset in its author's log, carried in the
event envelope and preserved verbatim across any number of relay hops via
`_bicdb_sync` metadata.

The exchange is two messages, transport-agnostic:

```
peer A → peer B:  my vector            {A: 182, B: 74, C: 91}
peer B → peer A:  delta bundle         every event B holds that the vector
                                       doesn't cover, any origin
```

Run it in both directions and any two nodes converge — no hub, no shared
history, no prior contact. `export_sync_bundle_delta` re-exports imported
foreign events with their origin envelopes intact, so store-and-forward
(A→B→C→D) is the data model, not a feature.

`bicdb-sync::run_mesh_initiator` / `run_mesh_responder` drive that exchange
over any ordered, reliable byte pipe (`Read + Write`): hello/hello,
delta/delta, complete/complete as a strict ping-pong, so it cannot deadlock
on small pipe buffers and works on half-duplex links. A session cut at any
byte is repaired by running another session — imports are idempotent and
vectors are recomputed from the durable log, so one-sided progress is safe.
The LAN, BLE, and cloud transports are "hand these functions a connected
stream"; the hub-shaped `SyncCoordinator`/`SyncEndpoint` remains for
client↔server topologies.

**Protocol minimalism (invariant).** The mesh protocol is exactly "here is
who I am, here is what I know, give me what I lack" — and it stays that
way. Identity, authorization, filtering, and timing evidence augment the
hello/session negotiation and the envelope; they never add application
semantics to the protocol layer. Every future transport (BLE, USB, WebRTC,
serial, embedded) inherits its simplicity from this restraint.

**Origin positions, not event counts.** The `sequence` carried in every
envelope (and therefore in vectors) is a **durable monotonic position in
the authoring node's log** — in practice a byte offset. Seeing
`origin A → 38194722` next to `origin B → 844193` is normal, not
corruption, and the two numbers are not comparable to each other. Positions
are strictly increasing in authorship order per origin and survive relays
verbatim; that monotonicity is all the vector mathematics uses. Say
"origin position" (or "origin cursor") when talking about them; the wire
field stays `sequence` for compatibility.

**Soundness (the prefix property).** Every export streams a given origin's
events in ascending origin position, and every import appends in bundle
order; therefore each node's holdings per origin are always a gapless prefix
of that origin's exportable events, even after a crash mid-import. A
max-watermark is thus a complete description of coverage. This is why
vectors must come from `BicDb::sync_vector()` (recomputed from the durable
log), never hand-built: a vector claiming events the node doesn't hold would
create silent holes.

Events targeting protected collections never export, so they are excluded
from vectors too: the vector describes exactly what a peer could receive.

### Bundle compatibility

Delta bundles carry the exporter's own vector as `source_vector` — a
receiver learns the sender's coverage without another round trip. The field
is deliberately **outside the v1 checksum**, so pre-vector binaries still
verify new bundles and vice versa; mixed fleets interoperate. Until frame
signing lands (Phase 2) the vector is advisory; per-event integrity is
covered by envelope payload hashes as before.

### Acceptance gate: the torture suite

`crates/bicdb-core/tests/sync_mesh_torture.rs`. Radios are delivery
mechanisms; convergence is the architecture. The suite requires eventual
convergence under:

- three offline devices completing one patient encounter purely by relay
  (registration → triage → doctor → back), with pairs that never met
- partition: a node disappears, its data travels two relay hops with origin
  attribution intact, it returns and catches up from a stranger — and the
  reverse delta is empty
- the same bundle arriving six times, and the same events arriving over two
  different paths
- transfers stopping halfway and payloads tampered in transit (refused
  whole, zero state change, clean retry)
- a device clock 45 minutes wrong
- a reboot mid-import (the durable half survives, the vector reflects
  exactly what landed, re-send finishes via dedupe)
- deltas containing only what the peer is missing, whatever path delivered
  the rest

## Conflict ordering

The engineering principle: **no data loss, deterministic convergence,
causality preserved, uncertainty never masquerades as certainty.**

Every audited write is stamped with the writer's coverage vector at write
time (`write_context`, inside the payload and therefore covered by the
envelope hash — relays cannot rewrite causality undetected). Resolution
per record is layered:

1. **Causal dominance.** A write whose context covers a rival's
   `(origin, sequence)` provably came after seeing it and wins outright.
   Time is never consulted: the phone that thinks it is 1970 still wins
   with an edit it made after receiving the original, and a device whose
   clock jumped four years backwards keeps its own newest edit.
2. **Deterministic projection over the concurrent frontier.** Writes none
   of which saw the others fall through to the `(timestamp, sequence,
   node_id)` order — every replica projects the same value, but the choice
   is a projection rule, not a truth claim.
3. **Surfaced concurrency.** A frontier wider than one is reported by
   `BicDb::record_conflict` / `list_record_conflicts` with all candidates
   retained, identically on every converged replica. Resolution is not a
   special mechanism: any subsequent write made after syncing the whole
   frontier covers it and clears the conflict camp-wide.

Timing evidence sits between layers 1 and 2 as a *tiebreak with an
uncertainty bound* — extra evidence, never a correctness dependency. Live
sessions stamp every frame (`FrameTiming`), so each ping-pong yields an
NTP-style offset estimate on both sides; tight estimates (≤5s error) become
**replicated clock observations** (`CLOCK_OBSERVATION_STREAM`, rate-limited
against jitter). Because observations replicate like any other event, every
converged replica derives the identical clock table, keeping timing-informed
resolution deterministic. For exactly two concurrent candidates from
different origins with an observation connecting them, the resolver corrects
the timestamps and picks the genuinely later write only when the difference
exceeds `2×error + 2s`; anything closer — or any pair without evidence (a
USB bundle that sat in a backpack for seven hours has infinite uncertainty)
— stays at layer 3. Audit events additionally carry a monotonic
`write_clock` (runtime session id + elapsed-ms): forensic evidence that
never jumps when the wall clock does; a changed session id is itself the
signal that comparability was lost.

Modeling guidance: append-only regulated data (observations, prescriptions,
consents — each its own record) never conflicts and remains the default;
the layers above exist for the genuinely mutable residue (demographics,
contact details).

## Signed frames (v1)

With `DbConfig::with_mesh_signing(true)`, a node generates an ed25519 key
(`mesh_signing_key.json`, 0600) and signs every envelope it authors over a
canonical message covering origin, event id, stream, origin position,
timestamp, and the payload hash — which itself covers the payload including
`write_context`, so causality metadata cannot be rewritten either.
Signatures ride in `_bicdb_sync` metadata and therefore **survive relays
verbatim**: a receiver two hops from the origin verifies "this really
originated on that node and has not changed" while extending zero
authorship trust to the nodes that moved the bytes. That is the role
separation the filtered-relay design needs: authorized transporter,
unauthorized author (unauthorized *reader* arrives with envelope
encryption in Phase 2).

Key distribution v1 is trust-on-first-use: session hellos announce the
verifying key; receivers pin it (`pin_node_key`). A pin never silently
changes — a conflicting key aborts the session as an impersonation alarm.
`with_require_signed_imports(true)` turns on strict mode: unsigned events
and unpinned origins are refused outright. Certificate chains (Phase 2)
layer legitimate rotation and organizational identity on top of pins.

Signatures sit outside the v1 bundle checksum (like `source_vector`), so
mixed fleets interoperate; the signature is the stronger integrity where
present. The signing secret is plaintext-at-rest in v1 — wrapping it with
the database encryption runtime is Phase 2 hardening.

## Known caveats (deliberate, tracked)

- **Full-log reconcile per import; O(edits²) frontier per record.** Fine at
  clinic scale, not at millions of events; incremental reconciliation is
  planned alongside disk-backed event reads. The coverage vector is already
  incremental (cached, folds in only the appended tail; log rewrites reset
  it).
- **Raw event appends (`events_mut().append`) do not project locally** —
  reconciliation runs on import. Application writes go through `insert`/
  `update`/`delete`, which project immediately and stamp contexts
  automatically.
- **`SyncCoordinator` is hub-shaped.** After importing, it advances the
  local export cursor past imported events without pushing them — correct
  anti-echo for client↔server, but it suppresses relay. Mesh sessions use
  vector deltas instead; the coordinator gains a vector-based mesh mode in a
  later PR.
- **No signatures yet.** Bundles are checksummed (integrity), not signed
  (authenticity). Signing is Phase 2, tied to identity.

## Roadmap

1. **Phase 0 (this)** — vectors, delta exchange, transitive relay,
   torture suite.
2. **Phase 1: LAN** — mDNS discovery + TCP/TLS `SyncEndpoint`; a travel
   router with no Internet is full camp infrastructure. Interruption is the
   normal case, not the error path: sessions resume by re-exchanging
   vectors. File bundles already cover sneakernet (USB, AirDrop/Nearby
   Share, messenger attachment) — "Share BicDB Sync Bundle" is UI over an
   existing code path.
3. **Phase 2: identity** — org root → site cert (expiring) → device cert →
   user role; QR join carrying cert + invite nonce; authors sign events;
   receivers enforce role filters against the *sender's certificate*, never
   its claims. Site end = cert expiry = the replication relationship dies by
   default. Lost-device revocation propagates as a signed in-band event.
4. **Phase 3: mobile + BLE** — FFI packaging; BLE advertise/GATT endpoint
   for zero-infrastructure proximity sync; byte-level resumability matters
   most here.
5. **Phase 4: cloud peer** — the cloud is one more mesh participant with a
   bigger disk; whichever device first regains Internet becomes the bridge.

### Filtered relay (protocol-level, designed with Phase 2)

Participation in replication must not imply permission to read replicated
content: a courier device carries envelope-encrypted events it cannot
decrypt, exposing only routing metadata (tenant, replication group, origin,
position, ciphertext, signature). Origin signatures make the roles fully
separable — a device can be an **authorized transporter, unauthorized
reader, unauthorized author**: receivers verify "this really originated on
A and has not changed" without extending any authorship trust to the nodes
that moved the bytes. This generalizes far beyond regulated-data and belongs in
the protocol, not in any application.

### Cryptographic revocation (language matters)

The current protected-data implementation uses one process-wide field-encryption key and
one separate blind-index lookup key. It does **not** yet provide per-record or
per-patient key wrapping: destroying the field key makes every field protected
by that key unreadable, not one selected patient's history. Blind-index tokens
also remain after field-key destruction and continue to reveal equality until
their separate lookup key and derived indexes are retired. Treat this as
all-or-nothing cryptographic revocation support, never selective erasure, and
never assume it automatically satisfies a statutory deletion obligation:
keys may have been cached, plaintext may exist in backups or derived
projections, other authorized nodes may have exported, and regulated-data
retention rules vary by jurisdiction.
