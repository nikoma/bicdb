# BicDB security audit — findings and remediation record

**Status:** Internal audit complete; all 51 actionable remediation items below
were closed on 2026-08-15 for release `1.0.175-beta`. **An independent external
review on 2026-08-16 found a further 5 CRITICAL / 14 HIGH class of issues the
internal audit did not cover — see the appended section at the end of this
file. Three CRITICALs are confirmed on current main.** (The storage-corruption team's
WAL/checkpoint sub-specialist is folded in; pure-MVCC visibility and
buffer-pool singleflight were not deeply covered — see Residual coverage
gaps. TID hints were subsequently hardened and covered by the page suite.)
**Date:** 2026-08-15. **Method:** parallel expert review across seven attack
surfaces (RLS/authz, pgwire/auth, page-store corruption, crypto/secrets, mesh
trust, DoS/deserialization, injection/unsafe/paths), each reading real code;
findings cross-verified and deduplicated. All items below are **CONFIRMED by
reading code** unless marked SUSPECTED.

Closure of the enumerated findings is not admission of the current general
server for a global PHI fleet. The target
[`BicDB Cell/Application Architecture`](bicdb-cell-application-architecture.md)
defines stronger process/microVM, per-cell key, complete at-rest encryption,
application/frontend supply-chain, orchestration, replication, device, and
cross-Cell-sharing gates. Its callable, industry-neutral Phases 0–8 now cover
the single-Cell construction, cryptographic storage, Cell-native application,
fleet lifecycle, HA/recovery, device-edge, recipient-grant, and exact-build
hardened-fleet evidence-verification boundaries. Phase 8 does not fabricate
the external reviews, production attestation, or authority ceremony it is
designed to verify; every shipped Cell profile therefore still explicitly
denies regulated production-data admission.

## Remediation verification

All P0, P1, P2, P3, and C1-C7 items have an implementation disposition. Where
the suggested mechanism was not the safest compatible option, the implemented
control closes the same boundary (for example, database-backed `bicdb_*`
functions are superuser-only rather than being allowed to reach raw storage
under an incomplete SQL RLS context, and the crypto-shred documentation now
states its actual global-key and blind-index limitations).

Verification includes the full `bicdb-page` library/integration suite, pgwire
authentication tests, sync/mesh and encrypted-bundle suites, targeted SQL role
authority, mutation-authority and FTS-budget tests, plus focused backup,
geometry, PHI, WAL-pruning and mesh-signature regressions. The workspace was
also compiled across the changed core, sync, SQL, pgwire, page, and CLI crates.

## Severity scale
- **P0** — remote/unauthenticated, or trivially reachable by any authenticated
  user, and collapses a security boundary (auth bypass, cross-tenant/RLS
  disclosure or write, whole-process crash, key/plaintext disclosure).
- **P1** — authenticated user or malicious peer breaks isolation, corrupts
  data, or takes the service down.
- **P2** — needs some access, unusual configuration, or has narrower impact.
- **P3** — hardening / defense-in-depth (no confirmed exploit).

## The ambient-authority bug family

H-14 through H-17 are one bug written four times: a value carrying security
meaning whose ABSENCE was read as the strongest possible answer. The pattern,
the doctrine, the heuristic that finds them, and the remaining hunt list are
written up in [ambient-authority-bug-family.md](ambient-authority-bug-family.md).

## Cross-cutting themes
1. **SQL RLS + GRANTs are a SQL-engine-layer construct** (`rls_allows_record_with_schema`,
   `require_relation_privilege`), enforced per-record only on the normal query
   path. Any code that reaches storage by another door — catalog scalar
   functions, the replication apply path, role DDL — bypasses it. This is the
   single largest structural risk and the source of three separate P0s.
2. **Secure-by-default is off** in two places that matter: mesh signing/import
   verification, and pgwire auth/TLS.
3. **No `catch_unwind` around SQL/DB handlers**, and several parsers recurse or
   allocate on unvalidated counts — so a small input can abort the whole
   process or poison shared mutexes.

---

# P0 — critical

### P0-1 · Self-service privilege escalation via unguarded role DDL
- **Where:** `crates/bicdb-sql/src/session.rs:7089` (`execute_create_role`),
  `:7123`/`:7147-7148` (`execute_alter_role`), `:6314`
  (`execute_raw_create_user`) → `schema_meta.rs:1701` (`create_role_record`,
  raw `db.insert`). Enforcer bypass at `records.rs:1860`.
- **Exploit:** any authenticated non-superuser LOGIN role runs
  `ALTER ROLE <self> WITH BYPASSRLS;` — no reconnect (the RLS check re-reads the
  role fresh per statement) → every later query returns `PreparedRls::Allow` →
  full cross-tenant read/write. Or `CREATE USER evil SUPERUSER LOGIN PASSWORD 'x'`.
- **Why:** none of the role-management paths check the caller's privilege;
  every *other* DDL path is gated (`CREATE DATABASE`, `SET ROLE`, `ALTER TYPE OWNER`),
  and the `CREATEROLE` bit is parsed and stored but never consulted.
- [x] **Fix:** gate `create_role`/`alter_role`/`create_user`/`grant-role`/`drop-role`
  on `session_user_is_superuser()`/`create_role`; forbid non-superusers from
  setting `superuser`/`bypassrls`/`createrole`/`replication`.

### P0-2 · Unauthenticated remote OOM-abort — unbounded `Vec::with_capacity` in pgwire binary decoders
- **Where:** `crates/bicdb-pgwire/src/lib.rs:9044` (geometric path/polygon, param
  OID 602/604), `:9251` (multirange, OID 4451), `:12361` + array twin `:9468`
  (composite/`record`, OID 2249 — the field-count guard is skipped because
  `pg_table_row_type_definition` returns `None` for 2249).
- **Exploit:** unauthenticated in the shipped default (`require_auth: false`).
  Send `Parse "SELECT $1"` with a chosen param OID, then `Bind` a ~5-byte binary
  value with `count = 0x7FFFFFFF` → `with_capacity` requests 34–51 GB →
  `handle_alloc_error` → `abort()`, uncatchable by `spawn_blocking`, kills the
  whole process (default Linux overcommit).
- [x] **Fix:** bound `count` against remaining payload before `with_capacity`,
  mirroring the sibling array decoder at `:9351`.

### P0-3 · Catalog/geo scalar functions bypass RLS + tenant policy + GRANTs
- **Root cause:** `eval_db_catalog_function_value(db: &BicDb, …)` (`eval.rs:992`)
  is handed a raw engine with **no `SecurityContext`** (the sibling broker
  evaluator *is* given one). The functions read rows straight through the core
  API and return row **content**.
  - **F1 (worst — bypasses even core tenant policy):** `bicdb_reverse_geocode`
    (`eval.rs:1390`) → `db.nearest()` (`db.rs:22125`), whose only gate
    (`ensure_spatial_point_query`, `db.rs:23067`) checks existence + finite
    coords — **no policy/tenant/RLS/privilege check at all**. Leaks even
    `meta.policy`-protected (tenant) collections.
  - **F2:** `bicdb_record_asof`/`bicdb_geometry_asof` (`eval.rs:1130`) →
    `snapshot_at` (`db.rs:11748`) reads `RECORD_AUDIT_STREAM` raw.
  - **F3:** `bicdb_tile_mvt` (emits every non-geom column as a tile tag),
    `bicdb_admin_hierarchy`, `bicdb_geocode`, `bicdb_record_conflict(s)`,
    `bicdb_raster_zonal_mean` (`eval.rs:1518/1437/1338/1607/1186`) → `scan_collection`/`get`:
    fail-closed on core `meta.policy` but skip **SQL RLS** and **table GRANTs**.
- **Exploit:** `SELECT bicdb_reverse_geocode('patients','home_geo',lon,lat,1000);`
  returns other tenants' full patient rows; sweep coordinates to enumerate.
  Scalar functions need no FROM, so the query's own RLS/GRANT checks never touch
  the named collection; table/column names are discoverable via `pg_catalog`.
- [x] **Fix:** thread the caller's `SecurityContext`/RLS engine into these
  functions; route reads through `scan_collection_with_context` + RLS +
  `require_relation_privilege` (the pattern already exists in
  `full_text_search_authorized`, `db.rs:30641`).

### P0-4 · Mesh trusts unsigned imports by default; connect → data exfiltration; LAN auto-sync
- **Where:** defaults `db.rs:998-999` (`mesh_signing: false`,
  `require_signed_imports: false`); `verify_envelope_signature` terminal
  `_ => Ok(())` (`db.rs:11472`); signature is **outside** the bundle checksum
  (`sync_mesh.rs:83`, `calculate_checksum` excludes it) so it can be stripped
  and the checksum still validates; `export_sync_bundle_delta` (`db.rs:11282`);
  LAN worker `lan.rs:216/226/270`.
- **Exploits (default config, no auth):**
  - **Write:** any peer sends a bundle of unsigned `RecordCreated/Updated/Deleted`
    events with an arbitrary claimed origin → applied. **Pinning is a no-op**
    unless `require_signed_imports` is also on (signature-strip downgrade).
  - **Read exfiltration:** connect + send an **empty** `SyncVector` → the node
    computes "everything you don't have" = **its entire event log** for every
    non-protected collection and streams it back.
  - **LAN:** a single UDP beacon (unicast accepted) makes the node auto-dial the
    advertised address and run a full session (export + import); `group` is
    documented as "a filter, not a security boundary."
- [x] **Fix:** require authenticated + signed sessions by default for network
  transport; enforce a pinned origin's signature regardless of the global flag;
  move the signature inside the checksum; authorize/scope exports per peer; do
  not export on unauthenticated sessions.

---

# P1 — high

### P1-1 · Replication apply path: protected-namespace squat + no tenant enforcement
- `event_targets_protected_collection` (`db.rs:27613`) treats a collection as
  protected only if it **already exists locally** with `meta.policy`;
  `apply_replicated_upsert` (`db.rs:27856`) auto-creates it with `policy: None`
  and never calls `validate_record_tenant`. A peer pre-creates an RLS/tenant
  collection name → writes rows RLS will never filter, with arbitrary
  `tenant_field`. `collection_mode` is taken from attacker payload.
- [x] **Fix:** authoritative replicated schema of protected/tenant names, gate
  imports against it (fail-closed for unknown protected-namespace matches),
  enforce `validate_record_tenant` on the replicated apply path, never
  auto-create a policy-governed name.

### P1-2 · Global DB-lock held across mesh socket I/O → single-connection total DoS
- `lan.rs:172` — the inbound session thread takes the process-wide
  `Mutex<BicDb>` and holds it for the whole responder session; `read_frame`
  blocks on `read_exact` up to `SESSION_IO_TIMEOUT` (30 s). One idle/slow
  connection stalls **every** query and write; repeat → indefinite outage. No
  auth. Amplifier: 256 MB `MAX_MESH_FRAME_BYTES`, per-event fsync, O(events)
  reconcile all under the lock.
- [x] **Fix:** never hold the DB lock across network I/O — buffer/validate
  off-lock, lock only for apply; add whole-session deadlines and per-event /
  per-bundle size+count caps; rate-limit per source.

### P1-3 · Signed-relay integrity gap — signature does not cover `event_type`
- `envelope_signing_message` (`sync_mesh.rs:91`) + `payload_hash` cover only
  `payload`, not `event.event_type`. A relay takes a validly signed
  `RecordCreated`, flips it to `RecordDeleted`, recomputes the (unsigned)
  checksum → `verify()` and signature both pass → **signed data destroyed in
  transit**, defeating the store-and-forward integrity claim.
- [x] **Fix:** include `event_type` (and the full canonical event: stream,
  timestamp, metadata sans derived `_bicdb_sync`) in the signed message; bump
  the signing domain tag/version.

### P1-4 · WKB `GeometryCollection` unbounded recursion → whole-process abort
- `geometry.rs:1461` (`read_wkb`), same shape `:363` (`from_bicdb_frame`). SQL-
  reachable via `ST_GeomFromWKB` (`records.rs:2315`). ~9 bytes per nesting
  level; a ~0.5 MB hex literal overflows the stack → SIGABRT → every connection
  dies. WKT via the third-party `wkt` crate is SUSPECTED to share this.
- [x] **Fix:** thread a depth limit (≈32) through `read_wkb` / `from_bicdb_frame`;
  verify/cap the `wkt` crate path.

### P1-5 · `h3_polygon_to_cells` unbounded iteration → unkillable CPU hang
- `records.rs:2862`. The 100k cap bounds the **output set**, never the
  **iteration count**. A big-bbox low-fill areal geometry at resolution 15 →
  ~10¹⁵ point-in-polygon tests from a ~120-byte literal. `bicdb_raster_zonal_mean`
  has the sibling `height×width×vertices` shape.
- [x] **Fix:** bound total iterations `(lat_span/step)*(lon_span/step)` up front
  and reject; cap the bbox/step ratio.

### P1-6 · `audit_concurrent_frontier` O(edits²) per record under the exclusive DB lock
- `db.rs:33333`, driven by `reconcile_record_audit_events` (`db.rs:27713`) on
  import. A mesh bundle with N `RecordUpdated` events to one record → ~N²
  dominance checks (each a heap alloc) with the whole DB frozen (`&mut self`).
- [x] **Fix:** compute the antichain in O(N log N) using the per-origin
  watermark prefix property; or cap per-record history with a deterministic
  fallback.

### P1-7 · Origin-position forgery → version-vector eclipse / silent withholding
- `sync_mesh.rs:136/671`, `db.rs:11234`. One forged event attributed to victim
  origin X with `sequence = u64::MAX` makes the node advertise it covers all of
  X → honest peers' delta skip-check means they **never send X's real events**.
- [x] **Fix:** signed origins; refuse advancing a foreign origin's watermark
  from events not signed by it; sanity-bound `sequence` growth.

### P1-8 · `write_context` forgery → conflict resolver always-wins / suppresses victims
- `db.rs:33321/33333`. A competing write with `write_context` claiming coverage
  of every origin at MAX dominates all rivals → victim writes silently dropped
  from the frontier (no conflict surfaced).
- [x] **Fix:** don't trust `write_context` as ground truth for dominance —
  bound it to positions the origin could have actually seen; never let a claimed
  context exclude an otherwise-present write without evidence.

### P1-9 · Keyspace aliasing via unchecked `as u16` name truncation
- `paged_collection.rs:4865/3216/3767`; `validate_collection_name` (`db.rs:38111`)
  has no length cap. Collection `"a"` and `"a"`+65536×`"b"` encode the same
  prefix → cross-collection read **and** byte-identical overwrite. Rejected by
  the B-tree at the default 8 KiB page, but **fully live at `paged_page_size`
  ≥256 KiB** (operator config up to 1 MiB) → P0 on such an instance.
- [x] **Fix:** cap name length in the validator; make the three encoders
  `u16::try_from(...)?`.

### P1-10 · `DROP TABLE` never purges the paged keyspace → row resurrection
- `db.rs:9594` (`drop_collection`) removes catalog/index/HNSW/segment but not the
  paged rows under `collection_prefix(name)`. `DROP t; CREATE t; SELECT * FROM t`
  returns the old rows (server-paged) → cross-tenant on name reuse + "delete
  doesn't delete" (GDPR). Intern-dictionary namespace leaks the same way.
- [x] **Fix:** `remove_raw_prefix(collection_prefix(name))` (and the intern ns)
  inside the drop transaction.

### P1-11 · SQL drops the schema qualifier → cross-schema collection collision
- `eval.rs:20863` (`relation_name`) returns the table part, dropping the schema.
  `CREATE TABLE tenant_a.users` and `tenant_b.users` both resolve to one
  collection → full cross-tenant access for anyone using a schema as a tenancy
  boundary (and `CREATE SCHEMA` is accepted, so the boundary looks real).
- [x] **Fix:** encode the schema into the collection name, or reject
  non-`public` qualifiers instead of silently dropping them.

### P1-12 · Unvalidated index name → `fs::remove_dir_all` (arbitrary recursive delete)
- `db.rs:33620` (`load_index_catalog`, no validation) →
  `spatial_pack_build.rs:114/162-163`. A planted `indexes.json`
  `{"name":"../../../../tmp/victim","kind":"Spatial"}` (via a restored backup —
  restore validates entry *paths*, not file *contents*) + `PACK SPATIAL INDEX
  "../../../../tmp/victim"` deletes an arbitrary tree.
- [x] **Fix:** validate every definition from `load_index_catalog`; hash the
  index name for the workspace dir like `fts_build.rs:553` already does.

### P1-13 · PHI keys derived via unsalted single SHA-256
- `phi.rs:587` (`material_key`): `SHA256(env_var)` with no salt, no work factor,
  for both the PHI field key and the blind-index HMAC key. Human-chosen
  passphrases → fast offline dictionary attack. (The passphrase *bundle* path
  correctly uses Argon2id — only the PHI path is weak.)
- [x] **Fix:** run env-supplied PHI secrets through Argon2id, or require raw
  32-byte key material and reject low-entropy input.

### P1-14 · "Crypto-shred" is single-global-key, not per-subject; blind-index residue survives
- `phi.rs:574` — one field key + one lookup key per process. Deleting the field
  key makes **every** record undecryptable (not per-patient), and the
  deterministic blind-index tokens (separate key) survive a shred and still
  reveal equality. Doctrine (`docs/bicdb-mesh.md:245`) overstates the granularity.
- [x] **Fix:** implement per-subject key wrapping, or downgrade the doc claim to
  "single-key, all-or-nothing" and document blind-index residue.

### P1-15 · Mesh signing secret stored plaintext on disk even under at-rest encryption
- `db.rs:37847` (`load_or_create_mesh_signing_key`) writes the ed25519 secret via
  the plain filesystem path, never the `EncryptionRuntime`. Disk/image/snapshot
  read → forge events as this node (accepted by any peer that pinned it / strict
  mode). Rider: `0600` is applied *after* rename, so a brief `0644` window
  exists, and the chmod result is dropped.
- [x] **Fix:** wrap with the DB encryption runtime or an OS keystore; create
  `O_CREAT` mode `0600` from the start; hard-fail if perms can't be set.

### P1-16 · TOFU first-contact auto-pins an attacker key (MITM)
- `mesh.rs:191` pins whatever key the peer announces in `Hello` before importing;
  both `node_id` and `public_key` are self-asserted. A first-contact MITM pins
  its own key and every later session "verifies" against it.
- [x] **Fix:** out-of-band fingerprint/cert confirmation for first contact;
  surface the pinned fingerprint and warn on first pin.

---

# P2 — medium

- [x] **P2-1 · pgwire pre-auth slowloris.** Slot admitted at accept; the whole
  startup+auth handshake runs under the 300 s idle timeout with no auth deadline
  (`lib.rs:3239/3545`); `max_connections` 100. 100 stalled connections lock
  everyone out (`FATAL 53300`) — the documented prod-incident signature. *Fix:*
  short wall-clock auth deadline; per-source cap.
- [x] **P2-2 · Cleartext password over plaintext by default.** `auth_method:
  Cleartext`, `require_tls: false` (`lib.rs:235/239`). *Fix:* default SCRAM;
  refuse cleartext without TLS. (TLS *is* correctly enforced when required.)
- [x] **P2-3 · Fast paths skip the GRANT check in trust mode.** count-star /
  fts-count / fts-select / ranked-topk run before `require_relation_privilege`
  (`engine.rs:3520` vs `:3695`) and skip it when `security_context == None`
  (no-auth deployments). (They *do* correctly decline on RLS.) *Fix:* privilege
  check at the top of each fast path.
- [x] **P2-4 · Blind index has no tenant binding.** `phi.rs:492` — identical
  plaintext yields an identical token across tenants/DBs sharing the lookup key
  → cross-tenant equality + confirmation attacks. *Fix:* include `tenant_id` in
  the HMAC input.
- [x] **P2-5 · Clock-table poisoning biases conflict resolution.** peer-supplied
  `FrameTiming` (`mesh.rs:106`) and forgeable `ClockObserved` events
  (`db.rs:11586`) skew `corrected_order_winner` (`db.rs:33407`). *Fix:* require
  signed origins for clock observations; widen/ignore uncorroborated offsets.
- [x] **P2-6 · Pin-poisoning = denial-of-identity.** An attacker who pins a
  victim's `node_id` to a wrong key makes the real peer's later sessions abort on
  key conflict forever (`mesh.rs:191`, `db.rs:11488`). *Fix:* OOB provisioning.
- [x] **P2-7 · Relay drop/withhold + false coverage claims.** A relay silently
  drops events (e.g. a delete) → replica divergence; a peer claiming coverage it
  lacks → withholding. Reorder/dup are handled; drop/withhold are not detectable
  (`db.rs:11300`). *Fix:* signed gapless coverage proofs; periodic anti-entropy.
- [x] **P2-8 · `storage.rs:1031` `zstd::stream::decode_all` unbounded.** Size
  check is post-hoc; a 128 MiB compressed-zeros frame expands to many GB before
  the check. Disk/backup bytes. *Fix:* decode through a `.take(limit+1)` capped
  reader.
- [x] **P2-9 · Symlink-follow / traversal in maintenance paths.** `.tmp`
  siblings bypass symlink guards (`storage.rs:898`, `backup.rs:2396`);
  attachment path built from an unvalidated `hash_sha256` record field
  (`large_value.rs:62/405`); backup/restore run-lock TOCTOU
  (`distribution_backup_run.rs:308`); HA `ship_to_standby` follows symlinks
  (`db.rs:32108`). *Fix:* randomized temp + `create_new` + `O_NOFOLLOW`;
  validate `hash_sha256` as 64 hex; lstat via `file_type()`.
- [x] **P2-10 · Derived internal collection names collide with user tables.**
  `db.rs:22399` — importing a graph named `routes` unconditionally drops a user
  table `routes_nodes`. *Fix:* namespace/validate derived names.
- [x] **P2-11 · `from_bicdb_frame`/`FrameCursor` unbounded `with_capacity` +
  recursion** (`geometry.rs:1095`; disk bytes only, no SQL callers). *Fix:*
  count guard + depth limit.
- [x] **P2-12 · Posting-block `with_capacity(doc_count)` from unvalidated
  on-disk count** (`paged_collection.rs:4600/4624/4032/4048`). *Fix:* bound by
  block length.

---

# P3 — hardening

- [x] Non-constant-time secret comparisons (password `lib.rs:14735`, SCRAM
  `:11146`) — use `subtle`/`ct_eq`; keep public-value comparisons as-is.
- [x] Username-enumeration timing oracle (`lib.rs:14722` vs Argon2 at `:14832`)
  — hash a dummy salt for unknown users.
- [x] No SCRAM channel binding (`-PLUS`); TLS downgrade possible when
  `require_tls=false`.
- [x] Negative `Int16` counts → `with_capacity(usize::MAX)` panic (per-connection
  self-DoS) — `lib.rs:7790/7819/7867`; mask to `u16`.
- [x] No `catch_unwind` around SQL/DB handlers → any panic poisons shared `std`
  mutexes; replace `unreachable!()`/`unwrap()` in `geometry.rs` with typed errors.
- [x] Fast paths skip `reject_encrypted_predicates` (defense-in-depth; no
  confirmed plaintext leak — `project_record` redacts). SUSPECTED — verify no
  row-evaluator sees unredacted encrypted values without `decrypt_roles`.
- [x] `bicdb_space_report` walks the whole data dir per call (stat storm) and
  reveals aggregate DB size + event count to a low-priv user — cache/rate-limit.
- [x] FTS query budget not applied on boolean/candidate paths
  (`full_text_candidate_ids`, `..._conjunctive_position_scan`, `..._impact_scan`).
- [x] Fixed sync-bundle AAD; path-bound PHI AAD (renaming the datadir makes PHI
  undecryptable — availability footgun) — prefer a durable DB UUID.
- [x] `decompress_record_value` has no *ratio* cap (absolute 64 MiB per decode is
  fine).
- [x] `apply_archived_wal` follows symlinks; cluster ids allow `..` via
  `#[serde(transparent)]`; `SyncBundle::read` unbounded `fs::read` + double-read
  TOCTOU (SUSPECTED); DROP INDEX may leak the entry-format namespace (SUSPECTED);
  TID-hint cache keyed by weak `FxHash` with no collection re-verify on hit
  (SUSPECTED); no length bound on record ids.

---

# Verified SOUND (checked, not vulnerable)
- `unsafe { self.db.as_ref()/as_mut() }` (`session.rs:1981/1995`) — exclusive-gated,
  no aliasing/UAF.
- L3 `fallocate` punch-hole (`manager.rs:1531`) — page-size guard prevents
  underflow, offset stays in-file.
- MVT protobuf writer (`mvt.rs`) — append-only, slices guarded by upstream length
  filters, saturating float→int casts.
- FTS SIMD loads (`fts_postings.rs:1629/1643`) — guarded by length early-returns.
- WKB *cursor* bounds + `count ≤ bytes/16+1` cap + `ring_count==0` guard (only
  the *recursion* is unbounded — P1-4).
- Core keyspace encoding is length-prefixed with a `0x00→0x00 0xFF` escape
  terminator (no prefix-containment; the *only* aliasing route is the u16
  truncation, P1-9).
- Backup restore is not zip-slip (`safe_join` walks components with
  `symlink_metadata`).
- No command injection anywhere (`Command::new` uses fixed literals, no shell).
- Crypto primitives: 192-bit random XChaCha20 nonces via `OsRng` (no reuse), v4
  UUIDs, bundle KDF is Argon2id + random salt, HMAC key separate from field key,
  `ed25519-dalek`/`chacha20poly1305` constant-time internally, at-rest paged
  encryption is fail-closed.
- L7 `decompress_record_value` — hard 64 MiB output cap enforced by
  `zstd::bulk::decompress`.
- WAL hard-cap backpressure (`WAL_BACKPRESSURE_FACTOR=4`) — forces a blocking
  checkpoint on the write path, not bypassable.
- Normal indexed plans (`PrimaryKeyInLookup`, spatial, knn, index range) route
  through RLS + core policy + privilege. RLS permissive/restrictive/`bypass_rls`/
  `row_security=off` logic matches PostgreSQL; UPDATE/INSERT `WITH CHECK`
  enforced (no read-back gap found).
- pgwire frame-length bounds before allocation; 122-bit cancel-key entropy;
  per-connection statement/portal isolation; auth fails closed; the single
  `unsafe` (setpriority) is off the wire path.

---

# Storage corruption — page store / WAL / checkpoint

### C1 · Page frees are unlogged → store-wide allocation failure after a crash (P1, top durability fix)
- **Where:** `PageStore::free` (`manager.rs:1155-1163`) writes the `Free` image
  straight to the data file via `write_page_raw` with **no WAL record**;
  `BufferPool::free_page` (`pool.rs:1040-1043`) then clears `needs_log`/`dirty`
  so it can never be logged. **On the normal write path:** update/delete a row
  with an overflow value → `free_chain_pooled` (`overflow.rs:120`) →
  `pool.free_page` → `store.free`.
- **Failure:** the page's only WAL record is its *creation* after-image. After a
  truncating checkpoint (`redo_lsn = 0`, `wal.rs:1103`), replay blindly rewrites
  that old image over the now-`Free` page (`wal.rs:1151`, no per-page LSN gate) →
  superblock says free-list head = P, disk says P is `Overflow` → `allocate`
  fails closed (`manager.rs:996-1002`) → **store-wide allocation failure.** This
  is exactly the state `truncate_free_list_at_corruption` was written to repair
  (`manager.rs:1266`, *"observed after an OOM-kill during a bulk import"*) — the
  root cause has **already been hit in production**. Subsumes the weaker
  vacuum-path variant (missing `wal.sync` after `adopt_free_pages`).
- [x] **Fix (closes C1+C2+C3 at once):** route every `free` through the pool +
  `log_dirty_pages(0)` + `adopt_free_pages` pattern (transaction 0 replays
  unconditionally, higher LSN wins), or add a typed `PageFree` WAL record.

### C2 · Archive retention runs tail reclaim on a non-truncated WAL (P1)
- `Wal::reset()` under retention keeps sealed segments (`wal.rs:834`) but
  `finish_checkpoint` still returns `wal_truncated: true` unconditionally
  (`wal.rs:1267`), which both checkpoint paths take as licence to reclaim the
  file tail (`paged.rs:2277`, `2511`) — violating the precondition at
  `manager.rs:1448` (*"the WAL holds no image of those pages"*). Any handle that
  called `archive_wal_segments` (enables retention) with a lagging archiver,
  then a crash with an untrusted redo point → replay reserves past the reclaimed
  tail and resurrects freed pages; zero-filled gaps then fail every subsequent
  checkpoint. (Concrete corruption path in the prune-archive area.)
- [x] **Fix:** gate tail reclaim on *actual* truncation, not the unconditional
  bool; C1's fix also removes the resurrection blast radius.

### C3 · Roll-forward restore never restores free-list metadata (P1)
- Page 0 (superblock: `free_list_head`/`free_page_count`/`page_count`) is never
  WAL-logged (`manager.rs:2152`); the online base is copied fuzzily with
  consistency delegated to WAL replay (`backup.rs:241`). Replay fixes page
  *content* but not the free list, captured at whatever instant page 0 was
  streamed → a page freed at copy-time but reallocated+filled afterward stays on
  the restored free list → `allocate` fails closed.
- [x] **Fix:** same as C1 (logging frees resolves it).

### C4 · LSN floor regression re-issues used LSNs → duplicate sealed-segment names (P2)
- Crash between `wal.reset()` and `publish_root` (`paged.rs:2273`) leaves
  `checkpoint_lsn` stale; reopen reseeds `next_lsn` from it and **re-issues LSNs
  already used**, so `store.wal.<seq>` names recur — violating the uniqueness
  invariant the archive depends on (`wal.rs:230`); `archive_wal_segments`
  silently skips the collision when sizes match (`db.rs:24672`).
- [x] **Fix:** publish the horizon *before* truncating.

### C5 · `end_offset` updated outside the file lock → backup consistency cut ships nothing (P2)
- `Wal::append` updates `end_offset` *after* dropping the file lock
  (`wal.rs:504`), so `seal_active` can read `active == 0` and no-op on a
  non-empty file — that's the online backup's consistency cut (`db.rs:24643`),
  which would then ship nothing.
- [x] **Fix:** move the atomic add inside the lock.

### C6 · `prune_archived_wal` cross-base hazard (P2) — refines L6
- Safe for its own floor's base, but pruning to the newest base's floor destroys
  older retained bases' roll-forward chains, and nothing verifies the `--base`
  dir is actually a *verified* base. (Partly mitigated: gap-detection at open
  fails closed rather than silently.)
- [x] **Fix:** verify the base before pruning; keep the oldest base's floor.

### C7 · Hardening (P3)
- [x] Generation is not monotonic per page (`allocate`/`free`/`punch` hardcode
  0/1; `publish_root` doesn't bump; the counter regresses after crash) —
  harmless today only because nothing compares generations across images and the
  affected images are header+zeros. Fragile; make it monotonic.
- [x] `committed_this_generation` grows unbounded while truncation is deferred
  under a backup pin.

### Confirmed sound (storage)
Record-format / torn-tail / LSN-chain validation (allocation only after
length+CRC checks); sealed-segment strictness (decode-length mismatch =
corruption, fail-closed); two-pass replay (outcomes then images, contradictions
refused); redo-point trust gated on the page files' own horizon; checkpoint
phase ordering **for logged state**. The only gap is state the WAL cannot see —
precisely C1/C2/C3.

### Residual coverage gaps (not deeply audited — recommend a follow-up pass)
- **MVCC read visibility** — whether a reader can ever see an uncommitted/aborted
  version or miss a committed one (frozen watermark, abort exceptions,
  `jump_frozen_to`, the poisoned-xid repair). The returned report was
  WAL/checkpoint-centric.
- **Buffer-pool singleflight** races (two threads, eviction losing a dirty page /
  serving a stale one).

---

# Post-1.0.175 addendum — availability

### C8 · Online backup aborts when the active WAL rotates mid-stream (P1 availability) — FIXED
- **Discovered from a production incident** after the 1.0.175 remediation: a
  multi-hour online backup produced no off-site snapshot (it failed *safe* —
  aborted, no corruption). NOT one of the original 51 items.
- **Root cause:** the backup pin (`paged.rs:2077/2277`) blocks checkpoint WAL
  truncation and sealed-segment deletion, but **append-time WAL segment
  rotation** (`wal.rs` `append` → `seal_active_locked`, at 64 MiB) does **not**
  consult the pin. The online *base* copied the active `paged/store.wal` as a
  fuzzy entry (`backup.rs:1851`), pinning its size at manifest time, then
  streamed it by pathname hours later (`backup.rs:1077`) requiring ≥ the pinned
  bytes. Rotation renamed the active file to `store.wal.<seq>` and created a
  fresh, shorter active → size mismatch → whole-backup abort (observed:
  48,470,336 → 30,249,784 bytes).
- [x] **Fix (`backup/wal-rotation-online-base`, 1.0.176-beta):** the online base
  no longer copies the active `store.wal` at all — its content is captured
  durably and completely by the post-cut WAL-tail seal
  (`seal_wal_for_backup` returns *every* sealed segment, and the pin retains all
  segments across rotation), and a restored base opens with a fresh empty active
  (`Wal::open` uses `create(true)`) that replays the sealed chain. This is the
  LSN/segment-range approach (retain the full sealed chain from the pinned floor
  through the cut) rather than assuming `store.wal` stays one file. Also plumbs a
  tunable `wal_segment_bytes` (`DbConfig::with_paged_wal_segment_bytes`).
- **Regression test:** `backup_online_wal_rotation.rs` — tiny (64 KiB) WAL
  segments + a concurrent writer forcing several rotations across the base
  stream; asserts the backup succeeds and, deterministically, that the restored
  base contains **no** `paged/store.wal`. Verified to FAIL on the pre-fix code
  and pass after.

---

# Recommended remediation order
1. **P0-1 role DDL** — one authorization gate, collapses everything if left.
2. **P0-2 pgwire `with_capacity`** — one-line bounds check, unauth remote crash.
3. **P0-4 mesh defaults** — flip signing/verification on for network transport;
   put the signature in the checksum; don't export on unauthenticated sessions.
4. **P0-3 catalog functions** — thread `SecurityContext`, authorize the named
   collection (or restrict these functions to authorized callers).
5. **C1 log page frees** — the top *durability* fix: closes C1+C2+C3, and the
   corruption it causes (store-wide allocation failure) has already been seen in
   production. Highest-value single change in the storage layer.
6. P1-4 / P1-5 (SQL-reachable crash/hang), P1-2 (mesh lock-hold DoS), P1-9/10/11
   (keyspace/tenant isolation), then the remaining P1s and P2s.

**Follow-up pass recommended** on the two residual storage gaps (MVCC
visibility and buffer-pool singleflight) — a corruption/disclosure bug there
would rank P0/P1 and was not deeply covered.

---

# External review — 2026-08-16 (independent, read-only)

**Reviewed revision:** `5c8138d3` (1.0.177-beta). **Important:** that is the
bot's local merge commit, **five PRs behind** `origin/main` at the time of
transcription (`ab4d9d4c`, 1.0.182-beta) — and one of those five (#522)
rewrote the COPY-target parsing that CR-1 is built on. Line numbers below are
the reviewer's and have drifted. **Re-verify against current HEAD before
acting.** Method: seven parallel review tracks; SQL-authz findings claimed
behaviourally reproduced; `cargo audit` + `cargo deny` on the locked tree.

## Verification performed here (2026-08-16, against `ab4d9d4c`)

| ID | Claim | My check |
| --- | --- | --- |
| CR-1 | COPY target injection → arbitrary SQL as bootstrap superuser | **Published payload does NOT reproduce.** Ran it over the real wire (`COPY t WHERE 'nextval'='x'; INSERT INTO marker VALUES ('pwned'); -- FROM STDIN`, then CopyDone) → marker = 0 rows. **But every structural precondition still holds**: `parse_copy_table_target` does not reject `;`, `normalize_identifier` only strips quotes, the `format!("SELECT * FROM {relation}")` sink exists at 3 sites, and `execute_server_sql_for_describe` runs with `SqlSessionGucState::default()` + `security_context = None`. **Fix regardless** — a different payload may land. |
| CR-2 | GRANT/REVOKE has no grantor/ownership check | **CONFIRMED** — `apply_privileges` contains no ownership/superuser/privilege check. |
| CR-3 | ALTER TABLE ungated (incl. `DISABLE ROW LEVEL SECURITY`, `OWNER TO`) | **CONFIRMED** — no owner/superuser gate. |
| CR-4 | CREATE/ALTER/DROP POLICY ungated | **CONFIRMED** — no gate on `execute_create_policy`. |
| CR-5 | server-paged rowid/PK fast path bypasses core tenant policy | **NOT CONFIRMED EITHER WAY** — `get_records_by_rowids` appears to reference the fail-closed check, `get_records_by_pks` does not obviously. Needs a proper read, not a grep. |

CR-2/3/4 are one missing layer (object-DDL authorization), not three bugs, and
do not depend on any recent change. Treat as the actionable headline.

## CRITICAL (reviewer's list)

- **CR-1** COPY target SQL injection → superuser, no security context — `bicdb-pgwire/src/lib.rs:7780,7393,5091`
- **CR-2** GRANT/REVOKE self-service — `bicdb-sql/src/session.rs:7368`
- **CR-3** ALTER TABLE ungated — `session.rs:10118` (RLS off-switch :10362, OWNER TO :10379)
- **CR-4** Policy DDL ungated — `session.rs:11053,11109,11166`
- **CR-5** rowid/PK fast path bypasses tenant policy — `engine.rs:14510` → `db.rs:9894,9933`

## HIGH

- **H-1** COPY FROM STDIN bypasses INSERT GRANT — `session.rs:2309`
- **H-2** Six streaming fast paths skip GRANT checks (P2-3 fixed only four of ten) — `engine.rs:6669,6851,8886,4575,8629,9191`
- **H-3** TRUNCATE missing TRUNCATE privilege — `session.rs:15024`
- **H-4** ~~CREATE TRIGGER ungated + `bicdb_notifications` exposes captured NEW values~~ — **FIXED (1.0.276-beta)**. Re-triaged as a P0: any authenticated role could attach a `pg_notify`-capturing trigger to another tenant's table and read the owner's future `NEW.<column>` plaintext out of `pg_catalog.bicdb_notifications`. Both CREATE paths and DROP TRIGGER now require table ownership, binding a function requires EXECUTE on it, and the notification catalog is filtered per role by SELECT on the source table (unattributed rows: superuser only). Regression suite: `crates/bicdb-sql/tests/trigger_authorization.rs`.
- **H-4b** ~~Ownership gates written for tables, never extended to sibling object kinds~~ — **FIXED (1.0.277-beta)**. Three verified P0s, one root shape: (a) `GRANT`/`REVOKE` gated only `PrivilegeObjectType::Table`, and that branch failed OPEN for views (`load_schema` misses the view store) — self-granting SELECT on a privileged role's definer view exfiltrated its source tables; sequences/schemas/databases were never gated. (b) `ALTER VIEW ... OWNER TO` never checked membership in the NEW owner, so a view could be laundered through any role and keep reading as it (definer semantics); same gap on ALTER SEQUENCE/SCHEMA, and `ALTER DATABASE OWNER TO` had no check at all. (c) `DROP VIEW`/`DROP SEQUENCE`/`DROP TYPE` had no ownership check though `DROP TABLE` beside them did. Fixed centrally: `current_user_holds_role` / `require_object_ownership` / `require_settable_new_owner` / `require_relation_ownership` in `session.rs`. Regression suite: `crates/bicdb-sql/tests/ownership_authorization.rs`.
- **H-4c** ~~Write paths and cascades reach effects their own gate never checked~~ — **FIXED (1.0.278-beta)**. Round-two findings, one shape: a gate that exists on one statement and is missing on a path reaching the same effect. `COPY FROM` enforced RLS but never the INSERT privilege (any pgwire client could bulk-load into any table plain INSERT refused). `RETURNING` projected the affected rows with no SELECT check, so a write-only grant was a full table read. `TRUNCATE ... CASCADE` authorized only the named targets and then appended FK children to the truncate set, emptying tables the caller never named. The recognized `DO $carrier_*$` provisioning blocks wrote the privilege store directly — one of them wiped every table grant to public/carrier_app — with no authorization at all. Also closed: `ALTER TABLE ... OWNER TO` missing the new-owner membership check, `LOCK TABLE` with no privilege check, `DROP INDEX` failing open on materialized-view indexes (`table_owning_index` returned `None`), `require_table_ownership` failing open for views (the root cause behind several Table-typed gates), and `ensure_user_type_usage` failing open on an unknown OID. Regression suite: `crates/bicdb-sql/tests/write_path_authorization.rs`.
- **H-4d** ~~Foreign keys attachable to relations the caller has no rights on~~ — **FIXED (1.0.279-beta)**. Reported as "FK ON DELETE CASCADE bypasses child-table privileges"; the cascade is not the defect (PostgreSQL runs referential actions with the constraint's authority, not the caller's). The missing gate is one step earlier: the REFERENCES privilege on the table an FK points at, which was never checked. Probing showed that gap is worse than the cascade — an FK onto an unreadable table is a **cross-tenant existence oracle** (the child insert succeeds only when the parent row exists, so the parent's key space is enumerable with no SELECT) and a **lock-in** (the parent's owner can no longer delete the referenced rows, and cannot drop a constraint living on the attacker's table). It is also the precondition for wiring a CASCADE into an unowned table at all. `require_reference_privilege` now gates CREATE TABLE and ALTER TABLE ADD CONSTRAINT; self-references are exempt. Regression suite: `crates/bicdb-sql/tests/foreign_key_authorization.rs`.
- **H-4e** ~~Virtual catalogs unfiltered; raw-SQL handlers ungated~~ — **FIXED (1.0.280-beta)**. Two shapes. (1) `filter_virtual_rows_for_role` filtered exactly one catalog and returned every other unfiltered — worst was `pg_stats`, which publishes most_common_vals/histogram_bounds (ACTUAL column values, sampled over every row with RLS ignored): a role with no privilege read data out of a table under FORCE ROW LEVEL SECURITY. `pg_policies`/`pg_policy` leaked policy quals and their embedded literals; `pg_attrdef` leaked column defaults. Now table-driven via `virtual_catalog_relation_key`, filtered by SELECT on the owning relation, failing closed on unattributable rows. ANALYZE (both entry points) requires ownership, since it force-feeds those statistics. (2) The raw-SQL fast-path handlers dispatched from `execute_inner` never got the ownership gate their parsed-`Statement` equivalents enforce: CREATE INDEX ON ONLY, CREATE/PACK SPATIAL INDEX, ADD GENERATED AS IDENTITY, identity RESTART, SET COMPRESSION, EXCLUDE constraints, CREATE TABLE PARTITION OF, ATTACH/DETACH PARTITION, and the MERGE/RECONCILE/EXPLAIN MATERIALIZED AGGREGATE commands. Also gated: the whole extension/app-hosting DDL surface (executable WASM registering HTTP routes and event handlers), TRIM AUDIT HISTORY, store-wide VACUUM, ALTER TABLE SET SCHEMA, and CREATE MATERIALIZED VIEW (which reads its sources). Regression suites: `crates/bicdb-sql/tests/catalog_and_admin_authorization.rs`, `crates/bicdb-sql/tests/raw_ddl_authorization.rs`. NOT fixed: CREATE SCHEMA still requires no database CREATE privilege — enforcing it broke an existing tested workflow (`SET ROLE` to a plain role, then CREATE SCHEMA), and the escalation it fed (SET SCHEMA) is independently gated.
- **H-4f** ~~`LIKE ... INCLUDING CONSTRAINTS` cloned foreign keys past the REFERENCES gate~~ — **FIXED (1.0.281-beta)**. A bypass of the #626 fix itself: that change gated the two declared FK-creation paths (inline CREATE TABLE constraints, ALTER TABLE ADD CONSTRAINT) but `apply_raw_create_table_like` clones `source.constraints` wholesale, so an unprivileged role recreated the same existence oracle and lock-in by copying someone else's table definition. `require_reference_privilege` now covers the clone path. Also hardened: `pg_class.reltuples`/`relpages` and the `pg_stat_*_tables` row counters are zeroed for roles that cannot SELECT the relation — the catalog rows stay visible so introspection keeps working, but another tenant's row counts (size, growth, whether a probe landed) do not. That one is tenancy hardening, not PostgreSQL parity: PostgreSQL exposes these broadly. Regression tests added to `foreign_key_authorization.rs` and `catalog_and_admin_authorization.rs`.
- **H-12** ~~SQL injection in the sync-server RLS-compose reverse pass (CWE-89)~~ — **FIXED (1.0.284-beta)**. `reverse_apply` (`crates/bicdb-cli/src/sync_server.rs`) replays client sync events onto the master, and the record's metadata KEYS — fully controlled by a compromised device — were interpolated into the composed UPDATE/INSERT verbatim. `json_literal` escaped the values; nothing validated the identifiers. Verified end to end: a metadata key of `v = 'pwned' WHERE 1=1 --` widened the UPDATE to every row, and a row the event never named came back as `pwned`. The statement runs under the user's `SecurityContext`, so RLS caps the blast radius to that user's own scope — which is why this is P2 and not a cross-tenant escape, but within that scope it is arbitrary DML. Fixed by validating each column name as a plain identifier and refusing the event otherwise; validation rather than double-quoting, so these identifiers do not silently change from case-folded to case-sensitive. Regression tests: `reverse_apply_tests` in the same file.
- **H-11b** ~~Old HA consensus does not bind candidate/leader identity to the authenticated peer~~ — **FIXED (1.0.285-beta)**. `consensus_handle_connection` (bicdb-cli `main.rs`) took the mTLS stream and passed vote/append frames straight to `handle_request_vote` / `handle_append_entries` (`consensus.rs:220/240`), which only check that `candidate_id` is a known voter and trust `leader_id` outright. Both are self-asserted in the frame, so one compromised voter holding a valid certificate could vote as any other voter or append as leader. The new cluster consensus already enforces the invariant (`MetadataVote`/`MetadataAppend` reject a mismatched caller at `distribution_transport.rs:1707/1719`); this is the same binding for the original HA path. `--peer` now accepts `node_id=addr=SHA256`, the leaf certificate is resolved to a voter, and a frame asserting a different identity is refused. OPERATIONAL NOTE: the pin is OPTIONAL — an existing deployment with no pins keeps starting and keeps the old (unbound) behaviour, because making it mandatory would stop every HA cluster on upgrade. Pinning every voter is what actually closes the hole, so it should be part of the upgrade runbook; consider making it mandatory once deployments have rolled.
- **H-8b** ~~Iterative parsers build recursive trees: flat chains abort the process~~ — **FIXED (1.0.286-beta)**. The #585 parse budget charges depth where the PARSER recurses, but several loops build a left-leaning tree without recursing at all — `$.a.a.a…` (Expr::Member), `$[0][0][0]…` (Expr::Array), `$?(1 || 1 || …)` (Expr::Binary), `to_tsquery('a | b | c …')` (PgTsQueryNode::Or). Each step wraps the accumulated node, so N steps build an N-deep tree; the nesting budget never fired because nothing recursed, and the node budget is a million. Evaluation and the tree's own recursive `Drop` then overflow the stack — a hardware fault, not a catchable panic, so one read-only query from any authenticated role aborts the process and every tenant in it. Reproduced at ~3k-200k steps on all four shapes. Fixed by charging tree depth per deepening step (`charge_tree_depth` in `jsonpath.rs` and `fts.rs`), with no matching `leave` since the level persists in the tree. Two of the four were found by generalising the class rather than from the report. Regression suite: `crates/bicdb-sql/tests/parser_tree_depth.rs`, which also pins that ordinary expressions and a 50-deep chain still parse.
- **H-13** ~~Long-lived realtime sessions never re-check the credential (stale authorization)~~ — **FIXED (1.0.287-beta)**. `prepare_carrier_realtime` authenticates the bearer once, at connect, and a WebSocket/SSE/long-poll stream then lives as long as the client holds it open. The periodic `revalidate_carrier_realtime` (every 5s on SSE and WebSocket, per wait loop on long-poll) re-ran only the application's `authorize_group` callable — with the actor CACHED at connect — and was a complete no-op for any contract declaring no `authorize_group`. So nothing re-examined the bearer: a session opened a second before its token expired streamed indefinitely, and revoking a token could not end a stream already in flight, which is the one control that matters for a leaked credential. `ActorContext::deadline_unix_ms` already carries `min(request_deadline, token_exp)` from `JwtAuthenticator`; it was simply never consulted. Now checked first in `revalidate_carrier_realtime`, so it applies to every contract rather than only those with a group callable. A zero/negative deadline means "not supplied" and is not treated as expired.
- **H-14** ~~Broker queue ACLs inert for any SQL session without a security context~~ — **FIXED (1.0.289-beta)**. `Broker::authorize` took `Option<&SecurityContext>` where `None` was documented as a trusted caller, so one value carried two unrelated facts: which identity this is, and whether the call arrived over the network. The embedded Rust API and an unauthenticated pgwire session both produced `None`, and `sql_session_for_server` derives the context from a SERVER-WIDE config option — so a deployment that never set one handed `None` to every ordinary authenticated client and every queue ACL went inert. VERIFIED before the fix: on a queue configured `{"publish_roles":["ops"],...}`, an identified user without the role was refused while an unidentified caller published and consumed. Absence of identity granted MORE authority than presence of an unprivileged identity. Fixed by making provenance an explicit input — `BrokerCaller::TrustedHost` vs `BrokerCaller::Sql { context, superuser }` — so the gate never divines what `None` means; a missing broker identity is an identity holding no roles, and only a context-less SUPERUSER SQL role is privileged (the path that declares an ACL in the first place). A present security context is the broker identity outright: the SQL role's superuser status adds nothing to it, which stops a `new_secure` session — whose effective SQL role falls back to bootstrap — becoming a broker superuser. `broker_stats` enumeration was filtered on the same `is_some()` test and is now filtered for every SQL caller. Regression suite: `crates/bicdb-sql/tests/broker_caller_provenance.rs`.
- **H-15** ~~Absent or unrecognised authentication method resolves to the STRONGEST principal~~ — **FIXED (1.0.290-beta)**. Found by applying the H-14 lesson as a search heuristic: look for a value where absence means maximum authority. `AuthenticationStrength::Internal` was simultaneously the strongest variant AND the enum's `#[default]`, and two paths reached it by omission. (1) `raw_sql_security_context` (`bicdb-app-runtime/src/host.rs`) mapped an actor's authentication method to a strength with a catch-all `_ => Internal`, so an actor with NO authentication method — an anonymous caller on a `route.public` endpoint — was stamped as an internal principal; a bearer token identifying a SERVICE rather than a user missed the `user_id.is_some()` guard and landed in the same arm. (2) `authentication_strength` is `#[serde(default)]`, so a `SecurityContext` deserialized without the field also became `Internal`. This matters because the strength is published to RLS policies through the trusted GUC `carrier.authentication_strength` (it is in `PROTECTED_SECURITY_SETTINGS` precisely so policies can rely on it) and because `invoke_resource_as_internal_admin` gates cross-tenant support access on `== Internal`. That gate additionally requires a `bypass_policy` and the `carrier:internal_admin` scope, neither of which this path sets, so the internal-admin route stayed closed — the exposure is against policy logic keyed on authentication strength. Fixed: the catch-all is now `Unauthenticated`, `jwt`/`bearer` maps to `Jwt` regardless of user id, an explicit `"internal"` method maps to `Internal`, and `Unauthenticated` is the enum default. Completed in 1.0.291-beta: `SecurityContext::new` no longer hard-codes `Internal`. `Internal` is now reachable only by naming it — `SecurityContext::trusted_internal` for an actual system principal, `SecurityContext::authenticated(.., strength)` where the originating strength is known. Every production call site was classified: the engine's own PHI sampler and tenant auditor (`db.rs`) are trusted system principals and say so; `mutation_security_context` and the sync-replay session (`sync_server.rs`) are DELEGATED end-user principals and now take the weakest value, because neither `MutationActor` nor the sync bundle carries the originating strength. DOCTRINE: execution provenance and authentication strength are different dimensions — a user's request does not become internally authenticated because an in-process worker replays it. Running inside the server is not being authenticated as the server. When the replay format can carry the originating strength, propagate it; never infer one from in-process execution.
- **H-16** ~~An ownerless view executes with bootstrap (superuser) read authority~~ — **FIXED (1.0.292-beta)**. Third member of the missing-information-means-trusted family. `ViewSchema::owner` is an `Option` and views persisted before ownership tracking carry `None`. Every OWNERSHIP site reads that as "owned by the bootstrap role", which is fail-CLOSED — only a superuser may alter or drop such a view. Definer EXECUTION used the same convention and it inverted: `view.owner.unwrap_or_else(current_role_name)` resolves to the bootstrap role, so an ownerless view ran its body with SUPERUSER read authority over every underlying table. VERIFIED: the same view, created by an unprivileged role over a table it cannot read, is refused as its creator's view (`permission denied for table secrets`) and returns `P0-CANARY` once its `owner` field is absent — reachable by anyone holding SELECT on the view, a grant that survives an upgrade. Fixed at the single definer-execution site (`engine.rs`): an ownerless view falls back to INVOKER semantics, which can never grant more than the caller already has, rather than to a hard refusal that would break every legacy view on upgrade. The ownership sites are unchanged, so a missing owner is still fail-closed there. Regression suite: `crates/bicdb-sql/tests/view_owner_execution_identity.rs`, which also pins that owned views keep definer semantics and that an ownerless view is still superuser-only to drop.
- **H-17** ~~An ownerless SECURITY DEFINER routine executes as the bootstrap role~~ — **FIXED (1.0.293-beta)**. Fourth member of the family, and the direct sibling of H-16 — found by applying its heuristic: a default read by BOTH an ownership check and an execution-identity path is almost certainly fail-closed in one and fail-open in the other. `RoutineSchema::owner` was a bare `String` with `#[serde(default = "default_routine_owner")]` returning `BOOTSTRAP_ROLE_NAME`, so a routine persisted before ownership tracking deserialized as bootstrap-owned. For ownership that is fail-closed (only a superuser may `ALTER FUNCTION ... OWNER TO` it); for SECURITY DEFINER execution it inverted — the body ran with superuser authority. Worse than the view case because a routine can WRITE. VERIFIED as a clean A/B where the only variable is the owner field: same function, same EXECUTE grant, same caller — owner recorded gives `permission denied for table secrets`, owner absent returns `P0-CANARY`. Fixed by making the field `Option<String>` with two explicit readings — `RoutineSchema::owner()` (bootstrap default, for ownership and display) and `RoutineSchema::definer_owner()` (`Option`, for execution) — so each site states which it means. An unrecorded owner now falls back to INVOKER semantics. Note: there are THREE definer-execution sites (SQL functions, procedures, and the PL/pgSQL engine path); changing the field's type is what surfaced the third, which patching the two known sites would have missed. Regression suite: `crates/bicdb-sql/tests/routine_owner_execution_identity.rs`.
- **H-5** ~~Routing functions bypass RLS/GRANTs~~ — **FIXED (1.0.282-beta)**. `shortest_path`/`route_distance`/`optimize_route` take a graph NAME and scan `{graph}_nodes`/`{graph}_edges` through the raw store, so the relation they read never appears in the query and no table gate fired. Verified: with `SELECT * FROM roads_nodes` denied, `shortest_path` returned the source rows verbatim — node ids, coordinates and topology. The earlier prefix-based check covered `travel_time` and the `bicdb_*` family; these three carry no prefix and dispatch through a different evaluator, and there were four ungated call sites. All now route through `eval_routing_function_value_authorized`, gated on SELECT over both backing collections (not superuser, so legitimate graph users keep working). Regression suite: `crates/bicdb-sql/tests/routing_authorization.rs`.
- **H-11b** ~~Relocation data plane not source-authenticated~~ — **FIXED (1.0.282-beta)**. Every range-write, repair, backup-fence and digest RPC is leader-fenced through `validate_range_write`, but the relocation data plane — PrepareLearner, ExportSnapshot, ApplySnapshot, LearnerWatermark, ExportCatchUp, ApplyCatchUp, CleanupSource, CompleteRelocation — was dispatched with no `caller_node_id` at all. The service's own `ensure_target`/`ensure_snapshot_source` validate the LOCAL node's role, never the sender's, and every `RangeRelocation` field is gossiped topology any member can replay, so any member with a valid certificate could drive forged batches into a live relocation's target (persisted via `bypass_commit_admission` + `write_upserts_unchecked`). `validate_relocation_participant` now fences all eight arms on the range leader / relocation source / target; `ExportCatchUp`, which carries no relocation, is fenced on range replica membership. Tightened in 1.0.283-beta to a strict SOURCE fence: relocation is driven from the source side (`TransportClusterRelocationDriver` runs as the coordinating actor and issues every arm, including those handled on the target), so the target is never a legitimate caller. Admitting it — as the first participant fence did — left a compromised learner able to pull the source's range out through ExportSnapshot/ExportCatchUp or destroy data on it through CleanupSource. Validated against the TCP relocation tests, which drive real relocations end to end through the dispatch; a deny-all mutation of the fence makes them fail, proving the gate is on the live path and not bypassed.
- **H-6** Grant resurrection via DROP ROLE + CREATE ROLE — `session.rs:9957`
- **H-7** Extended-protocol text bind params (int/bool) spliced unquoted, unvalidated — `pgwire lib.rs:9903,10164`
- **H-8** Unbounded recursion → stack overflow → **process abort** in four hand-rolled parsers: tsquery text (`fts.rs:869,877`), tsquery binary (`fts.rs:472`), array literal (`eval.rs:8898`), jsonpath (`jsonpath.rs:477`). Not catchable by `catch_unwind`.
- **H-9** tsvector binary decode allocation bomb — `fts.rs:151,558` (`bounded_binary_count` not applied here)
- **H-10** Kafka metadata allocation bomb — `products/carrier-broker/src/kafka.rs:516`
- **H-11** Consensus/Raft identity not bound to TLS cert; votes granted on self-asserted `candidate_id` — `consensus.rs:233`, `bicdb-cli/src/main.rs:8012`
- **H-12** carrier-broker unauthenticated by default on all protocols incl. :15672 management API — `amqp_mgmt.rs:149`, `amqp.rs:1130`, `mqtt.rs:413`, `kafka.rs:366`
- **H-13** Sync server unauthenticated by default; one global token authorizes every scope — `bicdb-cli/src/sync_server.rs:322`
- **H-14** App-runtime capability grants are package-declared, not operator-granted; flat global secret namespace — `app-runtime/src/host.rs:3381`, `abi_v2.rs:215`, `providers.rs:39`

## MEDIUM

M-1 EXPLAIN ANALYZE without SELECT priv (`engine.rs:4878`) · M-2 ungated ANALYZE +
unfiltered `pg_stats` leak column values (`session.rs:5116`, `catalog_rows.rs:832`) ·
M-3 DROP TABLE/VIEW/INDEX without ownership (`session.rs:9810,9829,9940`) ·
M-4 CREATE INDEX without ownership (`session.rs:11351`) · M-5 DROP FUNCTION/PROCEDURE
without ownership (`session.rs:12378`) · M-6 ungated `TRIM AUDIT HISTORY` (`session.rs:6118`),
`VACUUM` (:6070), carrier `DO` blocks (:11226) · M-7 `bicdb_server_connections` exposes
every session's query text to any role (`pgwire lib.rs:6574`) · M-8 Argon2 params from
`encryption.json` unbounded → DoS on open (`encryption.rs:523`) · M-9 storage-frame and
large-value AAD don't bind identity/position → frames movable between collections
(`storage.rs:1060`, `large_value.rs:444`) · M-10 `decrypt_blob` takes attacker KDF params
(`encryption.rs:399`) · M-11 PHI envelope nonce length unvalidated → panic (`phi.rs:505,143`) ·
M-12 replication allow-list checked against self-asserted Hello, not cert fingerprint
(`main.rs:8415`) · M-13 mesh sync wedgeable by a `_bicdb_user_metadata` key (`sync_mesh.rs:157`
vs `:776`) · M-14 HTTP rate limiter keyed on attacker-controlled `Authorization`
(`http.rs:2293`) · M-15 SSRF filter misses IPv4-mapped IPv6 (SUSPECTED, `providers.rs:1404`) ·
M-16 cross-app route shadowing via `{param}` templates (`runtime.rs:15879`, `http.rs:3320`) ·
M-17 unbounded host-side full-collection scans from extensions (`host.rs:2433`) ·
M-18 `statement_timeout`/`lock_timeout`/`idle_in_transaction_session_timeout` accepted but
**never enforced** (`session.rs:4957`) · M-19 Describe executes with no timeout/cancellation
(`pgwire lib.rs:5096`) · M-20 sync-server slowloris (`sync_server.rs:214`) · M-21 LAN mesh
inbound serialized, no session deadline (`lan.rs:164`) — **P1-2 only partially closed**

## LOW / hardening

L-1 `server_users.json` 0644 non-atomic · L-2 SCRAM dummy-verifier salt is a public
constant → user-existence oracle · L-3 CLI default `--auth-method cleartext` · L-4 COPY TO
bypasses `max_result_rows` · L-5 `redact_query_text` default false → passwords in logs ·
L-6 no key zeroization · L-7 PHI rotation leaves old-key ciphertext beside segments ·
L-8 UDP LAN beacon auto-dial residual · L-9 predictable temp files/symlink-following creates
(ha.json.tmp, consensus state, fs_endpoint, replication staging, spatial pack) · L-10 restore
TOCTOU on intermediate dirs · L-11 legacy backup v1/v2: no AAD, unbounded decode · L-12 OSM
PBF unbounded · L-13 posting-block over-reservation · L-14 `max_pending_accepts` never
enforced · **L-15 cancel key is 32-bit, not the "122-bit" my audit doc claimed** ·
L-16 sync-server: non-constant-time token, CORS `*`, checkpoint overwrite · L-17 HL7 ACK
injection · L-18 ABI v1 header filtering · L-19 no package anti-rollback · L-20 `/openapi/`
unauthenticated · L-21 `count(*)` leaks policy-collection size · L-22 REFERENCES not enforced ·
L-23 plan-cache point lookup (SUSPECTED, opt-in) · L-24 raw `import_events` bypasses signature ·
L-25 release profile has `overflow-checks` off · L-26 CLI `--key` on argv · L-27 auth-phase
write timeout uses 300 s `idle_timeout`

## Dependencies (locked tree currently FAILS `cargo deny check advisories`)

- **wasmtime 46.0.1 — RUSTSEC-2026-0223 and RUSTSEC-2026-0222.** Two advisories in
  **the extension sandbox itself**. Upgrade.
- crossbeam-epoch 0.9.18 — RUSTSEC-2026-0204 (debug-format path, low risk)
- rkyv 0.7.46 — RUSTSEC-2026-0235; no reverse dependency found, likely stale lock entry
- anyhow 1.0.102 unsoundness (fix ≥1.0.103); memmap2 unsoundness; unmaintained `paste`,
  `rustls-pemfile`, `smartstring`; yanked `spin`

## Reviewer's remediation order

1. CR-1 strict identifier parse + no superuser-context execution of client-shaped SQL
2. CR-2/3/4 — **one authorization pass over object DDL** (ownership or WITH GRANT OPTION)
3. CR-5 make rowid/PK fast path and `collection_record_count` policy-aware
4. H-8/H-9/H-10 depth caps + `bounded_binary_count` + clamp (all small fixes for whole-process aborts)
5. H-11 bind consensus votes to cert fingerprints (pattern exists in `distribution_transport.rs`)
6. H-12/H-13 fail closed when binding non-loopback without credentials
7. H-14 operator-granted secret allowlists + per-app namespaces
8. Remaining authz gaps; then M-8/M-10/M-9/M-11/M-18
9. Dependencies (wasmtime first)
10. Follow-up pass: MVCC visibility, buffer-pool singleflight, four `unsafe` lease-invariant sites

## Ruled out with a reason (do not re-chase)

Negative results worth keeping, because each cost real effort to establish and
the premise is plausible enough to be re-reported.

- **D-9 — anti-entropy digest-repair authority is unsigned.** True but not
  exploitable. `RangeDigestRootEvidence` is `{ root_sha256, node_ids }` with no
  per-node signature, and `apply_range_digest_repair_batch` only checks that the
  named nodes ARE current voters — nothing proves they certified the claimed
  root. The reported attack is "a malicious voter forges a repair batch", and
  that premise fails: the path calls `ensure_range_digest_fence`, which calls
  `validate_range_write_peer`, which requires `caller == range.leader`. So only
  the range leader can reach it, and a leader can already write arbitrary range
  data through the leader-fenced range-write path. The unsigned evidence is
  decorative, not an escalation. It would become one if the fence were ever
  relaxed to any voter — worth signing then.
- **`write_attachment_unchecked`.** Both wrappers are sound: the secure handle
  authorizes the collection and validates the record's tenant, and the raw
  `BicDb` wrapper refuses any collection carrying a policy
  (`ensure_unprotected_legacy_access`). The two variants of that gate
  (`ensure_unprotected_legacy_access` / `ensure_existing_unprotected_legacy_access`)
  carry the same two checks; the only difference is that the "existing" variant
  tolerates a missing collection, which has nothing to protect.
- **RLS under alternate relation names.** Policies hold across `public.x`,
  quoted, mixed-case, aliased, CTE and subquery forms — the RLS schema lookup
  and the executor resolve the same name.
- **`AS RESTRICTIVE` policies.** Correctly AND with permissive ones rather than
  silently becoming permissive; `pg_policies` reports the kind accurately.
- **`TableSchema::owner` and the RLS owner bypass.** Unlike the view and
  routine owners (H-16, H-17), this default is fail-CLOSED: losing an owner
  *removes* a bypass rather than granting one.
- **Adaptive (WASM) procedure host.** Embedded SQL re-enters the *same*
  `SqlSession` through a pointer, so identity and GUCs are preserved — not a
  repeat of the `ts_rewrite` sub-engine bug (#599).

## Reviewer's "verified sound" (no issue found)

WASM sandbox configuration (fuel, epoch, pooling allocator, single import, no WASI);
mesh trust model post-remediation; backup v3 format; path/name regime; crypto primitives
and nonces; pgwire TLS/SCRAM hygiene; deserialization depth limits; no ReDoS; no command
injection; 16 `unsafe` sites inventoried sound except four lease-invariant ones flagged
for review.
