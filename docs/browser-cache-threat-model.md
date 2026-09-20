# Browser cache threat model (draft — sign-off pending)

Scope: BicDB working-set caches in browsers (`web/bicdb-client` +
`bicdb sync-serve`), holding education data and narrowly-scoped regulated-data
views (e.g. today's appointments) on shared, cheap devices — school
Chromebooks are the design point: multi-user, loanable, sometimes managed.
This draft is the input for the regulatory review (TODO 3.8); regulated-data
rollout is blocked on that review, schools-only is not.

## Assets

1. Cached working set in OPFS (records + event history — the event log
   holds prior versions, not just current rows).
2. Attachment blobs (AES-GCM chunked files) and their manifest metadata.
3. The database encryption key (in worker memory while open).
4. Sync bearer tokens (client token; admin token server-side only).
5. Telemetry (low sensitivity; contains sizes/timings, never row data).

## Trust boundaries & mitigations in place

- **Same-device, different OS user**: OPFS is origin- and browser-profile-
  scoped; DB content is ChaCha20-Poly1305 at rest, attachments AES-GCM.
  Without the key (delivered per session, held only in worker memory),
  OPFS bytes are ciphertext — verified in tests (wrong-key open fails;
  no plaintext runs on disk).
- **Same browser profile, different web origin**: browser same-origin
  policy isolates OPFS entirely.
- **Network**: the sync listener is plaintext by design; DEPLOYMENT MUST
  TLS-terminate in front. Bundles additionally contain plaintext record
  data — TLS is load-bearing, not defense-in-depth.
- **Server compromise of one scope**: scopes are separate databases and
  directories; the admin SQL endpoint is a separate token and disabled
  unless configured.

## Gaps the review must weigh (known, not yet closed)

1. **Static bearer tokens.** One shared client token authorizes every
   scope: any student's token reads any scope today. MUST become
   per-session, per-scope Hub tokens before any real deployment (single
   function swap in `sync_server.rs`; tracked in the TODO).
2. **Key lifecycle discipline lives in the app.** Logout must drop the key
   and (for shared devices) `BicDbClient.destroy()` the cache. The library
   provides the primitives; Hub owns the policy. Key-in-memory is readable
   by anything with script execution in the origin (XSS is the real
   perimeter — CSP on Hub matters more than anything in this library).
3. **Event history bounded by horizon trimming** (was: outlives retention
   intent). `trim_event_horizon` drops superseded history and acknowledged
   deletes on both server (`--event-horizon`) and client (CacheManager
   pressure path). Residual: history younger than the horizon still holds
   prior versions — for regulated-data scopes run the client trim on a schedule,
   not only under pressure, and keep the server horizon age short.
4. **Deleted-then-evicted browser data** is subject to OPFS eviction
   semantics, not secure erasure; encryption at rest is the compensating
   control.
5. **Telemetry JSONL** is per-scope and un-rotated; treat as operational
   logs (no record data is ever sent by the built-in reporters).

## Test evidence

- Wrong-key open rejected; ciphertext-only on disk (browser-smoke).
- Wrong bearer token → 401 before any db access (sync-e2e).
- Admin SQL absent unless `--admin-token` set; separate token (sync-e2e
  uses both tokens distinctly).
- Retention deletion propagates to devices (sync-e2e).
