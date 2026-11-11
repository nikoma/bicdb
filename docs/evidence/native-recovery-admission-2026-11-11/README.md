# Native recovery admission checkpoint

This internal, unmerged image-runtime batch adds an opaque local database admission and read-only native-prefix observations. Neither grants a recovery role, quorum authority or strong-read permission.

The admission checks an actual native birth, fsync `server_paged` mode, the sticky reader fence, completed native initialization, exact cluster/bootstrap/member identity and current DB-open incarnation. The recovery RPC must separately verify its fresh authenticated role witness, restore lineage and current data quorum. A token from an earlier open is refused. Fresh learners can obtain local DB admission while having **no range floor**.

Observations use `None` for unknown, uninitialized or invalid lineage and `Some(0)` only for verified genesis. Pending and materialized-pending fields are currently always `None`: the native journal/prefix extension is not implemented by this checkpoint. The private command-intent/certificate types compile but are not connected to coordinator behavior or runtime acknowledgments.

Nine distinct local native tests passed on the exact pre-format source manifest in `source-inputs.json`. Afterward only the two-line reexport wrapping in `post-test-format-only.patch` changed; `current-source-inputs.json` records the final bytes, which await composed validation; the two focused admission tests are included in that count. `validation.json` records both runs, profiles, durations, test names and memory guards. This is local source validation, with core optimization disabled while reusing optimized dependencies. It provides no network, power-loss, physical HA, throughput, StrongGet or production qualification evidence. The original Azure runs used different immutable source/binaries and remain unchanged.
