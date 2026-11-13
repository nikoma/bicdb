# Future Native collection allocation identities

Exact private implementation `68de99c6450518e8d67d83b1118d2e7afa4e7374`, inherited
1.0.490-beta. This prerequisite is unmerged and does not fix portable rename
recovery by itself. The original promoted undecided rename positive is unchanged
and still fails: `missing, replaced, or ambiguous physical backing`.

Under explicit local format activation, a newly allocated backing receives a
birth UUID alongside its local namespace UUID in the durable Native catalog.
Activation and retries never assign a birth to an existing collection. A
migrated namespace UUID is therefore distinguishable from a creation-time birth.
Births survive same-backing rename and restart. Drop/recreate allocates another
birth even if a logical or storage name is reused. Portable schema contracts
exclude both local identities. Untrusted snapshot/frame metadata and contract
restoration cannot adopt donor births or retrofit an existing unborn target.
Actual cluster schema refresh preserves the target birth. Protected live image
capture pins its actual birth separately from namespace IDs and live allocation.

The current focused batch passed 22 Core tests, with zero failures and one
pre-existing ignored frozen-reader test: five birth lifecycle/import/schema
controls, six image binding controls, eight existing namespace controls and three
existing snapshot/schema reader regressions. The five birth controls iterate
both storage modes; import controls cover new, existing unborn and inactive
targets. Native ACK hashes and two reopens are checked. Core/SQL/wire all-target
compilation and synchronized inherited source version checks passed. These are
focused checks, not a full regression suite of this private source.

The same batch ran the original SQL promotion positive and failed it. Its one
failure makes the overall batch status `failed`; it is retained without skips,
assertion edits or donor-ID/name fallback. Source/target root attestation,
authenticated durable import mappings, record provenance, protected Native image
and legacy logical import producers, and the read-only SQL recovery resolver are
still required. A birth UUID, format flag, checksum or a decoded metadata claim
alone grants no recovery authority. The new capability remains explicit and
local; it is not propagated as a portable semantic reader feature.

Preserved history includes the v1 test-setup compile error; v2's 4 passing birth
checks and failed capture fixture (fresh live collection was not yet reopened
into the required purely paged representation); v3's 18 passing Core checks and
unchanged SQL failure; and the separate schema-refresh regression which proved
the target birth was erased before the fix. V4 fixes that real metadata refresh
loss and uses the actual paged reopen for the capture fixture. All original
failures, commands, source revisions and monitor logs remain in their own folders.

The JSON summary counts parent harness results. Each raw manifest validates
original bytes after gzip decompression. No production CLI, release, deployment,
full application acceptance, automatic capacity qualification or experimental
production promotion is implied. The full campaign remains active; no new cloud
spend was introduced.
