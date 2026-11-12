# Native authorized identity updates

This change adds `Transaction::update_identity_with_grant` to current main.
A caller holding actual transaction-local UPDATE authority may change a row's
Native ID within one collection. The capability must cover both addresses and
explicitly allow `id`. Native fetches the locked preimage, checks its observed
version, tenant/workspace, immutable fields, optimistic version and action
budget, and refuses an occupied destination. Two physical writes and one audit
share the Native decision. A staging failure restores writes, locks, audit and
the action's grant budget. Audit payloads retain the old ID and explicitly list
the replacement ID. This creates no cross-collection, replica or image authority.

SQL Native MutationDenied errors now report 42501. The existing SQL mutation
authority regression retains its assertions and also checks UPDATE/DELETE
refusals preserve an acknowledged row and commit sequence.

The current **1.0.492-beta integration candidate** is based on actual finance
main490 `702f0435a12944563c7751525f8429d80374a3d0`. Its tested implementation is
`2403deefde68869c15f02b73d40406ffdfb50148`. It retains the actual executing
application issuer on both physical writes; rollback retires both issuer entries
and retry obtains fresh authority. Immutable application facts continue to refuse
identity replacement, including for their owner. It also preserves six implemented
semantic reader requirements in Native image capture and rejects bindings made
before those requirements changed. Local storage/authority flags and unsupported
SQL rename capabilities are excluded from that portable reader list.

On that exact candidate, the complete Core library/integration command passed
**1,700 tests, zero failed and seventeen ignored**, across all 126 Cargo targets.
The canonical count uses each parent harness's final summary and excludes one
nested child summary. The fourteen issuer/grant checks are included in the full
count. Current-source SQL mutation authority and both version-identity tests also
passed. The earlier issuer/grant/SQL/version focused batch passed seventeen checks;
the reader fix passed twelve distinct checks in fourteen invocations after two
actual baseline failures. All original assertions and application timeouts remain
unchanged.

The current Core-only probe independently passed **38 process cuts, eight actual
EIO cases and ten actual KVM RAM-loss resets**. Four original fixture ACKs include
Native vector, timestamp, payload and unknown metadata. Verification requires exact
old/new records, schema, policy, indexes, a Native transaction witness and exactly
one matching audit for new state, with three stable reopens and grantless mutation
refusal. All externally acknowledged cases recover complete new state. Process
cases recovered ten complete-old and twenty-eight complete-new states; all eight
EIO cases recovered complete-old; VM cases recovered two complete-old and eight
complete-new, including both external VM ACKs.

The frozen current debug probe is
`/root/bicdb-durability-s3/qualification-artifacts-debug/2403deefde68869c15f02b73d40406ffdfb50148/native_identity_crash_probe`
on the existing qualification host, SHA256
`fd1c88d04014ec8913194efdd23956a1c910da5d1a03168d9585d10bc8d6e592`.
See [current candidate evidence](finance492/README.md). This is a qualification
probe; no new production CLI, published release or verified deployment is implied.
Candidate qualification is distinct from the final-head CI, merge/push
verification and temporary-branch cleanup recorded in the pull request. The older
head's passing CI does not certify this integration.

## Historical pre-finance 490 candidate

The earlier unmerged 1.0.490-beta candidate was based on actual main489
`b9394418bdf9d87864c2c12d6fe81bb7d4039d53`. Its tested implementation
`6cafd400844f0dc11200afed52ab2f015009ecf2` passed 1,696 Core tests, zero failed and
seventeen ignored, across 126 Cargo targets, plus SQL authority and version checks.
Its separate process/EIO/VM qualification passed 38/8/10 cases. The original raw
receipts, controls and hashes remain in [historical recovery](recovery/README.md)
and [historical validation](validation/). They do not certify later finance or
reader changes. Both that implementation and the evidence head
`52f8baa6b42b4a3595655a36e4a971f227bb1d59` completed all nine executed GitHub test
jobs and Rails successfully; the two unused focused selectors were skipped.
The terminal old-head job metadata is retained in
`finance492/historical-old52f8-terminal-CI.json`.

The broader campaign remains active: private partitioning, authenticated portable
Native collection origin for replica rename recovery, original Oracle/PostgreSQL
application inventories and timeouts, UI400 encrypted backup/full restore,
physical HA and approved P1/P2 gates remain separate. Experimental production
behavior stays disabled; there is no new cloud-resource purchase or promotion.
