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

Source 1.0.490-beta is based on actual main489
`b9394418bdf9d87864c2c12d6fe81bb7d4039d53` and excludes the separate private SQL
partition/generated-column implementation. The tested implementation revision
is `6cafd400844f0dc11200afed52ab2f015009ecf2`. Subsequent evidence edits do not
change production code, tests, dependencies or versions.

The exact versioned candidate passed all 1,696 Core library/integration tests,
zero failed and seventeen ignored, across 126 Cargo targets. The count uses the
last parent-harness summary per Cargo target and excludes one nested child
summary. The twelve focused Core grant tests also passed and are included in
that full count. The strengthened SQL authority regression and both SQL version
identity tests passed separately. Version synchronization checks all twenty-nine
packages, internal requirements, tracked lockfiles and the current-source
compatibility document. Original assertions and application timeouts are intact.

Its Core-only recovery probe passed 38 process cuts, eight actual EIO cases and
ten actual KVM resets; independent verification also passed. Four fixture ACKs
include Native vector, timestamp, payload and unknown metadata. The verifier
requires full old/new records, index definitions and policy, a Native transaction
witness, no audit for old state or exactly one matching audit for new state,
three stable reopens, valid indexes and continued refusal without authority.
All externally acknowledged cases recover complete new state. See
[recovery evidence](recovery/README.md) and [validation results](validation/).

GitHub Core, SQL, protocol, community/boundary, licensing, formatting and selector
jobs passed on that implementation revision; Rails passed too. The remaining
workspace job was still running at this evidence checkpoint. The final PR head
must satisfy all required CI before merge; no pending job is reported as passing.
This evidence does not imply a new production CLI, published release or verified
deployment. The retained probe is explicitly a debug qualification artifact.

The broader campaign remains active: private partitioning, portable Native
collection origin for replica rename recovery, original Oracle/PostgreSQL
application inventories and timeouts, UI400 encrypted backup/full restore,
physical HA and approved P1/P2 gates remain separate. Experimental production
behavior stays disabled; there is no new cloud-resource purchase or promotion.
