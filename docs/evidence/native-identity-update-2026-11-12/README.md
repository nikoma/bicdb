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

Source 1.0.490-beta is a delivery candidate based on actual main489
`b9394418bdf9d87864c2c12d6fe81bb7d4039d53`. It excludes the separate private SQL
partition/generated-column implementation. The Core method and twelve Core
grant tests are identical to the independently qualified private implementation;
that private composition passed seventy focused Core/SQL tests and a SQL-path
recovery matrix (38 process cuts, 8 EIO cases, 10 actual KVM resets). Those
results cover their recorded source, not this candidate. This candidate has a
Core-only recovery probe and requires its own versioned build, full Core suite,
SQL authority tests, process/EIO/VM recovery and CI before merge. Validation is
pending at this checkpoint. No CLI, release or deployment is implied.

The Core probe seeds two independently acknowledged records, including Native
vector, timestamp, payload and unknown metadata. It checks full old/new records,
index definitions and mutation policy, a Native transaction witness, one exact
matching audit (or none for an aborted update), three stable reopens, Native
indexes and continued refusal of writes lacking authority. Controllers, source
pins, results and artifact hashes will be attached after the actual runs.

The broader campaign remains active: private partitioning, portable Native
collection origin for replica rename recovery, original Oracle/PostgreSQL
application inventories and timeouts, UI400 encrypted backup/full restore,
physical HA and approved P1/P2 gates remain separate. Experimental production
behavior stays disabled; there is no new cloud-resource purchase or promotion.
