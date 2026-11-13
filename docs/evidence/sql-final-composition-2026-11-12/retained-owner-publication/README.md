# Retained rename owner and partition publication

Exact implementation `82a8311156e2293a92e7cf9c1ffd70d1df92d57a`, inherited
1.0.490-beta on the private combined branch. This is not a main delivery or
production deployment.

The unchanged original 95-test diagnostic passed 94 and failed one on both E8
and combined 4e. E8 tried global compaction during rename with pending owners.
The combined owner-preserving rename passed that point, then partition creation
failed because its derived certificate write exceeded the retained resources.
Both complete original runs, frozen class/assertion hashes and closed-process /
unchanged-baseline receipts are preserved in `original95-history/`.

A focused positive at `09473ee9` reproduced the same certificate failure.
The Native publication access now combines an admitted parent with explicitly
reserved temporary resources, checks foreign observations and scopes, copies
existing backing identities, follows the parent's real lifecycle and fences a
late child commit. It also remains alive across multiple internal transactions
during consumed-owner recovery. The parent reservation is unchanged. The SQL
writer supplies its checked actual owner or undecided recovery context. This
adds no Native data-policy grant or portable/import authority.

New descriptors, partition attachment and their derived certificate retain the
same atomic Native publication. Undo restores authored catalog rows and derives
the resulting certificate; it does not replay an old global certificate over
independent catalog state. The schema cache retains the actual saved execution
version. The first new Native control mistakenly checked a catalog-record fence
through generic row access; its failure is retained and the corrected test uses
the actual catalog read API. V2's three Native controls passed, while its unchanged
SQL positive advanced to an unowned attachment writer. That writer is connected
to the same checked publication context in 82a.

The final focused batch passed all three Native controls and the unchanged SQL
positive. The latter performs actual rename, old-name reuse, partition creation,
default attachment, commit and rollback, independent SQL reads, exact Native ACK
hashes and two clean reopens in both modes. The related batch passed all nine
commands, including Core/SQL/wire all-target compilation and 259 passing test
invocations: Core 34, SQL 54 and wire 171, zero failures or ignored. The three new
Native controls repeat within those related checks. These are focused checks,
not full Core/SQL/wire regression or application acceptance.

The explicit debug `oracle_server` was built from 82a and frozen at
`/root/bicdb-durability-s3/qualification-artifacts-debug/82a8311156e2293a92e7cf9c1ffd70d1df92d57a/oracle_server`,
SHA256 `9d66171db1afa6a5d3bf5257c7918bf3c59b44395812d56c74a33c6ddc762f7c`.
Its unchanged original 95-test run is recorded separately; this artifact is not
proof of a passing application run. Final broad, fault, VM and application gates,
and authenticated Native collection-birth/provenance producers at both image and
logical-import boundaries, remain required. The original promoted undecided
replicated rename positive remains unchanged and open. No name/donor UUID fallback,
P2 production promotion or new cloud spend is authorized by these results.
