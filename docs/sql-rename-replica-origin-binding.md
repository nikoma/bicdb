# SQL rename recovery across an imported namespace

Status: proposed Native/SQL contract; the binding producer and resolver are not
implemented. Replica promotion with an undecided imported rename remains an
open qualification gate. The current private reader accepts only an actual
local namespace UUID plus its checked storage backing. A donor UUID does not
authorize target-local repair.

## Journal origin

Native must provide a typed immutable backing-birth identity. A collection name,
storage alias, SQL OID, transaction counter or journal field chosen by SQL cannot
substitute for it. The proposed portable image carries these fields:

| Field | Meaning |
| --- | --- |
| `origin_version` | `1`; fenced by a new required-reader capability. |
| `source_backing_birth` | Opaque Native-issued collection birth/generation, serialized using Native's defined codec. |
| `source_namespace_uuid` | Actual non-nil namespace UUID of that source backing at capture. |

Both row and rename effects need the same origin envelope. The existing
`storage` field remains a source consistency check and diagnostic; it grants
no cross-store authority. Existing local-only images must not be silently
promoted to the portable format or supplied a fabricated birth identifier.

The journal header also needs Native's accepted-provenance context. SQL must
obtain it from Native, tied to the actual installed journal backing and accepted
image/log, rather than trust a provenance-root string from the journal payload.
The exact Native birth/provenance codecs and capability identifier are pending
Native implementation; no current public API supplies them.

## Binding producer

At actual target creation/import finalization, Native must durably bind the
source tuple `(source_backing_birth, source_namespace_uuid)` to the actual
target `(target_backing_birth, target_namespace_uuid)`. The binding includes
the authenticated accepted source provenance root, target installed-base
identity and restore/activation generation. Publication belongs to the same
protected Native creation/import boundary that makes the target usable.

An interrupted install must expose neither a usable half-binding nor a binding
to an old target generation. Reopen must either recover the complete binding
and target or continue to refuse resolution. A later drop/recreate invalidates
the old binding, including when names and storage aliases are reused.

Native-image installation and legacy logical replication/import require their
own actual producers. An image-install receipt cannot authenticate a legacy
logical import. Replication's current name/mode `CatalogOp::Create` alone does
not carry this authority.

## Read-only resolver and SQL consumption

The proposed resolver takes Native's accepted provenance context and the typed
source tuple. It returns a verified current target backing birth, actual target
namespace UUID and actual physical storage identity. It must refuse missing,
ambiguous, stale, wrong-installation or wrong-provenance mappings. Looking up a
mapping must never create, repair, replace or infer one.

The SQL reader would use the returned target identity to find the current
logical collection and preflight every remaining image before any mutation.
The current logical name may have changed; the target birth/UUID and physical
backing must still match Native's current catalog. Row identity, schema-image
identity, complete runtime scope coverage and the undecided journal decision
remain independent checks. Existing owner/delegation/recovery admission still
applies. No fallback may match by name, SQL OID, alias or donor UUID alone.

## Required evidence

- Successful same-store recovery still checks actual local identity.
- Actual authenticated image and logical-import producers each recover an
  undecided rename after promotion using fresh target-local UUIDs; following
  the source decision is tested separately from target-local undo.
- Missing, duplicate, forged, wrong-root, wrong-installation and stale-generation
  bindings refuse all images before changing acknowledged data or catalog rows.
- Drop/recreate and alias reuse cannot redirect old recovery to new data.
- Process death at binding persistence, target publication and activation either
  recovers a complete valid binding or refuses; independently recorded ACKs
  survive every qualified recovery outcome.
- Old readers reject the portable format; old local-only images are not
  retroactively authenticated. Original application and replica assertions stay
  intact, and final-source crash/VM/application qualification remains required.

## Local savepoint cleanup of a created namespace

The local creation-undo gate has a Native producer and consumer on private
implementation source `e1563f54103217751d71416683a01db0f2371e02`.
`CatalogCreatedCollectionUndo` is an opaque process-local receipt issued only
while actually creating an absent staging namespace under the real owner scope.
It retains the Native incarnation, actual local namespace UUID and physical
backing. Names, SQL OIDs and SQL-supplied UUIDs cannot mint or replace it.

Undo validates the pending owner, scope, current incarnation and absence of
surviving writes, index writes and row locks before changing metadata. The
actual durable drop must succeed before consuming the receipt and retiring
only that creator's bindings to that exact incarnation. Failed catalog I/O
leaves the receipt unconsumed and observations intact. Ordinary savepoint
rollback still retains unrelated reads and writes. This process-local receipt
supplies no portable image/import authority.

Across equivalent production code through private source
`6ef75198f4f0fab22bd09f0abfef595e8eb15b75`, 31 distinct focused tests passed:
four Native, ten SQL, sixteen wire and one original process-cut regression.
They include both storage modes, repeated create/rollback/recreate, original
commit/savepoint/full-rollback and ACL assertions, eight catalog-I/O failure
scenarios, independent acknowledged work and two clean reopens. These results
qualify the local component; they are neither broad regression nor final
application acceptance. The earlier failing evidence remains historical.

An unchanged original PostgreSQL application batch on the private debug binary
at `6ef75198` passed 94 of 95 tests. The remaining partition test reached a
prepared metadata query after its own rename, where protocol Describe entered
a standalone transaction and received a catalog reservation conflict. The
wire metadata ownership/descriptor corrections at `37891e076703` passed all
20 focused wire tests. A second unchanged 95-test application run at private
`2fe415214d74` still passed 94 and failed the partition test: a foreign live
transaction had observed the whole schema catalog while describing an unrelated
prepared query. A fresh-server protocol probe pinned this to statement Describe,
with Parse alone permitting the rename. Private `663cb2e80cb1` now derives column
origin OIDs directly from the admitted table schema and adds cold-cache foreign
transaction regressions with independent ACKs and two reopens. This second
correction passed 21 wire regressions and 151 protocol library tests. Its
unchanged application batch still passed 94 of 95 tests, advancing to an
unqualified `regclass` lookup that ignored the session search path.

Private `5421dbbb2835` fixes live-path relation identity and exact catalog
admission. Three SQL and 217 wire/protocol tests passed on that source;
46 earlier SQL positives have identical production code at `e637801f`.
The original 95-test run advanced to a missing `pg_indexes` descriptor:
statement Describe fell back to executing the query outside its owning
transaction. A raw-wire negative control at `b3e4b2110b04` reproduces that
same `40001`. The six partition/index catalog descriptors at `a68fe5f37d69`
match 37 actual native PostgreSQL columns and passed 13 SQL plus 218
wire/protocol checks, including both storage modes, the live owner, foreign
fences, acknowledged data and two reopens.

The unchanged application batch at `a68fe5f37d69` is still not passing:
93 passed and two failed. Partition migration reaches unsupported `ATTACH
PARTITION ... DEFAULT`; rollback then attempts an index rebuild while foreign
transactions are pending and retains its catalog reservation after refusal.
The later original partition-name assertion fails too. The successful drop
of a constraint-backed index also needs a dependency-enforcement audit: a
native PostgreSQL18 run of the unchanged MTable class instead failed with
`2BP01` at that drop (37 passed, one failed). That native18 fixture required
removing one invalid optional operator commutator from its copied restore;
all other SQL/COPY bytes, the frozen export, test bytecode and assertions
were preserved. It is a qualified fixture-migration control, not an untouched
Native16 baseline. It does not excuse BicDB's different parser or rollback
failures. Raw source-scoped receipts and failures are under
`docs/evidence/catalog-read-admission-2026-11-11/concurrent-producer/metadata-regclass`.

The authenticated portable binding producer/resolver described above remains
unimplemented. Successful local cleanup does not close the original replica
promotion gate or permit matching a target backing by donor identity or name.
