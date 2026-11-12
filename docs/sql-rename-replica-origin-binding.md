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
wire metadata ownership/descriptor follow-up remains under qualification.

The authenticated portable binding producer/resolver described above remains
unimplemented. Successful local cleanup does not close the original replica
promotion gate or permit matching a target backing by donor identity or name.
