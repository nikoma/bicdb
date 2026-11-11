# Table row routines and type metadata

This is source-scoped evidence for standalone source
`221aeeda361cdd3f1a2e0b46984ee7242ff92cd6`, based on actual main
`16962de1` (1.0.466-beta). All six commands pass: the direct regression and
version identity (5 tests), eight type/OID integration targets (45), a routine
subset (7), the full SQL library (864 passed, 4 ignored), and the full general
SQL target (419 passed). The total is 1,340 test executions, including the
routine subset repeated by the full general target; it is not a unique-test
count. The four ignored tests are existing microbenchmarks, including a catalog
benchmark requiring a disposable database copy. None were newly ignored here.

Tests-only baseline `43db056a` fails both new tests against unmodified main:
scalar routine execution refuses a cast to its table row type, and routine
creation refuses an array of that row type. Each test stops in its first
EmbeddedMemory case; later stores and reopen checks are not claimed for this
negative revision. The caller's payload, advertised type and reopen assertions
remain in the positive test. The fixes extend routine declaration parsing for
custom arrays and resolve routine return/frame coercion through the existing
cached table row/array OID descriptors, alongside standalone user types.

The first positive candidate `34620c26` returns both payloads correctly, but its
stronger metadata assertions fail: the array regtype text is the physical
`_typed_row` spelling and the scalar pg_proc result OID is text (25), rather
than the registered table row OID. The correction includes table row OIDs in
routine catalog metadata, retains builtin type identity, and shares a cached
logical SQL type-name formatter between regtype output and format_type. Array
output uses the element name plus `[]`. Standalone type formatting keeps its
existing priority and looks up table types only when needed.

At `3e633251`, both direct tests pass through both stores and reopen, including
procedure calls, payload/type results, exact argument/return OIDs and format_type.
The additional qualified quoted-name control then fails its regtype input
lookup. Source `221aeeda` uses the shared alias resolver for table types,
including schema-isolated logical names and the distinct array OID. All three
direct tests and all remaining command groups pass without weakening the
original assertions. Real metadata lookup failures are propagated rather than
silently converted into an unknown type.

Native PostgreSQL 16.13 references pass for both scalar/array function values
and types, both procedure calls, and qualified quoted type-name formatting.
Each reference uses and removes private temporary schemas; the protected
application schemas are unchanged. These are language/metadata references,
not PostgreSQL restart or BicDB durability qualification.

This evidence precedes final source versioning and parent integration. Later
versioned heads require their own final-source validation. The standalone
change covers routine calls and metadata; the separate SQL scoped rename and
recovery campaign remains open. These results do not establish all-workspace,
final iDempiere, VM/power-loss or production acceptance. Original application
assertions and the ten-second timeout are unchanged. No standard server binary,
release or deployment was produced by this batch.

Receipts preserve exact commands, source revisions and outcomes.
`test-observations.json` retains test summaries and original failure excerpts;
`sha256s.json` binds the included files.
