# Oracle DECODE and stable cursor argument types

New preview routine definitions select compiler version 9, language OID 999408.
Ordinary SQL in the preview profile also resolves DECODE. The compiler uses the
first search's resolved type for comparison and the first result's type for
return conversion. An untyped NULL or character first result produces text;
a typed numeric NULL retains NUMBER. Searches use nonpadded character comparison,
NULL matches NULL, and the first matching result wins. A simple CASE evaluates
the selector once and skips later searches, results and unselected conversions.
Opaque comparison keys supply these rules without changing PostgreSQL's NULL
comparison policy. Direct procedural DECODE is rejected when the routine is
compiled; the [Oracle diagnostic](extended-reference.json) requires SQL context.

The [scalar fixture](../../../examples/oracle/reference/decode-cases.json) has
48 independently measured expressions. The [SQL runner](../../../examples/oracle/reference/decode_sql.py)
checks their values, expected error mappings and result metadata, plus four
prepared PostgreSQL parameter combinations: **52/52 match** in the
[SQL comparison](sql-live-comparison.json). The
[stored fixture](../../../examples/oracle/reference/routine_decode.py) adds
formal types and an observable selector function: **57/57 match** in the
[stored comparison](stored-live-comparison.json). These exercise conversion
errors only when selected, typed/untyped NULL, numeric-looking strings, CHAR
padding, exact integers above binary64 precision, logical DATE truncation,
TIMESTAMP precision and the fixed default character formats. Oracle error codes
and PostgreSQL SQLSTATEs are asserted separately.

The [scalar](scalar-reference.json), [extended](extended-reference.json) and
[boundary](boundary-reference.json) source records precede the implementation's
live comparisons. The [date-input probe](date-input-reference.json) records
DD-MON-RR conversion behavior. The [Oracle DECODE documentation](https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/DECODE.html)
describes a 255-component limit. The installed Oracle Free 23.26.3.0.0 accepted
255 and 256 components in both live fixtures; its separate
[component probe](component-reference.json) accepted up to 2,050 with a bind
selector. That larger probe is source-only. These results do not qualify argument
limits or diagnostics on other Oracle versions.

Native PostgreSQL's binary `decode` remains its own function. The
[initial native controls](initial-native-controls.json) found an existing padded
CHAR CASE comparison failure. The fix uses typed bpchar equality in the generic
CASE evaluators, while the compact bound evaluator retains the typed path for
CHAR or unresolved row operands. Text padding and native NULL inequality retain
separate regressions. All **12 native decode/CASE controls** now
[match PostgreSQL 18.4](native-decode-controls.json), together with
[30 native cursor controls](native-cursor-controls.json) and
[62 previous controls](native-prior-controls.json).

The unchanged NEXTBUSINESSDAY source is explicitly
[recreated under version 9](original-definition-recreated.json). The previous
[three-call probe](../oracle-parameterized-cursors-2026-11-02/new-original-execution-probe.json)
matched only its NULL result; all [three calls now match](original-execution-probe.json).
Broader checks initially matched [95 of 96 calls](initial-business-date-comparisons.json).
The seeded holiday exposed a repeated-OPEN failure: COALESCE type inference
memoized cloned argument nodes, whose freed addresses could be reused for
another type. Borrowing the stable routine argument nodes removes that cache
alias. A synthetic cursor regression exercises nullable numeric and character
COALESCE calls across repeated DATE-parameter opens, independently on Oracle and
PostgreSQL. The [36 Oracle cursor cases](cursor-reopen-comparison.json) and 30
native cases match. The unchanged original function now matches
[96/96 expanded date calls](business-date-comparisons.json): both seed clients,
an absent client and NULL client, the seeded holiday and neighboring dates,
weekends, leap day, year boundaries, fractional input seconds and NULL dates.
Application values are retained only as row counts and canonical-result digests.

All [596 previous and extended generated Oracle cases](previous-public-rechecks.json)
pass under version 9. Including the 109 DECODE cases gives **705 generated
comparisons**. The [37 previous original calls](previous-original-comparisons.json)
and [21 configuration/baseline calls](previous-configuration-comparisons.json)
also match. The [inventory](original-definition-inventory.json) remains **43/62
accepted definitions**; executing more paths does not increase that count.

Versions 1–8 remain readable with their own compiler semantics. Tests verify all
nine pg_language identities and their pg_proc joins; the upgrade also corrects
missing legacy version-7/8 OID mappings. A version-8 binary rejects six version-9
calls with 0A000 and recovers afterward in a
[separate disposable database](reader-compatibility.json). It never opens the
application database. The storage format is unchanged. The
[validation record](validation.json) distinguishes full checks, the cursor-cache
follow-up, interrupted attempts, the built binary and development execution.

This qualifies bounded character, NUMBER, logical DATE and TIMESTAMP operations
under the recorded fixed default NLS settings. Implicit character-to-TIMESTAMP,
mutable NLS, national/LOB/binary floating types, collations, source column DATE
provenance, broader length/conversion rules and other Oracle versions remain
unqualified. The remaining 19 definitions, all 3,826 required foreign keys,
complete views, application startup and business workflows remain migration
gates. The full [compatibility programme](../../oracle-compatibility-programme.md)
is active; no release or production deployment is established here.
