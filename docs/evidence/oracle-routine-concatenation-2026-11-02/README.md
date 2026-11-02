# Typed Oracle routine concatenation

New preview routine definitions use compiler version 7, language OID 999406.
The implementation lowers concatenation and one-argument `TO_CHAR` using
resolved character, NUMBER and logical DATE types. It preserves exact numeric
values, character padding and single evaluation of operands. Native PostgreSQL
and versions 1–6 retain their concatenation and caller-result contracts.

The [source expectations](source-expectations.json) were measured independently
on Oracle Free 23.26.3.0.0. All 76 [live comparisons](live-comparison.json) match
through BicDB's PostgreSQL transport. The
[public fixture](../../../examples/oracle/reference/routine_concat.py) covers
NULL/empty operands, Unicode, scoped variables, padded CHAR, sequence evaluation,
numeric widths, default DATE text and caller-dependent byte limits. The runner
checks dot-decimal formatting, English DD-MON-RR dates, Gregorian calendar, BYTE
length semantics and AL32UTF8 before creating its synthetic objects.

SQL implicit NUMBER text has a measured 40-character field; procedural text has
100 characters. Scientific formatting fills the field when plain text would
exceed it, including exponent and sign. The [SQL](sql-number-width-reference.json)
and [procedural](procedural-number-width-reference.json) reference probes record
these differences. [Default TO_CHAR](default-to-char-reference.json) shares the
procedural conversion when called procedurally. Changing these to one common
format would change observable Oracle results.

The tested SQL concatenation limit is 4,000 bytes; procedural concatenation is
bounded at 32,767 bytes. A character-valued function returning to SQL must also
respect its SQL result bound. The [initial source fixture](initial-source-expectations.json)
incorrectly expected a 5,000-byte function result to return to SQL; Oracle raised
ORA-06502. The corrected fixture retains that error case and adds a procedural
caller that successfully measures the 5,000-byte internal value. Nested SQL
callers still fail. The [initial live run](initial-live-comparison.json) exposed
missing sequence result-type inference; the final fixture verifies NEXTVAL is
evaluated once. [Character probes](character-size-reference.json) also record
that the reference accepts a 2,400-character CHAR concatenation; a presumed
universal 2,000-character limit was not implemented.

The [native PostgreSQL controls](native-contracts.json) match all 62 cases against
PostgreSQL 18.4, comparing result values, type OIDs and error SQLSTATEs. Their
[public runner](../../../examples/oracle/reference/postgres_concat.py) includes
48 numeric masks/values plus an exact integer above the floating-point precision
boundary. The [initial controls](initial-native-contracts.json) matched 8/11;
the [previous binary](previous-native-contracts.json) had the same three gaps.
General fixes now return text metadata for text substring/overlay, reject the
invalid evaluated `substr(bit, …)` overload and implement exact numeric `9` masks
with optional `FM`, including display rounding to zero. Other numeric masks
remain unsupported. The [source-only numeric fixture](native-number-format-reference.json)
protects the measured formatting independently in Rust tests. An expanded check
also exposed a negative sign surviving rounding to zero; the final controls
include and pass those cases.

The [original definition inventory](original-definition-inventory.json) accepts
42 of 62 unchanged source bodies, up from 35. The seven newly accepted names are
DBA_CONSTRAINTCMD, DBA_DISPLAYTYPE, GENERATE_UUID, INVOICEOPEN,
INVOICEOPENTODATE, INVOICEWRITEOFF and PAYMENTTERMDUEDAYS. Only the explicit
destination namespace is adapted. This is catalog acceptance: GENERATE_UUID's
RAW/SYS_GUID dependency, for example, is not execution-qualified.

The [additional original calls](original-routine-comparisons.json) match 37/37:
29 display-type cases and eight selected payment-term cases. These do not qualify
all fixed-month payment branches. The [previous checks](previous-original-routine-rechecks.json)
still match 21/21, preserving the existing version-6 GET_SYSCONFIG definition.
Application rows are published only as counts and digests. The
[previous public routine suites](previous-public-routine-rechecks.json) match
266/266 under compiler version 7.

The [reader check](reader-compatibility.json) uses a separate disposable database.
The immediately preceding version-6 reader rejects all three version-7 calls
with 0A000 and remains usable afterward. It never opens the application database.
The storage format is unchanged; routine capability markers fence older code.
The [validation record](validation.json) identifies the source, development
binary, local checks and memory guard separately from releases and deployment.

Parameterized cursors, temporal column provenance, procedure compilation,
packages, autonomous transactions and hierarchical queries still block original
definitions. All 3,826 required foreign keys remain outside the imported set;
transaction timing and validation rules need implementation. Full application
startup and business workflows remain unqualified. Broader NLS, national types,
LOBs, binary floating point, BC dates, arbitrary standalone SQL conversions and
complete Oracle diagnostics are outside this bounded contract. The full
[compatibility programme](../../oracle-compatibility-programme.md) remains active.
