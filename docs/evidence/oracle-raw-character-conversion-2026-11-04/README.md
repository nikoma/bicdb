# Implicit RAW character conversion

An Oracle routine that assigns `LOWER(SYS_GUID())` to a `VARCHAR2(32)` buffer
failed because the native BYTEA display prefix made its value 34 characters.
Oracle implicitly converts RAW to hexadecimal characters before applying the
character operation. The binary `SYS_GUID()` result remains 16 bytes and is
generated independently on each evaluation.

The preview expression resolver now identifies the existing random-byte
intrinsic as a proven RAW producer and inserts a typed hexadecimal conversion
for character casts, case conversion, string length, substring, concatenation
and text-pattern operands. It uses operand provenance rather than inspecting
text prefixes. Ordinary character values beginning with `\x` retain that text;
physical BYTEA columns are not globally reclassified as RAW or BLOB.

The materializer's metadata now agrees with scalar projection metadata for the
BYTEA-producing random-byte, character-set conversion, digest and HMAC
intrinsics. Named PostgreSQL Parse/Describe consequently preserves binary
results instead of advertising text.

## Independent references

The reproducible [fixture](../../../examples/oracle/reference/raw_character_conversion.py)
uses nonce routines and no application objects. Both references satisfy their
own independent expectations:

| Prepared-query cases | Previous binary | Candidate |
| --- | ---: | ---: |
| Oracle 23.26.3.0.0 | 3/13 | 13/13 |
| PostgreSQL 18.4 with pgcrypto | 8/13 | 13/13 |

[Baseline](baseline.json) and [candidate comparison](comparison.json) retain
individual results, reference versions, fixture hash and binary hash. Random
values are checked by binary type, length, format and volatility rather than
requiring identical random bytes from different servers. Nonce routines and
the disposable reference container were removed.

The Oracle cases include RAW bytes, implicit case conversion, character casts,
length variants, substring, trim, regular-expression replacement, concatenation,
volatility, ordinary prefixed text and a stored routine with an unchanged
32-character local buffer. PostgreSQL controls independently cover binary
metadata, native text display, encoding, lengths, empty values and nulls.

## Scope

This is a correction to the existing preview character-conversion capability.
The transient compiler kind adds no serialized schema or routine-IR fields,
and routine source does not need an application-specific rewrite.

These checks do not qualify general RAW declarations/columns, all derived-value
lineage, RAW bind inference, explicit `TO_CHAR(RAW)`, general LOB conversion,
performance, or full application compatibility. Application restart and Trial
Balance reporting need their own runtime evidence. The overall Oracle
compatibility programme remains incomplete.

## Local validation

The [validation record](validation.json) identifies the tested source commit and
binary hash: 600 SQL tests passed (three existing benchmark ignores), 363
PostgreSQL-driver tests passed (two existing external interoperability ignores),
and 30 migration-tool Python tests passed. Formatting checks passed. Ignored
checks were not executed; no failing checks were suppressed.
