# BLOB declaration provenance and hexadecimal RAW conversion

This checkpoint retains durable Oracle BLOB column provenance and adds
`HEXTORAW` character-to-binary conversion. It is a prerequisite for LOB locator
work, not an implementation of complete LOB semantics or application acceptance.

Preview BLOB columns keep a logical modifier with the native binary payload and
PostgreSQL wire codec. Explicit BYTEA columns remain unmarked. Existing imported
BYTEA columns are not silently reclassified. New BLOB declarations upgrade the
preview feature marker to format six, preventing older readers from erasing the
new logical type. Other feature saves preserve the upgraded marker.

`HEXTORAW` validates hexadecimal characters, pads an odd digit count with a
leading zero and distinguishes NULL/empty input from binary payload. Invalid
characters, whitespace and binary prefixes report `ORA-01465` / SQLSTATE `22018`.
Type-aware default conversion distinguishes numeric `1.0` from text `'1.0'`.
Stored routines use contract 28; contract 27 retains its previous catalog OID
and rules. Native PostgreSQL binary input and authored `hextoraw` functions keep
their independent paths.

## Executed unit and source checks

- [209 Oracle unit regressions](unit-results.json): passed, including five BLOB
  declaration checks and four RAW conversion/bind/contract checks.
- [24 Oracle source controls](source-hex-results.json): passed for character,
  numeric, national-character, padded, NULL, invalid and bound input cases.

Earlier local runs exposed an unimplemented HEX conversion dependency, missing
binary result inference and incorrect new test assumptions about column position
and the two-stage SQL front end. Those failures were corrected before the passing
run. An owned compilation was interrupted to update a future-version rejection
assertion after adding the new registered contract; it was not counted as a pass.

## Remaining work

`EMPTY_BLOB()`, persistent locator identities, locking and mutation, temporary
LOB copy-on-write behavior, rollback rules, logical BLOB provenance through
expressions/views and complete LOB APIs remain open. Oracle rejects BLOB operands
to `HEXTORAW` with ORA-22849; the RAW conversion controls above do not qualify
that separate type family. General RAW size limits and other implicit conversion
families require further qualification.

The accepted running application is separate and has not been replaced by this
checkpoint. Its previous 19/20 selected PO test result belongs to the earlier
backend; this evidence does not count that result as a new application rerun.
No full Oracle conformance or completed compatibility programme is claimed.

## Combined source and protocol validation

Combined source `a72754c06be496c67b3ed23edcea9e0a8eeaaf2a` includes main through
`a34a568`. [Validation results](validation-results.json) record the complete local
batch: 209 Oracle unit tests, seven binary-storage tests, two ordinary-stack
regressions, seven routine-privilege tests, 45 Oracle protocol tests and 174
general protocol tests. Workspace/all-target compilation, licensing and open-core
boundary checks passed. The guard observed at least 9.96 GiB available host RAM.

All [24 native Oracle comparisons](hex-reference-results.json) match, including
bound character, NUMBER and RAW inputs. The first protocol comparison matched
20/24: an early typed-lowering optimization skipped HEXTORAW, and binary bind
classification also needed an explicit RAW rule. Both paths now have regression
coverage. Earlier routine contracts retain their previous rules.

The [seven BLOB protocol checks](blob-protocol-results.json) preserve binary
payloads and NULL across reopen, keep native BYTEA distinct, preserve the marker
when other feature metadata is saved, and verify rejection by the previous
server followed by successful reopen with the new server. All four disposable
servers exited cleanly. Backend SHA-256:
`2d856aa532033ecd18342cb42d9f55a7a60fd71c81276a39ea6782f12a7b28d5`.
The frozen local development server is separate from a published release or a
verified application deployment. No workflow was added for these checks.

The unchanged reference JDBC adapter also passes [112 checks](jdbc-protocol-results.json)
on this backend, across extended, simple and extended-cache-everything modes and
the default PostgreSQL profile rejection control. Its artifact SHA-256 remains
`9102b2ad40e5f9f1366e9d1a126556ec240c5e8423df5c30e02590506a3333ac`.
