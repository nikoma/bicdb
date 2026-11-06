# Native storage catalog and numeric expression checkpoint

**Result: bounded paged-store inventory, dictionary resolution and focused
numeric expression controls pass. Full application acceptance remains open.**

Qualified source: `675973280789ef2b414c935d26054178c04a88b5`. The allocator/core
implementation is unchanged from the separately tested page baseline
`aa5d3634afd2d4212cf02be2cbe0ca22913e86fa`.

The implementation maps actual reusable logical page runs to a restricted native
relation and Oracle-preview `DBA_FREE_SPACE`. Native grants, effective-role
checks, checksum/corruption handling, complete-inventory admission and cooperative
cancellation remain authoritative. Memory-backed storage is explicitly
unsupported. See the [storage contract](../../oracle-storage-catalog-contract.md)
for mapping, limits, operator grants and qualification gaps.

Numeric result carriers match their declared descriptors. Uppercase catalog and
CTE labels retain computed numeric types. Native maintenance keeps its superuser
gate through the preview, and dotted setting resets decode their quoted forms.
Scaled numeric ROUND preserves exact decimal literals and finite binary operand
values. Directly parsed quoted local calls bind before canonical lowering;
legacy unquoted routine behavior remains covered by independent regressions.
See the [ROUND contract](../../oracle-numeric-round.md) for bounded scope.

| Check | Result |
| --- | --- |
| Page-manager allocator baseline | 35 passed |
| SQL library, qualified source | 728 passed; 4 existing ignored tests |
| Authorization and JSON integration | 44 passed |
| PostgreSQL protocol | 183 passed |
| Oracle HR | 48 passed |
| Migration tools | 212 passed; 4 external Oracle fixtures skipped |
| Focused storage and ROUND checks | 6 + 2 passed; included in the tool-suite count |
| Offline storage-copy API | 2 passed |
| Development server and CLI | Built; binary hashes retained |

The skipped external fixtures cover separate source/export cases. They are not
counted as passed. Earlier failed candidate runs and the interrupted memory-
pressure build remain retained privately; this table reports completed checks
on the stated source, with the allocator baseline identified separately.

Read-only Oracle 23.26.3 controls, fetched as exact decimals, match 15 native
ROUND expressions, including fractional precisions, large exact literals and
binary floating-point ties. Native result OIDs are numeric. A paged fixture runs
the monitoring aggregate, with fractional free megabytes and its two-place
fragmentation value matching independent calculations. This does not qualify
the application's original background job on its existing memory-backed data.

The final guarded validation exits zero, uses one compiler job with incremental
compilation disabled, and retains the configured memory headroom. Full
repository Rust formatting and whitespace checks pass after formatting two
pre-existing row-type inference closures. That subsequent formatting changes no
expression behavior; the binaries identify the qualified source above.

[summary.json](summary.json) records scoped counts, hashes, reference values and
resource observations. The CLI binary is a local build, not a published release
or verified application deployment. The original foundation batch remains 89/89;
the original business batch remains 63/64 with its recorded ordering-portability
failure. Remaining application tests, full UI/workflows, exact Oracle metadata,
source security, recovery and production qualification remain open.
