# Oracle simple case v1 reference

The frozen mapping comes from read-only Oracle 23.26.3 AL32UTF8 UPPER/LOWER
queries over every Unicode scalar (1,112,064 values; surrogates excluded).
There are 1,485 changed uppercase and 1,486 changed lowercase mappings.
The separate validation checks all changed scalars plus ASCII individually,
256 deterministic random strings and fixed context/ligature/Greek/Deseret probes.
All 3,307 validation queries agree. No application rows were read or changed.

The first capture's product-name filter returned no version row because the
vendor now calls this product Oracle AI Database 26ai Free. The separate
vendor-version query records the actual 23.26.3.0.0 version. Database character
sets and session NLS settings are retained. Scripts read the existing local
qualification connection configuration; no password or credential is included.

Reference SHA-256: `76de86121dd9dee1275a954ba60a98966cf0c5d0e5fed5cec6110ac092527350`.
This is a versioned compatibility table, not a dependency on future host
Unicode/locale data. It must not change existing ASCII Upper/Lower index keys.
The implementation and persistence fences must be qualified separately.
