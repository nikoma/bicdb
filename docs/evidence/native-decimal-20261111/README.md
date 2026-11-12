# Exact Decimal predicate qualification

Native 479 evaluated Decimal arithmetic with the application Decimal type, but
compared its result through a repair parser restricted to 18 fractional digits.
A valid result retaining 19–28 digits therefore had no comparison ordering.
This rejected correct bounds and could accept incorrect inequality claims.

The 481 candidate compares complete signed fixed-point values by normalized
integer/fraction digit ordering. It does not round, convert to floating point,
change stored values, or relax the signed invariant. The bounded repair parser
and its separate repair contract remain unchanged.

`comparator-479.json` preserves the old defect: only 3 of 51 correct batches
passed, and deliberately wrong comparisons could commit. On the exact 481
binary identified in `evidence-manifest.json`, all 51 batches containing all six
operators pass and all 306 deliberately wrong batches fail atomically. Signed
zero, trailing-zero equivalence, 19–28 fractional digits, and 96-bit boundaries
use an independent Node BigInt oracle. Sixteen unchanged arithmetic reproducer
cases, SIGKILL/restart, and original-offline cold restore also pass.

The application-specific consolidation evidence includes exact high-precision
translation, reciprocal/margin eliminations, separately authenticated tenants,
authorization negatives, concurrent retries, and cold recovery. It is component
evidence; it does not claim a complete accounting release or human Hub login.

The portable regression rebuilds the preserved source through an official
Carrier compiler and signs both isolated fixtures with one ephemeral test key:

```sh
CARRIER_BINARY=/absolute/path/to/carrier \
BICDB_BINARY=/absolute/path/to/bicdb \
node scripts/qualify-native-decimal-predicates.mjs
```

It retains the full matrix, package/source/binary hashes, and recovery evidence
in the printed temporary directory. The original application reproduction was
also tested separately without changing its previously signed package.

## Existing stores and indexes

This comparator is shared with native Decimal index predicates. If an
operator-created Decimal predicate index was built under the old behavior,
fence writers, retain a complete cold backup, and verify/rebuild it with the
corrected binary before accepting traffic:

```sh
bicdb index verify /closed/database --all --json
bicdb index rebuild /closed/database --all --json
bicdb index verify /closed/database --all --json
```

Investigate any newly exposed duplicate or invalid financial fact; do not delete
facts or remove constraints to make verification pass. Ordinary SQL currently
rejects comparison-based partial indexes on typed NUMERIC columns, so these
results do not claim that unsupported SQL feature. Validating one tenant or one
test store does not establish that every older customer store is clean.
