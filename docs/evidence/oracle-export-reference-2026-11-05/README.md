# Original export replay and independent Oracle orphan counts

The [reference tool](../../oracle-export-reference-replay.md) replays all columns
and values from a saved export into isolated Oracle global temporary tables.
Every table's row multiset is verified before counting relationships. This is
reference data replay, not application/schema migration or native acceptance.

The [validation record](validation-results.json) reports Oracle 23.26.3.0.0:

- **915 tables / 185,471 rows** replayed and verified against the saved export.
- **3,826 foreign keys counted**, including 912 validated and 2,914 not validated.
- **Zero unmatched row references across all relationships** in this dataset.
- All temporary definitions dropped successfully after closing the loading
  session. The report retains no row values or credentials.
- **85 migration-tool tests passed**, with no failures or skips. Ten cover the
  replay tool, including real Oracle LOB/RAW/character/numeric/temporal cases,
  ordered projections, namespace/endpoint fencing and cleanup recovery.

New codecs preserve explicit timestamp offsets and empty LOBs separately from
NULL. The exporter and reference reader share projection/decoding rules. Oracle
array-bind state is isolated from read cursors; empty LOB values use SQL
constructors, while nonempty payloads use LOB locators. The tests include NULL,
empty and nonempty CLOB/BLOB values, Unicode byte-aware character padding,
38-digit numbers, RAW, DATE, microseconds and a nonzero timestamp offset.

An initial direct Oracle check incorrectly fetched zoned timestamps as naive
Python datetimes and stopped at AD_SCHEDULER. This was a reader mismatch, not
proof that source rows changed. A fresh read-only Oracle export with value
encoding version 2 was then compared to the original: **all 915 table row
multisets and the foreign-key declarations match**, with 185,471 rows in each.
The original export was retained; no source rows were repaired or reset.

The full replay first encountered the test reference's 256 MiB temporary-space
limit after 426 verified tables. Cleanup passed. The owned environment's
existing temporary file was allowed bounded automatic growth to 1 GiB, then the
entire replay completed. A private-temporary-table LOB probe had failed with
ORA-14451; global temporary tables passed the LOB fixture. Other intermediate
reference issues (integral RAW catalog strings, array-bind reuse, offset and
empty-LOB codecs) were resolved and covered before the final runs.

The native engine binary used by the Python integration cases is recorded
separately from this Python source change. No native engine rebuild, accepted
application upgrade, GitHub workflow modification or dispatch is claimed.

These zero counts do not upgrade source NOT VALIDATED declarations. Native
relationship installation, target orphan counts, full native value equality,
remaining original business classes and recovery/cutover remain separate gates.
No Oracle conformance, vendor certification or production qualification follows
from this reference result.
