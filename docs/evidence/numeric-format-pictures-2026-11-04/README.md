# Fixed numeric format pictures

TO_CHAR now renders mandatory and optional digit positions (`0`/`9`), decimal
and group separators (`D`/`G`, `.`/`,`), and `FM` padding through an exact-decimal
formatter. Oracle lowering uses the existing typed numeric helper. Native
PostgreSQL formatting retains its own sign and overflow rules. There are no
application, schema or table-name conditions.

[Independent comparisons](reference-comparisons.json) use
[numeric_format_pictures.py](../../../examples/oracle/reference/numeric_format_pictures.py)
with 120 explicit expectations per reference:

| Reference | Target before | First candidate | Final candidate |
| --- | --- | --- | --- |
| Oracle Free 23.26.3 | 23/120 | 120/120 | 120/120 |
| PostgreSQL 18.4 | 39/120 | 116/120 | 120/120 |

Each source independently meets all 120 expectations. The first candidate's four
PostgreSQL failures are retained: zero rounded into a picture with no integer
positions requires PostgreSQL overflow markers. Oracle keeps the minus sign for
negative values rounded to zero and fills the entire picture with hashes on
overflow; PostgreSQL suppresses the rounded-zero sign and retains separators
inside its overflow output. Mixed mandatory digits, literal separators, nulls,
rounding across an integer boundary and leading padding are included.

[Validation](validation.json) records 598 SQL unit tests, three existing ignored
tests, 34 PostgreSQL-driver tests and 25 Python tests. The new driver regression
reuses a prepared statement with changing numeric values and format parameters
in both profiles, including nulls and their distinct overflow behavior.
Builds use one job, no incremental compilation or debug information, and monitored
14 GiB start / 8 GiB minimum available-memory gates. Completed test executables
were removed after both batches, reclaiming 1,392,023,136 bytes while retaining
runnable binaries and reusable dependencies.

The final binary was built from `c92dad68bc60184174902acdfedce2422a170420`;
SHA-256 `32d2c0441d23b9a856e2cb45d2d85b263b3272b322b6c88acd2a9b6962eaa009`.
The first candidate was an uncommitted intermediate build; its binary hash is
recorded in the comparison evidence. No stored IR layout, helper signature or
compiler-language identity changed.

Qualification uses Oracle numeric characters `.,` and PostgreSQL
`lc_numeric=en_US.utf8`. Other locales, currency, additional sign or exponent
pictures and malformed-model error codes remain unqualified. At this checkpoint
the candidate has not been deployed to the application. These checks do not
establish complete application coverage, general Oracle compatibility, a
published release or production qualification.
