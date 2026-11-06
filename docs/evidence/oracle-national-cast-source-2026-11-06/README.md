# Oracle national-character CAST source observations

Read-only Oracle 23.26.3 observations distinguish bounded SQL CAST from national
column assignment and PL/SQL buffer assignment. This checkpoint records 24
predicate cases and 15 additional contexts. Thirteen contexts use DUMP(...,1016)
to inspect AL16UTF16 bytes without decoding the national value as client text.
The reference database uses AL32UTF8 and AL16UTF16; session NLS settings and the
reference driver version/mode are included in the observations.

Observed behavior:

- String CAST of 'abc' to NVARCHAR2(2) or NCHAR(2) yields 'ab'. Trailing spaces
  are also truncated to the bound. NCHAR pads shorter strings with spaces.
- Casting the supplementary character U+1F600 to either national type with
  width one yields the lone high-surrogate code unit D83D. Width two preserves
  the complete D83D DE00 pair. Lossless code-unit representation matters.
- Forcing the value of CAST(123 AS NVARCHAR2(2)) raises ORA-25137. Separately,
  the observed string-overflow IS NOT NULL predicates succeed; nullness alone
  does not establish the resulting value. This corpus does not compare numeric
  overflow across both expression contexts.
- Width zero raises ORA-01723. Under the observed STANDARD limits, NVARCHAR2(2001)
  and NCHAR(1001) raise ORA-00910, including predicates over an empty input.
- Numeric, RAW, DATE and TIMESTAMP conversions have separate value observations.
  Date/time strings depend on the recorded session formats; these samples do
  not establish behavior under every NLS setting.

The direct-value contract was also attempted. Python's thin reference driver
raised a UTF-16 UnicodeDecodeError on a bounded supplementary-character result;
that run did not complete its corpus. The completed raw-unit contract below is
separate evidence, not a successful direct-text transport test. No driver error
is relabelled as an Oracle semantic error.

BicDB runtime support is not qualified by these source observations. Remaining
work includes lossless national code-unit values, distinct CAST/assignment rules,
expression-context behavior, durable representation, and a defined and tested
PostgreSQL-driver codec. Full Oracle compatibility, original application
acceptance and production qualification remain open.

The observer's unit tests exercise report construction, contract separation and
error classification with a fake driver. They are not Oracle integration tests.
The actual source report is [observations.json](observations.json); source and
report hashes are in [summary.json](summary.json). No original application
objects were modified, and the reference container was restored to stopped.

To repeat against an authorized reference database with an existing private
connection configuration:

```sh
python3 examples/oracle/reference/like_escape_contract.py \
  --oracle-config /private/reference.json --contract national-cast-units \
  --output /private/national-cast-units.json
```

Use `--contract national-casts` for the distinct direct-value contract. A raw-unit
observation must never be presented as successful direct-text decoding.
