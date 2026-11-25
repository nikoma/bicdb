# PostgreSQL Collations and Custom Range Types

## Collations

Built-in collations: `default`, `"C"`, `"POSIX"`, `ucs_basic` and
`"en-x-icu"`. User collations are created with PostgreSQL syntax:

```sql
CREATE COLLATION de (provider = icu, locale = 'de-DE');
CREATE COLLATION ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false);
CREATE COLLATION bytes FROM "C";
ALTER COLLATION de RENAME TO german;
DROP COLLATION IF EXISTS german;
```

- `provider = icu` takes any ICU locale tag (BCP 47, `ll_CC` accepted and
  canonicalized). The `-u-` keywords `ks` (strength `level1` .. `level4`,
  `identic`), `ka` (`shifted`), `kc`, `kf` and `kn` are honoured.
- `provider = libc` is supported for the byte-order locales `C`, `POSIX` and
  `C.UTF-8`; other libc locales fail with 0A000 (their ordering is
  platform-defined). Nondeterministic libc collations fail with 0A000 as in
  PostgreSQL; `rules = ...` (ICU tailoring) and `provider = builtin` fail
  with 0A000.
- User collations appear in `pg_collation` and resolve in `COLLATE name`
  expressions and column definitions (`text COLLATE de`); comparison
  operators and `ORDER BY` use them, including a column's declared
  collation. DDL is transactional. `DROP COLLATION` of a collation a column
  uses fails with 2BP01; renaming such a collation, `DROP ... CASCADE` of it
  and `ALTER COLLATION ... SET SCHEMA` fail with 0A000.
- Deterministic collations order by the collator and break ties by bytes,
  so equality stays byte equality, as in PostgreSQL.
- Nondeterministic collations (`deterministic = false`) make the collator's
  equality the string equality. BicDB applies them to comparison operators
  (`=`, `<>`, `<`, `<=`, `>`, `>=`) and `ORDER BY`. Every other use (column,
  domain and index definitions, `GROUP BY`, `DISTINCT`, `IN`, joins, window
  partitions, ...) fails with 0A000 instead of silently comparing bytes. A
  case-insensitive lookup is written as `WHERE col = $1 COLLATE ci`; it scans
  the table (no index access path uses nondeterministic equality).

## Custom range types

`CREATE TYPE name AS RANGE (subtype = ...)` accepts the subtypes `int2`,
`int4`, `int8`, `numeric`, `float4`, `float8`, `date`, `timestamp`,
`timestamptz`, `time`, `timetz`, `interval` and `text`, with their
PostgreSQL default operator classes and difference functions (`float8mi`,
`float4mi`, `time_mi_time`, `interval_mi`) recorded in `pg_range`. The
automatic multirange type, range operators and functions, ordering and GiST
indexes work as for built-in ranges. Text ranges compare bounds in byte
order (the `default` collation).
