# PostgreSQL Extensions

`CREATE EXTENSION` records PostgreSQL's trusted contrib extensions (for
example `pgcrypto`, `citext`, `hstore`, `pg_trgm`, `uuid-ossp`) in
`pg_extension`. What each one provides is listed below; an extension that is
not listed here records the catalog entry only.

## pgcrypto

Implemented with PostgreSQL 18's results (verified against a PostgreSQL 18.4
server, see `fixtures/postgres-compat/pgsql-parity/a2.cases`):

- `crypt(password, setting)`, with the algorithm chosen by the setting as
  pgcrypto does: `$2a$`/`$2x$` blowfish, `$1$` md5-crypt, `$5$` sha256crypt,
  `$6$` sha512crypt, `_` extended DES, and traditional DES for any other
  setting (as in PostgreSQL, `$2y$` and `$2b$` therefore fall through to DES).
  Hashes produced by PostgreSQL verify here and the other way round. An
  invalid setting fails with `invalid salt`.
- `gen_salt(type [, rounds])` for `bf` (4-31, default 6), `md5`, `xdes` (odd
  1-16777215, default 725), `des`, `sha256crypt` and `sha512crypt`
  (1000-999999999, default 5000), with pgcrypto's error messages.
- `digest(data, type)` and `hmac(data, key, type)` for `md5`, `sha1`,
  `sha224`, `sha256`, `sha384` and `sha512`; `gen_random_bytes(n)`;
  `gen_random_uuid()`.

Limits:

- Blowfish with a password containing bytes above 0x7f, and sha-crypt with
  non-ASCII input, fail with 0A000: pgcrypto applies a historical
  compatibility transformation to such passwords that is not reproduced.
- The PGP functions (`pgp_sym_encrypt`, `pgp_pub_encrypt`, ...) and
  `encrypt`/`decrypt` are not implemented.
- These functions are available whether or not `CREATE EXTENSION pgcrypto`
  was run; PostgreSQL requires the extension.

## hstore

`CREATE EXTENSION hstore` creates the `hstore` type (in the extension's
schema) and its operators; `DROP EXTENSION hstore` removes them and fails
with 2BP01 while a table column uses the type. Values are kept in
PostgreSQL's canonical text form (one pair per key, ordered by key length
and then bytes, `"k"=>"v"` / `"k"=>NULL`), so they print, compare and
order as in PostgreSQL.

- Operators: `->` (text key), `?`, `?|`, `?&`, `@>`, `<@`, `||`, `-` (text
  key or text[] of keys).
- Functions: `akeys`, `avals`, `hstore(key, value)`, `hstore(text[],
  text[])`, `hstore_to_json`, `hstore_to_jsonb`, `hstore_to_array`.

Limits: the set-returning functions `each`, `skeys`, `svals`, the
functions `exist`, `defined`, `delete`, `slice`, `hstore(record)`,
`populate_record`, the `#=` operator, and GIN/GiST/btree/hash indexes on
hstore columns (refused with "no default operator class") are not
implemented. Syntax-error messages for invalid input differ in wording.

## citext

The `citext` type is not implemented: a column of the type fails with
"type ... does not exist".
