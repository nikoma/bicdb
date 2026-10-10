# Database branches

A branch is a complete, independent copy of a database at an exact commit,
taken while the source keeps serving. Use one to try a migration, reproduce a
bug against real data, or give a feature its own database, then compare it
with where it came from and throw it away.

Branches are databases of a cluster directory (`bicdb serve --cluster`).

## SQL

```sql
-- Branch the database "bicdb" (PostgreSQL's TEMPLATE syntax).
CREATE DATABASE feature_x TEMPLATE bicdb;

-- Connect to it like any database: psql -d feature_x ...

SHOW BRANCHES;
--  database  | parent | branched_at_commit |      branched_at       | owner
-- -----------+--------+--------------------+------------------------+-------
--  bicdb     |        |                    |                        | bicdb
--  feature_x | bicdb  |                 42 | 2026-10-10 16:13:47+00 | bicdb

-- What changed, per table: on the branch against its parent...
SHOW BRANCH DIFF feature_x;
--  table | status  | added | removed | changed
-- -------+---------+-------+---------+---------
--  items | changed |     1 |       2 |       1

-- ...or against any database, record by record.
SHOW BRANCH CHANGES feature_x AGAINST bicdb LIMIT 100;
--  table | id | change
-- -------+----+---------
--  items | 2  | changed
--  items | 3  | removed

DROP DATABASE feature_x;
```

`TEMPLATE template0` and `TEMPLATE template1` keep their PostgreSQL meaning:
an empty database.

## Command line

With no server running on the cluster directory:

```bash
bicdb branch create /srv/cluster feature_x --from bicdb
bicdb branch list /srv/cluster
bicdb branch diff /srv/cluster feature_x            # per table
bicdb branch diff /srv/cluster feature_x --changes  # per record
bicdb branch drop /srv/cluster feature_x
```

## Semantics

- **Exact.** The branch holds every commit of the source up to
  `branched_at_commit` and nothing after it. Commits on the source pause while
  the files are copied; on copy-on-write filesystems (XFS with reflink, btrfs)
  the copy shares blocks with the source and takes moments regardless of size.
  On other filesystems the files are copied in full.
- **Independent.** After the copy, the branch and the source share nothing:
  writes, schema changes, roles and settings of one never reach the other.
- **Diffs** compare records by primary key across every table of the two
  databases, streaming both sides in key order, so memory does not grow with
  table size. `added` means only on the branch, `removed` only on the base,
  `changed` on both with different contents.
- **Permissions.** Branching requires owning the source database (or being a
  superuser) and the right to create databases. Diffs reveal record keys, so
  they require owning both databases.
- **Dropping** refuses the default database, the database of the current
  connection, and any database that still has connections.
