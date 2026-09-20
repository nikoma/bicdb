# Role membership options

Hub's worker provisioning uses memberships that permit explicit `SET ROLE`
without automatically inheriting the worker's privileges:

```sql
GRANT document_worker TO migration_owner WITH INHERIT FALSE, SET TRUE;
```

BicDB stores and enforces `INHERIT` and `SET` per membership. Every link in an
indirect `SET ROLE` path must permit `SET`. Privilege and ownership checks use
only inherited membership paths. `pg_has_role(..., 'MEMBER')` tests membership
independently of either option, while `'USAGE'` and `'SET'` test the corresponding
capability. `pg_auth_members` reports the stored values.

For new memberships, `INHERIT` defaults to the member role's inheritance
attribute, `SET` defaults to true, and `ADMIN` defaults to false. Repeating a
grant without an option preserves its existing value. `OPTION` is a synonym for
`TRUE`. Explicit `FALSE` must not be discarded during provisioning.

Metadata created before per-membership options retains its previous inheritance
behavior until that membership is updated. New grants snapshot the member role's
inheritance default. Unquoted `CURRENT_USER`, `CURRENT_ROLE` and `SESSION_USER`
resolve against the executing session, rather than a fixed bootstrap identity.

These option semantics follow PostgreSQL's
[role grants](https://www.postgresql.org/docs/18/sql-grant.html#SQL-GRANT-DESCRIPTION-ROLES).
This does not claim complete PostgreSQL role-administration compatibility:
`GRANTED BY` and dependent-grant cascading are not implemented by this change.

Function grants also resolve session identity keywords. `WITH GRANT OPTION` is
accepted only when its recipient already has implicit grant authority as an
owner or superuser. Delegating that option to another role remains unsupported
and is rejected; it is never silently converted to an ordinary EXECUTE grant.
Function GRANT/REVOKE paths require the caller's ownership or superuser authority.

Hub's document workers also use `GRANT UPDATE(column, ...) ON table TO role`.
BicDB stores these separately from table grants and checks every assigned column
for UPDATE and ON CONFLICT DO UPDATE. A column grant does not satisfy
`has_table_privilege(..., 'UPDATE')`; `pg_attribute.attacl` exposes the column ACL.
Inherited and PUBLIC grants apply, while ownership and grantor checks remain
required. Column and table renames retain grants; dropping the object removes
them. Grant and revoke changes participate in transaction rollback.

Revoking table-level UPDATE also removes that recipient's column UPDATE grants,
matching PostgreSQL's [REVOKE semantics](https://www.postgresql.org/docs/16/sql-revoke.html).
Revoking a column grant does not remove an independently held table grant.
Column-level SELECT, INSERT and REFERENCES, and column grant options, remain
unsupported and are rejected explicitly.

`DROP ROLE` and `DROP USER` reject a referenced role with SQLSTATE `2BP01`.
Dependencies include table/view/sequence/routine/type/schema/database ownership,
table and column privileges, other object privileges, RLS policy role lists,
default-privilege grantors and grantees, and memberships the role granted between
other roles. Revoke those grants and policies, or reassign/drop the owned objects,
before deleting the role. Memberships to or from the deleted role are removed as
part of deletion. BicDB checks the entire requested role list before deleting
its first role, so a dependency failure does not partially apply a multi-role
DROP. Reusing a deleted role name does not restore revoked privileges.

`CREATE SCHEMA ... AUTHORIZATION role` stores the requested owner, including
the unnamed-schema form. The owner must exist and the caller must be allowed
to set that role; an authorization clause is not permission to assign arbitrary
ownership.
