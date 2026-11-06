# Explicit schema/relation-owner migration and host status

The [relation-owner migration phase](../../oracle-relation-owner-migration.md)
maps an exported namespace and its ordinary tables/views to an explicitly
selected registered non-superuser role. Native PostgreSQL and Oracle-preview
fixtures independently pass ownership readback, data access, duplicate/NULL
preservation, restart and isolation checks. A later DDL failure rolls back the
earlier namespace/table owner changes. Unknown/superuser targets, another owner,
extra relations, an unregistered migration identity and changed resume identity
are refused. Fixture relation/namespace names include embedded dots/quotes.

Two general engine corrections support this phase:

- Qualified `ALTER VIEW … OWNER TO` resolves the same complete relation identity
  as CREATE and reads, preserving the namespace and quoted name. It retains the
  existing current-owner and new-role membership checks.
- Preview connections preserve parsed native role/ownership administration
  commands. This is a PostgreSQL client administration interface; it does not
  globally reserve ROLE, OWNER or LOGIN in Oracle expressions, add Oracle account
  management syntax, or change authorization policy.

`pg_roles.rolsuper` and `pg_user.usesuper` now report the bootstrap administrator's
effective authority, which authorization already enforced. Stored role attributes
and authorization checks are unchanged. The tool discovers its registered
effective identity through the native PostgreSQL catalog boundary. These catalog
checks prevent mapping application relations to the implicit administrator.
The same final catalog tests fail on the retained earlier engine in both
profiles, with two expected assertion failures and no unrelated errors.

The host-status correction keeps existing exact pgwire status commands outside
Oracle application-name lowering in both simple and prepared requests. Status
authorization/redaction remains in the existing execution path. Independent
PostgreSQL/preview controls check activity/connections, stats, transaction counts
and quoted application relations with a similar name.

| Local validation | Result |
| --- | --- |
| SQL library | 724 passed, 4 ignored |
| Ownership/DDL authorization suites | 29 passed |
| PostgreSQL protocol suite | 183 passed |
| Oracle HR protocol suite | 48 passed |
| Migration tools | 191 passed, 4 skipped |
| Focused ownership / host-status cases | 13 / 4 passed; subsets of migration tools |

[summary.json](summary.json) pins the immutable source, executable digest,
counts, excluded external tests and guarded resource observations. The four
skips require disposable Oracle reference fixtures not configured in this run;
they are not passing comparisons. No hosted workflow result is claimed. The
source is `51cfb588721113d828e787be0648669b5e441c64`, using the local compatibility
example server rather than a production CLI deployment.

The original pristine application batch exposed access errors before its
business assertions. The full pristine dataset after applying this mapping,
and the original application rerun on that dataset, remain subsequent gates.
This phase does not map routine/package/type/sequence ownership, audit existing
role grants or inheritance, or qualify exact Oracle definer/invoker rights,
authentication, cutover, recovery, the full application or production behavior.
