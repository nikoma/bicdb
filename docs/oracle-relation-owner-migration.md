# Schema and relation-owner migration

Importing objects as a migration administrator does not make them owned by the
application login. Selecting the exported Oracle schema does not grant access
to it. Map the schema and its exported tables/views explicitly before testing
the application as its intended target role.

Provision the target role and its authentication through your deployment's
normal administration process. The migration tool requires an existing
non-superuser role and uses its exact registered `pg_roles.rolname`. It does
not create logins, set passwords, grant roles, or change unrelated objects.
Review the target role's existing grants and memberships separately; checking
`rolsuper` is not a qualification of its entire privilege closure.

With target data and schema writers quiesced, inspect the mapping first:

```sh
python3 examples/oracle/idempiere/relation_owners.py \
  --snapshot /private/application-export \
  --target-owner app_owner --writers-quiesced --check-only \
  --report /private/owners-preflight.json
```

Set `BICDB_PG_DSN` privately to the migration connection. Use a separate report
for apply:

```sh
python3 examples/oracle/idempiere/relation_owners.py \
  --snapshot /private/application-export \
  --target-owner app_owner --writers-quiesced \
  --report /private/owners-apply.json
```

The default profile is `oracle-preview-v1`; `--profile postgres` provides the
native PostgreSQL-profile control path. Both use PostgreSQL transport. Native
role-management and relation-owner DDL retain their PostgreSQL administration
syntax on preview connections; they still execute ordinary privilege and
ownership checks. This does not add Oracle account-management syntax.

The preflight requires the exact exported table/view inventory and kinds. Every
schema or relation owner must already be the migration role or the explicitly
selected target owner. Missing objects, additional tables/views, another owner,
a missing role or a superuser target are refused before ownership changes.
Names are quoted from the manifest, including embedded dots and quotes.

Apply changes the namespace, table and view owners in one transaction and reads
back the complete mapping before commit. Its success report is published after
commit. A later DDL failure rolls back earlier changes. Reports contain metadata
and endpoint identity hashes, without connection strings or row values.

An existing report requires `--resume` with the same snapshot, endpoint, profile,
mode and target role. Resume rechecks the current complete catalog and reapplies
the mapping transaction; it does not trust a previous success flag or skip
objects from a checkpoint. A check-only report cannot be resumed as apply.
Quiescence is an operator assertion; the tool does not acquire a writer lease.

This phase covers the namespace and exported ordinary tables/views. Routine,
package, type, sequence and dependent-index ownership, grants, role inheritance,
definer/invoker rights, application behavior and recovery need separate policies
and qualification. Run source-value, constraint and view-definition checks again
after mapping. Earlier stored-code journals record their original owners and
must not be silently rewritten to hide the intentional ownership change.
