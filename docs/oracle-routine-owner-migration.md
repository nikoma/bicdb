# Mapping imported routine ownership

After installing trusted exported code, map its functions and procedures to an
existing dedicated role. This is a separate phase from
[relation ownership](oracle-relation-owner-migration.md).

Changing a definer routine's owner changes the identity used to execute its
body. Review both the bodies and the target role's privileges before applying
this phase. The tool does not create roles, grant privileges, or infer source
security policies.

```sh
export BICDB_PG_DSN='host=127.0.0.1 port=5432 dbname=bicdb user=migration_role'
python3 examples/oracle/idempiere/routine_owners.py \
  --snapshot /srv/migration/export \
  --report /srv/migration/routine-owners-check.json \
  --target-owner application_owner --writers-quiesced --check-only
python3 examples/oracle/idempiere/routine_owners.py \
  --snapshot /srv/migration/export \
  --report /srv/migration/routine-owners.json \
  --target-owner application_owner --writers-quiesced
```

The default profile is `oracle-preview-v1`; `--profile postgres` selects the
PostgreSQL frontend. The PostgreSQL driver and native ownership administration
commands are used in either profile. This does not implement Oracle account
administration syntax.

The preflight requires an existing registered non-superuser target, an exact
function/procedure inventory, and owners limited to the registered migration
role and the explicitly mapped role. Extra routines, namespace collisions,
ambiguous folded names, and different routine kinds are refused. The preview
currently stores routines by folded bare name; this tool does not establish
quoted-name, overload, or cross-schema identity parity with Oracle.

All owner changes run in one transaction. Full catalog readback verifies the
mapped owners and unchanged routine identity, body, input types, return type,
and definer mode before commit. A later DDL/readback error rolls back the whole
mapping. The report marks success only after commit. `--check-only` performs
preflight without changing ownership and does not qualify owner mapping.

Use `--resume` with the same snapshot, endpoint, owner, profile, and mode. Resume
rechecks the full current inventory and reapplies the transaction; a journal
alone is never proof of current ownership. Quiesce application and schema
writers for both checks and changes. The flag is an operator assertion, not a
writer lease or a snapshot consistency protocol.

The report deliberately leaves routine execution, source privilege equivalence,
privilege closure, and application qualification false. Test those separately
with the application role. In particular, importing 62 definitions or mapping
62 owners does not establish that 62 routine bodies execute correctly.
