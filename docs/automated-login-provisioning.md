# Automated pgwire login provisioning

The CLI accepts a named environment source when creating a pgwire login:

```bash
bicdb user create application_login --path /data/instance \
  --password-env INSTANCE_LOGIN_PASSWORD \
  --user-id application-service --tenant customer-organization
```

The deployment worker supplies `INSTANCE_LOGIN_PASSWORD` through its secret
manager environment. The secret is not placed in process arguments or CLI
output. `--password` remains supported for compatibility, but the two options
are mutually exclusive. A missing, non-Unicode, or empty environment value
fails before storage creation. Environment secrets remain readable to an
appropriately privileged host operator; this is ordinary-server automation,
not the Cell KMS/attestation boundary.

SQL roles and pgwire login authentication use separate catalogs. Creating a SQL
role through offline `bicdb sql` does not create a pgwire password verifier.
Provision SQL privileges and the identity-bound login deliberately, then test
the resulting workload login over authenticated TLS. Do not treat a management
token or a SQL role name as proof of workload access.

Run `cargo test -p bicdb-cli --test user_password_env --release --locked` for the
CLI boundary tests. TLS connection and restore acceptance belongs in the
deployment adapter's integration suite.
