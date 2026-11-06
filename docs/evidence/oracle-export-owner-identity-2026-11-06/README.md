# Source export owner and dictionary identity

The public exporter passed **17 offline checks and two live checks on Oracle
23.26.3.0.0**, with zero failures or skips. [summary.json](summary.json) retains
source and log digests; no application rows, credentials or endpoint information
are published. The frozen executable source was commit `2977ac2f0bebd65a5b04c59b7b6a8e127bfa0309`.

The live owner-identity test uses an empty, quoted mixed-case owner. It creates
one ordinary table and an owned table named `USER_TABLES`, with distinct payloads.
On the exporter's new connection it changes `CURRENT_SCHEMA` to `SYS` before
calling the real exporter. The exporter must recover the authenticated owner,
restore that read schema before its read-only snapshot, read `SYS.USER_*`
dictionaries and fetch exactly both owned tables' values. It must publish the
exact owner spelling in both manifests and keep the observed SCN diagnostic.

The second live test compiles a function/procedure, an object type and body,
a package specification/body and enabled/disabled triggers. It compares captured
source and source line coverage with independent dictionary reads and checks
both trigger states. Both tests removed their owned objects; the supervisor
observed zero remaining objects, dropped the disposable owner, removed its
private credentials and restored the reference container to its stopped state.

See the [operator instructions](../../oracle-stored-source-export.md) for the
fixture configuration and permissions. Run both explicitly configured fixtures:

```sh
ORACLE_EXPORT_IDENTITY_CONFIG=/private/empty-quoted-owner.json \
ORACLE_SOURCE_CAPTURE_CONFIG=/private/empty-quoted-owner.json \
python3 -m unittest discover -s examples/oracle/idempiere \
  -p 'test_export_*.py' -v
```

The mixed-case login must retain the driver's double-quote syntax. An initial
fixture attempt omitted login quotes and failed both live connections with
ORA-01017; it created no test objects, and its owner/container cleanup succeeded.
The successful rerun corrected only the private login configuration and used
the same frozen exporter and test files. Earlier failed logs remain private.

This qualifies source capture, not BicDB privilege behavior, execution of the
captured packages/triggers, migration of arbitrary Oracle schemas, CDC/cutover
or full iDempiere acceptance. Schema writers must remain quiesced for capture;
no DDL lease or source change-stream position is established.
