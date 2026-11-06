# Pristine application view projection metadata

All **160 installed original-source views** passed PostgreSQL Parse/Describe
checks for **11,983 projected column labels and broad type families** on the
pristine schema-assembly target. [summary.json](summary.json) retains source,
binary, verifier and detailed private report digests. The native executable was
built from `5e5fecc3ca279f2e3ea3e37af0c244e11bbb69ea`; the public verifier was
frozen from `ec4794f7505c4de876c464c82aeb31f52e495b57`. No later engine revision is
implied by this result.

| Source family | Projected columns |
| --- | ---: |
| NUMBER | 6,140 |
| CHAR | 2,543 |
| VARCHAR2 | 2,196 |
| DATE | 1,104 |

The verifier uses the unnamed prepared-statement slot and sends no Execute
messages. Exact-number families allow integer/numeric OIDs, excluding approximate
floating point. Character families accept text/character OIDs; DATE requires the
timestamp-without-time-zone OID. This does not establish exact numeric domains,
precision, scale, character width/padding, nullability or returned view values.
View definitions were unchanged before/after this check. Only foreign-key DDL
was progressing in the concurrent migration; complete schema assembly was not
claimed by this early metadata result.

Six focused contract tests passed, with no skips, on the same executable. The
native controls run independently in PostgreSQL and Oracle preview profiles:
a view containing `1/0` describes successfully and then raises `22012` when
executed; replacing its numeric projection with character data is detected as a
family mismatch. This protects the distinction between metadata and execution.

Reproduce against an installed disposable target with view DDL quiesced:

```sh
python3 examples/oracle/idempiere/verify_view_metadata.py \
  --snapshot /private/original-export \
  --pg-dsn-env BICDB_PG_DSN --view-ddl-quiesced \
  --report /private/view-metadata.json

BICDB_ORACLE_SERVER=/private/oracle_server \
python3 -m unittest discover -s examples/oracle/idempiere \
  -p test_view_metadata.py -v
```

Routine execution/privileges, all view values and precise descriptors, completed
foreign keys and source-value reconciliation, the original application tests,
recovery/cutover and production qualification remain separate gates. The
accepted application deployment was not upgraded by this check.
