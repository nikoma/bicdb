# Native activation constraint indexes

Run `node qualify.mjs config.json` with an explicitly pinned native binary:

```json
{"binary":"/absolute/path/bicdb","binarySha256":"SHA256","source":"COMMIT","outputRoot":"/private/evidence"}
```

The source-authored signed public SDK fixture checks that SQL primary-key
indexes exist immediately after offline activation, before any serve. The first
serve must add no deferred indexes. Authenticated exact-Decimal writes, role and
tenant denials, a genuinely failed populated upgrade after index creation,
compensating rollback, SIGKILL restart and cold-copy recovery use the actual
native deployment target. Set `expectActivationIndexes:false` only to reproduce
the missing activation indexes on an older binary; that run does not qualify a
fix. Real ERP and Hub workflows require their separate integration evidence.
