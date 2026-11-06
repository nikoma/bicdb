# Migration journal publication qualification

Six filesystem regression tests passed for the shared JSON publication helper.
The previous implementation failed five controls: an unrelated fixed staging
sibling was consumed, a staging symlink changed another input, serialization and
replacement errors left staging files, and a permissive umask exposed the new
report. The corrected helper uses exclusive unique staging beside the output,
flushes/fsyncs JSON before replacement and cleans its staging on caught errors.

The complete importer/tool suite ran **170 tests: 166 passed, four skipped, zero
failures**. The four skipped checks require separately configured live Oracle
owners/replay endpoints; they were not run or counted as passing in this batch.
Native tests used the retained executable from
`5e5fecc3ca279f2e3ea3e37af0c244e11bbb69ea`. Generated JDBC provider tests used the
existing pinned adapter JAR and bounded Java heaps. No engine rebuild or accepted
application deployment change was made by this qualification.

[validation.json](validation.json) pins executable/source/log hashes, the exact
skips and memory-guard observations. See the [journal contract](../../oracle-migration-journals.md)
for reproduction and scope. These checks do not establish abrupt-kill cleanup,
power-loss/directory durability, a migration lease, full database/application
acceptance or production qualification.
