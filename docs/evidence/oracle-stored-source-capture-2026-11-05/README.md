# Live Oracle stored-source export qualification

The public empty-owner fixture passed against **Oracle 23.26.3.0.0**. It created
and compiled a function, procedure, object type and body, package specification
and body, and two triggers. One trigger was disabled before export. The test
verified validity, routine/package answers, exact source line continuity and
exported DDL against Oracle's own dictionary, and ENABLED/DISABLED trigger state.

The [validation record](validation.json) identifies the exporter, fixture and log
digests. All fixture objects were removed, the disposable owner was dropped,
and the owned reference container was restored to its prior stopped state.

A first setup attempt failed before fixture execution; its owner and container
were cleaned up. The dictionary-view grant uses the container-local SYS
administrator. A later fixture run encountered Oracle ORA-01466 immediately
after creating its DDL. The fixture now establishes a readable read-only snapshot
before export, with bounded rollback/retry for that preliminary read only.
The exporter still takes one snapshot and fails rather than mixing partial
exports across retries. The final test completed without skipped assertions.

This qualifies stored-source capture. BicDB execution of package bodies, type
methods, trigger lifecycle and complete application migration remain separate
programme gates. See the [source export contract](../../oracle-stored-source-export.md).
