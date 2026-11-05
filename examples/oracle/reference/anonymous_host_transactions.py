"""Compare bound anonymous-block rollback and explicit commit boundaries.

Only nonce-named tables created by this runner are modified. This qualifies
PostgreSQL protocol submission, not JDBC statement splitting or LOB APIs.
"""
import uuid

from row_lock_waits import main as comparison_main


def observe(factory):
    table = "BICDB_HOST_" + uuid.uuid4().hex.upper()
    report = dict(cases=[], cleanup_errors=[])
    created = False
    with factory() as connection:
        native = type(connection).__module__.startswith("oracledb")
        connection.autocommit = False if native else True
        report["server_version"] = connection.version if native else connection.info.server_version

        def execute(sql, parameters=None):
            with connection.cursor() as cursor:
                if parameters is None:
                    cursor.execute(sql)
                else:
                    cursor.execute(sql, parameters)
                return cursor.fetchall() if cursor.description else None

        def check(label, actual, expected):
            report["cases"].append(dict(id=label, actual=actual, qualified=actual == expected))

        def rows():
            return [int(row[0]) for row in execute(f"SELECT key_value FROM {table} ORDER BY key_value")]

        def begin():
            if not native:
                execute("BEGIN")

        def duplicate_block(commit):
            parameter = ":1" if native else "%s"
            block = (f"BEGIN INSERT INTO {table} VALUES({parameter}); "
                     + ("COMMIT; " if commit else "")
                     + f"INSERT INTO {table} VALUES(4); INSERT INTO {table} VALUES(1); END;")
            try:
                execute(block, [3])
            except Exception as error:
                if native:
                    import oracledb
                    if not isinstance(error, oracledb.DatabaseError):
                        raise
                    check("commit_error" if commit else "block_error", error.args[0].code, 1)
                else:
                    import psycopg
                    if not isinstance(error, psycopg.Error):
                        raise
                    check("commit_error" if commit else "block_error", error.sqlstate, "23505")
            else:
                check("commit_error" if commit else "block_error", "unexpected_success", "duplicate")

        try:
            execute(f"CREATE TABLE {table}(key_value INTEGER PRIMARY KEY)")
            created = True
            execute(f"INSERT INTO {table} VALUES(1)")
            connection.commit()
            begin()
            execute(f"INSERT INTO {table} VALUES(2)")
            duplicate_block(False)
            check("only_block_writes_removed", rows(), [1, 2])
            execute(f"INSERT INTO {table} VALUES(5)")
            check("subsequent_write_allowed", rows(), [1, 2, 5])
            connection.rollback()
            check("caller_rollback", rows(), [1])
            begin()
            execute(f"INSERT INTO {table} VALUES(2)")
            duplicate_block(True)
            check("explicit_commit_preserved", rows(), [1, 2, 3])
            begin()
            execute(f"INSERT INTO {table} VALUES(5)")
            check("write_after_committed_block_error", rows(), [1, 2, 3, 5])
            connection.rollback()
            check("later_rollback_preserves_commit", rows(), [1, 2, 3])
        finally:
            connection.rollback()
            if created:
                try:
                    execute(f"DROP TABLE {table}" + (" PURGE" if native else ""))
                except Exception as error:
                    report["cleanup_errors"].append(type(error).__name__)
    return report


def run(source_factory, target_factory):
    source, target = observe(source_factory), observe(target_factory)
    cases = [dict(id=a["id"], source=a, bicdb=b,
                  matched=a["id"] == b["id"] and a["qualified"] and b["qualified"])
             for a, b in zip(source["cases"], target["cases"], strict=True)]
    cleanup = source["cleanup_errors"] + target["cleanup_errors"]
    return dict(format=1, application_qualified=False, cases=cases, total=len(cases),
                matched=sum(item["matched"] for item in cases), cleanup_errors=cleanup,
                source_qualified=all(item["qualified"] for item in source["cases"]),
                success=len(cases) == 8 and all(item["matched"] for item in cases) and not cleanup)


if __name__ == "__main__":
    raise SystemExit(comparison_main(run, __doc__, references=("oracle",)))
