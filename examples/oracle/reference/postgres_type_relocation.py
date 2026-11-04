"""Native PostgreSQL function signatures across user-type moves and rollback.

Nonce schemas/functions are removed. A function returning an old type-name
literal independently checks that moving typed declarations preserves body data.
"""
import uuid

from row_lock_waits import main as comparison_main


def observe(factory, nonce):
    source, target = "move_from_" + nonce, "move_to_" + nonce
    identity, literal = "move_identity_" + nonce, "move_literal_" + nonce
    old, new = source + ".state", target + ".renamed_state"
    report = dict(cases=[], cleanup_errors=[])
    with factory() as conn:
        conn.autocommit = True
        report["server_version"] = conn.info.server_version

        def execute(sql):
            with conn.cursor() as cursor:
                cursor.execute(sql, prepare=False)
                return cursor.fetchall() if cursor.description else None

        def check(name, sql, expected):
            case = dict(id=name, expected=expected)
            try:
                case["actual"] = [list(row) for row in execute(sql)]
                case["qualified"] = case["actual"] == expected
            except Exception as exc:
                case.update(qualified=False, exception=type(exc).__name__, sqlstate=getattr(exc, "sqlstate", None))
            report["cases"].append(case)

        metadata = ("SELECT pg_get_function_arguments(oid),pg_get_function_result(oid) "
                    "FROM pg_proc WHERE proname='" + identity + "'")
        try:
            execute("CREATE SCHEMA " + source)
            execute("CREATE SCHEMA " + target)
            execute("CREATE TYPE " + old + " AS ENUM ('ready')")
            execute(f"CREATE FUNCTION public.{identity}(value {old}) RETURNS {old} LANGUAGE sql AS 'SELECT value'")
            execute(f"CREATE FUNCTION public.{literal}(value {old}) RETURNS text LANGUAGE sql AS $$SELECT '{old}'$$")
            check("initial_signature", metadata, [["value " + old, old]])
            execute("BEGIN")
            execute(f"ALTER TYPE {old} SET SCHEMA {target}")
            execute(f"ALTER TYPE {target}.state RENAME TO renamed_state")
            check("signature_in_transaction", metadata, [["value " + new, new]])
            execute("ROLLBACK")
            check("signature_after_rollback", metadata, [["value " + old, old]])
            execute(f"ALTER TYPE {old} SET SCHEMA {target}")
            execute(f"ALTER TYPE {target}.state RENAME TO renamed_state")
            check("committed_signature", metadata, [["value " + new, new]])
            check("relocated_argument_and_return", f"SELECT public.{identity}('ready'::{new})", [["ready"]])
            check("body_literal_preserved", f"SELECT public.{literal}('ready'::{new})", [[old]])
            with factory() as reopened, reopened.cursor() as cursor:
                cursor.execute(metadata, prepare=True)
                actual = [list(row) for row in cursor.fetchall()]
                report["cases"].append(dict(id="signature_after_reconnect", actual=actual,
                                            expected=[["value " + new, new]], qualified=actual == [["value " + new, new]]))
        finally:
            for sql in ["ROLLBACK", f"DROP FUNCTION IF EXISTS public.{identity}",
                        f"DROP FUNCTION IF EXISTS public.{literal}",
                        f"DROP SCHEMA IF EXISTS {source} CASCADE", f"DROP SCHEMA IF EXISTS {target} CASCADE"]:
                try:
                    execute(sql)
                except Exception as exc:
                    report["cleanup_errors"].append(dict(exception=type(exc).__name__, sqlstate=getattr(exc, "sqlstate", None)))
    return report


def run(source_factory, target_factory):
    nonce = uuid.uuid4().hex[:10]
    source, target = observe(source_factory, nonce), observe(target_factory, nonce)
    cases = [dict(id=a["id"], source=a, bicdb=b, source_qualified=a["qualified"],
                  matched=a["qualified"] and b["qualified"])
             for a, b in zip(source["cases"], target["cases"], strict=True)]
    cleanup = source["cleanup_errors"] + target["cleanup_errors"]
    return dict(format=1, application_qualified=False, reference_version=source["server_version"], cases=cases,
                total=len(cases), matched=sum(case["matched"] for case in cases), cleanup_errors=cleanup,
                source_qualified=all(case["source_qualified"] for case in cases),
                success=len(cases) == 7 and all(case["matched"] for case in cases) and not cleanup)


if __name__ == "__main__":
    raise SystemExit(comparison_main(runner=run, description=__doc__, references=("postgres",)))
