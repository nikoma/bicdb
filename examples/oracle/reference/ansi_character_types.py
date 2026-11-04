"""ANSI varying-character spellings in casts, declarations and stored views.

This bounded fixture compares values and transport families. It does not qualify
overlength casts, every length-unit spelling or all character-set semantics.
"""
import uuid

from join_wildcards import canonical, family
from row_lock_waits import main as comparison_main


def observe(factory):
    nonce = uuid.uuid4().hex[:12].upper()
    values, identifiers, view = ("BICDB_AC_" + nonce + suffix for suffix in ("T", "I", "V"))
    report = dict(cases=[], cleanup_errors=[])
    created = []
    with factory() as conn:
        conn.autocommit = True
        oracle = type(conn).__module__.startswith("oracledb")
        oracle_semantics = oracle or conn.info.parameter_status("bicdb_sql_profile") in (
            "oracle-preview-v1", "oracle-hr-v1")
        report["server_version"] = conn.version if oracle else conn.info.server_version

        def execute(sql):
            with conn.cursor() as cursor:
                cursor.execute(sql)
                if not cursor.description:
                    return None
                return dict(columns=[column.name for column in cursor.description],
                            types=[family(column.type_code) for column in cursor.description],
                            rows=[[canonical(value) for value in row] for row in cursor.fetchall()])

        def check(name, sql, columns, types, rows):
            row = dict(id=name, sql=sql, expected_columns=columns, expected_types=types, expected_rows=rows)
            try:
                result = execute(sql)
                row.update(result, qualified=[column.lower() for column in result["columns"]] == columns
                           and result["types"] == types and result["rows"] == rows)
            except Exception as exc:
                row.update(qualified=False, exception=type(exc).__name__, sqlstate=getattr(exc, "sqlstate", None), detail=str(exc)[:500])
            report["cases"].append(row)

        try:
            suffix = " FROM DUAL" if oracle_semantics else ""
            for ordinal, kind in enumerate(["CHARACTER VARYING(6)", "CHAR VARYING(6)", "VARCHAR(6)"]):
                check("cast_" + str(ordinal), f"SELECT CAST('en_US' AS {kind}) AS label" + suffix,
                      ["label"], ["text"], [["en_US"]])
            try:
                execute(f"CREATE TABLE {values}(first_value CHARACTER VARYING(6),second_value CHAR VARYING(6))")
                created.append(("TABLE", values))
                execute(f"INSERT INTO {values} VALUES('en_US','é')")
            except Exception as exc:
                report["table_setup_error"] = dict(exception=type(exc).__name__, detail=str(exc)[:500])
            check("declared_values", f"SELECT * FROM {values}", ["first_value", "second_value"], ["text", "text"], [["en_US", "é"]])
            try:
                execute(f"CREATE VIEW {view} AS SELECT CAST(second_value AS CHARACTER VARYING(6)) AS label FROM {values}")
                created.append(("VIEW", view))
            except Exception as exc:
                report["view_setup_error"] = dict(exception=type(exc).__name__, detail=str(exc)[:500])
            check("stored_view", f"SELECT * FROM {view}", ["label"], ["text"], [["é"]])
            first, second = ("CHARACTER", "VARYING") if oracle_semantics else ("character", "varying")
            execute(f'CREATE TABLE {identifiers}("{first}" INTEGER,"{second}" INTEGER)')
            created.append(("TABLE", identifiers))
            execute(f"INSERT INTO {identifiers} VALUES(7,9)")
            projection = "character varying" if oracle_semantics else '"character" AS "varying"'
            check("identifier_alias_pair", f"SELECT {projection} FROM {identifiers}", ["varying"], ["number"], [[canonical(7)]])
        finally:
            for kind, name in reversed(created):
                try:
                    execute(f"DROP {kind} {name}")
                except Exception as exc:
                    report["cleanup_errors"].append(dict(kind=kind, exception=type(exc).__name__))
    return report


def run(source_factory, target_factory):
    source, target = observe(source_factory), observe(target_factory)
    cases = [dict(id=a["id"], source=a, bicdb=b, source_qualified=a["qualified"],
                  matched=a["qualified"] and b["qualified"])
             for a, b in zip(source["cases"], target["cases"], strict=True)]
    cleanup = source["cleanup_errors"] + target["cleanup_errors"]
    return dict(format=1, application_qualified=False, reference_version=source["server_version"],
                cases=cases, total=len(cases), matched=sum(case["matched"] for case in cases),
                cleanup_errors=cleanup, source_qualified=all(case["source_qualified"] for case in cases),
                success=len(cases) == 6 and all(case["matched"] for case in cases) and not cleanup)


def qualified(row):
    return bool(row.get("qualified", False))


if __name__ == "__main__":
    raise SystemExit(comparison_main(runner=run, description=__doc__))
