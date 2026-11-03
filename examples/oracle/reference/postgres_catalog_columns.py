"""Compare logical catalog names and schema filters with real PostgreSQL."""
from compare import canonical


def run(reference, target):
    schemas = ["bicdb_cat_cols_a", "bicdb_cat_cols_b"]
    report = dict(format=1,postgres_version_num=reference.info.server_version,cases=[],cleanup_errors=[],success=False,
                  scope="Owner/admin catalog name and schema-filter parity; restricted-role catalog visibility is separate qualification")
    created=[]
    def query(conn,sql):
        with conn.cursor() as cur:
            cur.execute(sql)
            return [[canonical(v) for v in row] for row in cur.fetchall()] if cur.description else []
    try:
        for backend, conn in enumerate((reference,target)):
            for schema in schemas:
                query(conn,"CREATE SCHEMA "+schema);created.append((backend,schema))
            for schema in schemas:
                query(conn,f"CREATE TABLE {schema}.catalog_fixture(id INTEGER,label VARCHAR(3))")
            query(conn,f"CREATE VIEW {schemas[0]}.catalog_projection AS SELECT label FROM {schemas[0]}.catalog_fixture")
        projection="SELECT table_schema,table_name,column_name,data_type,character_maximum_length FROM information_schema.columns"
        plans=[("both_schemas", "table_name='catalog_fixture'"),
               ("first_schema",f"table_schema='{schemas[0]}' AND table_name='catalog_fixture'"),
               ("second_schema",f"table_schema='{schemas[1]}' AND table_name='catalog_fixture'"),
               ("view",f"table_schema='{schemas[0]}' AND table_name='catalog_projection'"),
               ("missing_schema", "table_schema='bicdb_cat_cols_missing' AND table_name='catalog_fixture'"),
               ("column_filter",f"table_schema='{schemas[1]}' AND table_name='catalog_fixture' AND column_name='label'")]
        for name, predicate in plans:
            sql=projection+" WHERE "+predicate+" ORDER BY table_schema,table_name,ordinal_position"
            row=dict(id=name,postgres=query(reference,sql),bicdb=query(target,sql));row["matched"]=row["postgres"]==row["bicdb"];report["cases"].append(row)
    finally:
        for backend,schema in reversed(created):
            try:query((reference,target)[backend],"DROP SCHEMA "+schema+" CASCADE")
            except Exception as error:report["cleanup_errors"].append(dict(backend=backend,error=type(error).__name__))
    report["matched"]=sum(row["matched"] for row in report["cases"]);report["total"]=len(report["cases"])
    report["success"]=report["matched"]==report["total"] and not report["cleanup_errors"]
    return report


def main():
    import argparse
    import json
    import os
    from pathlib import Path
    import psycopg

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    with psycopg.connect(os.environ["ORACLE_CONTROL_PG_DSN"], autocommit=True,
                         connect_timeout=10) as reference:
        with psycopg.connect(os.environ["BICDB_PG_DSN"], autocommit=True,
                             connect_timeout=10) as target:
            if target.info.parameter_status("bicdb_sql_profile") != "postgres":
                raise RuntimeError("a native PostgreSQL-profile target is required")
            report = run(reference, target)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ("success", "matched", "total")}))
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
