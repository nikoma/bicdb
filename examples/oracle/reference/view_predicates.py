"""Owned-view source comparisons for selective predicates and semantic boundaries."""
import json
import uuid


def execute(connection, sql, rows=False, params=None):
    with connection.cursor() as cur:
        if type(connection).__module__.startswith("psycopg"):
            cur.execute(sql, params, prepare=rows)
        else:
            cur.execute(sql, params or [])
        if rows:
            return [[None if value is None else str(value) for value in row] for row in cur.fetchall()]


def run(reference, target, native=False):
    token = uuid.uuid4().hex[:12].upper()
    names = {key: '"BICDB_VF_' + key + '_' + token + '"' for key in ['F', 'D', 'V', 'R', 'Q', 'G', 'L', 'W']}
    f, d, v, renamed, quoted, grouped, limited, windowed = [names[key] for key in ['F', 'D', 'V', 'R', 'Q', 'G', 'L', 'W']]
    prefix = f"SELECT f.owner_key,f.id,COALESCE(f.label,d.label) FROM {f} f LEFT JOIN {d} d ON f.owner_key=d.id"
    ddl = [
        ("TABLE", f, f"CREATE TABLE {f}(id INTEGER PRIMARY KEY,owner_key INTEGER,label VARCHAR(40),active VARCHAR(1))"),
        ("TABLE", d, f"CREATE TABLE {d}(id INTEGER PRIMARY KEY,label VARCHAR(40))"),
        ("VIEW", v, f"CREATE VIEW {v} AS SELECT f.owner_key AS owner_key,f.id AS field_id,COALESCE(f.label,d.label) AS label FROM {f} f LEFT JOIN {d} d ON f.owner_key=d.id WHERE f.active='Y'"),
        ("VIEW", renamed, f"CREATE VIEW {renamed}(owner_alias,field_alias,label_alias) AS {prefix}"),
        ("VIEW", quoted, f'CREATE VIEW {quoted}("Owner Key","Field Id","Label Value") AS {prefix}'),
        ("VIEW", grouped, f"CREATE VIEW {grouped} AS SELECT owner_key,COUNT(*) AS amount FROM {f} GROUP BY owner_key"),
    ]
    if native:
        ddl += [
            ("VIEW", limited, f"CREATE VIEW {limited} AS SELECT id FROM {f} ORDER BY id LIMIT 5"),
            ("VIEW", windowed, f"CREATE VIEW {windowed} AS SELECT id,ROW_NUMBER() OVER(ORDER BY id) AS ordinal FROM {f}"),
        ]
    ten = [[str(i)] for i in range(61, 71)]
    twenty = [[str(i)] for i in range(51, 71)]
    plans = [
        dict(id="selected_join_view", sql=f"SELECT field_id,label FROM {v} WHERE owner_key=7 ORDER BY field_id", expected=[[str(i), 'label 7'] for i in range(61,71)]),
        dict(id="qualified_alias", sql=f"SELECT z.field_id FROM {v} z WHERE z.owner_key=7 ORDER BY z.field_id", expected=ten),
        dict(id="mixed_alias", sql=f"SELECT vTag.field_id FROM {v} vTag WHERE vTag.owner_key=7 ORDER BY vTag.field_id", expected=ten),
        dict(id="quoted_alias", sql=f'SELECT "Owner.Alias".field_id FROM {v} "Owner.Alias" WHERE "Owner.Alias".owner_key=7 ORDER BY "Owner.Alias".field_id', expected=ten),
        dict(id="reverse_comparison", sql=f"SELECT field_id FROM {v} WHERE 7=owner_key ORDER BY field_id", expected=ten),
        dict(id="between", sql=f"SELECT field_id FROM {v} WHERE owner_key BETWEEN 6 AND 7 ORDER BY field_id", expected=twenty),
        dict(id="in_list", sql=f"SELECT field_id FROM {v} WHERE owner_key IN (6,7) ORDER BY field_id", expected=twenty),
        dict(id="in_with_null", sql=f"SELECT field_id FROM {v} WHERE owner_key IN (7,NULL) ORDER BY field_id", expected=ten),
        dict(id="not_in_with_null", sql=f"SELECT field_id FROM {v} WHERE owner_key NOT IN (7,NULL) ORDER BY field_id", expected=[]),
        dict(id="null_equality", sql=f"SELECT field_id FROM {v} WHERE owner_key=NULL ORDER BY field_id", expected=[]),
        dict(id="residual_computed_column", sql=f"SELECT field_id FROM {v} WHERE owner_key=7 AND label IS NULL ORDER BY field_id", expected=[]),
        dict(id="disjunction", sql=f"SELECT field_id FROM {v} WHERE owner_key=7 OR owner_key=8 ORDER BY field_id", expected=[[str(i)] for i in range(61,81)]),
        dict(id="renamed_view_columns", sql=f"SELECT field_alias FROM {renamed} WHERE owner_alias=7 ORDER BY field_alias", expected=ten),
        dict(id="quoted_view_columns", sql=f'SELECT "Field Id" FROM {quoted} WHERE "Owner Key"=7 ORDER BY "Field Id"', expected=ten),
        dict(id="inner_join", sql=f"SELECT z.field_id FROM {d} d JOIN {v} z ON d.id=z.owner_key WHERE z.owner_key=7 ORDER BY z.field_id", expected=ten),
        dict(id="nullable_join_input", sql=f"SELECT d.id FROM {d} d LEFT JOIN {v} z ON d.id=z.field_id WHERE z.owner_key IS NULL ORDER BY d.id", expected=[['999']]),
        dict(id="aggregate_boundary", sql=f"SELECT amount FROM {grouped} WHERE owner_key=7", expected=[['10']]),
        dict(id="bound_key", sql=f"SELECT field_id FROM {v} WHERE owner_key={{P}} ORDER BY field_id", params=[7], expected=ten),
        dict(id="bound_null_key", sql=f"SELECT field_id FROM {v} WHERE owner_key={{P}} ORDER BY field_id", params=[None], expected=[]),
    ]
    if native:
        plans += [
            dict(id="column_alias_positions", sql=f"SELECT x FROM {v} z(k,x,t) WHERE z.k=7 ORDER BY x", expected=ten),
            dict(id="mixed_column_aliases", sql=f"SELECT z.fieldAlias FROM {v} z(keyAlias,fieldAlias,labelAlias) WHERE z.keyAlias=7 ORDER BY z.fieldAlias", expected=ten),
            dict(id="limit_boundary", sql=f"SELECT id FROM {limited} WHERE id>5", expected=[]),
            dict(id="window_boundary", sql=f"SELECT ordinal FROM {windowed} WHERE id=10", expected=[['10']]),
        ]
    report = dict(format=1, reference='postgres' if native else 'oracle', application_qualified=False, target_protocol='prepared', cases=[], cleanup_errors=[], success=False)
    created = []
    try:
        for label, conn in [('source',reference), ('bicdb',target)]:
            for kind, name, sql in ddl:
                execute(conn,sql);created.append((label,conn,kind,name))
            for i in range(1,201):
                execute(conn,f"INSERT INTO {f} VALUES({i},{(i-1)//10+1},NULL,'Y')")
            for i in [*range(1,21),999]:
                execute(conn,f"INSERT INTO {d} VALUES({i},'label {i}')")
        for plan in plans:
            row = dict(id=plan['id'], sql=plan['sql'], expected=plan['expected'])
            if 'params' in plan:
                row['params'] = plan['params']
            for label, conn in [('source',reference), ('bicdb',target)]:
                marker = ':1' if label=='source' and not native else '%s'
                try:
                    row[label] = dict(rows=execute(conn,plan['sql'].replace('{P}',marker),True,plan.get('params')))
                except Exception as exc:
                    code=getattr(exc.args[0],'code',None) if exc.args else None
                    row[label]=dict(sqlstate=getattr(exc,'sqlstate',None),oracle_code=code,exception=type(exc).__name__)
            wanted=dict(rows=plan['expected'])
            row['source_qualified']=row['source']==wanted
            row['matched']=row['source_qualified'] and row['bicdb']==wanted
            report['cases'].append(row)
    finally:
        for label, conn, kind, name in reversed(created):
            try:execute(conn,f"DROP {kind} {name}")
            except Exception as exc:report['cleanup_errors'].append(dict(backend=label,exception=type(exc).__name__))
    report['matched']=sum(case['matched'] for case in report['cases'])
    report['total']=len(report['cases'])
    report['source_qualified']=len(report['cases'])==len(plans) and all(case['source_qualified'] for case in report['cases'])
    report['success']=report['source_qualified'] and report['matched']==report['total'] and not report['cleanup_errors']
    return report


def main():
    import argparse
    import os
    from pathlib import Path
    import psycopg
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference',choices=['oracle','postgres'],required=True)
    parser.add_argument('--oracle-config',type=Path)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if args.reference=='oracle':
        import oracledb
        if args.oracle_config is None:parser.error('--oracle-config required for Oracle')
        config=json.loads(args.oracle_config.read_text())
        if config['user'].upper() in ['SYS','SYSTEM']:raise ValueError('dedicated non-administrative reference schema required')
        password=Path(config['password_file'])
        if not password.is_absolute():password=args.oracle_config.parent/password
        source=oracledb.connect(user=config['user'],password=password.read_text().strip(),host=config['host'],port=config['port'],service_name=config['service_name'])
        source.call_timeout=15000
    else:
        source=psycopg.connect(os.environ['ORACLE_CONTROL_PG_DSN'],autocommit=True,connect_timeout=10)
    with source,psycopg.connect(os.environ['BICDB_PG_DSN'],autocommit=True,connect_timeout=10) as target:
        source.autocommit=True
        expected='postgres' if args.reference=='postgres' else 'oracle-preview-v1'
        if target.info.parameter_status('bicdb_sql_profile')!=expected:raise RuntimeError('target profile does not match selected reference')
        report=run(source,target,args.reference=='postgres')
        report['source_version']=source.info.server_version if args.reference=='postgres' else source.version
        report['target_sql_profile']=expected
    args.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({key:report[key] for key in ['success','matched','total']}))
    return 0 if report['success'] else 1


if __name__=='__main__':
    raise SystemExit(main())
