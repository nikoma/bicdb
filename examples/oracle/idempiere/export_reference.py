#!/usr/bin/env python3
"""Export the isolated iDempiere Oracle reference for BicDB migration tests.

Application SQL/source and seed values stay outside the BicDB source tree.
The schema and rows are read in one Oracle read-only transaction. The observed
SCN is diagnostic metadata, not a CDC resume position.
"""
import argparse
import base64
import datetime
import decimal
import hashlib
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parent


def cell(value):
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, decimal.Decimal):
        return {'number': str(value)}
    if isinstance(value, datetime.datetime):
        return {'timestamp': value.isoformat(sep=' ')}
    if isinstance(value, bytes):
        return {'bytes': base64.b64encode(value).decode('ascii')}
    raise TypeError(f'unhandled Oracle value type {type(value).__name__}')


def quote(name):
    return '"' + name.replace('"', '""') + '"'


def require_temporal_encoding(column):
    kind = column['type']
    if kind.startswith('TIMESTAMP') and re.fullmatch(r'TIMESTAMP(?:\([0-6]\))?(?: WITH TIME ZONE)?', kind) is None:
        raise ValueError('timestamp declaration exceeds the export codec contract: ' + kind)


def value_projection(columns):
    projection = []
    for column in columns:
        require_temporal_encoding(column)
        name = quote(column['name'])
        if column['type'].endswith('WITH TIME ZONE'):
            projection.append("TO_CHAR(" + name + ", 'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM') AS " + name)
        else:projection.append(name)
    return ','.join(projection)


def decode_reference_row(row, columns):
    values = list(row)
    for i, column in enumerate(columns):
        if column['type'].endswith('WITH TIME ZONE') and values[i] is not None:
            values[i] = datetime.datetime.fromisoformat(values[i])
        elif column['type'] in {'BLOB', 'CLOB'} and values[i] is not None and hasattr(values[i], 'read'):
            values[i] = values[i].read()
    return values


def export(config_path, directory):
    import oracledb
    oracledb.defaults.fetch_decimals = True
    oracledb.defaults.fetch_lobs = False
    directory = directory.resolve()
    if directory.is_relative_to(ROOT.parents[2]):
        raise ValueError('export application source and seed data outside the BicDB checkout')
    # An exclusive directory prevents accidental replacement of a prior snapshot.
    directory.mkdir(parents=True, mode=0o700)
    config = json.loads(config_path.read_text())
    if config['user'].upper() in {'SYS', 'SYSTEM', 'PDBADMIN'}:
        raise ValueError('connect as the dedicated application schema owner')
    password_path = Path(config['password_file'])
    if not password_path.is_absolute(): password_path = config_path.parent / password_path
    objects, tables, constraints, columns, indexes, routines, views, types = [], [], [], [], [], [], [], []
    started = datetime.datetime.now(datetime.timezone.utc).isoformat()
    with oracledb.connect(user=config['user'], password=password_path.read_text().strip(),
                          host=config['host'], port=config['port'], service_name=config['service_name']) as con:
        con.call_timeout = 30000
        with con.cursor() as cursor:
            cursor.execute('SET TRANSACTION READ ONLY')
            cursor.execute('SELECT current_scn FROM v$database')
            observed_scn = str(cursor.fetchone()[0])
            cursor.execute("SELECT object_type,object_name,status FROM user_objects ORDER BY object_type,object_name")
            objects = [dict(kind=kind, name=name, status=status) for kind, name, status in cursor]
            if any(obj['status'] != 'VALID' for obj in objects):
                raise ValueError('reference has invalid objects; resolve them before export')
            cursor.execute("SELECT table_name FROM user_tables WHERE nested='NO' AND secondary='N' AND (iot_type IS NULL OR iot_type='IOT') ORDER BY table_name")
            table_names = [row[0] for row in cursor]
            cursor.execute("SELECT table_name,column_name,column_id,data_type,data_length,data_precision,data_scale,char_used,char_length,nullable,data_default,virtual_column FROM user_tab_cols WHERE hidden_column='NO' ORDER BY table_name,column_id")
            for row in cursor:
                columns.append(dict(zip(['table','name','position','type','bytes','precision','scale','character_unit','characters','nullable','default','virtual'], row)))
            # Validate all exported base-table timestamps before creating row
            # files. FF6 and Python datetime cannot preserve submicroseconds;
            # local-zone timestamps also need a distinct encoding contract.
            base_tables = set(table_names)
            for column in columns:
                if column['table'] in base_tables:require_temporal_encoding(column)
            cursor.execute("SELECT table_name,constraint_name,constraint_type,status,validated,deferrable,deferred,r_owner,r_constraint_name,delete_rule,search_condition,index_name FROM user_constraints ORDER BY table_name,constraint_name")
            for row in cursor:
                constraints.append(dict(zip(['table','name','kind','status','validated','deferrable','deferred','referenced_owner','referenced_constraint','delete_rule','condition','index'], row)))
            cursor.execute('SELECT constraint_name,column_name,position FROM user_cons_columns ORDER BY constraint_name,position')
            key_columns = {}
            for name, column, position in cursor:
                key_columns.setdefault(name, []).append(column)
            for constraint in constraints:
                constraint['columns'] = key_columns.get(constraint['name'], [])
            cursor.execute('SELECT index_name,column_position,column_name,descend FROM user_ind_columns ORDER BY index_name,column_position')
            index_columns = {}
            for name, position, column, direction in cursor:
                index_columns.setdefault(name, []).append(dict(position=int(position), column=column, direction=direction))
            cursor.execute('SELECT index_name,column_position,column_expression FROM user_ind_expressions')
            index_expressions = {(name, int(position)): expression for name, position, expression in cursor}
            cursor.execute('SELECT index_name,table_name,index_type,uniqueness,status FROM user_indexes ORDER BY index_name')
            for name, table, kind, uniqueness, status in cursor:
                keys = index_columns.get(name, [])
                for key in keys:
                    key['expression'] = index_expressions.get((name, key['position']))
                indexes.append(dict(name=name, table=table, kind=kind, uniqueness=uniqueness, status=status, columns=keys))
            cursor.execute('SELECT view_name,text,read_only FROM user_views ORDER BY view_name')
            for name, definition, read_only in cursor:
                views.append(dict(name=name, query=definition, read_only=read_only))
            cursor.execute("SELECT name,type,line,text FROM user_source WHERE type IN ('FUNCTION','PROCEDURE','TYPE') ORDER BY type,name,line")
            definitions = {}
            for name, kind, line, text in cursor:
                definitions.setdefault((kind, name), []).append(text)
            for (kind, name), lines in definitions.items():
                (types if kind == 'TYPE' else routines).append(dict(kind=kind, name=name, ddl='CREATE OR REPLACE ' + ''.join(lines)))
            # Publish the catalog first, so schema implementation can progress
            # independently while the consistent row export is still running.
            catalog = dict(format=1, source_schema=config['user'].upper(), objects=objects,
                           columns=columns, constraints=constraints, indexes=indexes,
                           routines=routines, views=views, types=types)
            (directory / 'catalog.json').write_text(json.dumps(catalog, indent=2, default=str) + '\n')
            for table in table_names:
                filename = f'table-{len(tables):04d}.jsonl'
                count = 0
                with (directory / filename).open('x') as target:
                    table_columns = [column for column in columns if column['table'] == table]
                    # python-oracledb's default TIMESTAMP WITH TIME ZONE fetch
                    # returns a naive datetime. Preserve the actual source
                    # offset explicitly rather than guessing a session zone.
                    cursor.execute('SELECT ' + value_projection(table_columns) + ' FROM ' + quote(table), fetch_lobs=True)
                    names = [description.name for description in cursor.description]
                    while batch := cursor.fetchmany(500):
                        for row in batch:
                            values = decode_reference_row(row, table_columns)
                            target.write(json.dumps([cell(value) for value in values], ensure_ascii=False) + '\n')
                        count += len(batch)
                with (directory / filename).open('rb') as content:
                    digest = hashlib.file_digest(content, 'sha256').hexdigest()
                tables.append(dict(name=table, columns=names, rows=count, file=filename,
                                   sha256=digest))
                if len(tables) % 100 == 0:
                    print(f'Exported {len(tables)}/{len(table_names)} tables', flush=True)
        manifest = dict(format=1, application='iDempiere 13', oracle_database_version=con.version,
                        value_encoding=dict(version=2, timestamp_with_time_zone='offset-preserved', lob_empty_values='preserved',
                                            timestamp_fractional_precision_max=6,
                                            time_zone_region_identity_preserved=False, local_time_zone_supported=False),
                        source_schema=config['user'].upper(), started_at=started,
                        snapshot='Oracle read-only transaction', observed_scn=observed_scn,
                        cdc_resume_position=False, objects=objects, tables=tables, columns=columns,
                        constraints=constraints, indexes=indexes, routines=routines, views=views, types=types,
                        adaptations=['logical schema is represented by catalog metadata; physical storage is not copied',
                                     'foreign keys include source validation/deferment state for explicit migration'])
    (directory / 'manifest.json').write_text(json.dumps(manifest, indent=2, default=str) + '\n')
    summary = dict(tables=len(tables), rows=sum(t['rows'] for t in tables), views=len(views),
                   routines=len(routines), types=len(types), indexes=len(indexes),
                   oracle_database_version=manifest['oracle_database_version'], invalid_objects=0)
    print(json.dumps(summary, indent=2))
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--oracle-config', type=Path, required=True)
    parser.add_argument('--directory', type=Path, required=True)
    args = parser.parse_args()
    export(args.oracle_config, args.directory)


if __name__ == '__main__': main()
