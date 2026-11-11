"""Independent PostgreSQL oracle for historical index candidate visibility."""
import json
import pathlib
import time
import uuid
import psycopg
from psycopg import sql

output = pathlib.Path('/root/.local/state/bicdb/typed-compare/native-fixed-snapshot-index-extrema-control.json')
assert not output.exists()
options = dict(host='127.0.0.1', port=57940, user='adempiere', dbname='bicdb_native_app_ctrl_20261109', autocommit=True)
schema = 'snapshot_index_' + uuid.uuid4().hex[:12]
record = dict(status='running', started_unix=time.time(), protected_application_schema_mutated=False, cases=[])
queries = [
    ('SELECT id FROM history WHERE k=10', [1]),
    ('SELECT id FROM history WHERE k>=10 AND k<=20 ORDER BY id', [1, 2]),
    ('SELECT id FROM history WHERE k>=0 ORDER BY k LIMIT 1', [1]),
    ('SELECT min(k) FROM history', [10]),
    ('SELECT max(k) FROM history', [20]),
    ('SELECT min(id) FROM history', [1]),
    ('SELECT max(id) FROM history', [2]),
    ('''SELECT id FROM history WHERE body @> '{"status":"open"}'::jsonb''', [1]),
    ('SELECT id FROM history WHERE id>=0 ORDER BY id', [1, 2]),
]
with psycopg.connect(**options) as writer, psycopg.connect(**options) as reader:
    record['server_version'] = writer.execute('SHOW server_version').fetchone()[0]
    record['fsync'] = writer.execute('SHOW fsync').fetchone()[0]
    writer.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
    try:
        for connection in (reader, writer):
            connection.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(schema)))
        reader.execute('SET enable_seqscan=off')
        writer.execute('CREATE TABLE history(id INT PRIMARY KEY,k INT,body JSONB)')
        writer.execute('CREATE INDEX history_k ON history(k)')
        writer.execute('CREATE INDEX history_body ON history USING gin(body)')
        for isolation in ['REPEATABLE READ', 'REPEATABLE READ READ ONLY', 'SERIALIZABLE', 'SERIALIZABLE READ ONLY']:
            writer.execute('TRUNCATE history')
            writer.execute('''INSERT INTO history VALUES (1,10,'{"status":"open"}'),(2,20,'{"status":"closed"}')''')
            reader.execute('BEGIN ISOLATION LEVEL ' + isolation)
            reader.execute('SELECT 1')
            writer.execute('BEGIN')
            writer.execute('''UPDATE history SET k=30,body='{"status":"archived"}' WHERE id=1''')
            writer.execute('DELETE FROM history WHERE id=2')
            writer.execute('''INSERT INTO history VALUES (3,5,'{"status":"open"}')''')
            writer.execute('COMMIT')
            for query, expected in queries:
                plan = [row[0] for row in reader.execute('EXPLAIN (COSTS OFF) ' + query).fetchall()]
                rows = [row[0] for row in reader.execute(query).fetchall()]
                assert rows == expected, (isolation, query, rows, expected)
                record['cases'].append(dict(isolation=isolation, query=query, expected=expected, observed=rows, plan=plan))
            reader.execute('ROLLBACK')
    finally:
        reader.execute('ROLLBACK')
        writer.execute('ROLLBACK')
        writer.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))
        record['temporary_schema_removed'] = True
record.update(status='passed', finished_unix=time.time())
output.write_text(json.dumps(record, indent=2) + '\n')
print(json.dumps(dict(status=record['status'], server_version=record['server_version'], cases=len(record['cases']))))
