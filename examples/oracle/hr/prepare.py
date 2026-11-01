#!/usr/bin/env python3
"""Prepare the pinned Oracle HR sample; all adaptations are explicit here."""
import argparse
import hashlib
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parent


def statements(source):
    """Read the sample's SQL*Plus statement and slash-delimited block syntax.

    Client commands are recognized only between statements. Quoted strings,
    doubled quotes and line comments cannot introduce statement boundaries.
    """
    buffer = ''
    quote = None
    block = False
    for line in source.splitlines(keepends=True):
        if quote is None and not buffer.strip():
            if re.match(r'^\s*(?:rem\b|prompt\b|set\s+(?:feedback|numwidth|linesize|trimspool|tab|pagesize|echo|verify)\b)', line, re.I):
                continue
            block = bool(re.match(r'^\s*(?:begin\b|declare\b|create\s+(?:or\s+replace\s+)?(?:procedure|trigger)\b)', line, re.I))
        if quote is None and block and line.strip() == '/':
            if buffer.strip(): yield buffer.strip()
            buffer = ''; block = False
            continue
        i = 0
        while i < len(line):
            char = line[i]
            if quote:
                buffer += char
                if char == quote:
                    if i+1 < len(line) and line[i+1] == quote:
                        buffer += line[i+1]; i += 1
                    else: quote = None
            elif line[i:i+2] == '--':
                buffer += line[i:]; break
            elif char in "'\"":
                quote = char; buffer += char
            elif char == ';' and not block:
                if buffer.strip(): yield buffer.strip()
                buffer = ''
            else: buffer += char
            i += 1
    if quote: raise ValueError('unterminated string')
    if block: raise ValueError('unterminated SQL*Plus block')
    if buffer.strip(): yield buffer.strip()


def prepare():
    manifest = json.loads((ROOT/'upstream.json').read_text())
    for name, expected in manifest['files'].items():
        if hashlib.sha256((ROOT/'upstream'/name).read_bytes()).hexdigest() != expected:
            raise ValueError(f'upstream file changed: {name}')
    create = list(statements((ROOT/'upstream/hr_create.sql').read_text()))
    populate = list(statements((ROOT/'upstream/hr_populate.sql').read_text()))
    result, deferred = [], []
    for sql in create:
        if re.search(r'ADD\s*\(\s*CONSTRAINT\s+dept_mgr_fk\b',sql,re.I):
            deferred.append(sql); continue
        # Physical IOT storage is mapped explicitly to BicDB heap storage.
        sql = re.sub(r'\bORGANIZATION\s+INDEX\b', '', sql, flags=re.I)
        # BicDB views in this profile are read-only; tested by the acceptance.
        sql = re.sub(r'\bWITH\s+READ\s+ONLY\s*$', '', sql, flags=re.I)
        result.append(sql)
    for sql in populate:
        if re.match(r'ALTER\s+SESSION\s+SET\s+NLS_LANGUAGE\s*=\s*American\s*$',sql,re.I):
            continue  # Fixed English date formats in this sample.
        if re.match(r'ALTER\s+TABLE\s+departments\s+(DISABLE|ENABLE)\s+CONSTRAINT\s+dept_mgr_fk\s*$',sql,re.I):
            continue  # Create the cyclic FK after loading and validate it then.
        result.append(sql)
    result.extend(deferred)
    code = list(statements((ROOT/'upstream/hr_code.sql').read_text()))
    result.extend(sql for sql in code if re.match(r'CREATE\s+OR\s+REPLACE\s+PROCEDURE\s+add_job_history\b',sql,re.I))
    return dict(format=1, source_commit=manifest['commit'],
                license=dict(spdx='MIT', text=(ROOT/'upstream/LICENSE.txt').read_text()),
                expected_rows=dict(regions=5,countries=25,locations=23,departments=27,jobs=19,employees=107,job_history=10),
                statements=result,
                adaptations=['countries IOT -> heap storage', 'native views enforce read-only access',
                             'fixed English sample date language', 'cyclic department-manager FK validated after load',
                             'add_job_history imported; secure_dml and Oracle triggers deferred'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    encoded = json.dumps(prepare(),indent=2)+'\n'
    target = ROOT/'migration.json'
    if args.check:
        if target.read_text() != encoded: raise SystemExit('migration.json is stale')
    else: target.write_text(encoded)
