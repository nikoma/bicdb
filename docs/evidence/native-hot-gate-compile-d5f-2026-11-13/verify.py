#!/usr/bin/env python3
"""Offline failed-compile evidence consistency; no build or engine open."""
from pathlib import Path
import hashlib,gzip,json,re
HERE=Path(__file__).resolve().parent
sums=json.loads((HERE/'SHA256SUMS.json').read_text())
actual={p.name for p in HERE.iterdir() if p.is_file() and p.name!='SHA256SUMS.json'}
assert actual==set(sums)
for name,sha in sums.items():assert hashlib.sha256((HERE/name).read_bytes()).hexdigest()==sha,name
meta=json.loads((HERE/'source-and-classification.json').read_text());run=json.loads((HERE/'run.json').read_text())
assert run['headSha']==meta['source'] and run['databaseId']==meta['run'] and run['status']=='completed' and run['conclusion']=='failure'
jobs={j['databaseId']:j for j in run['jobs']}
assert jobs[meta['alltargets_job']]['conclusion']==jobs[meta['core_job']]['conclusion']=='failure'
core={s['name']:s['conclusion'] for s in jobs[meta['core_job']]['steps']}
assert core['Build projection chaos helper']=='failure'
assert core['Record source and list selected Core library tests']==core['Execute selected Core library tests']=='skipped'
workspace={s['name']:s['conclusion'] for s in jobs[meta['alltargets_job']]['steps']}
assert workspace['cargo check --workspace --all-targets']=='failure'
assert workspace['Workflow consumer compiles without Core defaults']=='skipped'
for key in ('alltargets','core'):
 text=gzip.decompress((HERE/(key+'.log.gz')).read_bytes()).decode()
 text=re.sub(r'\x1b\[[0-?]*[ -/]*[@-~]','',text)
 assert len(re.findall(r'error\[E\d+\]:',text))==6
 assert 'could not compile `bicdb-core` (lib) due to 6 previous errors' in text
assert meta['selected_tests']==meta['executed_tests']==0 and not meta['qualification']
print(json.dumps({'evidence_consistency':'passed','compile':'failed','unit_tests':'not tested','runtime_or_physical_qualification':False},sort_keys=True))
