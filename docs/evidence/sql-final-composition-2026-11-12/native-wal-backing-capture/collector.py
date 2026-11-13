import gzip,hashlib,json,pathlib,re,subprocess,time
S=pathlib.Path('/root/.local/state/bicdb/typed-compare');R=pathlib.Path('/root/bicdb-sql-final-composition')
SOURCE='ccde585361dcdbf65bd9da698a21ccba94941b09'
def git(*args):return subprocess.check_output(['git',*args],cwd=R,text=True).strip()
assert git('rev-parse','HEAD')==SOURCE and not git('status','--porcelain')
code=r'''import pathlib,json,gzip,hashlib,re,subprocess
root=pathlib.Path('/root/bicdb-durability-s3');dest=root/'host-control/native-wal-evidence-ccde';dest.mkdir(exist_ok=False)
labels=['native-birth-retained-wal-baseline','native-birth-retained-wal-baseline-v2','native-birth-retained-wal-baseline-v3']+['native-birth-wal-capture-focused-v'+str(i) for i in range(1,7)]
summary={}
for label in labels:
 unit=dict(line.split('=',1) for line in subprocess.check_output(['systemctl','show','bicdb-'+label,'-p','ActiveState','-p','MainPID'],text=True).splitlines())
 assert unit['MainPID']=='0' and unit['ActiveState'] in ['inactive','failed'],(label,unit)
 d=dest/label;d.mkdir();r=json.loads((root/('report-'+label+'.json')).read_text());spec=json.loads((root/'host-control'/(label+'.json')).read_text())
 assert r['source']==spec['source'];assert [x['command'] for x in r['steps']]==spec['commands'][:len(r['steps'])]
 log=(root/('tc-step-'+label+'.log')).read_bytes()
 results=[dict(outcome=m[0],passed=int(m[1]),failed=int(m[2]),ignored=int(m[3])) for m in re.findall(r'^test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;',log.decode(),re.M)]
 summary[label]=dict(source=r['source'],status=r['status'],step_exits=[s['exit'] for s in r['steps']],parent_results=results,unit_terminal=unit)
 files={'receipt.json':(root/('report-'+label+'.json')).read_bytes(),'spec.json':(root/'host-control'/(label+'.json')).read_bytes(),'runner.py':(root/'host-control'/(label+'-runner.py')).read_bytes(),'validation.log':log};manifest={}
 for name,raw in files.items():
  stored=name+'.gz' if name.endswith('.log') else name;(d/stored).write_bytes(gzip.compress(raw,mtime=0) if stored.endswith('.gz') else raw)
  manifest[name]=dict(stored_as=stored,raw_bytes=len(raw),raw_sha256=hashlib.sha256(raw).hexdigest())
 (d/'raw-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
current=summary[labels[-1]];assert current['source']=='ccde585361dcdbf65bd9da698a21ccba94941b09' and current['status']=='focused_passed' and current['step_exits']==[0]*8
expected=[(14,0,0),(6,0,0),(8,0,1),(1,0,0),(4,0,0),(3,0,0)]
assert [(v['passed'],v['failed'],v['ignored']) for v in current['parent_results']]==expected,current
summary['qualification']=dict(source=current['source'],Core_focused_passed=36,Core_failed=0,historical_ignored=1,parent_targets=6,actual_in_process_EIO_cases=4,actual_corrupted_cold_open_refusals=4,Core_SQL_wire_alltargets_compile_passed=True,independent_crash_EIO_KVM_qualified=False,source_root_authentication_implemented=False,portable_import_producers_implemented=False,SQL_promoted_original_positive_qualified=False,main_delivered=False,production_binary_built=False,release_published=False,deployed=False)
(dest/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(dict(remote_directory=str(dest),qualification=summary['qualification'])))
'''
result=json.loads(subprocess.check_output(['ssh','root@74.50.69.58','python3','-'],input=code.encode()))
d=R/'docs/evidence/sql-final-composition-2026-11-12/native-wal-backing-capture';assert not d.exists()
subprocess.run(['scp','-q','-r','root@74.50.69.58:'+result['remote_directory'],str(d)],check=True)
(d/'collector.py').write_bytes(pathlib.Path(__file__).read_bytes())
(d/'submit_s3_isolation_batch.py').write_bytes((S/'submit_s3_isolation_batch.py').read_bytes())
(d/'README.md').write_text('''# Historical Native backing contracts and local WAL recovery

Exact implementation `ccde585361dcdbf65bd9da698a21ccba94941b09`, inherited
1.0.490-beta on the private combined branch. This is unmerged prerequisite work.
It neither authenticates a replica peer nor grants imported rename recovery.

Both retained-WAL exporters used the current catalog to describe earlier writes.
After a rename and name reuse, an original Standard write was exported with the
replacement TimeSeries backing's contract. Two unchanged positive assertions
reproduced that defect in batch and live exports. A real local-consumer retention
hold keeps that history across checkpoints without counting as any replica ACK.
The first fixtures correctly hit the mesh-rename restriction and the checkpoint
retention floor; those setup failures are retained separately.

An explicitly fenced Native commit now captures each touched born backing's
actual allocation UUID, namespace UUID and contract alongside the same durable
write decision. The first vector's planned dimension is included. Prepared row
WAL cannot cache stale allocation data. Legacy/unborn backings and earlier WAL
acquire no invented identity. Historical batch and live exports use those actual
commit contracts, even after rename, replacement, drop and restart. The separate
local `native_wal_backing_capture_v1` capability fences birth-aware older writers;
activation is explicit, future-only, durable and retryable. It is excluded from
portable semantic reader requirements.

The first exporter fix exposed another real failure: after restart, retained
writes could reappear in the newer same-name collection. Local recovery now
resolves captured writes through their exact birth/namespace pair before row
replay. It uses the surviving actual backing, rejects partially changed pairs,
and skips pairs whose backing was dropped. It does not fall back to names,
storage aliases, donor UUIDs or SQL OIDs. Legacy/unborn local WAL retains its
existing semantics and gains no authenticated portable provenance from this
change. A later corruption control showed that materialized paged row bodies
were discarded before identity validation. A compact deduplicated pair history
now preserves those checks without retaining materialized row bodies.

The final exact source passed 36 focused Core tests, zero failures and one
pre-existing ignored frozen-reader test, across six parent harnesses: fourteen
birth/WAL controls, six image controls, eight namespace controls, one legacy WAL
serialization check, four sparse WAL checks and three snapshot/schema reader
regressions. Core/SQL/wire all-target compilation and all twenty-nine inherited
package/version identities passed. Four actual write/sync EIO injections cover
both modes; no failed write escapes the acknowledged durable prefix. Uncertain
recovery allows only a complete row or absence. Four corrupted cold opens cover
changed birth vs changed namespace in both modes, followed by catalog restoration,
exact Native ACK custody and two stable opens. These in-process checks do not
replace independent process, EIO and KVM qualification.

Capture checks cover immutable allocation data, exact payload/vector/timestamp/
unknown metadata, first-vector dimension, same-name isolation and two reopens.
The opaque in-process capture is checksum-bound to the exported frame, discarded
by JSON serialization and unavailable on asynchronous sources. Changed payload,
source node, sequence, transaction, reader requirements or a stolen capture on a
different frame cannot retain that capture. It is deliberately not a stable
source-root attestation or a target import receipt.

All nine baseline/validation histories remain in their own folders. V1 and V2
advanced past exporter assertions and failed the real restart isolation check.
V3 passed 34 checks; V4 added four EIO cases and passed 35; V5's new corruption
control failed before the materialized-history fix; V6 passed all 36. The JSON
summary counts parent harness results only. Raw manifests validate original
bytes after gzip decompression; command lists and source revisions remain exact.

Still required: actual source-root identity scoped at backing allocation,
authenticated producers at BOTH protected Native image and legacy logical-import
boundaries, durable target/root/generation bindings and record provenance,
revocation/cursor handling, and the read-only SQL recovery resolver. The original
promoted undecided rename positive remains unchanged and open. Its last actual
failure at `68de99c6` is preserved in the sibling `native-collection-birth`
evidence; it was not rerun or claimed passing on this WAL source. Full combined
regression, independent fault/VM, original application/UI/restore, physical HA and
capacity, and approved performance qualification remain separate full-goal gates.
No production CLI, main delivery, release or deployment is implied. Experimental
production remains off and no new cloud spend was introduced.
''')
manifest={}
for p in sorted(d.rglob('*')):
 if p.is_file():
  raw=p.read_bytes();manifest[str(p.relative_to(d))]=dict(bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest())
(d/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
for p in d.rglob('raw-manifest.json'):
 for name,item in json.loads(p.read_text()).items():
  path=p.parent/item['stored_as'];raw=path.read_bytes();raw=gzip.decompress(raw) if path.suffix=='.gz' else raw;assert len(raw)==item['raw_bytes'] and hashlib.sha256(raw).hexdigest()==item['raw_sha256'],name
assert git('rev-parse','HEAD')==SOURCE
subprocess.run(['git','diff','--check'],cwd=R,check=True)
subprocess.run(['git','add','--',str(d)],cwd=R,check=True)
subprocess.run(['git','commit','-q','-m','Preserve Native WAL capture, cold-recovery and IO qualification'],cwd=R,check=True)
assert not git('diff',SOURCE,'HEAD','--','crates','Cargo.lock','scripts','.github','POSTGRES_COMPATIBILITY.md')
record=dict(status='collected_committed_private_evidence',runtime_source=SOURCE,evidence_commit=git('rev-parse','HEAD'),manifest_entries=len(manifest),raw_entries=36,qualification=result['qualification'],directory=str(d),finished_unix=time.time())
(S/'native-wal-backing-capture-collection.json').write_text(json.dumps(record,indent=2)+'\n')
print(json.dumps(record,indent=2))
