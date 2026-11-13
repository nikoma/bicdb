import fcntl,gzip,hashlib,json,pathlib,re,subprocess,time
S=pathlib.Path('/root/.local/state/bicdb/typed-compare')
R=pathlib.Path('/root/bicdb-sql-final-composition')
SOURCE='68de99c6450518e8d67d83b1118d2e7afa4e7374'
LABEL='native-backing-birth-focused-v4'
OUT=S/'native-backing-birth-collection.json'
assert not OUT.exists()
lock=(S/'native-birth-evidence-delivery.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
record=dict(status='waiting_exact_current_batch',source=SOURCE,unit='bicdb-'+LABEL)
def save():OUT.write_text(json.dumps(record,indent=2)+'\n')
def ssh(code):return subprocess.check_output(['ssh','root@74.50.69.58','python3','-'],input=code.encode()).decode()
def git(*args):return subprocess.check_output(['git',*args],cwd=R,text=True).strip()
save()
try:
 while True:
  state=json.loads(ssh("import subprocess,json;print(json.dumps(dict(line.split('=',1) for line in subprocess.check_output(['systemctl','show','bicdb-"+LABEL+"','-p','ActiveState','-p','MainPID'],text=True).splitlines())))"))
  record['observed_unit']=state;record['observed_unix']=time.time();save()
  if state['ActiveState'] not in ['active','activating','deactivating','reloading'] and state['MainPID']=='0':break
  time.sleep(15)
 receipt=json.loads(ssh("import pathlib;print(pathlib.Path('/root/bicdb-durability-s3/report-"+LABEL+".json').read_text())"))
 assert receipt['source']==SOURCE and len(receipt['steps'])==7
 assert [s['exit'] for s in receipt['steps']]==[0,0,0,0,0,0,101],receipt
 assert receipt['status']=='failed'
 assert git('rev-parse','HEAD')==SOURCE and not git('status','--porcelain')
 assert not git('diff','3a5f232cb44b4bcf16da5747825b7e629cc6f7d6','HEAD','--','crates/bicdb-sql/src/session/rename_table.rs')
 remote_code=r'''import pathlib,json,gzip,hashlib,re
root=pathlib.Path('/root/bicdb-durability-s3');dest=root/'host-control/native-birth-evidence-68de';dest.mkdir(exist_ok=False)
labels=['native-backing-birth-focused-v1','native-backing-birth-focused-v2','native-backing-birth-focused-v3','native-backing-birth-schema-refresh-baseline','native-backing-birth-focused-v4']
summary={}
for label in labels:
 d=dest/label;d.mkdir()
 r=json.loads((root/('report-'+label+'.json')).read_text())
 assert r['status']=='failed'
 log=(root/('tc-step-'+label+'.log')).read_bytes()
 results=[dict(outcome=m[0],passed=int(m[1]),failed=int(m[2]),ignored=int(m[3])) for m in re.findall(r'^test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;',log.decode(),re.M)]
 summary[label]=dict(source=r['source'],step_exits=[s['exit'] for s in r['steps']],parent_harness_results=results)
 files={'receipt.json':(root/('report-'+label+'.json')).read_bytes(),'spec.json':(root/'host-control'/(label+'.json')).read_bytes(),'runner.py':(root/'host-control'/(label+'-runner.py')).read_bytes(),'validation.log':log}
 manifest={}
 for name,raw in files.items():
  stored=name+'.gz' if name.endswith('.log') else name
  (d/stored).write_bytes(gzip.compress(raw,mtime=0) if stored.endswith('.gz') else raw)
  manifest[name]=dict(stored_as=stored,raw_bytes=len(raw),raw_sha256=hashlib.sha256(raw).hexdigest())
 (d/'raw-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
current=summary[labels[-1]];assert current['source']=='68de99c6450518e8d67d83b1118d2e7afa4e7374'
assert current['parent_harness_results']==[dict(outcome='ok',passed=5,failed=0,ignored=0),dict(outcome='ok',passed=6,failed=0,ignored=0),dict(outcome='ok',passed=8,failed=0,ignored=1),dict(outcome='ok',passed=3,failed=0,ignored=0),dict(outcome='FAILED',passed=0,failed=1,ignored=0)],current
assert 'missing, replaced, or ambiguous physical backing' in (root/('tc-step-'+labels[-1]+'.log')).read_text()
summary['qualification']=dict(source=current['source'],core_focused_passed=22,core_focused_failed=0,core_historical_ignored=1,unchanged_SQL_promotion_positive_passed=0,unchanged_SQL_promotion_positive_failed=1,alltargets_Core_SQL_wire_compile_passed=True,portable_origin_producer_implemented=False,application_qualified=False,main_delivered=False,release_published=False,deployed=False)
(dest/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(dict(remote_directory=str(dest),qualification=summary['qualification'])))
'''
 collected=json.loads(ssh(remote_code))
 dest=R/'docs/evidence/sql-final-composition-2026-11-12/native-collection-birth';assert not dest.exists()
 subprocess.run(['scp','-q','-r','root@74.50.69.58:'+collected['remote_directory'],str(dest)],check=True)
 (dest/'collector.py').write_bytes(pathlib.Path(__file__).read_bytes())
 (dest/'submit_s3_isolation_batch.py').write_bytes((S/'submit_s3_isolation_batch.py').read_bytes())
 (dest/'README.md').write_text('''# Future Native collection allocation identities

Exact private implementation `68de99c6450518e8d67d83b1118d2e7afa4e7374`, inherited
1.0.490-beta. This prerequisite is unmerged and does not fix portable rename
recovery by itself. The original promoted undecided rename positive is unchanged
and still fails: `missing, replaced, or ambiguous physical backing`.

Under explicit local format activation, a newly allocated backing receives a
birth UUID alongside its local namespace UUID in the durable Native catalog.
Activation and retries never assign a birth to an existing collection. A
migrated namespace UUID is therefore distinguishable from a creation-time birth.
Births survive same-backing rename and restart. Drop/recreate allocates another
birth even if a logical or storage name is reused. Portable schema contracts
exclude both local identities. Untrusted snapshot/frame metadata and contract
restoration cannot adopt donor births or retrofit an existing unborn target.
Actual cluster schema refresh preserves the target birth. Protected live image
capture pins its actual birth separately from namespace IDs and live allocation.

The current focused batch passed 22 Core tests, with zero failures and one
pre-existing ignored frozen-reader test: five birth lifecycle/import/schema
controls, six image binding controls, eight existing namespace controls and three
existing snapshot/schema reader regressions. The five birth controls iterate
both storage modes; import controls cover new, existing unborn and inactive
targets. Native ACK hashes and two reopens are checked. Core/SQL/wire all-target
compilation and synchronized inherited source version checks passed. These are
focused checks, not a full regression suite of this private source.

The same batch ran the original SQL promotion positive and failed it. Its one
failure makes the overall batch status `failed`; it is retained without skips,
assertion edits or donor-ID/name fallback. Source/target root attestation,
authenticated durable import mappings, record provenance, protected Native image
and legacy logical import producers, and the read-only SQL recovery resolver are
still required. A birth UUID, format flag, checksum or a decoded metadata claim
alone grants no recovery authority. The new capability remains explicit and
local; it is not propagated as a portable semantic reader feature.

Preserved history includes the v1 test-setup compile error; v2's 4 passing birth
checks and failed capture fixture (fresh live collection was not yet reopened
into the required purely paged representation); v3's 18 passing Core checks and
unchanged SQL failure; and the separate schema-refresh regression which proved
the target birth was erased before the fix. V4 fixes that real metadata refresh
loss and uses the actual paged reopen for the capture fixture. All original
failures, commands, source revisions and monitor logs remain in their own folders.

The JSON summary counts parent harness results. Each raw manifest validates
original bytes after gzip decompression. No production CLI, release, deployment,
full application acceptance, automatic capacity qualification or experimental
production promotion is implied. The full campaign remains active; no new cloud
spend was introduced.
''')
 manifest={}
 for path in sorted(dest.rglob('*')):
  if not path.is_file():continue
  raw=path.read_bytes();manifest[str(path.relative_to(dest))]=dict(bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest())
 (dest/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
 assert git('rev-parse','HEAD')==SOURCE
 subprocess.run(['git','diff','--check'],cwd=R,check=True)
 subprocess.run(['git','add','--',str(dest)],cwd=R,check=True)
 subprocess.run(['git','commit','-q','-m','Preserve Native birth qualification and unchanged promotion recovery failure'],cwd=R,check=True)
 assert not git('diff',SOURCE,'HEAD','--','crates','Cargo.lock','scripts','.github','POSTGRES_COMPATIBILITY.md')
 record.update(status='collected_committed_private_evidence',evidence_commit=git('rev-parse','HEAD'),directory=str(dest),qualification=collected['qualification'],finished_unix=time.time());save()
except BaseException as error:
 record.update(status='needs_attention',error=repr(error),stopped_unix=time.time());save();raise
