import gzip,hashlib,json,pathlib,subprocess,time
S=pathlib.Path('/root/.local/state/bicdb/typed-compare');R=pathlib.Path('/root/bicdb-sql-final-composition');SOURCE='1f14ae1d7ed9ebdbde8a9cd2ae4af9f4bedb7c1f'
def git(*a):return subprocess.check_output(['git',*a],cwd=R,text=True).strip()
assert git('rev-parse','HEAD')==SOURCE and not git('status','--porcelain')
code=r'''import pathlib,json,gzip,hashlib,re,subprocess
root=pathlib.Path('/root/bicdb-durability-s3');dest=root/'host-control/native-origin-evidence-1f14';dest.mkdir(exist_ok=False);summary={}
for i in range(1,4):
 label='native-origin-root-focused-v'+str(i);unit=dict(line.split('=',1) for line in subprocess.check_output(['systemctl','show','bicdb-'+label,'-p','ActiveState','-p','MainPID'],text=True).splitlines());assert unit['MainPID']=='0' and unit['ActiveState'] in ['inactive','failed']
 d=dest/label;d.mkdir();r=json.loads((root/('report-'+label+'.json')).read_text());spec=json.loads((root/'host-control'/(label+'.json')).read_text());assert r['source']==spec['source'] and [s['command'] for s in r['steps']]==spec['commands'][:len(r['steps'])]
 rawlog=(root/('tc-step-'+label+'.log')).read_bytes();results=[dict(outcome=m[0],passed=int(m[1]),failed=int(m[2]),ignored=int(m[3])) for m in re.findall(r'^test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;',rawlog.decode(),re.M)]
 summary[label]=dict(source=r['source'],status=r['status'],exits=[s['exit'] for s in r['steps']],parent_results=results,unit_terminal=unit)
 files={'receipt.json':(root/('report-'+label+'.json')).read_bytes(),'spec.json':(root/'host-control'/(label+'.json')).read_bytes(),'runner.py':(root/'host-control'/(label+'-runner.py')).read_bytes(),'validation.log':rawlog};manifest={}
 for name,raw in files.items():
  stored=name+'.gz' if name.endswith('.log') else name;(d/stored).write_bytes(gzip.compress(raw,mtime=0) if stored.endswith('.gz') else raw);manifest[name]=dict(stored_as=stored,raw_bytes=len(raw),raw_sha256=hashlib.sha256(raw).hexdigest())
 (d/'raw-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
current=summary[label];assert current['source']=='1f14ae1d7ed9ebdbde8a9cd2ae4af9f4bedb7c1f' and current['status']=='focused_passed' and current['exits']==[0]*8
assert [(x['passed'],x['failed'],x['ignored']) for x in current['parent_results']]==[(8,0,0),(14,0,0),(6,0,0),(10,0,0),(8,0,1),(2,0,0),(3,0,0)]
summary['qualification']=dict(source=current['source'],Core_focused_passed=51,failed=0,historical_ignored=1,parent_targets=7,new_actual_publication_EIO_cases=2,prior_WAL_EIO_cases_included=4,Page_Core_SQL_wire_alltargets_compile_passed=True,physical_qualification='Unix directory device/inode/OS creation time',source_root_observation='actual Native factory and physical custody, not peer auth',authenticated_import_producers_and_record_lineage_and_SQL_resolver_implemented=False,independent_crash_KVM_qualified=False,main_delivered=False,production_binary_built=False,release_published=False,deployed=False)
(dest/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(dict(directory=str(dest),qualification=summary['qualification'])))
'''
result=json.loads(subprocess.check_output(['ssh','root@74.50.69.58','python3','-'],input=code.encode()))
d=R/'docs/evidence/sql-final-composition-2026-11-12/native-origin-root';assert not d.exists();subprocess.run(['scp','-q','-r','root@74.50.69.58:'+result['directory'],str(d)],check=True)
(d/'collector.py').write_bytes(pathlib.Path(__file__).read_bytes());(d/'submit_s3_isolation_batch.py').write_bytes((S/'submit_s3_isolation_batch.py').read_bytes())
(d/'README.md').write_text('''# Actual Native factory roots and allocation origin scope

Exact private source `1f14ae1d7ed9ebdbde8a9cd2ae4af9f4bedb7c1f`, inherited
1.0.490-beta. This is unmerged prerequisite work, not cross-node qualification.

A Native root can be activated from the actual atomic directory creation fact
held by the creating open, or from its durable private prepared factory record.
An existing empty directory is insufficient. The protected Native range factory
carries its own actual atomic create into this path before creating backings.
The private root record is fsynced before its sticky local format fence; new
containing directory entries are synced too. Its checksum supplies integrity,
not peer or import authority. No public constructor accepts a root UUID or a
caller-supplied root capsule as authority.

On the qualified Unix path, the root binds the claimed directory's OS device,
inode and creation time. Refusal preflight is read-only, followed by a check
under the actual directory lock. An ordinary copied store is refused without
changing its bytes. Moving the same physical directory preserves identity.
Live replacement is refused by source observations, mutation admission, Native
WAL capture and protected image bindings. Physical custody on other platforms
or filesystems without the required OS identity remains unqualified; this is
not a claim of general portable-directory identity.

A backing records the active Native root only at actual allocation. Earlier
born or legacy collections remain unscoped. Portable contracts strip the root,
birth and local namespace; snapshot import, schema refresh and contract restore
preserve an existing target's local state or allocate a genuinely new target
identity. They cannot adopt or retrofit a donor root. Retained WAL keeps the
actual root scope with its write-time backing contract and requires that root
even after every scoped collection has been dropped. The opaque source-root
observation in actual batch/live capture is discarded by JSON serialization.
Protected image bindings also pin this observation and every backing's actual
origin scope; a new observation does not retroactively scope old backings.

The final source passed 51 focused Core tests across seven parent harnesses:
eight origin controls, fourteen existing birth/WAL controls, six image-binding
controls, ten actual protected Native factory controls, eight namespace controls,
two directory-lock controls and three snapshot/schema reader controls. Zero
failed; one pre-existing frozen-reader test remains ignored. Page/Core/SQL/wire
all-target compilation and twenty-nine inherited source versions passed.

Two actual root-publication directory-sync EIO cases cover both storage modes.
The handle fences; reopen and retry finish the same actual prepared root, keep
old backings unscoped, preserve exact independent Native ACK hashes and survive
two further opens. Four prior actual WAL write/sync EIO cases are included in
the birth/WAL controls. These are in-process tests, not independent crash or KVM
qualification. Source root tests exercise both Embedded and Paged storage;
protected image tests remain Paged only.

All three runs and their raw bytes are retained. V1 passed 48 checks. V2's
WAL-only fixture correctly hit an earlier catalog fence because its internal
catalog log was still scoped. V3 allocates the independent ACK/catalog log
before activation, asserts all remaining collections are unscoped, and preserves
the exact WAL-history refusal assertion. Production code is unchanged from V2.
The final count is 51; parent-harness accounting excludes invented subcase totals.

Still required: authenticated source/peer acceptance, actual producers at BOTH
protected Native image and legacy logical-import boundaries, source allocation
and publisher facts, durable target/root/generation mappings and record lineage,
revocation/cursor checks, the read-only SQL resolver and the unchanged original
promoted undecided rename positive. Raw copied metadata, local root observations
and checksums alone grant none of that authority. Existing unowned journals or
targets must not be retroactively authenticated. Full combined regression,
independent process/EIO/KVM, application/UI/restore, physical capacity and approved
performance gates remain open. No main delivery, production binary, release,
deployment, experimental production promotion or new cloud spend is implied.
''')
manifest={}
for p in sorted(d.rglob('*')):
 if p.is_file():
  raw=p.read_bytes();manifest[str(p.relative_to(d))]=dict(bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest())
(d/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
for p in d.rglob('raw-manifest.json'):
 for name,item in json.loads(p.read_text()).items():
  path=p.parent/item['stored_as'];raw=path.read_bytes();raw=gzip.decompress(raw) if path.suffix=='.gz' else raw;assert len(raw)==item['raw_bytes'] and hashlib.sha256(raw).hexdigest()==item['raw_sha256'],name
assert git('rev-parse','HEAD')==SOURCE;subprocess.run(['git','diff','--check'],cwd=R,check=True);subprocess.run(['git','add','--',str(d)],cwd=R,check=True);subprocess.run(['git','commit','-q','-m','Preserve actual Native origin, custody and publication IO qualification'],cwd=R,check=True)
assert not git('diff',SOURCE,'HEAD','--','crates','Cargo.lock','scripts','.github','POSTGRES_COMPATIBILITY.md')
record=dict(status='collected_committed_private_evidence',source=SOURCE,evidence_commit=git('rev-parse','HEAD'),directory=str(d),manifest_entries=len(manifest),raw_entries=12,qualification=result['qualification'],finished_unix=time.time());(S/'native-origin-root-collection.json').write_text(json.dumps(record,indent=2)+'\n');print(json.dumps(record,indent=2))
