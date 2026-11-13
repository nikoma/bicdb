import pathlib,json,subprocess,time,hashlib,shutil,sys
root=pathlib.Path('/root/bicdb-durability-s3');label='partition-copy-pk-probes';unit='bicdb-'+label
while subprocess.check_output(['systemctl','show',unit,'-p','ActiveState','--value'],text=True).strip() in ['active','activating','reloading']:time.sleep(2)
build=json.loads((root/('report-'+label+'.json')).read_text());assert build['status']=='focused_passed',build
sql=pathlib.Path(build['binary']);startup_info=build['additional_binaries']['partition_startup_probe'];startup=pathlib.Path(startup_info['binary'])
for binary,expected in [(sql,build['binary_sha256']),(startup,startup_info['binary_sha256'])]:assert hashlib.file_digest(binary.open('rb'),'sha256').hexdigest()==expected
old=root/'partition-old-store-fixtures-482-20261112';fixtures=json.loads((old/'result.json').read_text());out=root/'partition-entry-startup-8ed4a67b';out.mkdir(exist_ok=False)
record={'status':'running','source':build['source'],'sql_binary':str(sql),'sql_binary_sha256':build['binary_sha256'],'startup_binary':str(startup),'startup_binary_sha256':startup_info['binary_sha256'],'old_source':fixtures['source'],'old_binary_sha256':fixtures['binary_sha256'],'started_unix':time.time(),'cases':[]}
def save():(out/'result.json').write_text(json.dumps(record,indent=2)+'\n')
def hashes(path):return {str(p.relative_to(path)):hashlib.file_digest(p.open('rb'),'sha256').hexdigest() for p in path.rglob('*') if p.is_file() and not p.is_symlink()}
def verify(case):assert hashes(pathlib.Path(case['directory'])/'db')=={name:info['sha256'] for name,info in case['files'].items()}
def run(mode,kind,fixture):
 verify(fixture);name=mode+'-'+kind;dest=out/(name+'-db');shutil.copytree(pathlib.Path(fixture['directory'])/'db',dest)
 item={'mode':mode,'case':kind,'database_copy':str(dest)}
 if kind=='entry_procedure':
  ddl="CREATE PROCEDURE add_partition_ack() LANGUAGE plpgsql AS $$ BEGIN INSERT INTO p VALUES(3,'entry-procedure-ack'); END; $$"
  setup=subprocess.run([fixtures['binary'],'sql',str(dest),ddl,'--json'],text=True,capture_output=True)
  (out/(name+'.old-setup.stdout')).write_text(setup.stdout);(out/(name+'.old-setup.stderr')).write_text(setup.stderr);item['old_procedure_setup_exit']=setup.returncode
  if setup.returncode:
   item.update(exit=None,harness_setup_failed=True);record['cases'].append(item);verify(fixture);save();return
 binary=sql if kind.startswith('entry_') else startup
 case=kind.removeprefix('startup_')
 result=subprocess.run([str(binary),mode,case,str(dest)],text=True,capture_output=True)
 (out/(name+'.stdout')).write_text(result.stdout);(out/(name+'.stderr')).write_text(result.stderr);item.update(exit=result.returncode,stdout=name+'.stdout',stderr=name+'.stderr')
 if result.returncode==0 and (kind.startswith('entry_') or kind=='startup_parent_heap'):
  before=hashes(dest);reader=subprocess.run([fixtures['binary'],'sql',str(dest),'SELECT k FROM p ORDER BY k','--json'],text=True,capture_output=True);after=hashes(dest)
  (out/(name+'.old-reader.stdout')).write_text(reader.stdout);(out/(name+'.old-reader.stderr')).write_text(reader.stderr)
  item.update(old_reader_exit=reader.returncode,old_reader_refused=reader.returncode!=0,byte_identity_preserved=before==after,changed_files=sorted(k for k in set(before)|set(after) if before.get(k)!=after.get(k)))
 verify(fixture);record['cases'].append(item);save()
for mode in ['embedded_memory','server_paged']:
 parent=next(c for c in fixtures['cases'] if c['mode']==mode and c['name']=='parent_heap')
 for kind in ['entry_text','entry_ast','entry_copy','entry_procedure']:run(mode,kind,parent)
 for name in ['parent_heap','bad_child_bound','independent_child_policy']:
  fixture=next(c for c in fixtures['cases'] if c['mode']==mode and c['name']==name);run(mode,'startup_'+name,fixture)
passed=all(c['exit']==0 and (not (c['case'].startswith('entry_') or c['case']=='startup_parent_heap') or (c.get('old_reader_refused') and c.get('byte_identity_preserved'))) for c in record['cases'])
record.update(status='entry_startup_passed' if passed else 'entry_startup_failed',finished_unix=time.time(),originals_unchanged=True,independent_old_fixture_acks=14,scope='Eight genuineold482parentfixture entry cases: SQLtext/AST/COPY/procedure, each firstproves readonlyengine55000+sharedsessionexclusivefallback with zero Nativecommits before repair; exactfullrecordhashes foralloldACKspreserved plus2reopens, physicalparentempty, Nativeindexes. Proceduremetadata producedbyactualold482CLI oncopy. Sixstartup-only serverconstructorcases: parentauto-upgrade, invalidbounds/ambiguouspolicies55000beforelistenerreturned, ACK/policyretained,2reopens. All10successfulupgradedcases actualold482readerrefusal+unchangedbytes. Not fullwireprotocol/listener, broad/app/fault/HAqualification.');save();print(json.dumps(record));sys.exit(0 if passed else 1)
