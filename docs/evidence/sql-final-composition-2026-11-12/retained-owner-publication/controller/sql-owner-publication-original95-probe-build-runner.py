import fcntl,hashlib,json,os,pathlib,re,shutil,subprocess,sys,time
spec=json.loads(pathlib.Path(sys.argv[1]).read_text());root=pathlib.Path('/root/bicdb-durability-s3');repo=root/spec.get('remote_repo_name','repo-tc');label=spec['label'];source=spec['source']
# Preserve every active build and its source tree. Observation delays never
# cause an existing compiler to be stopped or restarted.
for wait_unit in spec.get('wait_units', []):
 while subprocess.check_output(['systemctl','show',wait_unit,'-p','ActiveState','--value'],text=True).strip() in ['active','activating','reloading']:time.sleep(5)
for prerequisite in spec.get('required_receipts', []):
 evidence=json.loads((root/prerequisite['path']).read_text())
 for key,value in prerequisite['equals'].items():assert evidence.get(key)==value,(prerequisite['path'],key,evidence.get(key),value)
source_lock=(root/'host-control'/spec.get('remote_lock_name','source-build.lock')).open('a');fcntl.flock(source_lock,fcntl.LOCK_EX)
def process_identity(pid):
 try:
  proc=pathlib.Path('/proc')/str(pid);stat=(proc/'stat').read_text().rsplit(')',1)[1].split()
  return {'parent':int(stat[1]),'start_ticks':int(stat[19]),'cwd':str((proc/'cwd').resolve()),'argv':(proc/'cmdline').read_bytes().split(b'\0')}
 except FileNotFoundError:return None
def known_isolated_compiler(pid):
 current=process_identity(pid)
 if current is None:return True
 for allowed in spec.get('allow_isolated_builds',[]):
  # An exact process birth, source and separate target are required. This is
  # not a general exemption for other compilers or a new run with a reused PID.
  assert pathlib.Path(allowed['repo']).resolve()!=repo.resolve()
  assert pathlib.Path(allowed['target']).resolve()!=pathlib.Path(os.environ['CARGO_TARGET_DIR']).resolve()
  parent=process_identity(allowed['pid'])
  if parent is None or parent['start_ticks']!=allowed['start_ticks'] or parent['cwd']!=allowed['repo']:continue
  if current['cwd']!=allowed['repo']:continue
  if pid==allowed['pid']:return True
  if current['parent']!=allowed['pid']:continue
  args=current['argv']
  if b'--out-dir' in args:
   target=pathlib.Path(os.fsdecode(args[args.index(b'--out-dir')+1])).resolve()
   if target.is_relative_to(pathlib.Path(allowed['target']).resolve()):return True
 return False
def conflicting_compiler():
 active=[]
 for name in ['cargo','rustc']:
  found=subprocess.run(['pgrep','-x',name],text=True,capture_output=True)
  if found.returncode not in (0,1):return True
  active.extend(int(pid) for pid in found.stdout.split())
 if any(not known_isolated_compiler(pid) for pid in active):return True
 if active:
  available=int(next(line.split()[1] for line in pathlib.Path('/proc/meminfo').read_text().splitlines() if line.startswith('MemAvailable:')))
  # Our cgroup is capped at16GiB; retain the monitor's10GiB host floor plus
  #2GiB before allowing the sole known isolated build to overlap this one.
  if available<28*1024*1024:return True
 return False
while conflicting_compiler():time.sleep(5)
assert not subprocess.check_output(['git','status','--porcelain','--untracked-files=no'],cwd=repo,text=True)
subprocess.run(['git','fetch',str(root/'host-control'/(label+'.bundle')),spec.get('source_ref','HEAD')+':refs/heads/task-'+label],cwd=repo,check=True)
assert subprocess.check_output(['git','rev-parse','refs/heads/task-'+label],cwd=repo,text=True).strip()==source
subprocess.run(['git','checkout','--detach',source],cwd=repo,check=True)
receipt=root/('report-'+label+'.json');assert not receipt.exists()
record={'source':source,'scope':spec['scope'],'started_unix':time.time(),'steps':[],'status':'running'}
def save():receipt.write_text(json.dumps(record,indent=2)+'\n')
save()
for command in spec['commands']:
 step={'command':command,'started_unix':time.time()};record['steps'].append(step);save()
 step.update(exit=subprocess.call(command,cwd=repo),finished_unix=time.time());save()
 if step['exit']!=0 and not spec.get('continue_on_failure'):break
if all(s['exit']==0 for s in record['steps']) and len(record['steps'])==len(spec['commands']):
 record['status']='focused_passed'
 if spec.get('copy_example'):
  output=root/'qualification-artifacts-debug'/source;output.mkdir(exist_ok=False);binary=output/spec['copy_example'];shutil.copy2(pathlib.Path(os.environ['CARGO_TARGET_DIR'])/'debug/examples'/spec['copy_example'],binary)
  record.update(binary=str(binary),binary_sha256=hashlib.file_digest(binary.open('rb'),'sha256').hexdigest())
  for extra in spec.get('copy_additional_examples', []):
   assert re.fullmatch('[a-z0-9_]+', extra) and extra != spec['copy_example']
   added=output/extra;shutil.copy2(pathlib.Path(os.environ['CARGO_TARGET_DIR'])/'debug/examples'/extra,added)
   record.setdefault('additional_binaries', {})[extra]={'binary':str(added),'binary_sha256':hashlib.file_digest(added.open('rb'),'sha256').hexdigest()}
else:record['status']='failed'
record['finished_unix']=time.time();save();print(json.dumps(record),flush=True)
raise SystemExit(0 if record['status']=='focused_passed' else 1)
