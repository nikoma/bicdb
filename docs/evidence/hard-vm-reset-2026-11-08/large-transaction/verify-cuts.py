from pathlib import Path
import hashlib,json,subprocess,time
BASE=Path('/root/bicdb-durability-s3/vm-reset')
RUN=BASE/'large-tx-campaign-1794202882515055041'
OUT=RUN/'independent-verifier';OUT.mkdir(exist_ok=False)
BIN=Path('/root/bicdb-durability-s3/target-verify/debug/bicdb')
def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(1048576),b''):h.update(b)
 return h.hexdigest()
def snap(root):return {str(p.relative_to(root)):{'sha256':sha(p),'size':p.stat().st_size,'mtime_ns':p.stat().st_mtime_ns} for p in sorted(root.rglob('*')) if p.is_file()}
manifest={'verifier_source':'26051caf','binary_sha256':sha(BIN),'scope':'Journal recovery on disposable copies only; engine never opened. Original disk metadata and extracted file bytes/metadata verified unchanged.','cuts':[]}
for raw in sorted(RUN.glob('crashed-*.raw')):
 before=(raw.stat().st_size,raw.stat().st_mtime_ns)
 task=OUT/raw.stem;task.mkdir();copy=task/'journal-replay.raw'
 subprocess.run(['cp','--reflink=auto','--sparse=always',str(raw),str(copy)],check=True)
 fsck=subprocess.run(['e2fsck','-p',str(copy)],capture_output=True,text=True)
 (task/'e2fsck.log').write_text(fsck.stdout+fsck.stderr);assert fsck.returncode in (0,1),fsck.returncode
 extracted=task/'extracted';extracted.mkdir()
 dump=subprocess.run(['debugfs','-R',f'rdump /db/paged {extracted}',str(copy)],capture_output=True,text=True)
 (task/'debugfs.log').write_text(dump.stdout+dump.stderr)
 paged=extracted/'paged';assert (paged/'store.pages').is_file(),dump.stderr
 files=snap(paged)
 command=[str(BIN),'verify-store','--read-only',str(paged),'--max-memory-mb','64','--max-seconds','60']
 result=subprocess.run(command,capture_output=True,text=True,timeout=90)
 (OUT/(raw.stem+'.json')).write_text(result.stdout)
 (task/'verifier.stderr').write_text(result.stderr)
 assert files==snap(paged),'verifier changed extracted files'
 assert before==(raw.stat().st_size,raw.stat().st_mtime_ns),'original disk metadata changed'
 report=json.loads(result.stdout)
 item={'cut':raw.stem,'command':command,'exit_code':result.returncode,'report_sha256':sha(OUT/(raw.stem+'.json')),'original_size_mtime_unchanged':True,'extracted_files_unchanged':True,'page_file_bytes':(paged/'store.pages').stat().st_size,'files':files}
 manifest['cuts'].append(item);(OUT/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
 print(json.dumps({'cut':raw.stem,'exit_code':result.returncode,'page_file_bytes':item['page_file_bytes']}),flush=True)
 assert result.returncode==0,(raw,result.returncode,report)
 # Disposable extraction and raw copy are retained until evidence review.
assert len(manifest['cuts'])==7
print('ALL_SEVEN_CUTS_CLEAN',flush=True)
