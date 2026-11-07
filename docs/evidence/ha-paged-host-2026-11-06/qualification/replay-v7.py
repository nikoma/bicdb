from pathlib import Path
import subprocess,json,hashlib,shutil,os,re
os.umask(0o077)
root=Path('/root/ehr-bicdb-migration-20261104');inputs=Path('/root/ehr-bicdb-paged-idle-final-inputs-20261106');out=Path('/root/ehr-bicdb-paged-idle-qualification-20261106-v7');out.mkdir(mode=0o700)
state={'status':'IN_PROGRESS','production_admitted':False,'source_commit':'82a1ad0197837f4dd0209b8759c9f32cf55dc646'}
def note(status,**kwargs):
 state.update(status=status,**kwargs);(out/'status.json').write_text(json.dumps(state,indent=2)+'\n');print(json.dumps({'status':status,**kwargs}),flush=True)
def run(label,args,expected=0):
 note(label.upper())
 with (out/(label+'.status.log')).open('x') as f:
  result=subprocess.run(['bash','/root/ehr-bicdb-monitored-command.sh',str(out/(label+'.log')),'cargo','--config','profile.dev.debug=0',*args],stdout=f,stderr=subprocess.STDOUT,env={**os.environ,'EHR_BUILD_JOBS':'1'})
 text=(out/(label+'.log')).read_text();assert not re.search(r'error\[E[0-9]+\]|could not compile',text),label+' compile failure'
 assert result.returncode==expected,(label,result.returncode)
 return text
def replace_control(label,path,control,args,match):
 p=root/path;fixed=p.read_bytes();p.write_bytes(control)
 try:
  text=run(label,args,101);assert match in text,(label,'expected semantic failure missing')
 finally:p.write_bytes(fixed)
try:
 manifest=json.loads((root/'source-manifest.json').read_text());assert manifest['source_commit']=='82a1ad0197837f4dd0209b8759c9f32cf55dc646'
 hashes={entry['path']:entry['sha256']for entry in manifest['files']}
 for f,h in hashes.items():assert hashlib.sha256((root/f).read_bytes()).hexdigest()==h,f
 p=root/'crates/bicdb-pgwire/src/lib.rs';fixed=p.read_text();control=fixed.replace('                // Execution and reply flushing are busy time, including COPY.\n                // Give the client its full idle budget after the reply.\n                last_activity = Instant::now();','').replace('        stream.flush()?;\n        last_activity = Instant::now();','        stream.flush()?;')
 assert control!=fixed
 replace_control('idle-before','crates/bicdb-pgwire/src/lib.rs',control.encode(),['test','-p','bicdb-pgwire','--test','protocol','idle_timeout_excludes_','--','--test-threads=1'],'test result: FAILED. 0 passed; 3 failed;')
 before=(out/'idle-before.log').read_text();assert 'function pg_sleep is not supported' not in before and 'fixture must actually execute'not in before
 run('idle-after',['test','-p','bicdb-pgwire','--test','protocol','idle_timeout_excludes_','--','--test-threads=1'])
 run('core-index-regressions',['test','-p','bicdb-core','--test','paged_fts_postings','--test','paged_index_entries','--test','unique_index_build_identity','--test','transaction_rollback_mark','--test','paged_concurrency','--','--test-threads=1'])
 run('sql-library',['test','-p','bicdb-sql','--lib','--','--test-threads=1'])
 run('core-library',['test','-p','bicdb-core','--lib','--','--test-threads=1'])
 run('app-host',['test','-p','bicdb-app-host','--test','database','--test','replicated_state','--test','recovery','--test','sql_listener','--test','state_failover','--test','deployment','--test','private_runtime','--','--test-threads=1'])
 run('protocol',['test','-p','bicdb-pgwire','--test','protocol','--','--test-threads=1'])
 run('pgwire-library',['test','-p','bicdb-pgwire','--lib','--','--test-threads=1'])
 for f,h in hashes.items():assert hashlib.sha256((root/f).read_bytes()).hexdigest()==h,f
 note('TESTS_PASSED_PENDING_MAIN',source_manifest_sha256=hashlib.sha256((root/'source-manifest.json').read_bytes()).hexdigest())
except Exception as error:
 note('FAILED_NOT_ADMITTED',error_type=type(error).__name__,error=str(error)[:250]);raise
