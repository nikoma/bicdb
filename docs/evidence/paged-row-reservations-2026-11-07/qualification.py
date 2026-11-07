from pathlib import Path
import subprocess,json,hashlib,shutil,os,re
os.umask(0o077)
root=Path('/root/ehr-bicdb-migration-20261104');out=Path('/root/ehr-bicdb-paged-reservations-20261107')
revision='032c7c1df192e9fc1812867e6d787bc6059d5cb4'
state={'status':'IN_PROGRESS','production_admitted':False,'source_commit':revision}
def note(name,**kw):
 state.update(status=name,**kw);(out/'status.json').write_text(json.dumps(state,indent=2)+'\n');print(json.dumps(state),flush=True)
def run(label,args,expected=0):
 note(label.upper())
 with(out/(label+'.status.txt')).open('x')as log:
  result=subprocess.run(['bash','/root/ehr-bicdb-monitored-command.sh',str(out/(label+'.txt')),'cargo','--config','profile.dev.debug=0',*args],stdout=log,stderr=subprocess.STDOUT,env={**os.environ,'EHR_BUILD_JOBS':'1'})
 output=(out/(label+'.txt')).read_text();assert not re.search(r'error\[E[0-9]+\]|could not compile',output),label+' compile failure'
 assert result.returncode==expected,(label,result.returncode)
 return output
def verify(manifest):
 for item in manifest['files']:assert hashlib.sha256((root/item['path']).read_bytes()).hexdigest()==item['sha256'],item['path']
try:
 before=json.loads((out/'source.before.json').read_text());assert before['source_commit']=='ef12deca38d6b0bd80be89861235740eb936e8dc';verify(before)
 shutil.copyfile(out/'paged_row_reservations.rs',root/'crates/bicdb-core/tests/paged_row_reservations.rs')
 shutil.copyfile(out/'sql.tests.rs',root/'crates/bicdb-sql/src/tests.rs')
 baseline=run('core-before',['test','-p','bicdb-core','--test','paged_row_reservations','--','--test-threads=1'],101)
 assert 'cold_baselines_remain_present_for_every_read_committed_reservation ... FAILED'in baseline
 baseline=run('sql-before',['test','-p','bicdb-sql','--lib','server_paged_identity_trigger_and_row_lock_survive_cold_reopen','--','--test-threads=1'],101)
 assert 'meal_cursor_version_seq'in baseline and 'does not exist'in baseline
except Exception as error:
 note('FAILED_NOT_ADMITTED',error_type=type(error).__name__,error=str(error)[:240]);raise
finally:
 # Always restore the actual fixed source, including when a baseline is invalid.
 shutil.copyfile(out/'db.fixed.rs',root/'crates/bicdb-core/src/db.rs')
 shutil.copyfile(out/'source.fixed.json',root/'source-manifest.json')
try:
 manifest=json.loads((root/'source-manifest.json').read_text());assert manifest['source_commit']==revision;verify(manifest)
 run('core-after',['test','-p','bicdb-core','--test','paged_row_reservations','--','--test-threads=1'])
 run('sql-after',['test','-p','bicdb-sql','--lib','server_paged_identity_trigger_and_row_lock_survive_cold_reopen','--','--test-threads=1'])
 run('core-transactions',['test','-p','bicdb-core','--test','transaction_isolation','--test','mutation_grants','--test','transaction_rollback_mark','--test','paged_concurrency','--','--test-threads=1'])
 run('core-library',['test','-p','bicdb-core','--lib','--','--test-threads=1'])
 run('sql-library',['test','-p','bicdb-sql','--lib','--','--test-threads=1'])
 run('protocol',['test','-p','bicdb-pgwire','--test','protocol','--','--test-threads=1'])
 run('app-host',['test','-p','bicdb-app-host','--test','database','--test','replicated_state','--test','recovery','--test','sql_listener','--test','state_failover','--test','deployment','--test','private_runtime','--','--test-threads=1'])
 verify(manifest);note('TESTS_PASSED_PENDING_MAIN',source_manifest_sha256=hashlib.sha256((root/'source-manifest.json').read_bytes()).hexdigest())
except Exception as error:note('FAILED_NOT_ADMITTED',error_type=type(error).__name__,error=str(error)[:240]);raise
