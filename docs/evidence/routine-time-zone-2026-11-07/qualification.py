from pathlib import Path
import subprocess,os,json,re
os.umask(0o077)
root=Path('/root/ehr-bicdb-routine-time-zone-20261107');root.mkdir(mode=0o700)
state={'status':'IN_PROGRESS_NOT_ADMITTED','production_admitted':False,'source_commit':'acfc830f24aa33d3873d579fb59ce2ddeef9d0aa','patch_files':['crates/bicdb-sql/src/session/txn_update.rs','crates/bicdb-sql/tests/routine_time_zone.rs']}
def save(): (root/'status.json').write_text(json.dumps(state,indent=2)+'\n')
def run(label,args):
 state['status']=label;save()
 with(root/(label+'.status.txt')).open('x') as log:
  done=subprocess.run(['bash','/root/ehr-bicdb-monitored-command.sh',str(root/(label+'.txt')),'cargo','--config','profile.dev.debug=0',*args],stdout=log,stderr=subprocess.STDOUT,env={**os.environ,'EHR_BUILD_JOBS':'1'})
 assert done.returncode==0,(label,done.returncode)
try:
 focused=Path('/root/ehr-bicdb-routine-time-zone-after-20261107.log').read_text()
 assert 'test result: ok. 2 passed;' in focused,'focused timezone cases must pass first'
 run('sql-library',['test','-p','bicdb-sql','--lib','--','--test-threads=2'])
 run('sql-routine-integration',['test','-p','bicdb-sql','--test','routine_time_zone','--test','cold_routine_compilation_stack','--test','routine_expression_stack','--test','routine_assignment_types','--test','routine_cte_case_types','--test','routine_owner_execution_identity','--test','routine_pattern_context','--test','routine_privilege_lists','--test','ehr_routine_results','--test','lateral_routine_sources','--test','domain_types','--','--test-threads=1'])
 run('pgwire-protocol',['test','-p','bicdb-pgwire','--test','protocol','--test','procedural_migration','--','--test-threads=1'])
 run('pgwire-library',['test','-p','bicdb-pgwire','--lib','--','--test-threads=1'])
 state['status']='SOURCE_REGRESSIONS_VERIFIED_NOT_ADMITTED';save()
except Exception as error:
 state.update(status='FAILED_NOT_ADMITTED',error=str(error)[:250]);save();raise
