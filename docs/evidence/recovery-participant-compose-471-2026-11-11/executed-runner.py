import json,subprocess,os,signal,time,shutil,sys
from pathlib import Path
p=Path('/tmp/bicdb-range-recovery-authority-evidence');name=sys.argv[1];args=sys.argv[2:];cwd='/tmp/bicdb-range-recovery-authority'
env=os.environ.copy();env.update(CARGO_TARGET_DIR='/tmp/bicdb-azure-artifact-target',CARGO_BUILD_JOBS='1',CARGO_INCREMENTAL='0',RUST_TEST_THREADS='2')
command=['cargo','test','--release','--locked','-p','bicdb-core','--config','profile.release.package.bicdb-core.opt-level=0','--config','profile.release.package.bicdb-core.debug=0','--lib',*args]
start=time.time();paused=False;pause_start=None;minimum=10**30;reason=None;episodes=0
identity={'source_revision':subprocess.check_output(['git','rev-parse','HEAD'],cwd=cwd,text=True).strip(),'dirty_diff_sha256':__import__('hashlib').sha256(subprocess.check_output(['git','diff','HEAD'],cwd=cwd)).hexdigest(),'command':command,'environment':{k:env[k] for k in ['CARGO_TARGET_DIR','CARGO_BUILD_JOBS','CARGO_INCREMENTAL','RUST_TEST_THREADS']},'compiler':subprocess.check_output(['rustc','--version'],cwd=cwd,text=True).strip()}
(p/(name+'-inputs.json')).write_text(json.dumps(identity,indent=2)+'\n')
with (p/(name+'.stdout')).open('w') as out,(p/(name+'.stderr')).open('w') as err,(p/(name+'-resources.jsonl')).open('w') as samples:
 child=subprocess.Popen(command,cwd=cwd,env=env,stdout=out,stderr=err,start_new_session=True)
 while child.poll() is None:
  mem={x.split(':',1)[0]:int(x.split()[1])*1024 for x in Path('/proc/meminfo').read_text().splitlines()};available=mem['MemAvailable'];minimum=min(minimum,available);free=shutil.disk_usage('/tmp').free;pressure=Path('/proc/pressure/memory').read_text();full=float(next(x for x in pressure.splitlines() if x.startswith('full ')).split('avg10=')[1].split()[0]);samples.write(json.dumps({'unix':time.time(),'available_bytes':available,'swap_free_bytes':mem['SwapFree'],'disk_free_bytes':free,'memory_pressure':pressure,'paused':paused})+'\n');samples.flush()
  if (available<12*2**30 or full>1.0) and not paused:os.killpg(child.pid,signal.SIGSTOP);paused=True;pause_start=time.time();episodes+=1
  elif available>14*2**30 and full<0.1 and paused:os.killpg(child.pid,signal.SIGCONT);paused=False;pause_start=None
  if available<8*2**30 or free<8*2**30 or (pause_start and time.time()-pause_start>90) or time.time()-start>1800:
   reason='resource_or1800s_wall_guard'
   if paused:os.killpg(child.pid,signal.SIGCONT)
   os.killpg(child.pid,signal.SIGTERM)
   try:child.wait(timeout=20)
   except subprocess.TimeoutExpired:os.killpg(child.pid,signal.SIGKILL);child.wait()
   break
  time.sleep(1)
 result={**identity,'start_unix':start,'end_unix':time.time(),'exit_code':child.returncode,'guard_stop_reason':reason,'pause_episodes':episodes,'minimum_available_bytes':minimum};(p/(name+'-result.json')).write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result));raise SystemExit(child.returncode)
