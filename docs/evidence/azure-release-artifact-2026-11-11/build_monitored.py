import json, os, signal, subprocess, time, shutil
from pathlib import Path
root=Path('/tmp/bicdb-azure-artifact-evidence')
source=Path('/tmp/bicdb-azure-artifact-source')
expected='b05385d4b033edf0db03f3eccf4bdd86f7c8eddb'
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip()==expected
assert not subprocess.check_output(['git','status','--porcelain'],cwd=source,text=True).strip()
env=os.environ.copy();env.update(CARGO_TARGET_DIR='/tmp/bicdb-azure-artifact-target',CARGO_BUILD_JOBS='1',CARGO_INCREMENTAL='0')
cmd=['cargo','build','--release','--locked','-p','bicdb-cli']
start=time.time();minimum=10**30;paused=False;pauses=0;pause_start=None;reason=None
with (root/'build.log').open('w') as log,(root/'resources.jsonl').open('w') as samples:
 p=subprocess.Popen(cmd,cwd=source,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
 while p.poll() is None:
  mem={line.split(':',1)[0]:int(line.split()[1])*1024 for line in Path('/proc/meminfo').read_text().splitlines()}
  available=mem['MemAvailable'];minimum=min(minimum,available);free=shutil.disk_usage('/tmp').free
  samples.write(json.dumps({'time_unix':time.time(),'available_bytes':available,'swap_free_bytes':mem['SwapFree'],'disk_free_bytes':free,'paused':paused,'memory_pressure':Path('/proc/pressure/memory').read_text()})+'\n');samples.flush()
  if available<10*1024**3 and not paused:
   os.killpg(p.pid,signal.SIGSTOP);paused=True;pauses+=1;pause_start=time.time()
  elif available>12*1024**3 and paused:
   os.killpg(p.pid,signal.SIGCONT);paused=False;pause_start=None
  if free<10*1024**3 or available<6*1024**3 or (pause_start and time.time()-pause_start>90):
   reason='disk_headroom' if free<10*1024**3 else 'memory_headroom'
   if paused:os.killpg(p.pid,signal.SIGCONT)
   os.killpg(p.pid,signal.SIGTERM)
   try:p.wait(timeout=30)
   except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);p.wait()
   break
  time.sleep(2)
 result={'source_commit':expected,'command':cmd,'environment':{key:env[key] for key in ['CARGO_TARGET_DIR','CARGO_BUILD_JOBS','CARGO_INCREMENTAL']},'started_at_unix':start,'ended_at_unix':time.time(),'exit_code':p.returncode,'resource_stop_reason':reason,'min_available_bytes':minimum,'resource_pause_count':pauses,'compiler':subprocess.check_output(['rustc','--version'],cwd=source,text=True).strip()}
 (root/'result.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result),flush=True)
 raise SystemExit(p.returncode)
