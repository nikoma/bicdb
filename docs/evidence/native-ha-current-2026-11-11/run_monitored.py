import json,os,signal,subprocess,sys,time,shutil
from pathlib import Path
root=Path('/tmp/bicdb-ha-current-evidence')
name=sys.argv[1];cmd=sys.argv[2:]
env=os.environ.copy();env.update(CARGO_TARGET_DIR='/tmp/bicdb-ha-qualification-target',CARGO_BUILD_JOBS='2',CARGO_INCREMENTAL='0',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0',RUST_TEST_THREADS='2')
start=time.time();min_avail=10**30;paused=False;pause_count=0
with (root/(name+'.log')).open('w') as log,(root/(name+'.resources.jsonl')).open('w') as samples:
 p=subprocess.Popen(cmd,stdout=log,stderr=subprocess.STDOUT,env=env,start_new_session=True)
 while p.poll() is None:
  mem={a.strip(':'):int(b) for a,b,*_ in (line.split() for line in Path('/proc/meminfo').read_text().splitlines())}
  avail=mem['MemAvailable']*1024;min_avail=min(min_avail,avail)
  samples.write(json.dumps({'time_unix':time.time(),'available_bytes':avail,'swap_free_bytes':mem['SwapFree']*1024,'disk_free_bytes':shutil.disk_usage('/tmp').free,'paused':paused,'memory_pressure':Path('/proc/pressure/memory').read_text()})+'\n');samples.flush()
  if avail < 10*1024**3 and not paused:
   os.killpg(p.pid,signal.SIGSTOP);paused=True;pause_count+=1
  elif avail > 12*1024**3 and paused:
   os.killpg(p.pid,signal.SIGCONT);paused=False
  if shutil.disk_usage('/tmp').free < 5*1024**3:
   if paused:os.killpg(p.pid,signal.SIGCONT)
   os.killpg(p.pid,signal.SIGTERM)
  time.sleep(2)
 result={'command':cmd,'environment':{k:env[k] for k in ('CARGO_TARGET_DIR','CARGO_BUILD_JOBS','CARGO_INCREMENTAL','CARGO_PROFILE_DEV_DEBUG','CARGO_PROFILE_TEST_DEBUG','RUST_TEST_THREADS')},'started_at_unix':start,'ended_at_unix':time.time(),'exit_code':p.returncode,'min_available_bytes':min_avail,'resource_pause_count':pause_count,'source_commit':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()}
 (root/(name+'.result.json')).write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result));sys.exit(p.returncode)
