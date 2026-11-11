import json,os,signal,subprocess,time,shutil,hashlib
from pathlib import Path
source=Path('/tmp/bicdb-native-range-image-install'); out=Path('/tmp/bicdb-native-image-history-integration-evidence'); out.mkdir(exist_ok=True)
expected='8a7ddf6ead2bce6e3a41e04fdf68af1a7df3cf57'
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip(); assert head.startswith(expected)
assert not subprocess.check_output(['git','status','--porcelain'],cwd=source,text=True).strip()
files=['crates/bicdb-core/src/lib.rs','crates/bicdb-core/src/distribution_range_consensus.rs','crates/bicdb-core/src/distribution_range_consensus/image_install_fence.rs','crates/bicdb-core/src/distribution_range_consensus/image_install_state.rs','crates/bicdb-core/src/db/range_image_install.rs','crates/bicdb-core/src/distribution_range_image_install.rs','crates/bicdb-core/src/distribution_range_image.rs','crates/bicdb-core/src/distribution_range_image/source_cut.rs']
manifest={'source_commit':head,'files':{p:hashlib.sha256((source/p).read_bytes()).hexdigest() for p in files},'scope':'focused composed source/R1target/history software tests; no CurrentRepair factory/activation/physical qualification'}
(out/'capture-controls-source-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
# Exact unchanged source uses the already-compiled executable from attempt5.
prior=out/'attempt-5-image-install.result.json'
assert json.loads(prior.read_text())['source_commit']==head
binary=Path('/tmp/bicdb-azure-artifact-target/release/deps/bicdb_core-2804d0c8b396b15a')
manifest['binary_sha256']=hashlib.sha256(binary.read_bytes()).hexdigest()
manifest['prior_compile_receipt_sha256']=hashlib.sha256(prior.read_bytes()).hexdigest()
env=os.environ.copy();env.update(CARGO_TARGET_DIR='/tmp/bicdb-azure-artifact-target',CARGO_BUILD_JOBS='1',CARGO_INCREMENTAL='0',RUST_TEST_THREADS='2')
cmd=[str(binary),'source_lineage_requires_exact_committed_learner_transition_and_native_bindings','--test-threads=2']
start=time.time();minimum=2**64;paused=False;pauses=0;pause_start=None;reason=None
with (out/'capture-controls.log').open('w') as log,(out/'capture-controls.resources.jsonl').open('w') as samples:
 p=subprocess.Popen(cmd,cwd=source,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
 while p.poll() is None:
  mem={line.split(':',1)[0]:int(line.split()[1])*1024 for line in Path('/proc/meminfo').read_text().splitlines()}
  available=mem['MemAvailable'];minimum=min(minimum,available);free=shutil.disk_usage('/tmp').free
  samples.write(json.dumps({'time_unix':time.time(),'available_bytes':available,'swap_free_bytes':mem['SwapFree'],'disk_free_bytes':free,'paused':paused,'memory_pressure':Path('/proc/pressure/memory').read_text()})+'\n');samples.flush()
  if available<12*1024**3 and not paused:
   os.killpg(p.pid,signal.SIGSTOP);paused=True;pauses+=1;pause_start=time.time()
  elif available>14*1024**3 and paused:
   os.killpg(p.pid,signal.SIGCONT);paused=False;pause_start=None
  if free<8*1024**3 or available<8*1024**3 or (pause_start and time.time()-pause_start>90) or time.time()-start>1800:
   reason='disk_headroom' if free<8*1024**3 else ('time_bound' if time.time()-start>1800 else 'memory_headroom')
   if paused:os.killpg(p.pid,signal.SIGCONT)
   os.killpg(p.pid,signal.SIGTERM)
   try:p.wait(timeout=20)
   except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);p.wait()
   break
  time.sleep(2)
 result={'source_commit':head,'command':cmd,'started_at_unix':start,'ended_at_unix':time.time(),'exit_code':p.returncode,'resource_stop_reason':reason,'min_available_bytes':minimum,'resource_pause_count':pauses,'compiler':subprocess.check_output(['rustc','--version'],cwd=source,text=True).strip(),'environment':{key:env[key] for key in ['CARGO_TARGET_DIR','CARGO_BUILD_JOBS','CARGO_INCREMENTAL','RUST_TEST_THREADS']}}
 (out/'capture-controls.result.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result),flush=True)
 raise SystemExit(p.returncode)
