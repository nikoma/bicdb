from pathlib import Path
import hashlib,json,resource,subprocess
B=Path('/root/bicdb-durability-s3');ROOT=B/'oracle-case-oldreader-proof';DB=ROOT/'fixture';assert DB.is_dir()
BIN=B/'skr/bin/commit-bench-main-0af1b55e-release'
expected='9cf83ba35684055bf09f77b1e6c1d7cb7b17eef60fcca300c29698e0f64584bb'
assert hashlib.sha256(BIN.read_bytes()).hexdigest()==expected

def snapshot():
 return {str(p.relative_to(DB)):{'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'size':p.stat().st_size,'mtime_ns':p.stat().st_mtime_ns} for p in sorted(DB.rglob('*')) if p.is_file()}
before=snapshot();resource.setrlimit(resource.RLIMIT_CORE,(0,0))
command=[str(BIN),'recover',str(DB),'all-logs'];result=subprocess.run(command,capture_output=True,text=True,timeout=10)
(ROOT/'oldreader.stdout').write_text(result.stdout);(ROOT/'oldreader.stderr').write_text(result.stderr)
after=snapshot();assert result.returncode!=0;assert 'unknown format feature flag `oracle_simple_case_v1`' in result.stderr,result.stderr
assert before==after,'old reader changed the modern store before refusing it'
proof={'complete':True,'writer_source':'f039cdf0','reader_source':'0af1b55e3d586d4a6933098419e7ee46a7ae5a79','reader_binary_sha256':expected,'command':command,'exit_code':result.returncode,'unknown_feature_refusal':True,'all_file_sha256_size_mtime_and_listing_unchanged':True,'files':before,'stderr':result.stderr,'scope':'Real old native database open of a store with actual persisted Oracle-v1 and legacy indexes. Raw generated signing material is retained outside repository evidence.'}
(ROOT/'proof.json').write_text(json.dumps(proof,indent=2)+'\n');print(json.dumps({k:v for k,v in proof.items() if k not in ['files','stderr','command']}))
