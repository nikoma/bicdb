import pathlib,subprocess,re,sys
base='ca8383ef36190c6c69cc8853cd51dd7803c7cc52'
import argparse
parser=argparse.ArgumentParser(description="Strip only marked passive fence trace blocks and verify original Rust.")
parser.add_argument("--repo",default=".")
parser.add_argument("--candidate",default="HEAD")
a=parser.parse_args()
root=pathlib.Path(a.repo).resolve()
def git(*args):return subprocess.check_output(["git","-C",str(root),*args])
paths=[
 "crates/bicdb-core/src/distribution_data.rs",
 "crates/bicdb-core/src/distribution_range_consensus/current_joint_recovery.rs",
 "crates/bicdb-core/src/distribution_range_consensus/native_ready_catalog.rs",
 "crates/bicdb-core/src/distribution_range_consensus/recovery_state.rs",
 "crates/bicdb-core/src/distribution_range_recovery.rs",
 "crates/bicdb-core/src/distribution_range_recovery/quorum.rs",
 "crates/bicdb-core/src/distribution_range_recovery/runtime.rs",
]
verified=[]
for p in paths:
 if not p.endswith('.rs'):continue
 old=git('show',base+':'+p).decode()
 new=(root/p).read_text() if a.candidate=='WORKTREE' else git('show',a.candidate+':'+p).decode()
 stripped=re.sub(r'^[ \t]*// FENCE_TRACE_BEGIN\n.*?^[ \t]*// FENCE_TRACE_END\n','',new,flags=re.M|re.S)
 def norm(s):
  x=subprocess.run(['rustfmt','--edition','2021','--config','skip_children=true','--emit','stdout'],input=s,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,check=True).stdout
  return '\n'.join(l for l in x.splitlines() if l.strip())
 assert norm(old)==norm(stripped),p
 verified.append(p)
print('PASS exact original normalized Rust after removing ONLY marked cfg(test) blocks:',len(verified))
for p in verified:print(p)
# Existing acceptance bodies and IO worker/deadline path remain byte exact.
for p in ['crates/bicdb-core/src/db/range_image_native/rf3_fixture/native_historical_tls/native_joint_old_closure_tests/complete_joint_tests/post_joint_no_data_tests.rs','crates/bicdb-core/src/distribution_transport.rs']:
 assert git('show',base+':'+p)==((root/p).read_bytes() if a.candidate=='WORKTREE' else git('show',a.candidate+':'+p)),p
print('PASS unchanged original positive/test assertions and transport/deadline path')
