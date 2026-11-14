#!/usr/bin/env python3
"""Verify retained expiry-control evidence without opening any fixture DB."""
import argparse,hashlib,json,pathlib,re,zipfile
ANSI=re.compile(r'\x1b\[[0-9;]*m')
SELECTOR='native_registered_fifth_old_round_expiry_then_fresh_mq_counts_same_live_closes'

def require(value,why):
 if not value:raise ValueError(why)

def digest(value):return hashlib.sha256(value).hexdigest()

def verify(path,expected_zip,source):
 require(digest(path.read_bytes())==expected_zip,'projection digest differs')
 with zipfile.ZipFile(path) as z:
  names=z.namelist();require(len(names)==len(set(names)) and sum(i.file_size for i in z.infolist())<256*1024*1024,'invalid ZIP bounds')
  require(all('/custody/' not in n and not pathlib.PurePosixPath(n).is_absolute() and '..' not in pathlib.PurePosixPath(n).parts for n in names),'unsafe/custody path')
  require(z.read('source-revision.txt').decode().strip()==source,'source differs')
  selected=[l[:-6] for l in ANSI.sub('',z.read('selected-tests.log').decode()).splitlines() if l.endswith(': test')]
  logs=ANSI.sub('',z.read('tests.log').decode());executed=re.findall(r'^test ([^\s]+) \.\.\. (ok|FAILED|ignored)$',logs,re.M)
  require(len(selected)==len(executed)==1 and selected[0]==executed[0][0] and selected[0].endswith(SELECTOR),'not exactly one selected/executed expiry control')
  identities=[n for n in names if n.endswith('/execution-identity.json')];require(len(identities)==1,'not exactly one actual fixture identity')
  identity=json.loads(z.read(identities[0]));prefix=identities[0].rsplit('/',1)[0]+'/'
  require(identity['source_commit']==source and identity['actual_rust_test_thread']==selected[0] and identity['source_case']=='LocalOldExpiredRoundContinuation','actual fixture identity differs')
  terminal=json.loads(z.read(prefix+'terminal.json'));ledger=json.loads(z.read(prefix+'ack-ledger.json'))
  for op in ledger:
   value=op['value'];require(digest(json.dumps(value,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode())==op['payload_json_sha256'],'APP payload hash differs')
   if op.get('actual_api_inserted') is True:require(op['actual_full_record']['metadata']==value,'ACK full local record differs')
  report_name=prefix+'old-round-expiry-continuation.json'
  control=json.loads(z.read(report_name)) if report_name in names else None
  complete=control is not None and control['phase']=='complete_same_actual_close_facts_counted_under_distinct_mq'
  validations=[]
  if control is not None:
   require(control['original_budget_ms']==1000 and control['fresh_standard_budget_ms']==5000,'original/fresh budgets changed')
   require(control['no_caller_cancellation_claim'] and control['component_only'] and control['no_partial_debt_or_unclosable_peer_in_this_control'],'control scope differs')
  if complete:
   require(executed[0][1]=='ok','completed control contradicts actual CI result')
   require(control['original_record_error'].endswith('metadata observation request changed or expired'),'original MQ rejection differs')
   old,fresh=control['original_mq'],control['fresh_mq'];require(old['business']!=fresh['business'] and old['context']!=fresh['context'],'fresh MQ renewed original request/context')
   require(fresh['term']>=old['term'] and fresh['read_index']>=old['read_index'],'fresh MQ regressed actual authority')
   closes=control['closes'];require(len(closes)==3 and len({c['node'] for c in closes})==3 and set(c['node'] for c in closes)==set(control['all_old_nodes']),'not all actual old voters covered')
   facts=[c['actual_close'] for c in closes];require(all(c['actual_error'] is None for c in closes),'actual close failed')
   require(all(f['scope_sha256']==control['scope_sha256'] for f in facts),'actual close scope changed')
   require(control['same_actual_close_facts']==facts,'fresh count replaced retained original facts')
   checks=control['retained_fact_live_checks'];require(len(checks)==3 and all(c['role_error'] is None and c['plan_error'] is None and c['native_error'] is None for c in checks),'expired or changed fact was counted')
   counts=control['fresh_count_attempts'];require([c['actual_close'] for c in counts]==facts and all(c['actual_error'] is None and c['same_actual_arc_passed'] for c in counts),'fresh counting differs from original retained actual observations')
   require(control['fresh_finish_error'] is None,'fresh round finish failed')
   before,after=control['after_all_closes_before_fresh_count'],control['after_fresh_count'];require(before==after and len(before)==3,'native/catalogue/rows changed during fresh counting')
   for i,state in enumerate(before):
    raw=bytes(state['raw']);envelope=json.loads(raw);cat=envelope['catalog'];node=cat['node_id'];fact=next(f for f in facts if f['node']==node)
    # Scope/native hashes are selected from actual retained catalogue records,
    # not inferred from node names, illustrative index1 or an expected phase.
    matching=[v for v in cat.get('native_joint_handoffs',{}).values() if v.get('old_closed_native_sha256')==fact['native_close_sha256'] and v.get('scope_sha256')==control['scope_sha256']]
    require(len(matching)==1,'retained native close receipt differs from actual before/after catalogue')
    validations.append({'node':node,'range_log_sha256':digest(raw),'actual_close':fact,'matching_catalogue':matching[0],'exact_before_after':True})
  if executed[0][1]=='ok':require(complete,'passed test lacks complete actual control evidence')
 return {'source_commit':source,'projection_sha256':expected_zip,'actual_test_result':executed,'actual_identity':identity,
  'logical_ack_ledger':ledger,'terminal_phase':terminal['phase'],'terminal_primary_error':terminal['primary_error'],
  'component_complete':complete,'actual_control':control,'actual_catalogue_cross_checks':validations,
  'evidence_consistency_checked':True,'qualification':False,
  'limitations':['Same-Arc/liveness are actual source assertions; this verifier compares retained identities/content without recreating native capabilities.',
   'No partial-debt/unclosable peer, old caller cancellation semantics, target installation, serving, hardware failover or scale-out qualification.',
   'Original6d and7f failures remain separate failed campaigns; this is not their rerun or fix.']}

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('projection',type=pathlib.Path);p.add_argument('--expected-zip-sha256',required=True);p.add_argument('--expected-source',required=True);p.add_argument('--output',type=pathlib.Path,required=True);a=p.parse_args()
 r=verify(a.projection,a.expected_zip_sha256,a.expected_source);a.output.write_text(json.dumps(r,indent=2,sort_keys=True)+'\n');print(json.dumps({'source':r['source_commit'],'actual_test_result':r['actual_test_result'],'component_complete':r['component_complete'],'qualification':False}))
if __name__=='__main__':main()
