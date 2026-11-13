#!/usr/bin/env python3
"""Pure bounded timing DATA derivation from the retained full job log."""
import collections
import hashlib
import json
import re

SOURCE='5812d5750409e8b4f21d959fbaf367c8d4362f39'


def derive(raw):
    text=raw.decode();events=[];groups=[];current=None;case=None;cases={}
    for line in text.splitlines():
        heading=re.search(r'---- ([\w:]+) stdout ----',line)
        if heading:case=heading[1]
        if 'ACTUAL_POST_JOINT_FENCE recipient=' in line:
            cases[re.search(r'context=([^ ]+)',line)[1]]=case
        if 'NATIVE_FENCE_TRACE ' not in line:continue
        event=dict(re.findall(r'(\w+)=([^ ]+)',line.split('NATIVE_FENCE_TRACE ',1)[1]))
        for key in ['pid','epoch','t_us','elapsed_us','dropped']:event[key]=int(event[key])
        for key in ['caller_deadline_us','role_deadline_us','effective_deadline_us','caller_left_us','role_left_us','effective_left_us']:
            event[key]=None if event[key]=='None' else int(event[key].removeprefix('Some(').removesuffix(')'))
        events.append(event)
        if event['stage']=='context-entry':current=[];groups.append(current)
        assert current is not None
        current.append(event)
    output=[]
    for group in groups:
        first=group[0]
        assert all((x['phase'],x['node'],x['nonce'],x['context'],x['challenge'])==(first['phase'],first['node'],first['nonce'],first['context'],first['challenge']) for x in group)
        assert [x['t_us'] for x in group]==sorted(x['t_us'] for x in group)
        starts={};spans=[]
        for event in group:
            stage=event['stage']
            if stage.endswith('-begin'):starts[stage[:-6]]=event
            elif stage.endswith('-ok') and stage[:-3] in starts:
                start=starts.pop(stage[:-3]);spans.append(dict(stage=stage[:-3],elapsed_us=event['t_us']-start['t_us'],start_us=start['t_us'],end_us=event['t_us']))
        item=dict(phase=first['phase'],node=first['node'],nonce=first['nonce'],context=first['context'],challenge=first['challenge'],
            acquired_role_context=first['acquired_role_context'],event_count=len(group),dropped=max(x['dropped'] for x in group),
            start_us=first['t_us'],last_us=group[-1]['t_us'],observed_through_last_marker_us=group[-1]['t_us']-first['t_us'],
            initial_deadlines={k:v for k,v in first.items() if 'deadline' in k or 'left' in k},spans=spans,
            remaining_unpaired_begin=list(starts),events=group,test=cases.get(first['context']))
        bound=[x for x in group if x['stage']=='recipient-role-bound']
        if bound:item['role_binding']={k:v for k,v in bound[0].items() if 'deadline' in k or 'left' in k}
        if first['role_deadline_us'] is not None:item['role_slack_at_last_marker_us']=first['role_deadline_us']-group[-1]['t_us']
        output.append(item)
    result=dict(source=SOURCE,run=43343984328,core_job=131618814672,core_result=dict(pass_=4,fail=0,ignored=0,filtered=976,seconds=67.70),
        workflow_conclusion='failure',formatting_job=131618780186,alltarget_consumer_job=131618780247,
        raw_sha256=hashlib.sha256(raw).hexdigest(),event_count=len(events),groups=output,
        phase_counts=dict(collections.Counter(x['phase'] for x in output)),all_dropped_zero=all(x['dropped']==0 for x in output))
    result['core_result']['pass']=result['core_result'].pop('pass_')
    stats=collections.defaultdict(list)
    for item in output:
        if item['phase']=='receiver' and item['context'] in cases:
            for span in item['spans']:stats[span['stage']].append(span['elapsed_us'])
    result['current_joint_stage_stats']={key:dict(n=len(values),min_us=min(values),max_us=max(values)) for key,values in stats.items()}
    return result
