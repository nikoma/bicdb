#!/usr/bin/env python3
"""Correct an offline RangeID join; no engine open/replay/network or input writes."""
import argparse
import hashlib
import json
import pathlib


def range_index(topology):
    result={}
    for token_start,descriptor in topology['ranges'].items():
        key=str(descriptor['id'])
        if key in result:
            raise ValueError('duplicate range descriptor ID '+key)
        result[key]={'token_start_map_key':token_start,'descriptor':descriptor}
    return result


def correct_rows(catalog,topology):
    indexed=range_index(topology)
    rows=[]
    for range_id,log in catalog['ranges'].items():
        if range_id not in indexed:
            raise ValueError('missing range descriptor ID '+range_id)
        matched=indexed[range_id]
        rows.append({'range_id':int(range_id),'retained_command_epoch':log['current_epoch'],
            'last_index':log['last_index'],'resolved_through':log['resolved_through'],
            'compacted_through':log['compacted_through'],
            'token_start_map_key':matched['token_start_map_key'],
            'independently_copied_route_descriptor':matched['descriptor'],
            'routing_epoch_equality_is_not_a_recovery_health_gate':True})
    return rows


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def collect(capture):
    nodes=[]
    for node in range(4):
        folder=capture/f'n{node}';catalog_path=folder/'db__cluster-range-write-log.json';topology_path=folder/'db__cluster-topology.json'
        catalog=json.loads(catalog_path.read_text())['catalog'];topology=json.loads(topology_path.read_text())['topology']
        receipt=json.loads((folder/'capture.json').read_text())
        times={r['path']:{k:r.get(k)for k in ['copy_started_unix_ns','copy_finished_unix_ns','sha256']}for r in receipt['copies']if r['path']in ['db/cluster-range-write-log.json','db/cluster-topology.json']}
        rows=correct_rows(catalog,topology)
        nodes.append({'node':node,'raw_catalog_sha256':digest(catalog_path),'raw_topology_sha256':digest(topology_path),
            'topology_generation':topology['generation'],'independent_file_copy_intervals':times,'ranges':rows})
    return {'method':'Corrected offline lookup by descriptor.id, not token-start map key. Original buggy helper and null outputs preserved.',
        'routing_is_later_independent_capture_not_request_time':True,'coherent_global_snapshot':False,
        'missing_range_descriptors':0,'joined_range_rows':sum(len(n['ranges'])for n in nodes),'nodes':nodes,
        'helper_sha256':digest(pathlib.Path(__file__)),'engine_opened':False,'production_qualified':False}


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--capture-dir',type=pathlib.Path,required=True)
    parser.add_argument('--output',type=pathlib.Path,required=True)
    args=parser.parse_args();result=collect(args.capture_dir)
    with args.output.open('x')as output:json.dump(result,output,indent=2);output.write('\n')
    print(json.dumps({'joined_range_rows':result['joined_range_rows'],'missing_range_descriptors':0,'engine_opened':False}))
