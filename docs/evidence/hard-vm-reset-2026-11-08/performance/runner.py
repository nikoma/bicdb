#!/usr/bin/env python3
"""Adapted recovered PR54 runner: paired all-logs timing, not crash safety proof."""
import argparse,hashlib,json,os,pathlib,resource,signal,subprocess,time

def payload(path, field):
    for line in path.read_text().splitlines():
        try: value=json.loads(line)
        except json.JSONDecodeError: continue
        if field in value: return value
    raise RuntimeError(f'no {field} result in {path}')

def run(args):
    resource.setrlimit(resource.RLIMIT_CORE,(0,0))
    out=pathlib.Path(args.output);out.mkdir(parents=True,exist_ok=False)
    bins={'main':pathlib.Path(args.main_binary),'candidate':pathlib.Path(args.candidate_binary)}
    env=dict({k:v for k,v in os.environ.items() if not k.startswith('BICDB_')},BICDB_PAGED_BUFFER_POOL_BYTES=str(args.pool_bytes),
        BICDB_PAGED_WAL_SYNC_UNLOCKED='0',BICDB_PAGED_COMMIT_SYNC_OUTSIDE_LOCK='0',
        BICDB_PAGE_WAL_DELTAS='0',BICDB_SYNC_OUTBOX='off')
    manifest={'arguments':vars(args),'mode':'all-logs','storage_mode':'server_paged','fsync':True,
        'sync_outbox':False,'same_example_source':True,'scope':__doc__,
        'example_sha256':hashlib.sha256(pathlib.Path(args.example_source).read_bytes()).hexdigest(),
        'runner_sha256':hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),
        'binaries':{k:{'path':str(v),'sha256':hashlib.sha256(v.read_bytes()).hexdigest()} for k,v in bins.items()},
        'gate_environment':{k:v for k,v in env.items() if k.startswith('BICDB_')}}
    builds={}
    for side, binary in bins.items():
        build_path=binary.with_name(binary.name.removeprefix('commit-bench-')+'-manifest.json')
        build=json.loads(build_path.read_text())
        expected_source=args.main_source if side=='main' else args.candidate_source
        assert build['source']==expected_source,(side,build['source'],expected_source)
        assert build['benchmark_source_sha256']==manifest['example_sha256'],side
        assert build['artifacts']['commit_durability_bench']['sha256']==manifest['binaries'][side]['sha256'],side
        builds[side]=build
    manifest['artifact_manifests']=builds
    (out/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    print(out,flush=True)
    def invoke(name,cmd,expected_exit,field):
        before=time.monotonic();usage=resource.getrusage(resource.RUSAGE_CHILDREN);samples=[]
        services=subprocess.check_output(['systemctl','list-units','--state=running','--no-legend','--plain'],text=True)
        stdout_path=out/(name+'.stdout');stderr_path=out/(name+'.stderr')
        with stdout_path.open('w') as stdout,stderr_path.open('w') as stderr:
            proc=subprocess.Popen(cmd,stdout=stdout,stderr=stderr,env=env,start_new_session=True)
            while proc.poll() is None:
                memory=int(pathlib.Path('/proc/meminfo').read_text().split('MemAvailable:')[1].split()[0])
                samples.append({'seconds':time.monotonic()-before,'io':pathlib.Path('/proc/pressure/io').read_text(),'memory_available_kib':memory})
                if memory<10*1024*1024:
                    os.killpg(proc.pid,signal.SIGTERM);proc.wait(timeout=30)
                    raise RuntimeError('own benchmark stopped: host memory headroom below10GiB')
                time.sleep(.5)
        record={'exit':proc.returncode,'elapsed_seconds':time.monotonic()-before,
            'fs_output_blocks':resource.getrusage(resource.RUSAGE_CHILDREN).ru_oublock-usage.ru_oublock,
            'samples':samples,'services_before':services}
        (out/(name+'.json')).write_text(json.dumps(record,indent=2)+'\n')
        if proc.returncode!=expected_exit: raise RuntimeError(f'{name}: exit {proc.returncode}, expected {expected_exit}')
        record['bench']=payload(stdout_path,field)
        (out/(name+'.json')).write_text(json.dumps(record,indent=2)+'\n')
        return record
    for writers in args.writers:
        for trial in range(args.repeats):
            for side in (['main','candidate'] if trial%2==0 else ['candidate','main']):
                name=f'w{writers}-t{trial}-{side}';db=out/(name+'-db')
                result=invoke(name,[str(bins[side]),'run',str(db),'all-logs',str(writers),str(args.seconds),'crash'],-signal.SIGABRT,'commits')
                format_meta=json.loads((db/'format.json').read_text())
                assert format_meta['storage_mode']=='server_paged',format_meta
                bench=result['bench'];count=bench['commits']
                bench['page_wal_bytes_per_commit']=bench['page_wal_bytes']/count
                bench['transaction_log_bytes_per_commit']=bench['transaction_log_bytes']/count
                recoveries=[]
                for reopen in (1,2):
                    recovered=invoke(name+f'-recover{reopen}',[str(bins[side]),'recover',str(db),'all-logs'],0,'recovery_open_ms')
                    assert recovered['bench']['rows']==count+writers,(name,reopen,recovered['bench']['rows'],count+writers)
                    recoveries.append(recovered['bench'])
                result.update(side=side,writers=writers,trial=trial,persisted_format=format_meta,recoveries=recoveries)
                (out/(name+'.json')).write_text(json.dumps(result,indent=2)+'\n')
                print(json.dumps({k:v for k,v in result.items() if k not in ('samples','services_before')}),flush=True)
    (out/'complete').write_text('all paired timing and row-count recovery checks completed\n')
    print('complete',flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('main-binary','candidate-binary','main-source','candidate-source','example-source','output'):
        p.add_argument('--'+name,required=True)
    p.add_argument('--writers',type=int,nargs='+',default=[1,16,64])
    p.add_argument('--repeats',type=int,default=3);p.add_argument('--seconds',type=int,default=15)
    p.add_argument('--pool-bytes',type=int,default=268435456)
    run(p.parse_args())
