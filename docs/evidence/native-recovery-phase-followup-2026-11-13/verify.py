#!/usr/bin/env python3
"""Check this bounded historical packet. Exit0 is evidence integrity, not qualification."""
import gzip,hashlib,importlib.util,json,pathlib,re,sys,zipfile
sys.dont_write_bytecode=True
ROOT=pathlib.Path(__file__).resolve().parent

def require(value,why):
    if not value:raise ValueError(why)
def sha(data):return hashlib.sha256(data).hexdigest()
def relative(name):
    p=pathlib.PurePosixPath(name);require(not p.is_absolute() and '..' not in p.parts,'unsafe packet path');return ROOT.joinpath(*p.parts)
def load(name):
    spec=importlib.util.spec_from_file_location(name.replace('-','_'),ROOT/name);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m

def verify():
    manifest=json.loads((ROOT/'manifest.json').read_text());corpus=json.loads((ROOT/'corpus.json').read_text())
    require(manifest['qualification']==corpus['qualification']=='NOT QUALIFIED','qualification changed')
    require(not corpus['production_gate_changed'] and not corpus['physical_four_to_five_tested'] and not corpus['throughput_gain_tested'],'production or physical gate changed')
    require(len(corpus['jobs'])==17 and len({j['run_id'] for j in corpus['jobs']})==11 and len(corpus['artifacts'])==9,'bounded corpus changed')
    files={f['path']:f for f in manifest['files']};require(len(files)==len(manifest['files']),'duplicate manifest path')
    for name,f in files.items():
        raw=relative(name).read_bytes();require(len(raw)==f['bytes'] and sha(raw)==f['sha256'],'changed file: '+name)
    old=json.loads((ROOT/'previous-packet-identity.json').read_text());previous=ROOT.parent/'native-recovery-continuation-2026-11-13'
    require(len(old['files'])==70,'old packet identity changed')
    for f in old['files']:
        p=pathlib.PurePosixPath(f['path']);require(not p.is_absolute() and '..' not in p.parts,'unsafe previous packet path')
        raw=previous.joinpath(*p.parts).read_bytes();require(len(raw)==f['bytes'] and sha(raw)==f['sha256'],'earlier packet changed: '+f['path'])
    outcomes=[]
    for j in corpus['jobs']:
        i=json.loads(relative(j['index']).read_text());m=json.loads(relative(j['metadata']).read_text());compressed=relative(j['log']).read_bytes();raw=gzip.decompress(compressed)
        require(m['id']==i['job_id']==j['job_id'] and m['run_id']==i['run_id']==j['run_id'],'job/run differs')
        require(m['head_sha']==i['source_revision']==j['source'] and m['status']==i['job_status']=='completed','source/nonterminal differs')
        require(m['conclusion']==i['job_conclusion']==j['conclusion'],'job result differs')
        require(len(raw)==i['raw_bytes'] and sha(raw)==i['raw_sha256'] and sha(compressed)==i['compressed_sha256'],'incomplete/changed job log')
        text=re.sub(r'\x1b\[[0-9;]*m','',raw.decode())
        summaries=[l for l in text.splitlines() if re.match(r'^\S+Z test result: (?:ok|FAILED)\. \d+ passed;',l)]
        observations=[{'test':a,'result':b} for a,b in re.findall(r'^\S+Z test (.+) \.\.\. (ok|FAILED|ignored[^\n]*)$',text,re.M)]
        require(summaries==i['test_summaries']==j['test_summaries'] and observations==i['test_observations']==j['test_observations'],'test outcome relabeled')
        for prefix,error in [('0316','E0308'),('ea7','E0277'),('a5f','E0382')]:
            if j['label'].startswith(prefix):require(not summaries and not observations and f'error[{error}]' in text,'compile failure relabeled runtime')
        outcomes.append({'job':j['job_id'],'result':j['conclusion'],'runtime_summaries':summaries})
    projectors={sha((ROOT/n).read_bytes()):load(n) for n in ['project-native-first-app-evidence.py','project-native-first-app-cohort-evidence.py','project-native-old-round-expiry-evidence.py','project-native-registered-installing-evidence.py']}
    derived={};registered=[]
    for a in corpus['artifacts']:
        path=relative(a['path']);p=json.loads(relative(a['manifest']).read_text())
        require(sha(path.read_bytes())==a['sha256']==p['projection_zip_sha256'] and path.stat().st_size==a['bytes']==p['projection_zip_bytes'],'projection differs')
        require(p['source_commit']==a['source'] and p['original_zip_sha256']==a['original_sha256'] and p['projection_fixture_db_custody_present'] is False and a['custody'] is False,'custody/source differs')
        require(p['projector_sha256'] in projectors,'exact original projector unavailable')
        projector=projectors[p['projector_sha256']];included={f['path']:f for f in p['included_members']}
        require(len(included)==len(p['included_members']),'duplicate included member')
        with zipfile.ZipFile(path) as z:
            require(z.testzip() is None and set(z.namelist())==set(included),'damaged/unlisted ZIP member')
            require(z.read('source-revision.txt').decode().strip()==a['source'],'ZIP source differs')
            for info in z.infolist():
                require(projector.allowed(info.filename) and '/custody/' not in info.filename and info.file_size<=64*1024*1024,'non-whitelisted/private material path')
                raw=z.read(info);expected=included[info.filename];require(len(raw)==expected['bytes'] and sha(raw)==expected['sha256'],'member differs')
                raw.decode();require(not re.search(br'-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----',raw),'private PEM in public member')
        for omitted in p['omitted_members']:
            require(omitted['path'] not in included and re.fullmatch('[0-9a-f]{64}',omitted['sha256']) and omitted['bytes']>=0,'invalid omission identity')
        if a['source'].startswith('d09d'):
            derived['d09d']=load('verify-native-first-app-evidence.py').verify(path,a['sha256'],a['source'],ROOT/'verify-native-origin-evidence.py')
        elif a['source'].startswith('7f2c'):
            derived['7f2c']=load('verify-native-first-app-cohort-evidence.py').verify(path,a['sha256'],a['source'],ROOT/'verify-native-origin-evidence.py')
        elif a['source'].startswith('6d5'):
            derived['6d5']=load('verify-native-old-round-expiry-evidence.py').verify(path,a['sha256'],a['source'])
        elif a['source'].startswith(('9434','7349','fd4d')):
            registered.append(load('verify-registered-components.py').verify(path,a['sha256'],a['source']))
    for label in ['d09d','7f2c','6d5']:
        name={'d09d':'d09d-first-app-verifier-report.json.gz','7f2c':'7f2c-first-app-cohort-verifier-report.json.gz','6d5':'6d5-old-round-expiry-verifier-report.json.gz'}[label]
        expected=json.loads(gzip.decompress((ROOT/'reports'/name).read_bytes()));require(json.loads(json.dumps(derived[label]))==expected,'derived public report differs: '+label)
    expected=json.loads((ROOT/'reports/registered-components-verifier.json').read_text());require(registered==expected,'registered component reconciliation changed')
    cohort=derived['7f2c'];counts={}
    for c in cohort['cases']:
        for key,count in c['counts'].items():counts[key]=counts.get(key,0)+count
    require(counts=={'acknowledged_new_insert':7,'uncertain':1} and all(v=='FAILED' for _,v in cohort['actual_test_results']),'failed cohort classified differently')
    print(json.dumps({'jobs_verified':len(outcomes),'safe_projections':len(corpus['artifacts']),'previous_files_unchanged':70,'cohort_API_counts':counts,'qualification':'NOT QUALIFIED'}))

if __name__=='__main__':verify()
