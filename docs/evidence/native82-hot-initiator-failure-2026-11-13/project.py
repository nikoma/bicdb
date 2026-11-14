#!/usr/bin/env python3
"""Explicit logical projection. No extraction, database open, key decoding or repair."""
import argparse, hashlib, json, re, stat, zipfile
from pathlib import Path, PurePosixPath
ORIGINAL_SHA = 'bdf53f0eb21bf88397a14099f6a971460db1497104bcb799ae078b3d85c03561'
ORIGINAL_BYTES = 23829148
ROOT_FILES = {'helper-pressure.jsonl','list-pressure.jsonl','selected-tests.log','rustc.txt','source-revision.txt','tests.log'}
LOGICAL = set('''ack-ledger.json automatic-placement-plan.json cleanup.json closed-fence-control-intent.json closed-fence-controls.json custody-manifest.json evidence-errors.json execution-identity.json fence-ledger.json no-domain-1-hook.json no-domain-1-point.json no-domain-1-schema.json no-domain-2-point.json old-generic-cold-context-refusals.json old-sink-control-intent.json old-sink-control-result.json original-sdk-rpc-before-app.json original-sdk-rpc-index.json registered-catalogue-floor-before-assertions.json registered-catalogue-floor-directory-error-1.json registered-catalogue-floor-directory-error-2.json registered-catalogue-floor-missing-base.json registered-catalogue-floor-post-fsync-cancel.json registered-catalogue-prerequisite-outcome.json registered-final-image-after-wal-cancel.json registered-final-image-before-assertions.json registered-final-image-historical-integrity.json registered-final-image-independent-corruption.json registered-final-image-missing-scan.json registered-image-before-final-assertions.json registered-image-history-and-rows.json registered-image-inner-durable-apply-failure.json registered-image-missing-audit.json registered-image-wal-retention.json registered-installing-1.json registered-native-base-abort-recovery-outcome.json registered-native-base-before-assertions.json registered-native-base-canceled.json registered-native-base-invalidated-integrity.json registered-native-base-missing-final.json registered-native-base-real-role-expiry-outcome.json registered-native-image-critical-outcome.json registered-proposal-1.json registered-proposal-reader-fence-retry-1.json registered-transfer-1.json remote-old-ledger.json source-constraints-result.json source-discovery-1.json source-discovery-ledger.json source-discovery-process-revocation-native.json source-discovery-process-revocation.json source-frame-ledger.json source-frames-result.json terminal.json'''.split())
def digest(path):
 h=hashlib.sha256()
 with path.open('rb') as f:
  for chunk in iter(lambda:f.read(1048576),b''): h.update(chunk)
 return h.hexdigest()
def require(ok,message):
 if not ok: raise ValueError(message)
def allowed(name):
 parts=PurePosixPath(name).parts
 if len(parts)==1:return name in ROOT_FILES
 if len(parts)<3 or parts[0]!='fault-state' or not re.fullmatch(r'native-registered-fifth-image-[0-9a-f-]{36}',parts[1]):return False
 if len(parts)==3:return parts[2] in LOGICAL
 return len(parts)==4 and parts[2]=='original-sdk-rpc' and re.fullmatch(r'rpc-\d{6}\.json',parts[3]) is not None

def build(original, destination):
 require(original.stat().st_size==ORIGINAL_BYTES and digest(original)==ORIGINAL_SHA,'Original ZIP identity')
 included=[];omitted=[];all_names=set();key_paths=0;total=0
 with zipfile.ZipFile(original) as source, zipfile.ZipFile(destination,'w',compression=zipfile.ZIP_DEFLATED,compresslevel=9) as output:
  require(len(source.infolist())==9361,'Original member count')
  for item in source.infolist():
   name=item.filename; parts=PurePosixPath(name).parts
   require(not name.startswith('/') and '..'not in parts and name not in all_names and not item.is_dir(),'Unsafe/duplicate member')
   require(not stat.S_ISLNK(item.external_attr>>16),'Symlink member')
   all_names.add(name);total+=item.file_size
   take=allowed(name);h=hashlib.sha256()
   if take:
    target=zipfile.ZipInfo(name,date_time=(1980,1,1,0,0,0));target.compress_type=zipfile.ZIP_DEFLATED;target.external_attr=0o100644<<16
    with source.open(item) as inp,output.open(target,'w',force_zip64=True) as out:
     for chunk in iter(lambda:inp.read(65536),b''):h.update(chunk);out.write(chunk)
   else:
    # Hash complete omitted bytes; never decode or print credential contents.
    with source.open(item) as inp:
     for chunk in iter(lambda:inp.read(65536),b''):h.update(chunk)
   record={'path':name,'bytes':item.file_size,'sha256':h.hexdigest()}
   if take:included.append(record)
   else:
    record['reason']='entire_original_database_custody_excluded' if len(parts)>2 and parts[2]=='custody' else 'outside_explicit_allowlist'
    omitted.append(record)
   key_paths+=Path(name).name=='mesh_signing_key.json'
 require(total==379438111,'Original uncompressed bytes')
 return {'original_zip':{'sha256':ORIGINAL_SHA,'bytes':ORIGINAL_BYTES,'files':len(all_names),'uncompressed_bytes':total,'raw_database_custody_present':True,'mesh_signing_key_filename_count':key_paths},'projection_zip':{'sha256':digest(destination),'bytes':destination.stat().st_size,'files':len(included),'uncompressed_bytes':sum(x['bytes'] for x in included),'raw_database_custody_present':False},'included':included,'omitted':omitted,'scope':'Complete original remains outside Git; public projection contains exact whitelisted logical bytes and synthetic complete RPC frames, never DB/key trees. Literal/path checks do not prove absence of arbitrary secrets.'}

def derive_metadata(original, case_roots):
 result=[]
 with zipfile.ZipFile(original) as source:
  for case in case_roots:
   points={}
   for node in ['n0','n4']:
    prefix=f'{case}/custody/node{node[1:]}/'
    names=[n for n in source.namelist() if n.startswith(prefix) and n.endswith('/cluster-metadata-consensus.json')]
    require(len(names)==1,'Exact original metadata source member')
    name=names[0];raw=source.read(name);state=json.loads(raw)['state']
    # Only these non-credential fields are projected from omitted DB metadata.
    point={k:state.get(k) for k in ['node_id','current_term','role','leader_id','commit_index']}
    points[node]={'original_member':name,'original_bytes':len(raw),'original_sha256':hashlib.sha256(raw).hexdigest(),'derived_point':point}
   result.append({'case_root':case,'points':points})
 return {'scope':'Whitelisted fields from copied MStore JSON; originals omitted, not engine/fdatasync/cold-recovery proof. Default verification checks retained correlation; --original-zip independently rederives these fields.','records':result}
if __name__=='__main__':
 ap=argparse.ArgumentParser(description=__doc__);ap.add_argument('original',type=Path);ap.add_argument('projection',type=Path);ap.add_argument('manifest',type=Path);args=ap.parse_args()
 args.manifest.write_text(json.dumps(build(args.original,args.projection),indent=2)+'\n')
