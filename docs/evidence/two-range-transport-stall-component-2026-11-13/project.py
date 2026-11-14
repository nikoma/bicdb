#!/usr/bin/env python3
"""Explicit logical projection. No extraction, database open, key decoding or repair."""
import argparse, hashlib, json, re, stat, zipfile
from pathlib import Path, PurePosixPath
ORIGINAL_SHA = '1637503905e7db9dc971e4adc1285f0c978b472d59cda5614b2e4fe26d217ad1'
ORIGINAL_BYTES = 397780
ROOT_FILES = {'helper-pressure.jsonl', 'list-pressure.jsonl', 'exact-test-name.txt', 'inventory-pressure.jsonl', 'whole-inventory.log', 'selected-tests.log', 'source-revision.txt', 'rustc.txt', 'tests.log', 'requested-filter.txt', 'expected-inventory.json'}
LOGICAL = {'cleanup.json', 'remote-old-ledger.json', 'ack-ledger.json', 'original-sdk-rpc-before-app.json', 'original-sdk-rpc-index.json', 'terminal.json', 'source-frame-ledger.json', 'two-range-stall-external-ledger.json', 'custody-manifest.json', 'two-range-stall-result.json', 'fence-ledger.json', 'evidence-errors.json', 'execution-identity.json'}
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
  require(len(source.infolist())==153,'Original member count')
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
 require(total==5595680,'Original uncompressed bytes')
 return {'original_zip':{'sha256':ORIGINAL_SHA,'bytes':ORIGINAL_BYTES,'files':len(all_names),'uncompressed_bytes':total,'raw_database_custody_present':True,'mesh_signing_key_filename_count':key_paths},'projection_zip':{'sha256':digest(destination),'bytes':destination.stat().st_size,'files':len(included),'uncompressed_bytes':sum(x['bytes'] for x in included),'raw_database_custody_present':False},'included':included,'omitted':omitted,'scope':'Complete original remains outside Git; public projection contains exact whitelisted logical bytes and synthetic complete RPC frames, never DB/key trees. Literal/path checks do not prove absence of arbitrary secrets.'}
