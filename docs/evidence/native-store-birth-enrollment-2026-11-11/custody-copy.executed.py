import pathlib,os,hashlib,shutil,json,time
src=pathlib.Path('/tmp/.tmpv3ISIJ');dst=pathlib.Path('/root/.local/state/bicdb/ha-scaling-2026-11-11/private/native-store-birth-ee5cb188');public=pathlib.Path('/tmp/bicdb-native-operator-gap-docs/docs/evidence/native-store-birth-enrollment-2026-11-11')
assert src.is_dir() and not dst.exists()
users=[]
for proc in pathlib.Path('/proc').iterdir():
 if not proc.name.isdigit():continue
 for sub in ['fd','map_files']:
  try:
   for link in (proc/sub).iterdir():
    try:target=os.readlink(link)
    except OSError:continue
    if target==str(src) or target.startswith(str(src)+'/'):users.append(proc.name)
  except OSError:pass
assert not users,'source has live owners'
sha=lambda b:hashlib.sha256(b).hexdigest()
paths=sorted(p for p in src.rglob('*') if p.is_file());assert paths and not any(p.is_symlink() for p in src.rglob('*'))
original={p.relative_to(src).as_posix():(len(p.read_bytes()),sha(p.read_bytes())) for p in paths}
dst.mkdir(mode=0o700,parents=False)
for p in paths:
 relative=p.relative_to(src);out=dst/'read-r2'/relative;out.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
 b=p.read_bytes();assert (len(b),sha(b))==original[relative.as_posix()]
 with out.open('xb') as f:f.write(b);f.flush();os.fsync(f.fileno())
 out.chmod(0o600)
 assert (len(out.read_bytes()),sha(out.read_bytes()))==original[relative.as_posix()]
records=[]
for p in sorted(dst.rglob('*')):
 if p.is_dir():p.chmod(0o700)
 elif p.is_file():b=p.read_bytes();records.append({'path':p.relative_to(dst).as_posix(),'bytes':len(b),'sha256':sha(b)})
manifest=(json.dumps({'source_revision':'ee5cb1880a946b97816a15aa4e0cb12f7d03778d','source_root':str(src),'files':records},indent=2,sort_keys=True)+'\n').encode()
with (dst/'MANIFEST.json').open('xb') as f:f.write(manifest);f.flush();os.fsync(f.fileno())
(dst/'MANIFEST.json').chmod(0o600)
for d in sorted([dst,*[p for p in dst.rglob('*') if p.is_dir()]],key=lambda p:len(p.parts),reverse=True):
 fd=os.open(d,os.O_RDONLY|os.O_DIRECTORY);os.fsync(fd);os.close(fd)
for p in paths:assert (len(p.read_bytes()),sha(p.read_bytes()))==original[p.relative_to(src).as_posix()]
receipt={'source_revision':'ee5cb1880a946b97816a15aa4e0cb12f7d03778d','fixture':'existing genuine R2 read-observation fixture; not new enrollment/factory fixture roots','source_root_printed_in_final_reads_stderr':str(src),'custody_root':str(dst),'files':len(records),'bytes':sum(r['bytes'] for r in records),'manifest_sha256':sha(manifest),'database_opened_for_copy':False,'active_source_fd_or_map_users':0,'source_destination_hash_parity':True,'source_unchanged_after_copy':True,'directories_fsynced':True,'files_fsynced':True,'directory_mode':'0700','file_mode':'0600','contains_private_generated_database_keys':True,'external_ack_ledger_retained':False,'new_factory_fixture_roots_retained':False,'new_enrollment_tls_fixture_roots_retained':False}
(public/'private-custody.json').write_text(json.dumps(receipt,indent=2,sort_keys=True)+'\n');print(json.dumps({'files':receipt['files'],'bytes':receipt['bytes'],'manifest_sha256':receipt['manifest_sha256']}))
