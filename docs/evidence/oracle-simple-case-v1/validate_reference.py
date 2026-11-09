from pathlib import Path
import hashlib,json,random,time
import oracledb
out=Path(__file__).parent;path=out/'oracle-simple-case-full-reference.json';ref=json.loads(path.read_text());u=dict(ref['upper_changed']);l=dict(ref['lower_changed'])
cfg=json.loads(Path('/root/.local/state/bicdb/idempiere-reference/qualification-connection.json').read_text());c=oracledb.connect(user=cfg['user'],password=Path(cfg['password_file']).read_text().strip(),dsn=f"{cfg['host']}:{cfg['port']}/{cfg['service_name']}");q=c.cursor();q.execute('SET TRANSACTION READ ONLY');q.execute('SELECT product,version,version_full FROM product_component_version');vendor=q.fetchall()
q.execute("SELECT parameter,value FROM nls_session_parameters WHERE parameter IN ('NLS_LANGUAGE','NLS_SORT','NLS_COMP') ORDER BY parameter");nls=q.fetchall();checks=0

def check(s):
 global checks
 q.execute('SELECT UPPER(:s),LOWER(:s) FROM DUAL',s=s);a,b=q.fetchone();assert a==''.join(chr(u.get(ord(ch),ord(ch))) for ch in s),(repr(s),repr(a));assert b==''.join(chr(l.get(ord(ch),ord(ch))) for ch in s),(repr(s),repr(b));checks+=1
for cp in sorted(set(u)|set(l)|set(range(128))):check(chr(cp))
rng=random.Random(1190147)
for _ in range(256):
 chars=[]
 for _ in range(128):
  cp=rng.randrange(0x110000)
  if 0xD800<=cp<=0xDFFF:cp=0x20
  chars.append(chr(cp))
 check(''.join(chars))
for s in ['Straße','ﬃΐσς','ǄÉẞİ','ǆéßı','ΟΣ','ΟΣ Α','AΣA','\U00010400\U00010428']:check(s)
c.rollback();c.close();result={'complete':True,'reference_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'oracle_version':vendor,'session_nls':nls,'standalone_mapping_keys_and_ascii_plus_random_and_context_string_checks':checks,'random_seed':1190147,'random_strings':256,'scope':'Independent standalone checks for every changed scalar and ASCII, plus fixed context probes and random scalar strings; no application data read or modified.'};(out/'validation.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
