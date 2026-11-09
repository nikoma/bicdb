"""Read-only Oracle scalar case reference. No source application data selected."""
from pathlib import Path
import hashlib,json,time
import oracledb
OUT=Path(__file__).parent
cfg=json.loads(Path('/root/.local/state/bicdb/idempiere-reference/qualification-connection.json').read_text())
pw=Path(cfg['password_file']).read_text().strip()
c=oracledb.connect(user=cfg['user'],password=pw,dsn=f"{cfg['host']}:{cfg['port']}/{cfg['service_name']}")
q=c.cursor();q.execute('SET TRANSACTION READ ONLY')
q.execute("SELECT product,version_full FROM product_component_version");version=q.fetchall()
q.execute("SELECT parameter,value FROM nls_database_parameters WHERE parameter IN ('NLS_CHARACTERSET','NLS_NCHAR_CHARACTERSET') ORDER BY parameter");charset=q.fetchall()
upper=[];lower=[];non_scalar_outputs=[];tested=0;queries=0;started=time.monotonic()
def collect(chars):
 global queries,tested
 text=''.join(chars);q.execute('SELECT UPPER(TO_NCHAR(:value)),LOWER(TO_NCHAR(:value)) FROM DUAL',value=text);u,l=q.fetchone();queries+=1
 if len(u)!=len(text) or len(l)!=len(text):
  assert len(chars)>1,(ord(chars[0]),repr(u),repr(l))
  for char in chars:collect([char])
  return
 for char,a,b in zip(chars,u,l):
  if a!=char:upper.append([ord(char),ord(a)])
  if b!=char:lower.append([ord(char),ord(b)])
 tested+=len(chars)
for start in range(0,0x110000,512):
 chars=[chr(cp) for cp in range(start,min(start+512,0x110000)) if not 0xD800<=cp<=0xDFFF]
 if chars:collect(chars)
 if start%0x10000==0:print(hex(start),tested,flush=True)
assert tested==0x110000-0x800
c.rollback();c.close()
result={'scope':'Read-only UPPER/LOWER over TO_NCHAR of every Unicode scalar value in concatenated bind chunks; no application rows read or changed. Surrogate codepoints are not Unicode scalars.','oracle_version':version,'database_character_sets':charset,'tested_scalars':tested,'queries':queries,'elapsed_seconds':time.monotonic()-started,'upper_changed':upper,'lower_changed':lower}
path=OUT/'oracle-simple-case-national-reference.json';assert not path.exists();path.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({k:v for k,v in result.items() if not k.endswith('_changed')}),len(upper),len(lower),hashlib.sha256(path.read_bytes()).hexdigest(),flush=True)
