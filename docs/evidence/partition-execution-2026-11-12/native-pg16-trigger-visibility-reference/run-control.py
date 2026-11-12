import json,pathlib,subprocess,time,uuid
root=pathlib.Path('/root/.local/state/bicdb/typed-compare/native-pg16-partition-trigger-visibility-reference');root.mkdir(exist_ok=True)
schema='root_part_visibility_'+uuid.uuid4().hex[:12]
setup=f'''\\set ON_ERROR_STOP on
BEGIN;
CREATE SCHEMA {schema};
SET LOCAL search_path={schema};
CREATE TABLE audit(seq BIGSERIAL,op TEXT,phase TEXT,rel TEXT,oldk INT,newk INT,low_n INT,high_n INT,parent_n INT);
CREATE TABLE p(k INT PRIMARY KEY,payload TEXT) PARTITION BY RANGE(k);
CREATE TABLE low PARTITION OF p FOR VALUES FROM(0) TO(10);
CREATE TABLE high PARTITION OF p FOR VALUES FROM(10) TO(20);
CREATE FUNCTION audit_fn() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN
 INSERT INTO audit(op,phase,rel,oldk,newk,low_n,high_n,parent_n)
 VALUES(TG_OP,TG_WHEN,TG_TABLE_NAME,OLD.k,NEW.k,(SELECT COUNT(*) FROM low),(SELECT COUNT(*) FROM high),(SELECT COUNT(*) FROM p));
 IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF; END$$;
CREATE FUNCTION veto() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NULL; END$$;
CREATE TRIGGER parent_b BEFORE INSERT OR UPDATE OR DELETE ON p FOR EACH ROW EXECUTE FUNCTION audit_fn();
CREATE TRIGGER parent_a AFTER INSERT OR UPDATE OR DELETE ON p FOR EACH ROW EXECUTE FUNCTION audit_fn();
'''
cases={
 'insert_multi':("",None,"INSERT INTO p VALUES(1,'first-ack'),(11,'second-ack') RETURNING k,payload"),
 'move':("INSERT INTO p VALUES(1,'original-ack');",None,"UPDATE p SET k=11 WHERE k=1 RETURNING k,payload"),
 'veto_source_update':("INSERT INTO p VALUES(1,'original-ack');",'CREATE TRIGGER z_veto BEFORE UPDATE ON low FOR EACH ROW EXECUTE FUNCTION veto();',"UPDATE p SET k=11 WHERE k=1 RETURNING k,payload"),
 'veto_source_delete':("INSERT INTO p VALUES(1,'original-ack');",'CREATE TRIGGER z_veto BEFORE DELETE ON low FOR EACH ROW EXECUTE FUNCTION veto();',"UPDATE p SET k=11 WHERE k=1 RETURNING k,payload"),
 'veto_destination_insert':("INSERT INTO p VALUES(1,'original-ack');",'CREATE TRIGGER z_veto BEFORE INSERT ON high FOR EACH ROW EXECUTE FUNCTION veto();',"UPDATE p SET k=11 WHERE k=1 RETURNING k,payload"),
 'update_multi':("INSERT INTO p VALUES(1,'first-ack'),(2,'second-ack');",None,"UPDATE p SET k=k+10 RETURNING k,payload"),
 'delete_multi':("INSERT INTO p VALUES(1,'first-ack'),(2,'second-ack');",None,"DELETE FROM p RETURNING k,payload"),
}
sql=setup
for label,(seed,trigger,mutation) in cases.items():
 sql+=f'''SAVEPOINT {label};
{seed}
DELETE FROM audit;
{trigger or ''}
WITH changed AS ({mutation}) SELECT '{label}_returned',COALESCE(jsonb_agg(jsonb_build_object('k',k,'payload',payload) ORDER BY k),'[]'::jsonb) FROM changed;
SELECT '{label}_rows',COALESCE(jsonb_agg(jsonb_build_object('k',k,'payload',payload) ORDER BY k),'[]'::jsonb) FROM p;
SELECT '{label}_events',COALESCE(jsonb_agg(jsonb_build_object('op',op,'phase',phase,'relation',rel,'old',oldk,'new',newk,'low_n',low_n,'high_n',high_n,'parent_n',parent_n) ORDER BY seq),'[]'::jsonb) FROM audit;
ROLLBACK TO SAVEPOINT {label};
'''
sql+=f"ROLLBACK;\nSELECT 'scratch_schema_absent',NOT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='{schema}');\n";(root/'control.sql').write_text(sql)
image=subprocess.check_output(['docker','inspect','--format','{{.Image}}','bicdb-pgprov-ref'],text=True).strip();name='root-part-trigger-visibility-'+uuid.uuid4().hex[:10];started=time.time()
try:
 subprocess.run(['docker','run','--detach','--name',name,'--network','none','--memory','512m','--memory-swap','512m','--cpus','1','--tmpfs','/var/lib/postgresql/data:rw,size=256m','-e','POSTGRES_HOST_AUTH_METHOD=trust',image],check=True,stdout=subprocess.DEVNULL)
 for _ in range(60):
  if subprocess.run(['docker','exec',name,'pg_isready','-U','postgres'],capture_output=True).returncode==0:break
  time.sleep(.5)
 else:raise RuntimeError('owned ephemeral PG did not start')
 result=subprocess.run(['docker','exec','-i',name,'psql','-X','-A','-t','-F','|','-U','postgres'],input=sql,text=True,capture_output=True)
 (root/'stdout').write_text(result.stdout);(root/'stderr').write_text(result.stderr);assert result.returncode==0,result.stderr
 data={}
 for line in result.stdout.splitlines():
  if '|' in line:
   label,value=line.split('|',1)
   if label=='scratch_schema_absent':assert value=='t'
   elif any(label==case+suffix for case in cases for suffix in ['_returned','_rows','_events']):data[label]=json.loads(value)
 assert len(data)==len(cases)*3
 version=subprocess.check_output(['docker','exec',name,'psql','-X','-A','-t','-U','postgres','-c','SELECT version()'],text=True).strip()
 (root/'events.json').write_text(json.dumps(data,indent=2)+'\n');(root/'result.json').write_text(json.dumps({'status':'native_pg_control_passed','version':version,'cases':list(cases),'image_id':image,'started_unix':started,'finished_unix':time.time(),'scope':'Actual isolated tmpfs PostgreSQL control for statement trigger visibility/suppression only; default fsync unchanged. Existing reference data untouched; no physical durability claim.'},indent=2)+'\n')
 dest=pathlib.Path('/root/bicdb-partition-runtime/crates/bicdb-sql/tests/fixtures/postgres/partition_trigger_visibility_pg16.json');dest.write_text(json.dumps(data,indent=2)+'\n')
 print(json.dumps(data))
finally:subprocess.run(['docker','rm','-f',name],stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
