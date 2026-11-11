"""Compare the retained four-session cold-login failure using native Oracle UI."""
import asyncio,hashlib,json,pathlib,re,subprocess,time
from playwright.async_api import async_playwright
root=pathlib.Path(__file__).parent;out=root/'ui-login-control-v3';out.mkdir();record={'status':'waiting_native_ui','started_unix':time.time(),'engine':'Oracle23.26.3','clone_service':'BICDBAPPCTRL','sdk_adapter_installed':False,'sessions':{},'original_login_waits_unchanged':True,'full_bicdb_application_qualified':False}
def save():
 p=out/'result.tmp';p.write_text(json.dumps(record,indent=2)+'\n');p.replace(out/'result.json')
save()
while True:
 unit=subprocess.check_output(['systemctl','show','bicdb-native-oracle-ui-control-v3','-p','ActiveState','--value'],text=True).strip();assert unit in ['active','activating','reloading'],unit
 if (root/'ui-control-v3.json').exists():
  native=json.loads((root/'ui-control-v3.json').read_text())
  if native['status']=='http_ready_login_control_pending':break
 time.sleep(5)
assert native['clone_service']=='BICDBAPPCTRL' and native['sdk_adapter_installed'] is False
record.update(status='logging_in',native_ui_record_sha256=hashlib.sha256((root/'ui-control-v3.json').read_bytes()).hexdigest(),native_ui_base=native['base']);save()
async def main():
 async with async_playwright() as playwright:
  browser=await playwright.chromium.launch(executable_path='/root/.cache/ms-playwright/chromium-1248/chrome-linux64/chrome',headless=True,args=['--no-sandbox','--disable-dev-shm-usage'])
  async def login(number,username):
   session={'username':username,'status':'logging_in'};record['sessions'][str(number)]=session;save();context=await browser.new_context(viewport={'width':1440,'height':1000});page=await context.new_page()
   try:
    await page.goto('http://127.0.0.1:'+str(native['port'])+'/webui/',wait_until='domcontentloaded');await page.locator('input[type=password]').wait_for(timeout=60000);await page.locator('input[type=text]').first.fill(username);await page.locator('input[type=password]').fill(username);await page.get_by_role('button',name=re.compile(r'^\s*OK\s*$')).first.click();await page.wait_for_function("() => /Warehouse|Log Out|Invalid User ID/.test(document.body.innerText)",timeout=120000);body=await page.locator('body').inner_text();assert 'Invalid User ID' not in body,body
    if 'Log Out' not in body:await page.get_by_role('button',name=re.compile(r'^\s*OK\s*$')).first.click()
    await page.get_by_text('Log Out',exact=True).wait_for(timeout=120000)
    favorite=page.get_by_text('Sales Order',exact=True).filter(visible=True).first;await favorite.wait_for(timeout=30000);await favorite.click();await page.locator('a[title="New    Alt+N"]').filter(visible=True).first.wait_for(timeout=60000);session['status']='ready'
   except Exception as error:
    session.update(status='error',error=repr(error));session['body']=(await page.locator('body').inner_text())[-6000:]
   finally:
    await page.screenshot(path=str(out/('session-'+str(number)+'.png')));session['native_preference_constraint_failure']='AD_PREFERENCE_ATTRIBUTE' in session.get('body','').upper();save();await context.close()
  # Match the BicDB fixture's previously used GardenAdmin preferences.
  await login('warm-admin','GardenAdmin');assert record['sessions']['warm-admin']['status']=='ready'
  # Read and reset only this task-owned clone's web-login preference keys so
  # GardenUser's first use matches the retained BicDB first-use condition.
  code="""import json,pathlib,oracledb
state=pathlib.Path('/root/.local/state/bicdb/idempiere-reference');cfg=json.loads((state/'qualification-connection.json').read_text());password=pathlib.Path(cfg['password_file']).read_text().strip()
with oracledb.connect(user=cfg['user'],password=password,dsn=f\"{cfg['host']}:{cfg['port']}/BICDBAPPCTRL\") as c:
 q=c.cursor();q.execute(\"SELECT sys_context('USERENV','CON_NAME') FROM dual\");assert q.fetchone()[0]=='BICDBAPPCTRL'
 keys=['Language','Role','Client','Organization','Warehouse','MenuCollapsed','HelpCollapsed','HeaderCollapsed','RecordInfoDefaultTab']
 q.execute(\"SELECT COUNT(*) FROM AD_Preference WHERE AD_User_ID=102 AND AD_Window_ID IS NULL AND AD_Process_ID IS NULL AND PreferenceFor='W'\");before=q.fetchone()[0]
 for key in keys:q.execute(\"DELETE FROM AD_Preference WHERE AD_User_ID=102 AND Attribute=:key AND AD_Window_ID IS NULL AND AD_Process_ID IS NULL AND PreferenceFor='W'\",key=key)
 c.commit();print(json.dumps({'clone_only':True,'user_id':102,'preference_rows_before':before,'keys_reset':keys}))
"""
  record['clone_cold_preference_setup']=json.loads(subprocess.check_output(['/root/.local/state/bicdb/oracle-reference/venv/bin/python3','-c',code],text=True));save()
  await asyncio.gather(*(login(number,['GardenAdmin','GardenUser'][number%2]) for number in range(4)))
  await browser.close()
try:
 asyncio.run(main());record.update(status='native_login_control_completed',concurrent_sessions_reported=all(str(i) in record['sessions'] for i in range(4)),native_gardenuser_preference_failure_reproduced=any(record['sessions'][str(i)].get('native_preference_constraint_failure') for i in [1,3]),finished_unix=time.time());save()
finally:
 (pathlib.Path(native['base'])/'stop-requested').write_text('native login control complete; preserve clone and source evidence\n')
