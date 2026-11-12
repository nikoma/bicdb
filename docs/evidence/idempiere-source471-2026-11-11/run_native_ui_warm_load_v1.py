"""Create orders through four isolated ERP browser sessions with durable client ACKs."""
import asyncio
import json
import os
from pathlib import Path
import re
import socket
import sqlite3
import subprocess
import time
import uuid

from playwright.async_api import async_playwright, expect

state = Path('/root/.local/state/bicdb/native-oracle-app')
folder = state / 'native-ui-warm-load-v1'
folder.mkdir()
source = 'native-oracle-23.26.3'
prefix = 'NativeOracleWarm' + uuid.uuid4().hex[:12]
record = {'profile': 'existing_preferences_after_retained_native_reproduced_cold_login_failure', 'source': source, 'engine': 'Oracle23.26.3.0.0', 'sdk_adapter_installed': False, 'status': 'waiting_native_ui', 'prefix': prefix, 'sessions': {}, 'started_unix': time.time(), 'process_crash_qualified': False, 'physical_power_loss': False}
database = sqlite3.connect(folder / 'client-acks.sqlite')
database.execute('PRAGMA journal_mode=WAL')
database.execute('PRAGMA synchronous=FULL')
database.execute('CREATE TABLE attempts(marker TEXT PRIMARY KEY, worker INTEGER, username TEXT, user_id INTEGER, started REAL, ack_time REAL, document_no TEXT, error TEXT)')
database.commit()


def save():
    temporary = folder / 'result.tmp'
    temporary.write_text(json.dumps(record, indent=2) + '\n')
    temporary.replace(folder / 'result.json')


while True:
    unit = subprocess.check_output(['systemctl', 'show', 'bicdb-native-oracle-ui-control-v4', '-p', 'ActiveState', '--value'], text=True).strip()
    assert unit in ['active', 'activating', 'reloading'], unit
    if (state / 'ui-control-v4.json').exists():
        native = json.loads((state / 'ui-control-v4.json').read_text())
        if native['status'] == 'http_ready_login_control_pending':
            break
    time.sleep(5)
assert native['clone_service'] == 'BICDBAPPCTRL' and native['sdk_adapter_installed'] is False
record['native_ui_base'] = native['base']
record['original_provider_sha256'] = native['original_provider_sha256']

async def run():
    port = native['port']
    record['direct_loopback_native_ui_port'] = port
    save()
    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(
                executable_path='/root/.cache/ms-playwright/chromium-1248/chrome-linux64/chrome',
                headless=True, args=['--no-sandbox', '--disable-dev-shm-usage'])

            async def worker(number):
                username, user_id = [('GardenAdmin', 101), ('GardenUser', 102)][number % 2]
                session = {'username': username, 'user_id': user_id, 'status': 'logging_in', 'acks': 0}
                record['sessions'][str(number)] = session
                save()
                context = await browser.new_context(viewport={'width': 1440, 'height': 1000})
                page = await context.new_page()
                try:
                    await page.goto(f'http://127.0.0.1:{port}/webui/', wait_until='domcontentloaded')
                    await page.locator('input[type=password]').wait_for(timeout=60000)
                    await page.locator('input[type=text]').first.fill(username)
                    await page.locator('input[type=password]').fill(username)
                    await page.get_by_role('button', name=re.compile(r'^\s*OK\s*$')).first.click()
                    await page.wait_for_function(
                        "() => /Warehouse|Log Out|Invalid User ID/.test(document.body.innerText)", timeout=120000)
                    body = await page.locator('body').inner_text()
                    assert 'Invalid User ID' not in body, body
                    if 'Log Out' not in body:
                        await page.get_by_role('button', name=re.compile(r'^\s*OK\s*$')).first.click()
                    await page.get_by_text('Log Out', exact=True).wait_for(timeout=120000)
                    favorite = page.get_by_text('Sales Order', exact=True).filter(visible=True).first
                    await favorite.wait_for(timeout=30000)
                    await favorite.click()
                    await page.locator('a[title="New    Alt+N"]').filter(visible=True).first.wait_for(timeout=60000)
                    session.update(status='ready', desktop=(await page.locator('body').inner_text())[:250])
                    await page.screenshot(path=str(folder / f'worker-{number}-ready.png'))
                    save()
                    while not (folder / 'start-load').exists():
                        assert not (folder / 'stop').exists()
                        await asyncio.sleep(.2)
                    session['status'] = 'working'
                    save()
                    for index in range(100):
                        if (folder / 'stop').exists() or (folder / 'crash-requested').exists():
                            break
                        marker = f'{prefix}-{number}-{index}'
                        session['stage'] = 'new_record'
                        save()
                        await page.locator('a[title="New    Alt+N"]').filter(visible=True).first.click()
                        await page.wait_for_timeout(1000)
                        session['stage'] = 'organization'
                        save()
                        await page.locator('[instancename="C_Order0AD_Org_ID"] input').filter(visible=True).fill('HQ')
                        await page.locator('[instancename="C_Order0AD_Org_ID"] input').filter(visible=True).press('Tab')
                        await expect(page.locator('[instancename="C_Order0M_Warehouse_ID"] input').filter(visible=True)).to_have_value('HQ Warehouse', timeout=30000)
                        session['stage'] = 'partner'
                        save()
                        partner = page.locator('[instancename="C_Order0C_BPartner_ID"] input').filter(visible=True)
                        await partner.fill('JoeBlock')
                        await partner.press('Tab')
                        await expect(page.locator('[instancename="C_Order0C_BPartner_Location_ID"] input').filter(visible=True)).to_have_value('Hartford', timeout=30000)
                        await page.wait_for_timeout(250)
                        session['stage'] = 'document_type'
                        save()
                        doctype = page.locator('[instancename="C_Order0C_DocTypeTarget_ID"] input').filter(visible=True)
                        await doctype.fill('Standard Order')
                        await doctype.press('Tab')
                        await expect(page.locator('input[instancename="C_Order0DocumentNo"]').filter(visible=True)).to_have_value(re.compile(r'^<5[0-9]+>$'), timeout=30000)
                        await page.wait_for_timeout(250)
                        session['stage'] = 'save'
                        save()
                        description = page.locator('textarea[instancename="C_Order0Description"]').filter(visible=True)
                        await description.fill(marker)
                        await description.press('Tab')
                        database.execute('INSERT INTO attempts(marker,worker,username,user_id,started) VALUES(?,?,?,?,?)',
                                         (marker, number, username, user_id, time.time()))
                        database.commit()
                        try:
                            await page.locator('a[title="Save Changes    Alt+S"]').filter(visible=True).first.click()
                            await page.wait_for_function(
                                """marker => {
                                    const doc = document.querySelector('input[instancename="C_Order0DocumentNo"]');
                                    const description = document.querySelector('textarea[instancename="C_Order0Description"]');
                                    return doc && /^[0-9]+$/.test(doc.value) &&
                                        description && description.value === marker &&
                                        document.body.innerText.includes('Record saved');
                                }""", arg=marker, timeout=30000)
                            document_no = await page.locator('input[instancename="C_Order0DocumentNo"]').filter(visible=True).input_value()
                            database.execute('UPDATE attempts SET ack_time=?,document_no=? WHERE marker=?',
                                             (time.time(), document_no, marker))
                            database.commit()
                            session['acks'] += 1
                            session['last_document'] = document_no
                            save()
                        except Exception as error:
                            database.execute('UPDATE attempts SET error=? WHERE marker=?', (repr(error), marker))
                            database.commit()
                            raise
                    session['status'] = 'finished'
                except Exception as error:
                    session.update(status='error', error=repr(error))
                    try:
                        session['fields'] = await page.locator('input:visible,textarea:visible').evaluate_all(
                            """elements => elements.map(e=>({instance:e.closest('[instancename]')?.getAttribute('instancename'),value:e.type==='password'?'[redacted]':e.value}))""")
                        session['body'] = (await page.locator('body').inner_text(timeout=3000))[-5000:]
                        await page.screenshot(path=str(folder / f'worker-{number}-error.png'), timeout=3000)
                    except Exception:
                        pass
                    save()
                finally:
                    await context.close()
                    save()

            tasks = [asyncio.create_task(worker(number)) for number in range(4)]
            record['status'] = 'preparing_sessions'
            save()
            while any(not task.done() for task in tasks):
                if len(record['sessions']) == 4 and all(
                    session['status'] == 'ready' for session in record['sessions'].values()):
                    record['status'] = 'all_sessions_ready'
                    save()
                if (folder / 'start-load').exists() and not (folder / 'crash-requested').exists():
                    record['status'] = 'load_running'
                    save()
                await asyncio.sleep(.25)
            await asyncio.gather(*tasks)
            await browser.close()
            record['status'] = ('stopped_after_requested_crash' if (folder / 'crash-requested').exists()
                                else 'sessions_finished')
            record['finished_unix'] = time.time()
            save()
    finally:
        database.close()
        (Path(native['base']) / 'stop-requested').write_text('Original four-session native UI workload control finished; preserve evidence and restore owned clone state\n')

save()
asyncio.run(run())
