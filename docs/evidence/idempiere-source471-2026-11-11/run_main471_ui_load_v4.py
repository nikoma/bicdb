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

state = Path('/root/.local/state/bicdb/typed-compare')
folder = state / 'main471-ui-load-v4'
folder.mkdir()
source = 'b0e2498842be7b01e355bb8da1cc249ff16be056'
prefix = 'BicdbLoad471' + uuid.uuid4().hex[:12]
record = {'profile': 'existing_login_and_listbox_preferences_after_retained_native_reproduced_first_use_failures', 'prior_failed_runs': ['main471-ui-load-v1','main471-ui-load-v2'], 'native_comparison': 'main471-ui-load-v1/native-preference-race-comparison.json', 'fixture_state': 'same private fixture after original UI created login and listbox preferences in retained failed first-use runs; no SQL preference seeding', 'frontend_synchronization': 'Wait for installed ZK zAu.processing() and zk.processing queues to become idle; actions/assertions/values/users unchanged', 'source': source, 'source_version': '1.0.471-beta', 'status': 'starting',
          'binary_sha256': 'e23d7c1a078b7ce6722237f35cfa67a822727d9762c928c808f3ce2767c543c2', 'coordinator_receipt':'main471-ui-coordinator.json',
          'prefix': prefix, 'sessions': {}, 'started_unix': time.time(),
          'process_crash_qualified': False, 'physical_power_loss': False}
database = sqlite3.connect(folder / 'client-acks.sqlite')
database.execute('PRAGMA journal_mode=WAL')
database.execute('PRAGMA synchronous=FULL')
database.execute('CREATE TABLE attempts(marker TEXT PRIMARY KEY, worker INTEGER, username TEXT, user_id INTEGER, started REAL, ack_time REAL, document_no TEXT, error TEXT)')
database.commit()


def save():
    temporary = folder / 'result.tmp'
    temporary.write_text(json.dumps(record, indent=2) + '\n')
    temporary.replace(folder / 'result.json')


async def wait_ui_idle(page):
    await page.wait_for_function("() => window.zAu && !zAu.processing() && !zk.processing", timeout=30000)

async def run():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    tunnel = subprocess.Popen([
        'ssh', '-N', '-o', 'BatchMode=yes', '-o', 'ExitOnForwardFailure=yes',
        '-o', 'ServerAliveInterval=15',
        '-L', f'127.0.0.1:{port}:127.0.0.1:43845', 'root@74.50.69.58',
    ], stdout=subprocess.DEVNULL, stderr=(folder / 'tunnel.log').open('ab'))
    record['tunnel_pid'] = tunnel.pid
    save()
    try:
        deadline = time.monotonic() + 15
        while True:
            assert tunnel.poll() is None
            try:
                with socket.create_connection(('127.0.0.1', port), timeout=.2):
                    break
            except OSError:
                assert time.monotonic() < deadline
                await asyncio.sleep(.1)
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
                    await wait_ui_idle(page)
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
                        await wait_ui_idle(page)
                        session['stage'] = 'new_record'
                        save()
                        await page.locator('a[title="New    Alt+N"]').filter(visible=True).first.click()
                        await page.wait_for_timeout(1000)
                        await wait_ui_idle(page)
                        session['stage'] = 'organization'
                        save()
                        await page.locator('[instancename="C_Order0AD_Org_ID"] input').filter(visible=True).fill('HQ')
                        await page.locator('[instancename="C_Order0AD_Org_ID"] input').filter(visible=True).press('Tab')
                        await expect(page.locator('[instancename="C_Order0M_Warehouse_ID"] input').filter(visible=True)).to_have_value('HQ Warehouse', timeout=30000)
                        await wait_ui_idle(page)
                        session['stage'] = 'partner'
                        save()
                        partner = page.locator('[instancename="C_Order0C_BPartner_ID"] input').filter(visible=True)
                        await partner.fill('JoeBlock')
                        await partner.press('Tab')
                        await expect(page.locator('[instancename="C_Order0C_BPartner_Location_ID"] input').filter(visible=True)).to_have_value('Hartford', timeout=30000)
                        await page.wait_for_timeout(250)
                        await wait_ui_idle(page)
                        session['stage'] = 'document_type'
                        save()
                        doctype = page.locator('[instancename="C_Order0C_DocTypeTarget_ID"] input').filter(visible=True)
                        await doctype.fill('Standard Order')
                        await doctype.press('Tab')
                        await expect(page.locator('input[instancename="C_Order0DocumentNo"]').filter(visible=True)).to_have_value(re.compile(r'^<5[0-9]+>$'), timeout=30000)
                        await page.wait_for_timeout(250)
                        await wait_ui_idle(page)
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
        tunnel.terminate()
        tunnel.wait(timeout=10)

while subprocess.check_output(['systemctl','show','bicdb-main471-ui-load-v3','-p','ActiveState','--value'],text=True).strip() in ['active','activating','reloading','deactivating']:
    time.sleep(2)

save()
asyncio.run(run())
