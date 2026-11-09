from pathlib import Path
import os,re,shutil,subprocess
base=Path('/root/bicdb-durability-s3/vm-reset')
root=base/'initroot'
for directory in ['bin','sbin','lib64','proc','sys','dev','run','data','tmp','etc']:(root/directory).mkdir(parents=True,exist_ok=True)
shutil.copy2('/usr/bin/busybox',root/'bin/busybox')
if (base/'proxy').exists():shutil.copy2(base/'proxy',root/'bin/proxy')
for name in ['sh','mount','mkdir','ifconfig','sleep','sync','reboot','cat','echo','poweroff']:
 p=root/'bin'/name
 if not p.exists():p.symlink_to('busybox')
for binary,target in [(os.environ.get('BICDB_VM_BINARY','/root/bicdb-durability-s3/target-verify/debug/bicdb'),'bin/bicdb'),('/usr/sbin/e2fsck','sbin/e2fsck')]:
 shutil.copy2(binary,root/target)
 subprocess.run(['strip',str(root/target)],check=True)
 for library in re.findall(r'(/[^\s()]+)',subprocess.check_output(['ldd',binary],text=True)):
  dest=root/library.lstrip('/');dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(library,dest)
(root/'etc/passwd').write_text('root:x:0:0:root:/root:/bin/sh\n')
(root/'init').write_text('''#!/bin/sh
export PATH=/bin:/sbin
mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t devtmpfs devtmpfs /dev
mount -t tmpfs tmpfs /run
ifconfig lo 127.0.0.1 up
ifconfig eth0 10.0.2.15 netmask 255.255.255.0 up
/sbin/e2fsck -p /dev/vda
mount -t ext4 /dev/vda /data || exec /bin/sh
printf 'VM_DISK_MOUNTED\\n'
export BICDB_SYNC_OUTBOX=off MALLOC_ARENA_MAX=2
/bin/bicdb serve-pg /data/db --host 0.0.0.0 --port 5432 --allow-remote-no-auth --storage-sync durable > /data/server.log 2>&1 &
server=$!
sleep 2
cat /data/server.log
printf 'VM_SERVER_STARTED pid=%s\\n' "$server"
ifconfig -a
while true; do /bin/sh </dev/ttyS0 >/dev/ttyS0 2>&1; sleep 1; done
''')
if os.environ.get('BICDB_VM_SERVER_KIND') == 'oracle':
 text=(root/'init').read_text()
 text=text.replace('/bin/bicdb serve-pg /data/db --host 0.0.0.0 --port 5432 --allow-remote-no-auth --storage-sync durable', '/bin/bicdb /data/db 5433 postgres 64 16 server_paged')
 text=text.replace('server=$!', 'server=$!\n/bin/proxy 5432 5433 &')
 text=text.replace('export BICDB_SYNC_OUTBOX=off MALLOC_ARENA_MAX=2', 'export BICDB_SYNC_OUTBOX=off MALLOC_ARENA_MAX=2\nexport BICDB_PAGED_BUFFER_POOL_BYTES='+os.environ.get('BICDB_VM_POOL','268435456')+'\nexport BICDB_PAGED_WAL_MAX_BYTES='+os.environ.get('BICDB_VM_WAL','1073741824')+'\nexport BICDB_PAGED_WAL_SEGMENT_BYTES='+os.environ.get('BICDB_VM_SEGMENT','0'))
 (root/'init').write_text(text)
os.chmod(root/'init',0o755)
subprocess.run("find . -print0 | cpio --null -o --format=newc 2>/dev/null | gzip -1 > ../initrd.gz",shell=True,cwd=root,check=True)
kernel=Path('/boot')/('vmlinuz-'+os.uname().release)
shutil.copy2(kernel,base/'vmlinuz')
disk=Path(os.environ.get('BICDB_VM_DISK', str(base/'smoke.raw')))
assert disk.parent == base
if not disk.exists():
 with disk.open('wb') as file:file.truncate(2*1024**3)
 subprocess.run(['mkfs.ext4','-q','-F',str(disk)],check=True)
print('prepared',base,'initrd', (base/'initrd.gz').stat().st_size)
