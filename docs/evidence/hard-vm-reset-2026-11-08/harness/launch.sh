#!/bin/bash
set -euo pipefail
V=/root/bicdb-durability-s3/vm-reset
Q=$V/qemu
export LD_LIBRARY_PATH=$Q/usr/lib/x86_64-linux-gnu
exec "$Q/usr/bin/qemu-system-x86_64" -L "$Q/usr/share/qemu" -name bicdb-disposable-recovery-smoke -nodefaults -no-user-config -machine q35,accel=kvm -cpu host -m 4096 -smp 2 -display none -chardev socket,id=serial0,path=$V/console.sock,server=on,wait=off,logfile=$V/console.log -serial chardev:serial0 -monitor none -qmp unix:$V/qmp.sock,server=on,wait=off -bios "$Q/usr/share/seabios/bios-256k.bin" -kernel "$V/vmlinuz" -initrd "$V/initrd.gz" -append 'console=ttyS0 panic=-1 reboot=t quiet' -drive "file=${BICDB_VM_DISK:-$V/smoke.raw},format=raw,if=virtio,cache=writeback,aio=threads" -netdev user,id=n0,restrict=on,hostfwd=tcp:127.0.0.1:56388-:5432 -device virtio-net-pci,netdev=n0,romfile=
