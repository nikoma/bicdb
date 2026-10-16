#!/usr/bin/env python3
"""Write a pinned, owner-only microVM host configuration from operator assets."""
import argparse
import hashlib
import ipaddress
import json
import os
import re
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
for asset in ['firecracker', 'jailer', 'kernel', 'rootfs']:
    parser.add_argument('--' + asset, required=True, type=Path)
parser.add_argument('--application', required=True)
parser.add_argument('--host-ip', required=True, type=ipaddress.IPv4Address)
parser.add_argument('--guest-ip', required=True, type=ipaddress.IPv4Address)
parser.add_argument('--api-port', default=8789, type=int)
parser.add_argument('--uid', required=True, type=int)
parser.add_argument('--gid', required=True, type=int)
parser.add_argument('--vcpus', default=1, type=int)
parser.add_argument('--output', required=True, type=Path)
args = parser.parse_args()
if os.geteuid() != 0:
    parser.error('run as the operator that owns the privileged supervisor (root)')
if args.output.exists():
    parser.error('output already exists')
if args.uid < 10000 or args.gid < 10000 or not 1 <= args.vcpus <= 8 or not 1024 <= args.api_port <= 65535:
    parser.error('require dedicated UID/GID >= 10000, 1..8 vCPUs and API port >= 1024')
if not re.fullmatch(r'[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?', args.application):
    parser.error('invalid application name')
private_ranges = [ipaddress.IPv4Network(value) for value in ['10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16']]
network = ipaddress.IPv4Network(str(args.host_ip) + '/30', strict=False)
if not any(args.host_ip in block for block in private_ranges) or not any(args.guest_ip in block for block in private_ranges) or args.host_ip == args.guest_ip or args.host_ip not in network.hosts() or args.guest_ip not in network.hosts():
    parser.error('use two distinct private host addresses from the same /30')
configuration = {'format': 1, 'allocations': {args.application: {
    'host_ip': str(args.host_ip), 'guest_ip': str(args.guest_ip), 'api_port': args.api_port,
    'uid': args.uid, 'gid': args.gid, 'vcpus': args.vcpus,
}}}
for name in ['firecracker', 'jailer', 'kernel', 'rootfs']:
    path = getattr(args, name).resolve(strict=True)
    with path.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    configuration[name] = str(path)
    configuration[name + '_sha256'] = digest
with os.fdopen(os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as output:
    json.dump(configuration, output, indent=2)
    output.write('\n')
    output.flush()
    os.fsync(output.fileno())
print(json.dumps({'configuration': str(args.output.resolve()), 'application': args.application}))
