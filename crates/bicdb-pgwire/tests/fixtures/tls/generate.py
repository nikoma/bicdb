#!/usr/bin/env python3
"""Regenerate public test certificates and keys using OpenSSL; never use for hosting."""
from pathlib import Path
import subprocess
import tempfile

root = Path(__file__).resolve().parent

def openssl(*args):
    subprocess.run(['openssl', *map(str, args)], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

with tempfile.TemporaryDirectory() as scratch:
    scratch = Path(scratch)
    rsa = scratch / 'rsa.key'
    openssl('genpkey', '-algorithm', 'RSA', '-pkeyopt', 'rsa_keygen_bits:2048', '-out', rsa)
    for name, algorithm, digest, extra in [
        ('sha256', 'prime256v1', '-sha256', []),
        ('sha384', 'secp384r1', '-sha384', []),
        ('sha224', None, '-sha224', []),
        ('sha512', None, '-sha512', []),
        ('sha1', None, '-sha1', []),
        ('md5', None, '-md5', []),
        ('pss384', None, '-sha384', ['-sigopt', 'rsa_padding_mode:pss', '-sigopt', 'rsa_mgf1_md:sha256']),
        ('pss_default', None, '-sha1', ['-sigopt', 'rsa_padding_mode:pss', '-sigopt', 'rsa_pss_saltlen:20']),
        ('ed25519', 'ED25519', None, []),
    ]:
        key = scratch / (name + '.key')
        if algorithm == 'ED25519':
            openssl('genpkey', '-algorithm', algorithm, '-out', key)
        elif algorithm:
            openssl('genpkey', '-algorithm', 'EC', '-pkeyopt', 'ec_paramgen_curve:' + algorithm, '-out', key)
        else:
            key = rsa
        openssl('req', '-new', '-x509', '-key', key, '-out', root / (name + '.pem'),
                '-days', '14610', '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost,IP:127.0.0.1',
                *([digest] if digest else []), *extra)
        if name in ['sha256', 'sha384', 'ed25519']:
            (root / (name + '.key')).write_bytes(key.read_bytes())
print('Generated public test-only fixtures in', root)
