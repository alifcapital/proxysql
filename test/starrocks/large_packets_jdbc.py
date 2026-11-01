#!/usr/bin/env python3
"""Compile tracked Java fixture and run an unmodified Connector/J jar.

Example: python3 large_packets_jdbc.py --config session-test.json
         --connector-jar mysql-connector-j-8.4.0.jar --proxy-port 16043
Credentials are supplied through child environment variables, never argv.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--config', type=Path, required=True)
p.add_argument('--connector-jar', type=Path, required=True)
p.add_argument('--proxy-port', type=int, default=16043)
a = p.parse_args()
source = Path(__file__).resolve().with_name('LargePacketsJdbc.java')
repository = source.parents[2]
subprocess.run(['git', '-C', str(repository), 'ls-files', '--error-unmatch', str(source.relative_to(repository))], check=True, stdout=subprocess.PIPE)
commit = subprocess.check_output(['git', '-C', str(repository), 'rev-parse', 'HEAD'], text=True).strip()
print('# source ' + json.dumps({'commit': commit, 'path': str(source), 'sha256': hashlib.sha256(source.read_bytes()).hexdigest(), 'driver_sha256': hashlib.sha256(a.connector_jar.read_bytes()).hexdigest()}, sort_keys=True), flush=True)
config = json.loads(a.config.read_text())
env = dict(os.environ)
for prefix, endpoint in [('BACKEND_', config['backend']), ('PROXY_', config['proxy'])]:
    for key in ['host', 'port', 'user', 'password']:
        env[prefix + key.upper()] = str(a.proxy_port if prefix == 'PROXY_' and key == 'port' else endpoint[key])
with tempfile.TemporaryDirectory(prefix='proxysql-jdbc-test-') as output:
    subprocess.run(['javac', '-d', output, str(source)], check=True)
    result = subprocess.run(['java', '-Xmx256m', '-cp', output + os.pathsep + str(a.connector_jar.resolve()), 'LargePacketsJdbc'], env=env)
    raise SystemExit(result.returncode)
