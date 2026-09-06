"""Run one comparison with bounded checks. Main demo must precede non-Fluss examples."""
import argparse
import json
import re
import subprocess
import time
from pathlib import Path
from urllib.request import urlopen

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument('example', choices=['flink', 'fluss', 'materialize', 'arroyo'])
example = parser.parse_args().example
compose = ['docker', 'compose']
if example != 'fluss':
    compose += ['-f', 'compose.yaml']
compose += ['-f', f'examples/{example}/compose.yaml', '--profile', 'tools']


def command(*args):
    result = subprocess.run([*compose, *args], cwd=root, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=600)
    print(result.stdout, flush=True)
    result.check_returncode()
    if '[ERROR]' in result.stdout:
        raise RuntimeError('SQL client reported an error')
    return result.stdout


def lab(script, *args):
    return command('run', '--rm', '--entrypoint', 'python', 'lab', script, *args)


def sql(service, filename):
    return command('exec', '-T', service, '/opt/flink/bin/sql-client.sh', '-f', '/sql/' + filename)


if example == 'flink':
    command('up', '-d', '--build', 'flink-jobmanager', 'flink-taskmanager')
    lab('examples/flink/verify.py', '--ready')
    sql('flink-jobmanager', 'windows.sql')
    lab('examples/flink/verify.py')
elif example in {'materialize', 'arroyo'}:
    command('up', '-d', example)
    lab(f'examples/{example}/run.py')
else:
    command('up', '-d')
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        try:
            with urlopen('http://localhost:8083/taskmanagers', timeout=5) as response:
                if json.load(response)['taskmanagers']:
                    break
        except OSError:
            pass
        time.sleep(2)
    else:
        raise TimeoutError('Fluss example TaskManager not registered')
    sql('jobmanager', '01-pipeline.sql')
    for amount in (1700, 1800):
        if amount == 1800:
            sql('jobmanager', '03-mutate.sql')
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            output = sql('jobmanager', '02-query.sql')
            rows = set(re.findall(r'\|\s*(u[12])\s*\|\s*(\d+)\s*\|', output))
            if rows == {('u1', str(amount)), ('u2', '700')}:
                break
            time.sleep(2)
        else:
            raise AssertionError(f'Fluss aggregate mismatch: {rows}')
    print('PASS: Fluss initial, updated and deleted order aggregates')
