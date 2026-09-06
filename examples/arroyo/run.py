"""Create an Arroyo pipeline through its API, then check Kafka output."""
import json
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen
from confluent_kafka import Consumer
from streaming_lab.transport import BROKER, emit_events

base = 'http://arroyo:5115/api/v1'


def api(path, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    with urlopen(Request(base + path, data=data, headers={'Content-Type': 'application/json'}), timeout=30) as response:
        return json.load(response)


deadline = time.monotonic() + 180
while True:
    try:
        api('/pipelines')
        break
    except (URLError, TimeoutError):
        if time.monotonic() > deadline:
            raise
        time.sleep(2)
pipeline = api('/pipelines', {'name': 'kafka-lab-errors',
                             'query': Path(__file__).with_name('errors.sql').read_text(),
                             'parallelism': 1})
while time.monotonic() < deadline:
    jobs = api(f'/pipelines/{pipeline["id"]}/jobs')
    jobs = jobs.get('data', []) if isinstance(jobs, dict) else jobs
    if any(job.get('state') == 'Running' for job in jobs):
        break
    if any(job.get('state') == 'Failed' for job in jobs):
        raise RuntimeError(f'Arroyo job failed: {jobs}')
    time.sleep(2)
else:
    raise TimeoutError('Arroyo pipeline did not reach Running')
emit_events()
consumer = Consumer({'bootstrap.servers': BROKER, 'group.id': 'lab-arroyo-verifier',
                     'enable.auto.commit': False, 'auto.offset.reset': 'earliest'})
seen = set()
try:
    consumer.subscribe(['lab.arroyo.alerts'])
    while time.monotonic() < deadline and seen != {'e002', 'e003', 'e004'}:
        message = consumer.poll(1)
        if message is None:
            continue
        if message.error():
            raise RuntimeError(str(message.error()))
        row = json.loads(message.value())
        assert row['service'] == 'checkout' and row['event_id'] in {'e002', 'e003', 'e004'}, row
        seen.add(row['event_id'])
    assert seen == {'e002', 'e003', 'e004'}, seen
finally:
    consumer.close()
print('PASS: Arroyo API pipeline and Kafka ERROR output', sorted(seen))
