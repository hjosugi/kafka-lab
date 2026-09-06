"""Check real task registration, checkpointed input, and finalized Kafka windows."""
import json
import runpy
import sys
import time
from pathlib import Path
from urllib.request import urlopen
from confluent_kafka import Consumer, TopicPartition
from streaming_lab.transport import BROKER

deadline = time.monotonic() + 180
if '--ready' in sys.argv:
    while time.monotonic() < deadline:
        try:
            with urlopen('http://flink-jobmanager:8081/taskmanagers', timeout=5) as response:
                if json.load(response)['taskmanagers']:
                    print('PASS: Flink TaskManager registered')
                    sys.exit(0)
        except OSError:
            pass
        time.sleep(2)
    raise TimeoutError('Flink TaskManager not registered')

input_offsets = Consumer({'bootstrap.servers': BROKER, 'group.id': 'lab-flink-windows',
                          'enable.auto.commit': False})
try:
    while time.monotonic() < deadline:
        offsets = input_offsets.committed([TopicPartition('lab.events', p) for p in range(3)], timeout=10)
        if sum(max(p.offset, 0) for p in offsets) >= 11:
            break
        time.sleep(2)
    else:
        raise TimeoutError('Flink did not checkpoint the 11 fixture records')
finally:
    input_offsets.close()
runpy.run_path(str(Path(__file__).with_name('advance_watermark.py')))
consumer = Consumer({'bootstrap.servers': BROKER, 'group.id': 'lab-flink-verifier',
                     'enable.auto.commit': False, 'auto.offset.reset': 'earliest'})
expected = {('catalog', 1, 0), ('checkout', 5, 4)}
seen = set()
try:
    consumer.subscribe(['lab.flink.windows'])
    while time.monotonic() < deadline and seen != expected:
        message = consumer.poll(1)
        if message is None:
            continue
        if message.error():
            raise RuntimeError(str(message.error()))
        row = json.loads(message.value())
        value = (row['service'], row['requests'], row['errors'])
        assert value in expected, row
        assert row['window_start'].replace('T', ' ') == '2026-09-06 00:00:00', row
        assert row['window_end'].replace('T', ' ') == '2026-09-06 00:01:00', row
        seen.add(value)
    assert seen == expected, seen
finally:
    consumer.close()
print('PASS: Flink checkpointed input and event-time windows', sorted(seen))
