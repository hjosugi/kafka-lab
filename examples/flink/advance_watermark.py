"""Advance every source partition beyond the demo window."""
from confluent_kafka import Producer
from streaming_lab.transport import BROKER
import json

p = Producer({'bootstrap.servers': BROKER, 'enable.idempotence': True})
errors = []
for partition in range(3):
    event = {'event_id': f'watermark-{partition}', 'event_time': '2026-09-06T00:02:00Z',
             'kind': 'watermark', 'service': '', 'level': ''}
    p.produce('lab.events', partition=partition, value=json.dumps(event).encode(),
              on_delivery=lambda error, msg: errors.append(str(error)) if error else None)
if p.flush(30) or errors:
    raise RuntimeError(f'Watermark marker delivery failed: {errors}')
