"""Run after demo in an isolated lab: real Kafka redelivery across a DB commit."""
import json
from streaming_lab import transport as t
from streaming_lab.cli import verify
from streaming_lab.projection import connect


def crash_after_db_commit(change):
    raise RuntimeError('injected failure after SQLite commit')


changes = json.loads((t.ROOT / 'data/order_changes.json').read_text())
# Republish the exact acknowledged payloads to exercise the duplicate boundary.
t.send('lab.orders', [(c['order_id'], c) for c in changes])
try:
    t.consume_snapshot(before_offset_commit=crash_after_db_commit)
except RuntimeError as error:
    if str(error) != 'injected failure after SQLite commit':
        raise
else:
    raise AssertionError('Expected a failure between target and offset commits')
assert t.consume_snapshot() == len(changes), 'Failed record must be redelivered'
verify()
db = connect(t.TARGET)
try:
    assert db.execute('SELECT COUNT(*) FROM inbox').fetchone()[0] == len(changes)
finally:
    db.close()
t.emit_events()
verify()
print('PASS: redelivery after DB commit, durable inbox, analytics deduplication')
