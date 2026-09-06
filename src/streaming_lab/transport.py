"""Network adapters. All waits are bounded and delivery failures are fatal."""
import json
import os
import time
from pathlib import Path
from .domain import canonical, validate_change, unique_events
from .projection import apply_change, connect

ROOT = Path(__file__).resolve().parents[2]
BROKER = os.getenv('KAFKA_BOOTSTRAP', 'localhost:19092')
PG_DSN = os.getenv('PG_DSN', 'postgresql://lab:lab-only@localhost:15432/shop')
RW_DSN = os.getenv('RW_DSN', 'postgresql://root@localhost:4566/dev')
TARGET = os.getenv('TARGET_DB', 'runtime/target.sqlite')


def sql_connect(dsn, timeout=120):
    import psycopg
    deadline = time.monotonic() + timeout
    while True:
        try:
            return psycopg.connect(dsn, autocommit=True, connect_timeout=5)
        except psycopg.OperationalError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(1)


def ensure_topics():
    from confluent_kafka import KafkaException, KafkaError
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({'bootstrap.servers': BROKER})
    futures = admin.create_topics([
        NewTopic(name, num_partitions=3, replication_factor=1)
        for name in ['lab.events', 'lab.orders', 'lab.arroyo.alerts', 'lab.flink.windows']
    ], request_timeout=30)
    for future in futures.values():
        try:
            future.result(35)
        except KafkaException as error:
            if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise


def send(topic, records):
    from confluent_kafka import Producer
    producer = Producer({'bootstrap.servers': BROKER, 'enable.idempotence': True,
                         'acks': 'all', 'delivery.timeout.ms': 30000})
    failures = []
    def delivered(error, message):
        if error:
            failures.append(str(error))
    for key, payload in records:
        producer.produce(topic, key=str(key).encode(), value=canonical(payload).encode(),
                         on_delivery=delivered)
        producer.poll(0)
    remaining = producer.flush(35)
    if failures or remaining:
        raise RuntimeError(f'Kafka delivery failed: {failures}, pending={remaining}')


def initialize():
    ensure_topics()
    with sql_connect(RW_DSN) as db:
        for path in sorted((ROOT / 'sql/risingwave').glob('*.sql')):
            sql = '\n'.join(line for line in path.read_text().splitlines()
                            if not line.lstrip().startswith('--'))
            for statement in sql.split(';'):
                if statement.strip():
                    db.execute(statement)


def emit_events():
    events = [json.loads(line) for line in (ROOT / 'data/events.jsonl').read_text().splitlines()]
    unique_events(events)
    send('lab.events', [(e['user_id'] or e['service'], e) for e in events])
    return len(events)


def write_order(change):
    from psycopg.types.json import Jsonb
    validate_change(change)
    with sql_connect(PG_DSN) as db, db.transaction():
        # One lock per entity, including deletes and first inserts.
        db.execute('SELECT pg_advisory_xact_lock(%s)', (change['order_id'],))
        existing = db.execute('SELECT payload FROM outbox WHERE event_id=%s',
                              (change['event_id'],)).fetchone()
        if existing:
            if existing[0] != change:
                raise ValueError('Conflicting event_id')
            return
        version = db.execute('SELECT MAX(version) FROM outbox WHERE order_id=%s',
                             (change['order_id'],)).fetchone()[0]
        if version is not None and change['version'] <= version:
            raise ValueError('Source versions must increase')
        if change['op'] == 'delete':
            db.execute('DELETE FROM orders WHERE order_id=%s', (change['order_id'],))
        else:
            db.execute('''INSERT INTO orders VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT(order_id) DO UPDATE SET customer_id=excluded.customer_id,
                amount_cents=excluded.amount_cents, status=excluded.status,
                version=excluded.version''',
                tuple(change[k] for k in ['order_id', 'customer_id', 'amount_cents', 'status', 'version']))
        db.execute('INSERT INTO outbox(event_id,order_id,version,payload) VALUES (%s,%s,%s,%s)',
                   (change['event_id'], change['order_id'], change['version'], Jsonb(change)))


def publish_outbox():
    # A crash after Kafka acknowledgement but before PG commit may publish twice.
    count = 0
    with sql_connect(PG_DSN) as db, db.transaction():
        rows = db.execute('''SELECT event_id,order_id,payload FROM outbox
            WHERE published_at IS NULL ORDER BY order_id,version
            FOR UPDATE SKIP LOCKED LIMIT 1000''').fetchall()
        for event_id, order_id, payload in rows:
            send('lab.orders', [(order_id, payload)])
            db.execute('UPDATE outbox SET published_at=now() WHERE event_id=%s', (event_id,))
            count += 1
    return count


def snapshot_start(committed, low, high, replay=False):
    """Reject missing retained history unless a partial replay is explicit."""
    if replay:
        return low
    start = 0 if committed < 0 else committed
    if start < low:
        raise RuntimeError('Committed offset has expired; rebuild and reconcile the target')
    if start > high:
        raise RuntimeError('Committed offset exceeds the topic end')
    return start


def consume_snapshot(replay=False, timeout=90, before_offset_commit=None):
    from confluent_kafka import Consumer, TopicPartition
    consumer = Consumer({'bootstrap.servers': BROKER, 'group.id': 'lab-migration-v1',
                         'enable.auto.commit': False, 'enable.auto.offset.store': False})
    Path(TARGET).parent.mkdir(parents=True, exist_ok=True)
    db = connect(TARGET)
    count = 0
    try:
        info = consumer.list_topics('lab.orders', timeout=15).topics['lab.orders']
        if info.error:
            raise RuntimeError(str(info.error))
        partitions = [TopicPartition('lab.orders', p) for p in sorted(info.partitions)]
        committed = consumer.committed(partitions, timeout=15)
        assigned, bounds, positions = [], {}, {}
        for tp in committed:
            low, high = consumer.get_watermark_offsets(tp, timeout=15)
            start = snapshot_start(tp.offset, low, high, replay)
            assigned.append(TopicPartition(tp.topic, tp.partition, start))
            bounds[tp.partition], positions[tp.partition] = high, start
        # This bounded lab owns every partition. Run exactly one consumer process.
        consumer.assign(assigned)
        deadline = time.monotonic() + timeout
        while any(positions[p] < end for p, end in bounds.items()):
            if time.monotonic() > deadline:
                raise TimeoutError(f'Consumer timed out: {positions} / {bounds}')
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            if message.offset() >= bounds[message.partition()]:
                continue
            change = json.loads(message.value())
            if message.key() != str(change['order_id']).encode():
                raise ValueError('Kafka key must match order_id')
            apply_change(db, change)
            if before_offset_commit is not None:
                before_offset_commit(change)
            consumer.commit(message=message, asynchronous=False)
            positions[message.partition()] = message.offset() + 1
            count += 1
        return count
    finally:
        try:
            consumer.close()
        finally:
            db.close()
