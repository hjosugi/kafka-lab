"""Run with PYTHONPATH=src python -m streaming_lab.cli."""
import argparse
import json
import time
from pathlib import Path
from .domain import summarize
from .projection import connect, live_rows


def wait_rows(db, query, expected, timeout=120):
    deadline = time.monotonic() + timeout
    while True:
        actual = [list(row) for row in db.execute(query).fetchall()]
        if actual == expected:
            return actual
        if time.monotonic() >= deadline:
            raise AssertionError(f'{query}: expected {expected!r}, got {actual!r}')
        time.sleep(1)


def verify():
    from . import transport as t
    expected = json.loads((t.ROOT / 'data/expected.json').read_text())
    with t.sql_connect(t.RW_DSN) as db:
        wait_rows(db, 'SELECT service,level,total FROM log_counts ORDER BY service,level', expected['log_counts'])
        wait_rows(db, 'SELECT user_id,category,score FROM recommendation_scores ORDER BY user_id,category', expected['recommendation_scores'])
        wait_rows(db, 'SELECT service,requests,errors FROM alerts ORDER BY service,minute', [['checkout', 4, 3]])
        wait_rows(db, 'SELECT order_id,customer_id,amount_cents,status,version FROM cdc_orders ORDER BY order_id', expected['orders'])
        wait_rows(db, 'SELECT customer_id,revenue_cents,orders FROM revenue_by_customer ORDER BY customer_id', [['u1', 1800, 1], ['u2', 700, 1]])
    with t.sql_connect(t.PG_DSN) as db:
        source = db.execute('SELECT * FROM orders ORDER BY order_id').fetchall()
        if db.execute('SELECT COUNT(*) FROM outbox WHERE published_at IS NULL').fetchone()[0]:
            raise AssertionError('Unpublished outbox rows remain')
    target = connect(t.TARGET)
    try:
        if source != live_rows(target):
            raise AssertionError(f'Reconciliation failed: source={source}, target={live_rows(target)}')
    finally:
        target.close()
    print('PASS: logs, recommendations, alerts, CDC, migration reconciliation')


def demo():
    from . import transport as t
    with t.sql_connect(t.PG_DSN) as db:
        if db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0]:
            raise RuntimeError('demo needs a fresh lab. Use verify/replay for an existing run, or scripts/lab.sh reset')
    t.initialize()
    t.emit_events()
    changes = json.loads((t.ROOT / 'data/order_changes.json').read_text())
    for change in changes[:2]:
        t.write_order(change)
    t.publish_outbox()
    t.consume_snapshot()
    with t.sql_connect(t.RW_DSN) as db:
        wait_rows(db, 'SELECT order_id,amount_cents FROM cdc_orders ORDER BY order_id', [[1001, 1200], [1002, 500]])
    for change in changes[2:]:
        t.write_order(change)
    t.publish_outbox()
    t.consume_snapshot()
    verify()
    print(f'Replaying retained Kafka records: {t.consume_snapshot(replay=True)}')
    verify()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['offline', 'init', 'events', 'publish', 'consume', 'replay', 'verify', 'demo', 'query'])
    args = parser.parse_args()
    if args.command == 'offline':
        root = Path(__file__).resolve().parents[2]
        events = [json.loads(x) for x in (root / 'data/events.jsonl').read_text().splitlines()]
        print(json.dumps(summarize(events), indent=2))
        return
    from . import transport as t
    if args.command == 'query':
        with t.sql_connect(t.RW_DSN) as db:
            for name in ['log_counts', 'recommendation_scores', 'alerts', 'cdc_orders', 'revenue_by_customer']:
                print(name, db.execute(f'SELECT * FROM {name} LIMIT 20').fetchall())
        return
    actions = {'init': t.initialize, 'events': t.emit_events,
               'publish': t.publish_outbox, 'consume': t.consume_snapshot,
               'replay': lambda: t.consume_snapshot(replay=True), 'verify': verify, 'demo': demo}
    result = actions[args.command]()
    if result is not None:
        print(result)


if __name__ == '__main__':
    main()
