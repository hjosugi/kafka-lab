"""Durable idempotent target. SQLite commit precedes Kafka offset commit."""
import sqlite3
from .domain import canonical, validate_change


def connect(path):
    db = sqlite3.connect(path)
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('PRAGMA synchronous=FULL')
    db.executescript('''
        CREATE TABLE IF NOT EXISTS inbox (
            event_id TEXT PRIMARY KEY, payload TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY, version INTEGER NOT NULL,
            deleted INTEGER NOT NULL, customer_id TEXT NOT NULL,
            amount_cents INTEGER NOT NULL, status TEXT NOT NULL,
            payload TEXT NOT NULL
        );
    ''')
    return db


def apply_change(db, change):
    validate_change(change)
    payload = canonical(change)
    # BEGIN IMMEDIATE serializes the read/compare/write transaction.
    db.execute('BEGIN IMMEDIATE')
    try:
        previous = db.execute('SELECT payload FROM inbox WHERE event_id = ?',
                              (change['event_id'],)).fetchone()
        if previous:
            if previous[0] != payload:
                raise ValueError('Conflicting payload for an existing event_id')
            db.commit()
            return 'duplicate'
        current = db.execute('SELECT version, payload FROM orders WHERE order_id = ?',
                             (change['order_id'],)).fetchone()
        if current and current[0] == change['version'] and current[1] != payload:
            raise ValueError('Conflicting payload at the same entity version')
        db.execute('INSERT INTO inbox VALUES (?, ?)', (change['event_id'], payload))
        result = 'stale'
        if current is None or change['version'] > current[0]:
            db.execute('''INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(order_id) DO UPDATE SET version=excluded.version,
                deleted=excluded.deleted, customer_id=excluded.customer_id,
                amount_cents=excluded.amount_cents, status=excluded.status,
                payload=excluded.payload''',
                (change['order_id'], change['version'], change['op'] == 'delete',
                 change['customer_id'], change['amount_cents'], change['status'], payload))
            result = 'applied'
        db.commit()
        return result
    except BaseException:
        db.rollback()
        raise


def live_rows(db):
    return db.execute('''SELECT order_id, customer_id, amount_cents, status, version
                         FROM orders WHERE deleted = 0 ORDER BY order_id''').fetchall()
