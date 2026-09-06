"""Pure validation and deterministic reference calculations."""
from collections import Counter, defaultdict
from datetime import datetime, timezone
import json


def validate_event(event):
    required = {'event_id', 'event_time', 'kind', 'service', 'level',
                'user_id', 'category', 'action', 'latency_ms'}
    if not isinstance(event, dict) or set(event) != required:
        raise ValueError('Invalid event fields')
    if any(not isinstance(event[field], str) for field in required - {'latency_ms'}):
        raise ValueError('Event text fields must be strings')
    if not event['event_id'] or not event['service']:
        raise ValueError('event_id and service must not be empty')
    ts = datetime.fromisoformat(event['event_time'].replace('Z', '+00:00'))
    if ts.tzinfo is None:
        raise ValueError('event_time requires a timezone')
    if event['kind'] not in {'log', 'interaction'}:
        raise ValueError('Unknown event kind')
    if event['level'] not in {'DEBUG', 'INFO', 'WARN', 'ERROR'}:
        raise ValueError('Unknown log level')
    if type(event['latency_ms']) is not int or event['latency_ms'] < 0:
        raise ValueError('latency_ms must be a nonnegative integer')
    if event['kind'] == 'interaction' and event['action'] not in {'view', 'cart', 'buy'}:
        raise ValueError('Unknown interaction action')
    if event['kind'] == 'interaction' and (not event['user_id'] or not event['category']):
        raise ValueError('Interactions require user_id and category')
    return event


def unique_events(events):
    seen = {}
    for event in events:
        validate_event(event)
        old = seen.get(event['event_id'])
        if old is not None and old != event:
            raise ValueError('An event_id must identify an immutable payload')
        seen[event['event_id']] = event
    return list(seen.values())


def summarize(events):
    logs, scores, minutes = Counter(), Counter(), defaultdict(lambda: [0, 0, 0])
    for e in unique_events(events):
        if e['kind'] == 'interaction':
            scores[e['user_id'], e['category']] += {'view': 1, 'cart': 3, 'buy': 5}[e['action']]
        else:
            logs[e['service'], e['level']] += 1
            ts = datetime.fromisoformat(e['event_time'].replace('Z', '+00:00'))
            minute = ts.astimezone(timezone.utc).replace(second=0, microsecond=0).isoformat()
            bucket = minutes[e['service'], minute]
            bucket[0] += 1
            bucket[1] += e['level'] == 'ERROR'
            bucket[2] += e['latency_ms']
    return {
        'log_counts': [[*key, n] for key, n in sorted(logs.items())],
        'recommendation_scores': [[*key, n] for key, n in sorted(scores.items())],
        'alerts': [[*key, n, errors] for key, (n, errors, _) in sorted(minutes.items())
                   if n >= 3 and errors * 2 >= n],
    }


def validate_change(change):
    if not isinstance(change, dict) or set(change) != {'event_id', 'order_id', 'version', 'op', 'customer_id', 'amount_cents', 'status'}:
        raise ValueError('Invalid change fields')
    if not isinstance(change['event_id'], str) or not change['event_id']:
        raise ValueError('Missing event_id')
    if type(change['order_id']) is not int or change['order_id'] <= 0:
        raise ValueError('Invalid order_id')
    if type(change['version']) is not int or change['version'] <= 0:
        raise ValueError('Invalid version')
    if not isinstance(change['op'], str) or change['op'] not in {'upsert', 'delete'}:
        raise ValueError('Invalid operation')
    if type(change['amount_cents']) is not int or change['amount_cents'] < 0:
        raise ValueError('Invalid amount_cents')
    if not isinstance(change['customer_id'], str) or not isinstance(change['status'], str):
        raise ValueError('Invalid order fields')
    if not 1 <= len(change['customer_id']) <= 64 or not 1 <= len(change['status']) <= 32:
        raise ValueError('Order text fields exceed schema bounds or are empty')
    if any(change[field] > 2**63 - 1 for field in ('order_id', 'version', 'amount_cents')):
        raise ValueError('Order integer exceeds BIGINT range')
    return change


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))
