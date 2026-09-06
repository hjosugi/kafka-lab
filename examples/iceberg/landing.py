"""A bounded Iceberg landing example using the same event fixture."""
import json
from pathlib import Path
import duckdb
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

root = Path(__file__).resolve().parents[2]
warehouse = root / 'runtime/iceberg'
warehouse.mkdir(parents=True, exist_ok=True)
catalog = SqlCatalog('lab', uri=f'sqlite:///{warehouse / "catalog.db"}',
                     warehouse=warehouse.as_uri())
catalog.create_namespace_if_not_exists('lab')
events = [json.loads(line) for line in (root / 'data/events.jsonl').read_text().splitlines()]
events = list({e['event_id']: e for e in events}.values())
arrow = pa.Table.from_pylist(events)
if not catalog.table_exists('lab.events'):
    table = catalog.create_table('lab.events', schema=arrow.schema)
else:
    table = catalog.load_table('lab.events')
# Each run replaces the bounded fixture atomically with a new Iceberg snapshot.
table.overwrite(arrow)
snapshot = table.current_snapshot()
assert snapshot is not None
print(f'Iceberg snapshot: {snapshot.snapshot_id}')
data = table.scan().to_arrow()
connection = duckdb.connect()
connection.register('events', data)
rows = connection.execute('''SELECT service, level, count(*) FROM events
    WHERE kind='log' GROUP BY service,level ORDER BY service,level''').fetchall()
expected = [('catalog', 'INFO', 1), ('checkout', 'ERROR', 3), ('checkout', 'INFO', 1)]
assert rows == expected, rows
print(rows)
print('PASS: real Iceberg metadata, data files, snapshot and DuckDB query')
