"""Apply the comparison SQL and wait for the exact fixture result."""
from pathlib import Path
from streaming_lab.transport import sql_connect
from streaming_lab.cli import wait_rows

with sql_connect('postgresql://materialize@materialize:6875/materialize?sslmode=disable') as db:
    for statement in Path('/app/examples/materialize/logs.sql').read_text().split(';'):
        if statement.strip():
            db.execute(statement)
    wait_rows(db, 'SELECT service,level,total FROM log_counts ORDER BY service,level',
              [['catalog', 'INFO', 1], ['checkout', 'ERROR', 3], ['checkout', 'INFO', 1]])
print('PASS: Materialize log counts')
