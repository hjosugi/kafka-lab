import json
import tempfile
import unittest
from pathlib import Path
from streaming_lab.projection import connect, apply_change, live_rows

DATA = Path(__file__).resolve().parents[1] / 'data'


class ProjectionTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / 'target.sqlite')
        self.db = connect(self.path)
        self.changes = json.loads((DATA / 'order_changes.json').read_text())

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def test_insert_update_delete(self):
        for c in self.changes:
            apply_change(self.db, c)
        expected = json.loads((DATA / 'expected.json').read_text())['orders']
        self.assertEqual([list(row) for row in live_rows(self.db)], expected)

    def test_full_replay_is_idempotent(self):
        for c in self.changes:
            apply_change(self.db, c)
        expected = live_rows(self.db)
        for c in self.changes:
            self.assertEqual(apply_change(self.db, c), 'duplicate')
        self.assertEqual(live_rows(self.db), expected)

    def test_stale_update_cannot_overwrite_newer_state(self):
        apply_change(self.db, self.changes[2])
        self.assertEqual(apply_change(self.db, self.changes[0]), 'stale')
        self.assertEqual(live_rows(self.db)[0][2], 1800)

    def test_delete_before_create_does_not_resurrect(self):
        apply_change(self.db, self.changes[3])
        self.assertEqual(apply_change(self.db, self.changes[1]), 'stale')
        self.assertEqual(live_rows(self.db), [])

    def test_restart_after_db_commit_before_offset_commit(self):
        apply_change(self.db, self.changes[0])
        self.db.close()
        self.db = connect(self.path)
        self.assertEqual(apply_change(self.db, self.changes[0]), 'duplicate')
        self.assertEqual(len(live_rows(self.db)), 1)

    def test_conflict_rolls_back_inbox_and_projection(self):
        apply_change(self.db, self.changes[0])
        before = live_rows(self.db)
        bad = dict(self.changes[0], event_id='another', amount_cents=9999)
        with self.assertRaises(ValueError):
            apply_change(self.db, bad)
        self.assertEqual(live_rows(self.db), before)
        self.assertIsNone(self.db.execute('SELECT * FROM inbox WHERE event_id=?', ('another',)).fetchone())

    def test_conflicting_event_id_does_not_mutate_state(self):
        apply_change(self.db, self.changes[0])
        with self.assertRaises(ValueError):
            apply_change(self.db, dict(self.changes[0], amount_cents=9999))
        self.assertEqual(live_rows(self.db)[0][2], 1200)

    def test_invalid_change_has_no_effect(self):
        with self.assertRaises(ValueError):
            apply_change(self.db, dict(self.changes[0], version=0))
        self.assertEqual(live_rows(self.db), [])

    def test_later_recreation_is_allowed(self):
        apply_change(self.db, self.changes[3])
        recreated = dict(self.changes[1], event_id='recreated', version=3)
        self.assertEqual(apply_change(self.db, recreated), 'applied')
        self.assertEqual(live_rows(self.db)[0][-1], 3)
