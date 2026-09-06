import unittest
from streaming_lab.transport import snapshot_start


class OffsetTest(unittest.TestCase):
    def test_new_group_reads_initial_history(self):
        self.assertEqual(snapshot_start(-1001, 0, 7), 0)

    def test_existing_group_resumes(self):
        self.assertEqual(snapshot_start(4, 2, 7), 4)

    def test_retention_loss_is_fatal_for_existing_and_new_groups(self):
        for offset in (-1001, 1):
            with self.subTest(offset=offset), self.assertRaisesRegex(RuntimeError, 'expired'):
                snapshot_start(offset, 2, 7)

    def test_truncated_topic_is_fatal(self):
        with self.assertRaisesRegex(RuntimeError, 'exceeds'):
            snapshot_start(8, 0, 7)

    def test_explicit_replay_uses_retained_start(self):
        self.assertEqual(snapshot_start(7, 2, 7, replay=True), 2)
