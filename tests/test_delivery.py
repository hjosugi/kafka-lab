import sys
import types
import unittest
from unittest.mock import patch
from streaming_lab.transport import send


class DeliveryTest(unittest.TestCase):
    def test_broker_rejection_is_not_successful_flush(self):
        class Producer:
            def __init__(self, config):
                pass
            def produce(self, *args, **kwargs):
                kwargs['on_delivery']('broker rejected the record', None)
            def poll(self, timeout):
                pass
            def flush(self, timeout):
                return 0
        with patch.dict(sys.modules, {'confluent_kafka': types.SimpleNamespace(Producer=Producer)}):
            with self.assertRaisesRegex(RuntimeError, 'broker rejected'):
                send('test', [('id', {'value': 1})])

    def test_unacknowledged_records_are_not_success(self):
        class Producer:
            def __init__(self, config):
                pass
            def produce(self, *args, **kwargs):
                pass
            def poll(self, timeout):
                pass
            def flush(self, timeout):
                return 1
        with patch.dict(sys.modules, {'confluent_kafka': types.SimpleNamespace(Producer=Producer)}):
            with self.assertRaisesRegex(RuntimeError, 'pending=1'):
                send('test', [('id', {'value': 1})])
