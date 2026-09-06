import copy
import json
import unittest
from pathlib import Path
from streaming_lab.domain import summarize, unique_events, validate_event

DATA = Path(__file__).resolve().parents[1] / 'data'


class AnalyticsTest(unittest.TestCase):
    def setUp(self):
        self.events = [json.loads(s) for s in (DATA / 'events.jsonl').read_text().splitlines()]
        self.expected = json.loads((DATA / 'expected.json').read_text())

    def test_three_use_cases_match_explicit_expectations(self):
        self.assertEqual(summarize(self.events), {k: self.expected[k] for k in
                         ['log_counts', 'recommendation_scores', 'alerts']})

    def test_replay_does_not_double_count(self):
        self.assertEqual(summarize(self.events * 3), summarize(self.events))

    def test_out_of_order_delivery_preserves_result(self):
        self.assertEqual(summarize(list(reversed(self.events))), summarize(self.events))

    def test_conflicting_identity_is_rejected(self):
        conflicting = copy.deepcopy(self.events[0])
        conflicting['level'] = 'ERROR'
        with self.assertRaises(ValueError):
            unique_events(self.events + [conflicting])

    def test_empty_input(self):
        self.assertEqual(summarize([]), {'log_counts': [], 'recommendation_scores': [], 'alerts': []})

    def test_naive_time_is_rejected(self):
        e = dict(self.events[0], event_time='2026-09-06T00:00:01')
        with self.assertRaises(ValueError):
            validate_event(e)

    def test_negative_latency_is_rejected(self):
        with self.assertRaises(ValueError):
            validate_event(dict(self.events[0], latency_ms=-1))

    def test_malformed_payloads_are_rejected_before_delivery(self):
        invalid = [None, [], dict(self.events[0], event_id=[]),
                   dict(self.events[0], event_time=123),
                   dict(self.events[0], latency_ms=True),
                   dict(self.events[0], level='EROR'),
                   dict(self.events[6], user_id=''),
                   dict(self.events[6], category='')]
        for event in invalid:
            with self.subTest(event=event), self.assertRaises(ValueError):
                validate_event(event)

    def test_equivalent_timezone_bucket(self):
        original = summarize(self.events)
        for e in self.events:
            e['event_time'] = e['event_time'].replace('T00:', 'T09:').replace('Z', '+09:00')
        self.assertEqual(summarize(self.events), original)
