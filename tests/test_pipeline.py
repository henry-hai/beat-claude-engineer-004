"""Tests over the pipeline. This is the answer to "how do you validate data
accuracy?" in code rather than in prose.

Three layers, deliberately:

1. UNIT tests on the pure functions. Cheap and exhaustive, and they are only
   cheap because normalize.py and rules.py are pure functions of one event.
   That is the practical payoff of the stateless/stateful split - the moment a
   classifier needed shared state, testing it would need fixtures, ordering,
   and teardown. Architecture that is easy to test is not a coincidence.

2. PROPERTY tests. Purity is a claim the design makes, so it gets checked:
   classifiers must not mutate their input, and must return the same output for
   the same input regardless of what ran before them. If that ever stops being
   true, the "lifts into a stream processor unchanged" claim is dead.

3. FIXTURE REGRESSION. The exact counts produced against the real 25-line
   fixture are pinned. If any rule changes behaviour, this fails loudly and
   names what moved. This is the layer that would catch a bad deploy in
   production: same input, same output, or explain yourself.

Standard library only, matching the rest of the repo:

    python -m unittest discover -s tests -v
"""

from __future__ import annotations

import json
import unittest
from datetime import timedelta
from pathlib import Path

from pipeline.normalize import parse_line, normalize_event, parse_and_normalize
from pipeline.rules import (
    AnomalyClass,
    Severity,
    classify_event,
    classify_null_tenant,
    classify_pii,
    classify_privacy_request,
    classify_client_asserted_aggregate,
    classify_suspicious_referrer,
    classify_timestamps,
    findings_from_normalization,
)
from pipeline.state import DedupeWindow, IdentityGraph, StreamState, VisitorWindow

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "event_sample.jsonl"


def event(**overrides):
    """A minimal well-formed event. Overrides make the anomaly under test."""
    base = {
        "event_id": "evt-test",
        "tenant_id": "t-001",
        "anonymous_id": "anon-1",
        "user_id": None,
        "type": "page_view",
        "ts": "2026-06-15T14:00:00.000Z",
        "received_at": "2026-06-15T14:00:00.100Z",
        "properties": {"path": "/"},
    }
    base.update(overrides)
    return base


# --------------------------------------------------------------------------
# 1. Parsing and normalization
# --------------------------------------------------------------------------

class TestParsing(unittest.TestCase):
    def test_malformed_json_becomes_a_dead_letter_not_an_exception(self):
        result = parse_line('{"event_id":"evt-0020","properties":{"a":1}', 21)
        self.assertFalse(result.ok)
        self.assertIsNotNone(result.dead_letter)
        self.assertEqual(result.dead_letter.line_no, 21)
        self.assertIn("invalid JSON", result.dead_letter.reason)

    def test_dead_letter_preserves_the_original_bytes_untruncated(self):
        raw = '{"event_id":"evt-0020","properties":{"selector":"#signup"}'
        result = parse_line(raw, 21)
        self.assertEqual(result.dead_letter.raw, raw)

    def test_blank_line_is_neither_event_nor_dead_letter(self):
        result = parse_line("   \n", 5)
        self.assertIsNone(result.event)
        self.assertIsNone(result.dead_letter)

    def test_non_object_json_is_dead_lettered(self):
        result = parse_line('["not", "an", "object"]', 7)
        self.assertIsNotNone(result.dead_letter)
        self.assertIn("expected object", result.dead_letter.reason)


class TestNormalization(unittest.TestCase):
    def test_legacy_sdk_shape_maps_to_canonical(self):
        """evt-0009's exact shape: the SDK generation the brief forbids changing."""
        legacy = {
            "event_id": "evt-0009",
            "tenant_id": "t-042",
            "anonymous_id": "anon-1be",
            "user_id": None,
            "type": "pageview",
            "timestamp": "2026-06-15T14:08:02Z",
            "properties": {"page_path": "/pricing", "ref": "https://bing.com"},
        }
        result = normalize_event(legacy)
        self.assertEqual(result.event["type"], "page_view")
        self.assertEqual(result.event["ts"], "2026-06-15T14:08:02Z")
        self.assertNotIn("timestamp", result.event)
        self.assertEqual(result.event["properties"]["path"], "/pricing")
        self.assertEqual(result.event["properties"]["referrer"], "https://bing.com")

    def test_missing_received_at_is_recorded_never_synthesized(self):
        """The design claim: a missing value you can see beats a fake one you cannot."""
        result = normalize_event({"event_id": "e", "ts": "2026-06-15T14:00:00Z"})
        self.assertIsNone(result.event["received_at"])
        self.assertTrue(any("received_at" in n for n in result.notes))

    def test_every_transformation_is_recorded(self):
        result = normalize_event({"event_id": "e", "type": "pageview"})
        self.assertTrue(result.was_normalized)
        self.assertTrue(any("pageview" in n for n in result.notes))

    def test_normalization_does_not_mutate_its_input(self):
        original = {"event_id": "e", "type": "pageview", "properties": {"ref": "x"}}
        snapshot = json.dumps(original, sort_keys=True)
        normalize_event(original)
        self.assertEqual(json.dumps(original, sort_keys=True), snapshot)

    def test_clean_event_produces_no_notes_beyond_absent_fields(self):
        result = normalize_event(event())
        self.assertEqual([n for n in result.notes if "renamed" in n], [])


# --------------------------------------------------------------------------
# 2. Stateless classifiers, one test per anomaly class
# --------------------------------------------------------------------------

class TestStatelessRules(unittest.TestCase):
    def test_null_tenant_is_critical(self):
        findings = classify_null_tenant(event(tenant_id=None))
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].severity, Severity.CRITICAL)

    def test_empty_string_tenant_counts_as_null(self):
        self.assertEqual(len(classify_null_tenant(event(tenant_id="   "))), 1)

    def test_valid_tenant_produces_nothing(self):
        self.assertEqual(classify_null_tenant(event()), [])

    def test_small_clock_skew_is_tolerated(self):
        """Under CLOCK_TOLERANCE, ordinary jitter, not worth flagging."""
        findings = classify_timestamps(event(
            ts="2026-06-15T14:00:02.000Z", received_at="2026-06-15T14:00:00.000Z"))
        self.assertEqual(
            [f for f in findings if f.anomaly_class == AnomalyClass.RECEIVED_BEFORE_SENT], [])

    def test_moderate_skew_warns_not_criticals(self):
        """evt-0005 shape: 47s ahead. Real, but a bad clock rather than a broken one."""
        findings = classify_timestamps(event(
            ts="2026-06-15T14:05:00.120Z", received_at="2026-06-15T14:04:12.930Z"))
        skew = [f for f in findings if f.anomaly_class == AnomalyClass.RECEIVED_BEFORE_SENT]
        self.assertEqual(len(skew), 1)
        self.assertEqual(skew[0].severity, Severity.WARN)

    def test_hour_scale_skew_escalates_to_critical(self):
        """evt-0006 shape: 65 minutes. Timezone bug territory."""
        findings = classify_timestamps(event(
            ts="2026-06-15T15:11:03.552Z", received_at="2026-06-15T14:06:03.552Z"))
        skew = [f for f in findings if f.anomaly_class == AnomalyClass.RECEIVED_BEFORE_SENT]
        self.assertEqual(skew[0].severity, Severity.CRITICAL)

    def test_far_future_timestamp_raises_both_flags(self):
        """evt-0016 shape. Impossible AND dangerous to a watermark - two findings."""
        findings = classify_timestamps(event(
            ts="2027-06-15T14:11:27.310Z", received_at="2026-06-15T14:11:27.522Z"))
        classes = {f.anomaly_class for f in findings}
        self.assertIn(AnomalyClass.RECEIVED_BEFORE_SENT, classes)
        self.assertIn(AnomalyClass.FUTURE_TIMESTAMP, classes)

    def test_missing_received_at_blocks_skew_detection_and_says_so(self):
        findings = classify_timestamps(event(received_at=None))
        self.assertEqual(
            [f.anomaly_class for f in findings], [AnomalyClass.MISSING_RECEIVED_AT])

    def test_unparseable_timestamp_does_not_raise(self):
        findings = classify_timestamps(event(ts="not-a-date"))
        self.assertTrue(any(f.anomaly_class == AnomalyClass.INVALID_TIMESTAMP
                            for f in findings))

    def test_pii_detected_by_key_name(self):
        findings = classify_pii(event(properties={"contact_email": "a@b.com"}))
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].severity, Severity.CRITICAL)

    def test_pii_detected_by_value_shape_under_an_innocuous_key(self):
        """A customer can name a custom property anything. Key names alone are not enough."""
        findings = classify_pii(event(properties={"field_7": "maria@example-corp.com"}))
        self.assertEqual(len(findings), 1)

    def test_ordinary_properties_are_not_pii(self):
        self.assertEqual(classify_pii(event(properties={"path": "/pricing"})), [])

    def test_privacy_request_is_flagged_with_its_scope(self):
        findings = classify_privacy_request(event(
            type="privacy_request", user_id="u-1077",
            properties={"request": "delete_all_data", "regulation": "GDPR"}))
        self.assertEqual(len(findings), 1)
        self.assertIn("u-1077", findings[0].detail)

    def test_client_asserted_aggregate_is_flagged(self):
        """evt-0019: the number the browser computed. The trap."""
        findings = classify_client_asserted_aggregate(event(
            type="custom", properties={"name": "viewed_pricing", "count_today": 3}))
        self.assertEqual(len(findings), 1)
        self.assertIn("count_today", findings[0].detail)

    def test_booleans_are_not_mistaken_for_aggregates(self):
        self.assertEqual(classify_client_asserted_aggregate(
            event(properties={"count_verified": True})), [])

    def test_suspicious_referrer_warns_and_says_do_not_delete(self):
        findings = classify_suspicious_referrer(event(
            properties={"referrer": "https://scanner.example-bot.net"}))
        self.assertEqual(findings[0].severity, Severity.WARN)
        self.assertIn("do not", findings[0].detail.lower())

    def test_ordinary_referrer_is_clean(self):
        self.assertEqual(classify_suspicious_referrer(
            event(properties={"referrer": "https://google.com"})), [])

    def test_schema_drift_finding_comes_from_normalization_notes(self):
        findings = findings_from_normalization(event(), ["renamed 'timestamp' -> 'ts'"])
        self.assertEqual(findings[0].anomaly_class, AnomalyClass.SCHEMA_DRIFT)


# --------------------------------------------------------------------------
# 3. Purity - the property the architecture claims
# --------------------------------------------------------------------------

class TestPurity(unittest.TestCase):
    def test_classify_event_does_not_mutate_its_input(self):
        e = event(tenant_id=None, properties={"contact_email": "a@b.com"})
        snapshot = json.dumps(e, sort_keys=True)
        classify_event(e)
        self.assertEqual(json.dumps(e, sort_keys=True), snapshot)

    def test_classify_event_is_order_independent(self):
        """No hidden shared state: running other events first changes nothing."""
        target = event(tenant_id=None)
        first = classify_event(target)
        for _ in range(50):
            classify_event(event(properties={"contact_email": "x@y.com"}))
        second = classify_event(target)
        self.assertEqual(first, second)

    def test_same_input_same_output(self):
        e = event(ts="2027-01-01T00:00:00Z")
        self.assertEqual(classify_event(e), classify_event(e))


# --------------------------------------------------------------------------
# 4. Keyed state
# --------------------------------------------------------------------------

class TestDedupe(unittest.TestCase):
    def test_repeat_event_id_is_a_duplicate(self):
        window = DedupeWindow()
        self.assertEqual(window.observe(event(event_id="evt-0002")), [])
        findings = window.observe(event(event_id="evt-0002"))
        self.assertEqual(findings[0].anomaly_class, AnomalyClass.DUPLICATE_EVENT)

    def test_same_id_different_tenant_is_not_a_duplicate(self):
        """The key is (tenant_id, event_id). Collapsing tenants deletes real data."""
        window = DedupeWindow()
        window.observe(event(event_id="evt-1", tenant_id="t-A"))
        self.assertEqual(window.observe(event(event_id="evt-1", tenant_id="t-B")), [])

    def test_state_is_bounded(self):
        """Unbounded state is a memory leak with a schedule."""
        window = DedupeWindow(capacity=10)
        for i in range(50):
            window.observe(event(event_id=f"evt-{i}"))
        self.assertLessEqual(len(window.seen), 10)

    def test_eviction_loses_old_duplicates_and_that_is_the_documented_trade(self):
        window = DedupeWindow(capacity=3)
        window.observe(event(event_id="evt-old"))
        for i in range(5):
            window.observe(event(event_id=f"evt-{i}"))
        self.assertEqual(window.observe(event(event_id="evt-old")), [])


class TestBurstDetection(unittest.TestCase):
    def _burst(self, n, ms_apart):
        window = VisitorWindow()
        findings = []
        for i in range(n):
            ts = f"2026-06-15T14:10:00.{i * ms_apart:03d}Z"
            findings += window.observe(event(event_id=f"evt-{i}", ts=ts))
        return findings

    def test_four_events_in_51ms_is_a_burst(self):
        """evt-0012..evt-0015 shape."""
        findings = self._burst(4, 17)
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].anomaly_class, AnomalyClass.VISITOR_BURST)

    def test_three_events_is_below_threshold(self):
        self.assertEqual(self._burst(3, 17), [])

    def test_burst_reports_once_not_per_event(self):
        self.assertEqual(len(self._burst(8, 10)), 1)

    def test_events_spread_over_time_are_not_a_burst(self):
        window = VisitorWindow()
        findings = []
        for i in range(6):
            findings += window.observe(event(
                event_id=f"evt-{i}", ts=f"2026-06-15T14:{10 + i:02d}:00.000Z"))
        self.assertEqual(findings, [])


class TestIdentity(unittest.TestCase):
    def test_identity_resolves_backwards_over_prior_anonymous_events(self):
        """evt-0007 then evt-0022: PII written anonymously, named fifteen events later."""
        graph = IdentityGraph()
        graph.observe(event(event_id="evt-0007", anonymous_id="anon-3d0", user_id=None))
        findings = graph.observe(event(
            event_id="evt-0022", anonymous_id="anon-3d0", user_id="u-7304"))
        self.assertEqual(findings[0].anomaly_class, AnomalyClass.IDENTITY_STITCH)
        self.assertIn("evt-0007", findings[0].detail)

    def test_identity_collision_is_flagged_not_silently_overwritten(self):
        graph = IdentityGraph()
        graph.observe(event(anonymous_id="anon-x", user_id="u-1"))
        findings = graph.observe(event(anonymous_id="anon-x", user_id="u-2"))
        self.assertEqual(findings[0].severity, Severity.WARN)
        self.assertIn("u-1", findings[0].detail)

    def test_deletion_scope_reaches_pre_identification_history(self):
        """The compliance chain: deleting by user_id alone is an incomplete deletion."""
        graph = IdentityGraph()
        graph.observe(event(event_id="evt-0006", anonymous_id="anon-77a", user_id=None))
        graph.observe(event(event_id="evt-x", anonymous_id="anon-77a", user_id="u-1077"))
        findings = graph.deletion_scope(event(
            event_id="evt-0017", anonymous_id="anon-77a", user_id="u-1077",
            type="privacy_request",
            properties={"request": "delete_all_data", "regulation": "GDPR"}))
        self.assertEqual(findings[0].anomaly_class, AnomalyClass.DELETION_SCOPE)
        self.assertIn("evt-0006", findings[0].detail)

    def test_deletion_scope_only_fires_for_privacy_requests(self):
        self.assertEqual(IdentityGraph().deletion_scope(event()), [])


# --------------------------------------------------------------------------
# 5. Fixture regression - same input, same output, or explain yourself
# --------------------------------------------------------------------------

class TestFixtureRegression(unittest.TestCase):
    """Pinned counts against the real 25-line fixture.

    This is the layer that catches a bad deploy. Any change to any rule that
    moves these numbers fails here and names what moved.
    """

    EXPECTED_LINES = 25
    EXPECTED_PARSED = 24
    EXPECTED_DEAD_LETTERED = 1
    EXPECTED_CLASSES = 13
    EXPECTED_FINDINGS = 21
    STATEFUL_CLASSES = {
        AnomalyClass.DUPLICATE_EVENT,
        AnomalyClass.VISITOR_BURST,
        AnomalyClass.IDENTITY_STITCH,
        AnomalyClass.DELETION_SCOPE,
    }

    @classmethod
    def setUpClass(cls):
        state = StreamState()
        cls.findings, cls.dead, cls.parsed, cls.lines = [], [], 0, 0
        with FIXTURE.open(encoding="utf-8") as fh:
            for line_no, raw in enumerate(fh, 1):
                if raw.strip():
                    cls.lines += 1
                normalized, dead = parse_and_normalize(raw, line_no)
                if dead is not None:
                    cls.dead.append(dead)
                    continue
                if normalized is None:
                    continue
                cls.parsed += 1
                cls.findings += findings_from_normalization(
                    normalized.event, normalized.notes)
                cls.findings += classify_event(normalized.event)
                cls.findings += state.observe(normalized.event)
        cls.classes = {f.anomaly_class for f in cls.findings}

    def test_the_fixture_is_the_one_this_answer_was_written_against(self):
        import hashlib
        digest = hashlib.sha256(FIXTURE.read_bytes()).hexdigest()
        self.assertEqual(
            digest,
            "1aeb24b415009e89fcf8acb5a178410faf216dc17b16920d9849ecc8bbb24235",
            "Fixture has changed. Every count below was measured against the "
            "original, and the written answer quotes them.",
        )

    def test_every_line_is_accounted_for(self):
        """Nothing silently dropped: parsed + dead-lettered == lines read."""
        self.assertEqual(self.lines, self.EXPECTED_LINES)
        self.assertEqual(self.parsed + len(self.dead), self.lines)

    def test_exactly_one_dead_letter_and_it_is_evt_0020s_line(self):
        self.assertEqual(len(self.dead), self.EXPECTED_DEAD_LETTERED)
        self.assertEqual(self.dead[0].line_no, 21)

    def test_thirteen_anomaly_classes(self):
        self.assertEqual(len(self.classes), self.EXPECTED_CLASSES)

    def test_nine_stateless_four_stateful(self):
        """The split is the architecture argument. If it moves, the answer is wrong."""
        stateful = self.classes & self.STATEFUL_CLASSES
        self.assertEqual(len(stateful), 4)
        self.assertEqual(len(self.classes - self.STATEFUL_CLASSES), 9)

    def test_total_finding_count_is_pinned(self):
        self.assertEqual(len(self.findings), self.EXPECTED_FINDINGS)

    def test_each_class_lands_on_the_event_ids_cited_in_the_answer(self):
        """Every event_id quoted in the written answer, verified here."""
        expected = {
            AnomalyClass.NULL_TENANT: {"evt-0011"},
            AnomalyClass.PII_IN_PROPERTIES: {"evt-0007"},
            AnomalyClass.PRIVACY_REQUEST: {"evt-0017"},
            AnomalyClass.FUTURE_TIMESTAMP: {"evt-0016"},
            AnomalyClass.RECEIVED_BEFORE_SENT: {"evt-0005", "evt-0006", "evt-0016"},
            AnomalyClass.MISSING_RECEIVED_AT: {"evt-0009"},
            AnomalyClass.SCHEMA_DRIFT: {"evt-0009"},
            AnomalyClass.CLIENT_ASSERTED_AGGREGATE: {"evt-0019"},
            AnomalyClass.SUSPICIOUS_REFERRER: {
                "evt-0012", "evt-0013", "evt-0014", "evt-0015"},
            AnomalyClass.DUPLICATE_EVENT: {"evt-0002"},
            AnomalyClass.DELETION_SCOPE: {"evt-0017"},
        }
        for anomaly_class, event_ids in expected.items():
            actual = {f.event_id for f in self.findings
                      if f.anomaly_class == anomaly_class}
            self.assertEqual(actual, event_ids, f"{anomaly_class} moved")

    def test_no_finding_is_missing_its_event_id(self):
        """A finding a reviewer cannot trace to a record is not evidence."""
        self.assertEqual([f for f in self.findings if not f.event_id], [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
