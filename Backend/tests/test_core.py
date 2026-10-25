import hashlib
import math
from function_app import canonical_value, fingerprint, bloom_parameters, bloom_positions


def test_text_fingerprint_is_exact_and_stable():
    assert fingerprint("abc") == hashlib.sha256(b"txt:abc").hexdigest()
    assert fingerprint("abc") != fingerprint("abc ")


def test_json_key_order_does_not_change_fingerprint():
    assert fingerprint({"a": 1, "b": 2}) == fingerprint({"b": 2, "a": 1})


def test_timestamp_is_ignored_for_json_events():
    assert fingerprint({"event_name": "payment_successful", "user_id": 5, "timestamp": 1}) == fingerprint({"event_name": "payment_success", "user_id": 5, "timestamp": 2})


def test_event_identity_and_time_are_ignored_for_json_events():
    first = {
        "event_id": "evt-001",
        "event_time": "2026-10-25T10:00:00Z",
        "event_name": "payment_success",
        "user_id": "u-10",
        "amount": 125,
    }
    retry = {
        "eventId": "evt-999",
        "eventTime": "2026-10-25T10:05:00Z",
        "event_name": "payment_success",
        "user_id": "u-10",
        "amount": 125,
    }
    assert fingerprint(first) == fingerprint(retry)


def test_order_identity_uses_event_user_order():
    a = {"event_name": "Order Created", "user_id": "u1", "order_id": "o1", "timestamp": 1, "extra": 1}
    b = {"event_name": "order_created", "user_id": "u1", "order_id": "o1", "timestamp": 2, "extra": 99}
    assert fingerprint(a) == fingerprint(b)


def test_bloom_parameters_are_valid():
    bits, hashes = bloom_parameters(10_000, 0.01)
    assert bits > 0 and hashes >= 1 and math.ceil(bits / 8) > 0


def test_bloom_positions_stay_in_range():
    bits, hashes = bloom_parameters(1000, 0.01)
    positions = list(bloom_positions("ab" * 32, bits, hashes))
    assert len(positions) == hashes
    assert all(0 <= p < bits for p in positions)
