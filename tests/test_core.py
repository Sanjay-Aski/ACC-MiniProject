from app.core import fingerprint, decision, RotatingBloom


def test_canonical_json():
    assert fingerprint({"b": [1, True], "a": "é"}) == fingerprint({"a": "é", "b": [1, True]})
    assert fingerprint({"x": 1}) != fingerprint({"x": True})
    assert fingerprint({"x": 1}) != fingerprint({"x": 1.0})
    assert fingerprint({"x": [1, 2]}) != fingerprint({"x": [2, 1]})


def test_sliding_boundary():
    hour = 3600000
    start = 0
    assert decision(None, start) == "unique"
    assert decision(start + hour, 50 * 60000) == "duplicate"
    assert decision(110 * 60000, 109 * 60000) == "duplicate"
    assert decision(110 * 60000, 110 * 60000) == "unique"


def test_bloom_rotation_and_delayed_arrival():
    bloom = RotatingBloom()
    bloom.add("event", 0, now_ms=0)
    assert bloom.contains("event", now_ms=60 * 60000)
    assert not bloom.contains("event", now_ms=61 * 60000)
    bloom.add("old", 0, now_ms=100 * 60000)
    assert not bloom.contains("old", now_ms=100 * 60000)
    assert len(bloom.buckets) <= 61
