import hashlib
import json
import math
import time


def canonicalize(payload: dict) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False)


def fingerprint(payload: dict) -> str:
    return hashlib.sha256(canonicalize(payload).encode("utf-8")).hexdigest()


def decision(previous_expiry: int | None, arrival: int) -> str:
    return "duplicate" if previous_expiry is not None and arrival < previous_expiry else "unique"


class RotatingBloom:
    """Bounded advisory filters: current minute plus sixty preceding minutes."""
    def __init__(self, capacity=10000, false_positive_rate=0.01):
        self.bits = math.ceil(-capacity * math.log(false_positive_rate) / math.log(2) ** 2)
        self.hashes = max(1, round(self.bits / capacity * math.log(2)))
        self.buckets = {}

    def prune(self, now_ms=None):
        minute = (now_ms if now_ms is not None else int(time.time() * 1000)) // 60000
        self.buckets = {k: v for k, v in self.buckets.items() if minute - 60 <= k <= minute}
        return minute

    def positions(self, value):
        digest = hashlib.sha256(value.encode()).digest()
        first = int.from_bytes(digest[:16], "big")
        step = int.from_bytes(digest[16:], "big") | 1
        return [(first + i * step) % self.bits for i in range(self.hashes)]

    def contains(self, value, now_ms=None):
        self.prune(now_ms)
        positions = self.positions(value)
        return any(all(data[p // 8] & (1 << (p % 8)) for p in positions)
                   for data in self.buckets.values())

    def add(self, value, arrival_ms, now_ms=None):
        minute = self.prune(now_ms)
        bucket = arrival_ms // 60000
        if minute - 60 <= bucket <= minute:
            data = self.buckets.setdefault(bucket, bytearray((self.bits + 7) // 8))
            for p in self.positions(value):
                data[p // 8] |= 1 << (p % 8)
