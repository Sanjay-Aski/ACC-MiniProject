"""Live HTTP pipeline check; optionally verify a saved submission after restart."""
import argparse
import json
import time
import urllib.request
import uuid


def request(path, payload=None):
    raw = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request("http://localhost:8000" + path, data=raw,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as response:
        return json.load(response)


def wait(sid, status):
    for _ in range(60):
        event = request("/events/" + sid)
        if event["delivery_status"] == status:
            return event
        time.sleep(0.5)
    raise AssertionError(f"Submission {sid} did not reach {status}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify-first", help="Submission ID saved before stopping the stack")
    args = parser.parse_args()
    for _ in range(60):
        try:
            request("/health")
            break
        except Exception:
            time.sleep(0.5)
    else:
        raise AssertionError("API did not become ready")
    if args.verify_first:
        first = wait(args.verify_first, "consumed")
        assert first["decision"] == "unique"
        payload = first["payload"]
        first_id = args.verify_first
    else:
        payload = {"smoke_test": str(uuid.uuid4()), "amount": 500}
        first_id = request("/events", payload)["submission_id"]
        assert wait(first_id, "consumed")["decision"] == "unique"
    second_id = request("/events", payload)["submission_id"]
    assert wait(second_id, "not_applicable")["decision"] == "duplicate"
    print(json.dumps({"first_id": first_id, "duplicate_id": second_id,
                      "result": "unique consumed; duplicate suppressed"}))


if __name__ == "__main__":
    main()
