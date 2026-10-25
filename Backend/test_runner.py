#!/usr/bin/env python3
"""TXT end-to-end runner for the Azure Dedup Virtual Lab API."""
from __future__ import annotations
import argparse
import collections
import json
import os
import sys
import time
import uuid
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def request_json(url: str, key: str | None, data: dict | None = None, timeout: int = 30):
    headers = {"Accept": "application/json"}
    body = None
    method = "GET"
    if data is not None:
        method = "POST"
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json; charset=utf-8"
    if key:
        headers["x-functions-key"] = key
    req = Request(url, data=body, headers=headers, method=method)
    try:
        with urlopen(req, timeout=timeout) as res:
            raw = res.read().decode("utf-8")
            return res.status, json.loads(raw) if raw else {}
    except HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = {"raw": raw}
        return exc.code, parsed
    except URLError as exc:
        raise RuntimeError(f"Cannot reach API: {exc.reason}") from exc


def request_text(url: str, key: str | None, timeout: int = 30):
    headers = {"Accept": "text/plain"}
    if key:
        headers["x-functions-key"] = key
    req = Request(url, headers=headers, method="GET")
    try:
        with urlopen(req, timeout=timeout) as res:
            return res.status, res.read().decode("utf-8")
    except HTTPError as exc:
        raise RuntimeError(f"Output download failed HTTP {exc.code}: {exc.read().decode('utf-8', errors='replace')}") from exc


def main() -> int:
    ap = argparse.ArgumentParser(description="Upload a TXT file to the deduplication API and download accepted output.")
    ap.add_argument("input", help="UTF-8 TXT file; each line is one record")
    ap.add_argument("--limit", type=int, default=None, help="test at most N input lines")
    ap.add_argument("--rate", type=float, default=0, help="submission rate in messages/minute; 0 means no client-side delay")
    ap.add_argument("--batch-size", type=int, default=100, help="records per HTTP request (1-100)")
    ap.add_argument("--base-url", default=os.getenv("DEDUP_API_URL", "http://localhost:7071/api"), help="base URL ending in /api")
    ap.add_argument("--function-key", default=os.getenv("DEDUP_FUNCTION_KEY"), help="Function key; prefer environment variable")
    ap.add_argument("--run-id", default=None, help="UUID to resume/replay a test run")
    ap.add_argument("--output", default="output.txt")
    ap.add_argument("--report", default="report.json")
    ap.add_argument("--timeout-seconds", type=int, default=600)
    args = ap.parse_args()
    if args.batch_size < 1 or args.batch_size > 100:
        ap.error("--batch-size must be from 1 through 100")
    if args.rate < 0:
        ap.error("--rate must be >= 0")
    source = Path(args.input)
    if not source.is_file():
        ap.error(f"Input file not found: {source}")
    lines = source.read_text(encoding="utf-8-sig").splitlines()
    if args.limit is not None:
        if args.limit < 1:
            ap.error("--limit must be a positive integer")
        lines = lines[:args.limit]
    if not lines:
        ap.error("Input file contains no lines after applying --limit")
    base = args.base_url.rstrip("/")
    key = args.function_key
    run_id = str(uuid.UUID(args.run_id)) if args.run_id else str(uuid.uuid4())
    report_path = Path(args.report)
    output_path = Path(args.output)
    report = {
        "testRunId": run_id, "inputFile": str(source), "limit": args.limit,
        "inputCount": len(lines), "apiBaseUrl": base, "batches": [],
        "startedAtEpoch": time.time(), "status": "running",
    }
    print(f"Test run: {run_id}")
    print(f"Input lines: {len(lines)}; batch size: {args.batch_size}; API: {base}")
    code, health = request_json(base + "/health", key)
    report["healthBefore"] = health
    if code != 200 or not health.get("ready", health.get("healthy", False)):
        report["status"] = "failed_health_check"
        report["error"] = {"httpStatus": code, "health": health}
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"Test stopped: health check returned HTTP {code}: {json.dumps(health)}", file=sys.stderr)
        return 2
    for start in range(0, len(lines), args.batch_size):
        batch = lines[start:start + args.batch_size]
        request_body = {
            "testRunId": run_id,
            "expectedCount": len(lines),
            "records": [
                {"lineNumber": start + i + 1, "payload": line}
                for i, line in enumerate(batch)
            ],
        }
        code, result = request_json(base + "/ingest", key, request_body)
        report["batches"].append({"startLine": start + 1, "count": len(batch), "httpStatus": code, "response": result})
        if code not in (200, 202):
            report["status"] = "ingest_failed"
            report["error"] = {"httpStatus": code, "response": result}
            report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
            print(f"Ingest failed HTTP {code}: {json.dumps(result)}", file=sys.stderr)
            return 3
        if args.rate > 0:
            time.sleep(len(batch) * 60.0 / args.rate)

    deadline = time.monotonic() + args.timeout_seconds
    latest = {}
    while time.monotonic() < deadline:
        code, latest = request_json(f"{base}/test-status?testRunId={run_id}", key)
        if code != 200:
            time.sleep(2)
            continue
        print("Progress: {}/{} processed | accepted={} duplicates={} failed={}".format(
            latest.get("processedCount", 0), latest.get("expectedCount", len(lines)),
            latest.get("acceptedCount", 0), latest.get("duplicateCount", 0), latest.get("failedCount", 0)))
        if latest.get("status") == "completed":
            break
        time.sleep(2)
    report["statusResponse"] = latest
    if latest.get("status") != "completed":
        report["status"] = "timed_out_or_incomplete"
        report["endedAtEpoch"] = time.time()
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"Timed out waiting for completion. Current status: {json.dumps(latest)}", file=sys.stderr)
        return 4

    code, output = request_text(f"{base}/test-output?testRunId={run_id}", key)
    output_path.write_text(output, encoding="utf-8", newline="")
    output_lines = output.splitlines()
    accepted = int(latest.get("acceptedCount", 0))
    input_counts = collections.Counter(lines)
    output_counts = collections.Counter(output_lines)
    output_is_subset = all(output_counts[line] <= input_counts[line] for line in output_counts)
    count_matches = len(output_lines) == accepted
    no_exact_duplicates = all(count <= 1 for count in output_counts.values())
    report["outputValidation"] = {
        "outputLineCount": len(output_lines), "acceptedCount": accepted,
        "outputCountMatchesAccepted": count_matches,
        "outputLinesBelongToInput": output_is_subset,
        "noExactDuplicateOutputLines": no_exact_duplicates,
    }
    report["outputFile"] = str(output_path)
    report["status"] = "completed" if count_matches and output_is_subset and no_exact_duplicates else "completed_validation_failed"
    report["endedAtEpoch"] = time.time()
    report["elapsedSeconds"] = round(report["endedAtEpoch"] - report["startedAtEpoch"], 3)
    report["healthAfter"] = request_json(base + "/health", key)[1]
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Output: {output_path} ({len(output_lines)} lines)")
    print(f"Report: {report_path}")
    print(f"Result: {report['status']}")
    return 0 if report["status"] == "completed" else 5


if __name__ == "__main__":
    raise SystemExit(main())
