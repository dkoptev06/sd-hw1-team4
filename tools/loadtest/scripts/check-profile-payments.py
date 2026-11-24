#!/usr/bin/env python3
"""Check payments for a deterministic sample of finished rides from each measured window."""
import argparse
from collections import Counter
import hashlib
import http.client
import json
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlsplit
from uuid import UUID

ROOT = Path(__file__).resolve().parents[3]
SAMPLE_SQL = """
WITH cohort AS (
    SELECT order_id, status FROM orders
    WHERE created_at >= :'win_from'::timestamptz AND created_at < :'win_to'::timestamptz
    UNION ALL
    SELECT order_id, status FROM orders_history
    WHERE created_at >= :'win_from'::timestamptz AND created_at < :'win_to'::timestamptz
), eligible AS (
    SELECT DISTINCT order_id FROM cohort WHERE status IN ('COMPLETED', 'CANCELLED')
)
SELECT json_build_object(
    'window_orders', (SELECT count(*) FROM cohort),
    'finished_rows', (SELECT count(*) FROM cohort WHERE status IN ('COMPLETED','CANCELLED')),
    'eligible_orders', (SELECT count(*) FROM eligible),
    'ids', ARRAY(SELECT order_id FROM eligible
                 ORDER BY md5(:'run_id' || order_id::text), order_id LIMIT :'sample_size'::int));
"""


def compose(*args, stdin=None):
    return subprocess.check_output(["docker", "compose", *args], cwd=ROOT, input=stdin, text=True).strip()


def verify(path, sample_size):
    profile = json.loads(path.read_text())
    run_id = str(UUID(profile["run_id"]))
    win_from, win_to = profile["window_from"], profile["window_to"]
    expected_count = profile["database"]["created_in_window"]
    sample = json.loads(compose("exec", "-T", "postgres", "psql", "-XqAt", "-U", "rental", "-d", "rental",
                                "-v", "ON_ERROR_STOP=1", "-v", "run_id=" + run_id,
                                "-v", "win_from=" + win_from, "-v", "win_to=" + win_to,
                                "-v", f"sample_size={sample_size}", stdin=SAMPLE_SQL))
    ids = [str(UUID(value)) for value in sample.pop("ids")]
    errors = []
    if sample["window_orders"] != expected_count:
        errors.append(f"database has {sample['window_orders']} rows in window, profile recorded {expected_count}")
    if sample["eligible_orders"] != sample["finished_rows"]:
        errors.append("duplicate finished order IDs in current and historical data")
    if len(ids) != sample_size or len(set(ids)) != sample_size:
        errors.append(f"expected {sample_size} distinct finished orders, found {len(set(ids))}")
    statuses = Counter()
    mismatches = []
    if not errors:
        # Resolve the published port for this specific Compose project.
        target = urlsplit("//" + compose("port", "payment", "8080"))
        host = {"0.0.0.0": "127.0.0.1", "::": "::1"}.get(target.hostname, target.hostname)
        if not host or target.port is None:
            raise ValueError("the Payment service has no published HTTP port")
        conn = http.client.HTTPConnection(host, target.port, timeout=5)
        try:
            for order_id in ids:
                conn.request("GET", "/payment/" + order_id)
                reply = conn.getresponse()
                body = reply.read()
                status = f"HTTP_{reply.status}"
                if reply.status == 200:
                    try:
                        result = json.loads(body)
                        if not isinstance(result, dict) or not isinstance(result.get("status"), str):
                            raise ValueError("missing status")
                        status = result["status"]
                    except ValueError:
                        status = "INVALID_RESPONSE"
                statuses[status] += 1
                if status != "PAID" and len(mismatches) < 10:
                    mismatches.append({"order_id": order_id, "payment": status})
        except (OSError, http.client.HTTPException) as err:
            errors.append(f"GET /payment/{order_id}: {err}")
        finally:
            conn.close()
    return {"profile": path.name, "run_id": run_id, "window_from": win_from, "window_to": win_to,
            **sample, "requested_sample": sample_size, "checked": sum(statuses.values()),
            "sample_ids_sha256": hashlib.sha256(("\n".join(ids) + "\n").encode()).hexdigest(),
            "payment_status_counts": dict(statuses), "mismatch_examples": mismatches, "errors": errors,
            "pass": not errors and statuses["PAID"] == sample_size}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profiles", type=Path, nargs="+", help="JSON output from run-profile.py")
    parser.add_argument("--sample-size", type=int, default=1000, help="distinct order IDs per profile (default 1000)")
    args = parser.parse_args()
    if args.sample_size < 1:
        parser.error("--sample-size must be positive")
    reports = []
    for path in args.profiles:
        try:
            reports.append(verify(path, args.sample_size))
        except (OSError, ValueError, KeyError, subprocess.SubprocessError) as err:
            reports.append({"profile": path.name, "pass": False, "error": str(err)})
    passed = all(r["pass"] for r in reports)
    print(json.dumps({"profiles": reports, "pass": passed}, indent=2))
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
