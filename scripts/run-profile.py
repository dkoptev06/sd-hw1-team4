#!/usr/bin/env python3
"""Run one open-loop profile against a dedicated Compose stack and verify its SQL outcomes."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ["docker", "compose", "-f", "compose.yaml", "-f", "compose.loadtest.yaml", "--profile", "loadtest"]


def compose(*args, **kwargs):
    return subprocess.run([*COMPOSE, *args], cwd=ROOT, check=True,
                          text=True, capture_output=True, **kwargs).stdout.strip()


def sql(query, values=None):
    variables = [x for k, v in (values or {}).items() for x in ("-v", f"{k}={v}")]
    return compose("exec", "-T", "postgres", "psql", "-XqAt", "-U", "rental", "-d", "rental",
                   "-v", "ON_ERROR_STOP=1", *variables, input=query)


def sample():
    try:
        ids = compose("ps", "-q", "--status", "running").splitlines()
        if not ids:
            raise ValueError("no running test containers")
        return collect_sample(ids)
    except (subprocess.SubprocessError, ValueError, KeyError) as exc:
        # Keep the generator's measurements if a service restarts between
        # discovery and inspection. The report records the missing observation.
        return {"utc": datetime.now(timezone.utc).isoformat(), "sampling_error": str(exc)}


def collect_sample(ids):
    # Counts use indexed working tables. Read I/O is PostgreSQL buffer misses.
    db = sql("""SELECT json_build_object(
        'pending', (SELECT count(*) FROM tasks WHERE status='PENDING'),
        'dlq', (SELECT count(*) FROM tasks WHERE status='DEAD_LETTER'),
        'archive_backlog', (SELECT count(*) FROM orders WHERE closed_at IS NOT NULL),
        'archive_lag_s', (SELECT coalesce(extract(epoch FROM now()-min(closed_at)),0)
                           FROM orders WHERE closed_at IS NOT NULL),
        'pg_blocks_read', blks_read, 'pg_blocks_hit', blks_hit,
        'db_bytes', pg_database_size(current_database()))
        FROM pg_stat_database WHERE datname=current_database();""")
    stats = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{json .}}", *ids],
                           check=True, capture_output=True, text=True)
    containers = []
    for line in stats.stdout.splitlines():
        v = json.loads(line)
        containers.append({"name": v["Name"], "cpu_percent": float(v["CPUPerc"].rstrip("%")),
                           "memory": v["MemUsage"].split("/")[0].strip(), "block_io": v["BlockIO"]})
    return {"utc": datetime.now(timezone.utc).isoformat(), **json.loads(db),
            "maintenance_rows": maintenance_counters(), "containers": containers}


def fixture_state():
    # Full inventory is taken outside the load window so the scan cannot warm
    # every historical index between user GETs.
    return json.loads(sql("""SELECT json_build_object(
        'retained', count(*) FILTER (WHERE scooter_id='load-history-retained'),
        'expired_fixture_rows', count(*) FILTER (WHERE scooter_id='load-history-expired'),
        'mean_row_bytes', round(avg(pg_column_size(h)) FILTER (WHERE scooter_id='load-history-retained')),
        'partitions_with_retained', count(DISTINCT tableoid) FILTER (WHERE scooter_id='load-history-retained'))
        FROM orders_history h;"""))


def maintenance_counters():
    counters = {}
    services = map(json.loads, compose("ps", "--status", "running", "--format", "json").splitlines())
    for service in services:
        if service["Service"] != "rental-maintenance":
            continue
        metrics = subprocess.check_output(
            ["docker", "exec", service["ID"], "wget", "-qO-", "http://127.0.0.1:8080/metrics"], text=True)
        for line in metrics.splitlines():
            if line.startswith("rental_maintenance_rows_total{"):
                key, value = line.split()
                step = key.split('"')[1]
                counters[step] = counters.get(step, 0) + int(float(value))
    return counters


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--history-file", type=Path)
    parser.add_argument("--sample-every", type=int, default=60)
    parser.add_argument("--no-build", action="store_true", help="use the existing emulator image")
    args, extra = parser.parse_known_args()
    if args.sample_every <= 0:
        parser.error("--sample-every must be positive")
    if extra[:1] == ["--"]:
        extra = extra[1:]
    # Only this Compose project is observed. Every test gets a new container.
    ids = compose("ps", "-q", "--status", "running").splitlines()
    if not ids:
        parser.error("start the dedicated Compose project first")
    # The companion image contains only the driver. Core images come from compose.yaml.
    if not args.no_build:
        subprocess.run([*COMPOSE, "build", "loadtest"], cwd=ROOT, check=True)
    cmd = [*COMPOSE, "run", "--rm", "-T", "--no-deps"]
    if args.history_file:
        cmd += ["-v", f"{args.history_file.resolve()}:/history.ids:ro"]
        extra += ["-history-file", "/history.ids"]
    cmd += ["loadtest", "-url", "http://nginx:8080", *extra]
    fixtures_before = fixture_state() if args.history_file else None
    samples = [sample()]
    with tempfile.TemporaryFile(mode="w+") as stdout:
        with subprocess.Popen(cmd, cwd=ROOT, stdout=stdout, text=True) as proc:
            while True:
                try:
                    rc = proc.wait(timeout=args.sample_every)
                    break
                except subprocess.TimeoutExpired:
                    samples.append(sample())
        stdout.seek(0)
        raw = stdout.read().strip()
        if not raw:
            raise SystemExit(f"Load generator exited with code {rc} without a report")
        result = json.loads(raw)
    for _ in range(60):
        if sql("SELECT count(*) FROM tasks WHERE status='PENDING'") == "0":
            break
        time.sleep(2)
    end = datetime.now(timezone.utc)
    summary = json.loads(sql((ROOT / "scripts/check-profile.sql").read_text(), {
        "run_start": result["start"], "run_end": end.isoformat(),
        "win_from": result["window_from"], "win_to": result["window_to"],
    }))
    seconds = (datetime.fromisoformat(result["window_to"].replace("Z","+00:00"))
               - datetime.fromisoformat(result["window_from"].replace("Z","+00:00"))).total_seconds()
    for k in ("activated", "finished"):
        summary[k + "_rps"] = summary[k + "_in_window"] / seconds
    result["database"] = summary
    # Wait for a maintenance cycle to verify that settled rides actually move to
    # history and remove any fully expired fixture partition.
    deadline = time.monotonic() + 150
    while time.monotonic() < deadline:
        current = int(sql("SELECT count(*) FROM orders WHERE created_at >= :'start' AND created_at < :'end'",
                          {"start": result["start"], "end": end.isoformat()}))
        if current == 0:
            break
        time.sleep(3)
    result["unarchived_after_drain"] = current
    result["samples"] = samples + [sample()]
    cleanup_pass = True
    if fixtures_before:
        remaining = fixture_state()
        result["history_before"] = fixtures_before
        result["history_after"] = remaining
        cleanup_pass = remaining["retained"] == fixtures_before["retained"] and remaining["expired_fixture_rows"] == 0
    result["observability_complete"] = all("sampling_error" not in s for s in result["samples"])
    result["maintenance_pass"] = current == 0 and cleanup_pass
    result["pass"] = (rc == 0 and result["http_profile_pass"] and result["maintenance_pass"] and result["observability_complete"]
                      and summary["trips"] == summary["finished"] == result["scheduled_trips"]
                      and all(summary[k] == 0 for k in ("failed_starts", "still_starting", "outstanding_tasks",
                                                        "reused_offers", "overlapping_rentals", "wrong_terms", "wrong_bills"))
                      and summary["activation_p95_ms"] is not None and summary["activation_p95_ms"] <= 2000
                      and all(.95 * result["rate"] <= summary[k] <= 1.05 * result["rate"]
                              for k in ("activated_rps", "finished_rps")))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2) + "\n")
    print(f"{args.out}: {'PASS' if result['pass'] else 'FAIL'}", file=sys.stderr)
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
