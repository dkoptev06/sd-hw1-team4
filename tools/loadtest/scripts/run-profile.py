#!/usr/bin/env python3
"""Run one open-loop profile against a dedicated Compose stack and verify its SQL outcomes."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import tempfile
import time

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parents[2]


def compose_base(project=None):
    cmd = ["docker", "compose"]
    if project:
        cmd += ["-p", project]
    return cmd + ["-f", "compose.yaml", "-f", "compose.loadtest.yaml", "--profile", "loadtest"]


def compose(*args, project=None, **kwargs):
    return subprocess.run(
        [*compose_base(project), *args], cwd=ROOT, check=True, text=True, capture_output=True, **kwargs
    ).stdout.strip()


def sql(query, values=None, project=None):
    variables = [x for k, v in (values or {}).items() for x in ("-v", f"{k}={v}")]
    return compose(
        "exec", "-T", "postgres", "psql", "-XqAt", "-U", "rental", "-d", "rental",
        "-v", "ON_ERROR_STOP=1", *variables, project=project, input=query,
    )


def parse_utc(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def extract_flag_int(extra, name, default=0):
    """Parse a Go-style integer flag (-name N / --name N / -name=N) from extra args."""
    value = default
    i = 0
    while i < len(extra):
        arg = extra[i]
        if arg in (f"-{name}", f"--{name}") and i + 1 < len(extra):
            value = int(extra[i + 1])
            i += 2
            continue
        for prefix in (f"-{name}=", f"--{name}="):
            if arg.startswith(prefix):
                value = int(arg[len(prefix):])
                break
        i += 1
    return value


def has_flag(extra, name):
    return any(
        arg in (f"-{name}", f"--{name}") or arg.startswith(f"-{name}=") or arg.startswith(f"--{name}=")
        for arg in extra
    )


def detect_project(project):
    if project:
        return project
    raw = compose("ps", "--status", "running", "--format", "json")
    for line in raw.splitlines():
        info = json.loads(line)
        if info.get("Project"):
            return info["Project"]
    return None


def sample(project=None):
    last_exc = None
    for _ in range(2):
        try:
            ids = compose("ps", "-q", "--status", "running", project=project).splitlines()
            if not ids:
                raise ValueError("no running test containers")
            return collect_sample(ids, project=project)
        except (subprocess.SubprocessError, ValueError, KeyError) as exc:
            # A one-shot loadtest --rm container can exit between `compose ps`
            # and `docker stats`; retry once before recording a sampling error.
            last_exc = exc
    return {"utc": datetime.now(timezone.utc).isoformat(), "sampling_error": str(last_exc)}


def collect_sample(ids, project=None):
    # Counts use indexed working tables. Read I/O is PostgreSQL buffer misses.
    db = sql(
        """SELECT json_build_object(
        'pending', (SELECT count(*) FROM tasks WHERE status='PENDING'),
        'dlq', (SELECT count(*) FROM tasks WHERE status='DEAD_LETTER'),
        'archive_backlog', (SELECT count(*) FROM orders WHERE closed_at IS NOT NULL),
        'archive_lag_s', (SELECT coalesce(extract(epoch FROM now()-min(closed_at)),0)
                           FROM orders WHERE closed_at IS NOT NULL),
        'pg_blocks_read', blks_read, 'pg_blocks_hit', blks_hit,
        'db_bytes', pg_database_size(current_database()))
        FROM pg_stat_database WHERE datname=current_database();""",
        project=project,
    )
    stats = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{json .}}", *ids],
        check=True, capture_output=True, text=True,
    )
    containers = []
    for line in stats.stdout.splitlines():
        v = json.loads(line)
        containers.append({
            "name": v["Name"],
            "cpu_percent": float(v["CPUPerc"].rstrip("%")),
            "memory": v["MemUsage"].split("/")[0].strip(),
            "block_io": v["BlockIO"],
        })
    return {
        "utc": datetime.now(timezone.utc).isoformat(),
        **json.loads(db),
        "maintenance_rows": maintenance_counters(project=project),
        "containers": containers,
    }


def fixture_state(project=None):
    # Full inventory is taken outside the load window so the scan cannot warm
    # every historical index between user GETs. Matches both exact legacy
    # identifiers and per-scooter identifiers (load-history-retained-<N>).
    return json.loads(sql(
        """SELECT json_build_object(
        'retained', count(*) FILTER (WHERE scooter_id LIKE 'load-history-retained%'),
        'expired_fixture_rows', count(*) FILTER (WHERE scooter_id LIKE 'load-history-expired%'),
        'mean_row_bytes', round(avg(pg_column_size(h)) FILTER (WHERE scooter_id LIKE 'load-history-retained%')),
        'partitions_with_retained', count(DISTINCT h.tableoid) FILTER (WHERE scooter_id LIKE 'load-history-retained%'))
        FROM orders_history h;""",
        project=project,
    ))


def maintenance_counters(project=None):
    counters = {}
    services = map(json.loads, compose("ps", "--status", "running", "--format", "json", project=project).splitlines())
    for service in services:
        if service["Service"] != "rental-maintenance":
            continue
        metrics = subprocess.check_output(
            ["docker", "exec", service["ID"], "wget", "-qO-", "http://127.0.0.1:8080/metrics"], text=True
        )
        for line in metrics.splitlines():
            if line.startswith("rental_maintenance_rows_total{"):
                key, value = line.split()
                step = key.split('"')[1]
                counters[step] = counters.get(step, 0) + int(float(value))
    return counters


def analyze_backlog(samples, win_from, win_to):
    """Detect unbounded worker queue or archiving lag growth during the measured window."""
    t_from, t_to = parse_utc(win_from), parse_utc(win_to)
    in_window = [
        s for s in samples
        if "sampling_error" not in s and t_from <= parse_utc(s["utc"]) <= t_to
    ]
    if not in_window:
        return {"window_samples": 0, "backlog_stable": True}
    max_pending = max(s["pending"] for s in in_window)
    max_dlq = max(s["dlq"] for s in in_window)
    max_backlog = max(s["archive_backlog"] for s in in_window)
    max_lag = max(s["archive_lag_s"] for s in in_window)
    summary = {
        "window_samples": len(in_window),
        "max_pending": max_pending,
        "max_dlq": max_dlq,
        "max_archive_backlog": max_backlog,
        "max_archive_lag_s": round(max_lag, 3),
    }
    stable = max_dlq == 0
    if len(in_window) >= 6:
        mid = len(in_window) // 2
        first, second = in_window[:mid], in_window[mid:]
        first_med_lag = statistics.median(s["archive_lag_s"] for s in first)
        second_med_lag = statistics.median(s["archive_lag_s"] for s in second)
        second_min_lag = min(s["archive_lag_s"] for s in second)
        first_max_pending = max(s["pending"] for s in first)
        second_min_pending = min(s["pending"] for s in second)
        summary.update({
            "first_half_median_archive_lag_s": round(first_med_lag, 3),
            "second_half_median_archive_lag_s": round(second_med_lag, 3),
            "second_half_min_archive_lag_s": round(second_min_lag, 3),
            "first_half_max_pending": first_max_pending,
            "second_half_min_pending": second_min_pending,
        })
        # Allow normal periodic maintenance sawtooth (up to ~60-75s with 1m interval),
        # but fail on sustained unbounded growth across the two halves of the window.
        if second_min_lag > 120.0 or second_med_lag > first_med_lag + 90.0:
            stable = False
        if second_min_pending > max(first_max_pending, 100):
            stable = False
    summary["backlog_stable"] = stable
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument(
        "--project",
        default=os.environ.get("COMPOSE_PROJECT_NAME"),
        help="dedicated Docker Compose project (defaults to COMPOSE_PROJECT_NAME)",
    )
    parser.add_argument("--history-file", type=Path)
    parser.add_argument("--sample-every", type=int, default=60)
    parser.add_argument("--no-build", action="store_true", help="use the existing emulator image")
    parser.add_argument(
        "--start-maintenance",
        action="store_true",
        help="start rental-maintenance shortly after load begins so expired partitions drop under live traffic",
    )
    args, extra = parser.parse_known_args()
    if args.sample_every <= 0:
        parser.error("--sample-every must be positive")
    if extra[:1] == ["--"]:
        extra = extra[1:]
    try:
        historical = extract_flag_int(extra, "historical", default=0)
    except ValueError as err:
        parser.error(f"invalid -historical value: {err}")
    if args.history_file and not args.history_file.is_file():
        parser.error(f"history file does not exist: {args.history_file}")
    if args.history_file and historical <= 0:
        parser.error("--history-file requires passing -- -historical <N> with N > 0")
    if not args.history_file and historical > 0 and not has_flag(extra, "history-file"):
        parser.error("-historical > 0 requires --history-file")

    # Only this Compose project is observed. Every test gets a new container.
    project = detect_project(args.project)
    ids = compose("ps", "-q", "--status", "running", project=project).splitlines()
    if not ids:
        parser.error("start the dedicated Compose project first")

    # Capture fixture state before building when maintenance is already running,
    # so image build time does not hide expired fixture rows before measurement.
    fixtures_before = fixture_state(project=project) if (args.history_file and not args.start_maintenance) else None

    # The companion image contains only the driver. Core images come from compose.yaml.
    if not args.no_build:
        subprocess.run([*compose_base(project), "build", "loadtest"], cwd=ROOT, check=True)
    if args.history_file and args.start_maintenance:
        fixtures_before = fixture_state(project=project)

    cmd = [*compose_base(project), "run", "--rm", "-T", "--no-deps"]
    if args.history_file:
        cmd += ["-v", f"{args.history_file.resolve()}:/history.ids:ro"]
        extra += ["-history-file", "/history.ids"]
    cmd += ["loadtest", "-url", "http://nginx:8080", *extra]
    samples = [sample(project=project)]
    with tempfile.TemporaryFile(mode="w+") as stdout:
        with subprocess.Popen(cmd, cwd=ROOT, stdout=stdout, text=True) as proc:
            if args.start_maintenance:
                # Let the load generator establish traffic before starting maintenance
                # so DETACH PARTITION CONCURRENTLY / DROP TABLE run under live reads.
                try:
                    rc = proc.wait(timeout=min(2, args.sample_every))
                except subprocess.TimeoutExpired:
                    subprocess.run(
                        [*compose_base(project), "up", "-d", "--wait", "rental-maintenance"],
                        cwd=ROOT, check=True,
                    )
                    rc = None
            else:
                rc = None
            while rc is None:
                try:
                    rc = proc.wait(timeout=args.sample_every)
                    break
                except subprocess.TimeoutExpired:
                    samples.append(sample(project=project))
        stdout.seek(0)
        raw = stdout.read().strip()
        if not raw:
            raise SystemExit(f"Load generator exited with code {rc} without a report")
        result = json.loads(raw)

    for _ in range(60):
        if sql("SELECT count(*) FROM tasks WHERE status='PENDING'", project=project) == "0":
            break
        time.sleep(2)
    end = datetime.now(timezone.utc)
    summary = json.loads(sql(
        (SCRIPT_DIR / "check-profile.sql").read_text(),
        {
            "run_start": result["start"],
            "run_end": end.isoformat(),
            "win_from": result["window_from"],
            "win_to": result["window_to"],
        },
        project=project,
    ))
    seconds = (parse_utc(result["window_to"]) - parse_utc(result["window_from"])).total_seconds()
    for k in ("activated", "finished"):
        summary[k + "_rps"] = summary[k + "_in_window"] / seconds
    result["project"] = project
    result["database"] = summary

    # Wait for maintenance to purge completed tasks, move all settled rides to
    # orders_history, AND finish drop_partitions (which runs after archive_orders).
    deadline = time.monotonic() + 180
    remaining = None
    while True:
        current = int(sql(
            "SELECT count(*) FROM orders WHERE created_at >= :'start' AND created_at < :'end'",
            {"start": result["start"], "end": end.isoformat()},
            project=project,
        ))
        completed_tasks = int(sql("SELECT count(*) FROM tasks WHERE status = 'COMPLETED'", project=project))
        if fixtures_before:
            remaining = fixture_state(project=project)
            expired_left = remaining["expired_fixture_rows"]
        else:
            expired_left = 0
        if current == 0 and completed_tasks == 0 and expired_left == 0:
            break
        if time.monotonic() >= deadline:
            break
        time.sleep(3)

    result["unarchived_after_drain"] = current
    result["completed_tasks_after_drain"] = completed_tasks
    result["samples"] = samples + [sample(project=project)]
    backlog = analyze_backlog(result["samples"], result["window_from"], result["window_to"])
    result["backlog_summary"] = backlog

    cleanup_pass = True
    if fixtures_before:
        result["history_before"] = fixtures_before
        result["history_after"] = remaining
        cleanup_pass = (
            remaining["retained"] == fixtures_before["retained"]
            and remaining["expired_fixture_rows"] == 0
        )
    result["observability_complete"] = all("sampling_error" not in s for s in result["samples"])
    result["maintenance_pass"] = current == 0 and completed_tasks == 0 and cleanup_pass
    result["pass"] = (
        rc == 0
        and result["http_profile_pass"]
        and result["maintenance_pass"]
        and result["observability_complete"]
        and backlog["backlog_stable"]
        and summary["trips"] == summary["finished"] == result["scheduled_trips"]
        and all(
            summary[k] == 0
            for k in (
                "failed_starts",
                "still_starting",
                "outstanding_tasks",
                "fallback_orders",
                "reused_offers",
                "overlapping_rentals",
                "overlapping_user_rentals",
                "wrong_terms",
                "wrong_bills",
                "wrong_retention",
            )
        )
        and summary["activation_p95_ms"] is not None
        and summary["activation_p95_ms"] <= 2000
        and summary["mean_order_row_bytes_in_window"] is not None
        and 900 <= summary["mean_order_row_bytes_in_window"] <= 1150
        and all(0.95 * result["rate"] <= summary[k] <= 1.05 * result["rate"] for k in ("activated_rps", "finished_rps"))
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2) + "\n")
    print(f"{args.out}: {'PASS' if result['pass'] else 'FAIL'}", file=sys.stderr)
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
