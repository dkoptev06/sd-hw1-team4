#!/usr/bin/env python3
"""Prepare and verify data for one emulator scenario in an isolated Compose database."""

import argparse
import json
import subprocess
import sys
from pathlib import Path
from uuid import UUID

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parents[2]

EMPTY = """
SELECT NOT EXISTS (SELECT FROM orders) AND NOT EXISTS (SELECT FROM orders_history)
   AND NOT EXISTS (SELECT FROM offers) AND NOT EXISTS (SELECT FROM tasks);
"""

HISTORY_SUMMARY = """
SELECT json_build_object(
    'retained_rows', count(*) FILTER (WHERE scooter_id = 'load-history-retained'),
    'expired_rows', count(*) FILTER (WHERE scooter_id = 'load-history-expired'),
    'retained_weeks', count(DISTINCT tableoid) FILTER (WHERE scooter_id = 'load-history-retained'),
    'expired_weeks', count(DISTINCT tableoid) FILTER (WHERE scooter_id = 'load-history-expired'),
    'zones', count(DISTINCT zone_id),
    'mean_row_bytes', round(avg(pg_column_size(h))),
    'invalid_rows', count(*) FILTER (
        WHERE status <> 'COMPLETED' OR cost <> 350 OR started_at IS NULL OR finish_time IS NULL
           OR closed_at > now() OR finish_time - started_at <> interval '5 minutes'
           OR delete_after - closed_at <> interval '365 days'
           OR (scooter_id = 'load-history-expired') IS DISTINCT FROM
               (delete_after < date_trunc('week', now(), 'UTC')))
)
FROM orders_history h;
"""

RETAINED_IDS = "SELECT order_id FROM orders_history WHERE scooter_id = 'load-history-retained';"


def psql(project, query, variables=None):
    command = [
        "docker", "compose", "-p", project, "exec", "-T", "postgres",
        "psql", "-XqAt", "-U", "rental", "-d", "rental", "-v", "ON_ERROR_STOP=1",
    ]
    for key, value in (variables or {}).items():
        command += ["-v", f"{key}={value}"]
    return subprocess.run(command, cwd=ROOT, input=query, text=True, check=True, stdout=subprocess.PIPE).stdout


def prepare(args):
    if args.profile == "mixed" and args.ids.exists():
        raise ValueError(f"{args.ids} already exists")
    if psql(args.project, EMPTY).strip() != "t":
        raise ValueError("use a fresh, migrated database for each profile")
    manifest = {"profile": args.profile, "project": args.project, "initial_database_empty": True}
    if args.profile == "baseline":
        return manifest

    printed = psql(args.project, (SCRIPT_DIR / "seed-history.sql").read_text(), {
        "rows": args.history_count,
        "expired": args.expired_count,
    }).split()
    ids = {UUID(value) for value in printed}
    stored = {UUID(value) for value in psql(args.project, RETAINED_IDS).split()}
    summary = json.loads(psql(args.project, HISTORY_SUMMARY))
    expected_counts = {
        "retained_rows": args.history_count,
        "expired_rows": args.expired_count,
        "retained_weeks": min(args.history_count, 53),
        "expired_weeks": int(args.expired_count > 0),
        "zones": min(4, max(args.history_count, args.expired_count)),
        "invalid_rows": 0,
    }
    if (len(printed) != args.history_count or len(ids) != len(printed) or ids != stored
            or any(summary[k] != v for k, v in expected_counts.items())
            or not 900 <= summary["mean_row_bytes"] <= 1150):
        raise ValueError(f"fixture audit failed: {summary}")
    args.ids.parent.mkdir(parents=True, exist_ok=True)
    with args.ids.open("x") as output:
        output.write("\n".join(printed) + "\n")
    return {**manifest, **summary, "ids_file": str(args.ids), "ids_verified_in_postgres": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", choices=("baseline", "mixed"))
    parser.add_argument("--project", required=True, help="dedicated, already migrated Docker Compose project")
    parser.add_argument("--ids", type=Path, help="new output file for stable archived IDs, required for mixed")
    parser.add_argument("--history-count", type=int, default=53_000)
    parser.add_argument("--expired-count", type=int, default=0, help="rows in a fully expired weekly partition")
    args = parser.parse_args()
    if args.profile == "mixed":
        if args.ids is None:
            parser.error("mixed requires --ids")
        if args.history_count < 1 or args.expired_count < 0:
            parser.error("history count must be positive and expired count cannot be negative")
    elif args.ids is not None:
        parser.error("--ids applies only to mixed")
    try:
        print(json.dumps(prepare(args), indent=2))
    except (OSError, subprocess.CalledProcessError, ValueError) as error:
        print(error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
