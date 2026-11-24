#!/usr/bin/env python3
"""Prepare and verify data for one emulator scenario in an isolated Compose database."""

import argparse
import hashlib
import json
import math
import os
import subprocess
import sys
from pathlib import Path
from uuid import UUID

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parents[2]
# Empirical bytes per seeded history row including heap + 3 local indexes (~1,340 B).
ESTIMATED_BYTES_PER_ROW = 1340

EMPTY = """
SELECT NOT EXISTS (SELECT FROM orders) AND NOT EXISTS (SELECT FROM orders_history)
   AND NOT EXISTS (SELECT FROM offers) AND NOT EXISTS (SELECT FROM tasks);
"""

HISTORY_SUMMARY = """
WITH sizing AS (
    SELECT coalesce(sum(pg_table_size(inhrelid)), 0) AS history_table_bytes,
           coalesce(sum(pg_indexes_size(inhrelid)), 0) AS history_index_bytes,
           pg_size_bytes(current_setting('shared_buffers')) AS shared_buffers_bytes
    FROM pg_inherits WHERE inhparent = 'orders_history'::regclass
)
SELECT json_build_object(
    'retained_rows', count(*) FILTER (WHERE scooter_id LIKE 'load-history-retained-%'),
    'expired_rows', count(*) FILTER (WHERE scooter_id LIKE 'load-history-expired-%'),
    'retained_weeks', count(DISTINCT h.tableoid) FILTER (WHERE scooter_id LIKE 'load-history-retained-%'),
    'expired_weeks', count(DISTINCT h.tableoid) FILTER (WHERE scooter_id LIKE 'load-history-expired-%'),
    'zones', count(DISTINCT zone_id),
    'completed_rows', count(*) FILTER (WHERE status = 'COMPLETED'),
    'cancelled_rows', count(*) FILTER (WHERE status = 'CANCELLED'),
    'start_failed_rows', count(*) FILTER (WHERE status = 'START_FAILED'),
    'mean_row_bytes', round(avg(pg_column_size(h))),
    'history_table_bytes', s.history_table_bytes,
    'history_index_bytes', s.history_index_bytes,
    'history_total_bytes', s.history_table_bytes + s.history_index_bytes,
    'shared_buffers_bytes', s.shared_buffers_bytes,
    'exceeds_shared_buffers', (s.history_table_bytes + s.history_index_bytes) > s.shared_buffers_bytes,
    'invalid_rows', count(*) FILTER (
        WHERE closed_at > now()
           OR delete_after <> closed_at + interval '8760 hours'
           OR (scooter_id LIKE 'load-history-expired-%') IS DISTINCT FROM
               (delete_after < date_trunc('week', now(), 'UTC'))
           OR (scooter_id NOT LIKE 'load-history-retained-%' AND scooter_id NOT LIKE 'load-history-expired-%')
           OR status NOT IN ('COMPLETED', 'CANCELLED', 'START_FAILED')
           OR (status = 'START_FAILED' AND (started_at IS NOT NULL OR finish_time IS NOT NULL OR cost <> 0))
           OR (status = 'CANCELLED' AND (started_at IS NULL OR finish_time IS NULL OR cost <> 0
               OR finish_time < started_at OR finish_time - started_at >= interval '5 seconds'))
           OR (status = 'COMPLETED' AND (started_at IS NULL OR finish_time IS NULL
               OR finish_time - started_at < interval '5 seconds'
               OR cost IS DISTINCT FROM unlock_fee + trunc(floor(EXTRACT(epoch FROM finish_time - started_at)) * minute_rate / 60)))
    )
)
FROM orders_history h CROSS JOIN sizing s
GROUP BY s.history_table_bytes, s.history_index_bytes, s.shared_buffers_bytes;
"""

RETAINED_IDS = """
SELECT order_id FROM orders_history
WHERE scooter_id LIKE 'load-history-retained-%'
ORDER BY order_id::text COLLATE "C";
"""


def psql_command(project, variables=None):
    command = [
        "docker", "compose", "-p", project, "exec", "-T", "postgres",
        "psql", "-XqAt", "-U", "rental", "-d", "rental", "-v", "ON_ERROR_STOP=1",
    ]
    for key, value in (variables or {}).items():
        command += ["-v", f"{key}={value}"]
    return command


def psql(project, query, variables=None):
    return subprocess.run(
        psql_command(project, variables), cwd=ROOT, input=query, text=True, check=True, stdout=subprocess.PIPE
    ).stdout


def stream_uuids(lines, writer=None):
    """Validate sorted unique UUID lines in O(1) memory, returning (count, sha256)."""
    digest = hashlib.sha256()
    count = 0
    prev = None
    for raw in lines:
        value = raw.strip()
        if not value:
            continue
        canonical = str(UUID(value))
        if prev is not None and canonical <= prev:
            raise ValueError(f"UUID stream not strictly ordered/unique near {canonical}")
        prev = canonical
        line = canonical + "\n"
        digest.update(line.encode("ascii"))
        if writer is not None:
            writer.write(line)
        count += 1
    return count, digest.hexdigest()


def run_psql_stream(project, query, variables=None, writer=None):
    proc = subprocess.Popen(
        psql_command(project, variables),
        cwd=ROOT,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert proc.stdin is not None and proc.stdout is not None and proc.stderr is not None
    proc.stdin.write(query)
    proc.stdin.close()
    try:
        count, digest = stream_uuids(proc.stdout, writer=writer)
    finally:
        proc.stdout.close()
    stderr = proc.stderr.read()
    proc.stderr.close()
    rc = proc.wait()
    if rc != 0:
        raise subprocess.CalledProcessError(rc, proc.args, stderr=stderr)
    return count, digest


def resolve_history_count(history_count, target_mib):
    if target_mib is not None:
        return max(53, math.ceil(target_mib * 1024 * 1024 / ESTIMATED_BYTES_PER_ROW))
    return history_count


def prepare(args):
    if args.profile == "mixed" and args.ids.exists():
        raise ValueError(f"{args.ids} already exists")
    if psql(args.project, EMPTY).strip() != "t":
        raise ValueError("use a fresh, migrated database for each profile")
    manifest = {"profile": args.profile, "project": args.project, "initial_database_empty": True}
    if args.profile == "baseline":
        return manifest

    history_count = resolve_history_count(args.history_count, args.target_mib)
    args.ids.parent.mkdir(parents=True, exist_ok=True)
    seed_sql = (SCRIPT_DIR / "seed-history.sql").read_text()
    try:
        with args.ids.open("x", encoding="ascii") as output:
            written_count, written_sha = run_psql_stream(
                args.project,
                seed_sql,
                {"rows": history_count, "expired": args.expired_count},
                writer=output,
            )
        stored_count, stored_sha = run_psql_stream(args.project, RETAINED_IDS)
        summary = json.loads(psql(args.project, HISTORY_SUMMARY))
        expected_counts = {
            "retained_rows": history_count,
            "expired_rows": args.expired_count,
            "retained_weeks": min(history_count, 53),
            "expired_weeks": int(args.expired_count > 0),
            "zones": min(4, max(history_count, args.expired_count)),
            "invalid_rows": 0,
        }
        if (written_count != history_count or stored_count != history_count or written_sha != stored_sha
                or any(summary[k] != v for k, v in expected_counts.items())
                or not 900 <= summary["mean_row_bytes"] <= 1150):
            raise ValueError(
                f"fixture audit failed: written={written_count} stored={stored_count} "
                f"sha_match={written_sha == stored_sha} summary={summary}"
            )
    except Exception:
        args.ids.unlink(missing_ok=True)
        raise
    return {
        **manifest,
        **summary,
        "ids_file": str(args.ids),
        "ids_sha256": written_sha,
        "ids_verified_in_postgres": True,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", choices=("baseline", "mixed"))
    parser.add_argument(
        "--project",
        default=os.environ.get("COMPOSE_PROJECT_NAME"),
        help="dedicated, already migrated Docker Compose project (defaults to COMPOSE_PROJECT_NAME)",
    )
    parser.add_argument("--ids", type=Path, help="new output file for stable archived IDs, required for mixed")
    size_group = parser.add_mutually_exclusive_group()
    size_group.add_argument("--history-count", type=int, default=53_000, help="retained historical rows (default 53000)")
    size_group.add_argument(
        "--target-mib",
        type=float,
        help="approximate total orders_history size (tables+indexes) in MiB (e.g. 700 to exceed 512MiB shared_buffers)",
    )
    parser.add_argument("--expired-count", type=int, default=0, help="rows in a fully expired weekly partition")
    args = parser.parse_args()
    if not args.project:
        parser.error("specify --project or export COMPOSE_PROJECT_NAME")
    if args.profile == "mixed":
        if args.ids is None:
            parser.error("mixed requires --ids")
        if args.history_count < 1 or args.expired_count < 0:
            parser.error("history count must be positive and expired count cannot be negative")
        if args.target_mib is not None and args.target_mib <= 0:
            parser.error("--target-mib must be positive")
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
