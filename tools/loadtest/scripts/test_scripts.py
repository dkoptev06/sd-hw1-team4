#!/usr/bin/env python3
"""Unit tests for the loadtest helper scripts (no Docker required)."""
import csv
from datetime import datetime, timedelta, timezone
import http.client
from http.server import BaseHTTPRequestHandler, HTTPServer
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import threading
import unittest
from urllib.parse import parse_qs, urlsplit
from uuid import UUID

SCRIPTS_DIR = Path(__file__).resolve().parent


def load_module(filename, name):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS_DIR / filename)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


prepare_mod = load_module("prepare-emulator-data.py", "prepare_emulator_data")
run_mod = load_module("run-profile.py", "run_profile")
payments_mod = load_module("check-profile-payments.py", "check_profile_payments")
export_mod = load_module("export-window.py", "export_window")


class PrepareDataTests(unittest.TestCase):
    def test_stream_uuids_validates_order_uniqueness_and_digest(self):
        ids = [
            "00000000-0000-4000-8000-000000000001\n",
            "00000000-0000-4000-8000-000000000002\n",
            "ffffffff-ffff-4fff-bfff-ffffffffffff\n",
        ]
        buf = io.StringIO()
        count, digest = prepare_mod.stream_uuids(ids, writer=buf)
        self.assertEqual(count, 3)
        self.assertEqual(buf.getvalue(), "".join(ids))
        count2, digest2 = prepare_mod.stream_uuids(ids)
        self.assertEqual((count, digest), (count2, digest2))

    def test_stream_uuids_rejects_duplicates_and_unsorted(self):
        dup = [
            "00000000-0000-4000-8000-000000000001\n",
            "00000000-0000-4000-8000-000000000001\n",
        ]
        with self.assertRaises(ValueError):
            prepare_mod.stream_uuids(dup)
        unsorted = [
            "00000000-0000-4000-8000-000000000002\n",
            "00000000-0000-4000-8000-000000000001\n",
        ]
        with self.assertRaises(ValueError):
            prepare_mod.stream_uuids(unsorted)

    def test_resolve_history_count_from_target_mib(self):
        self.assertEqual(prepare_mod.resolve_history_count(53_000, None), 53_000)
        # 700 MiB / 1340 B/row ~= 547,764 rows (> 512 MiB shared_buffers)
        count = prepare_mod.resolve_history_count(53_000, 700)
        self.assertGreater(count, 500_000)
        self.assertEqual(prepare_mod.resolve_history_count(53_000, 0.001), 53)


class RunProfileTests(unittest.TestCase):
    def test_extract_flag_int_supports_go_flag_forms(self):
        self.assertEqual(run_mod.extract_flag_int([], "historical"), 0)
        self.assertEqual(run_mod.extract_flag_int(["-historical", "15"], "historical"), 15)
        self.assertEqual(run_mod.extract_flag_int(["--historical=15"], "historical"), 15)
        self.assertEqual(run_mod.extract_flag_int(["-historical", "5", "-historical=15"], "historical"), 15)

    def test_compose_base_includes_project(self):
        self.assertEqual(run_mod.compose_base("mixed-lab")[:4], ["docker", "compose", "-p", "mixed-lab"])
        self.assertEqual(run_mod.compose_base(None)[:3], ["docker", "compose", "-f"])

    def test_analyze_backlog_allows_healthy_sawtooth(self):
        start = datetime(2026, 11, 24, 12, 0, 0, tzinfo=timezone.utc)
        end = start + timedelta(minutes=10)
        # Normal 1-minute maintenance sawtooth between 5s and 58s lag, low pending.
        lags = [10, 55, 8, 52, 12, 58, 7, 54, 15, 50]
        samples = [
            {
                "utc": (start + timedelta(minutes=i, seconds=30)).isoformat(),
                "pending": 5 if i % 2 else 0,
                "dlq": 0,
                "archive_backlog": int(lag * 100),
                "archive_lag_s": float(lag),
            }
            for i, lag in enumerate(lags)
        ]
        summary = run_mod.analyze_backlog(samples, start.isoformat(), end.isoformat())
        self.assertTrue(summary["backlog_stable"], summary)
        self.assertEqual(summary["window_samples"], 10)

    def test_analyze_backlog_catches_runaway_archive_lag_and_dlq(self):
        start = datetime(2026, 11, 24, 12, 0, 0, tzinfo=timezone.utc)
        end = start + timedelta(minutes=10)
        # Archive lag climbs monotonically without draining.
        samples = [
            {
                "utc": (start + timedelta(minutes=i, seconds=30)).isoformat(),
                "pending": 0,
                "dlq": 0,
                "archive_backlog": (i + 1) * 5000,
                "archive_lag_s": float((i + 1) * 35),
            }
            for i in range(10)
        ]
        summary = run_mod.analyze_backlog(samples, start.isoformat(), end.isoformat())
        self.assertFalse(summary["backlog_stable"], summary)

        # Any DLQ task during window also marks backlog unstable.
        dlq_samples = [{
            "utc": (start + timedelta(seconds=30)).isoformat(),
            "pending": 0, "dlq": 1, "archive_backlog": 0, "archive_lag_s": 0.0,
        }]
        self.assertFalse(run_mod.analyze_backlog(dlq_samples, start.isoformat(), end.isoformat())["backlog_stable"])


class CheckPaymentsTests(unittest.TestCase):
    def test_fetch_payment_status_reconnects_on_idle_close(self):
        calls = {"n": 0}

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self):
                calls["n"] += 1
                if calls["n"] == 1:
                    # Simulate a server closing an idle keep-alive socket before replying.
                    self.close_connection = True
                    return
                body = b'{"status":"PAID"}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        srv = HTTPServer(("127.0.0.1", 0), Handler)
        host, port = srv.server_address
        thread = threading.Thread(target=srv.serve_forever, daemon=True)
        thread.start()
        conn = http.client.HTTPConnection(host, port, timeout=5)
        try:
            conn, status, body = payments_mod.fetch_payment_status(conn, host, port, "ord-1")
            self.assertEqual(status, 200)
            self.assertEqual(json.loads(body), {"status": "PAID"})
            self.assertEqual(calls["n"], 2)
        finally:
            conn.close()
            srv.shutdown()
            srv.server_close()


class ExportWindowTests(unittest.TestCase):
    def test_queries_include_all_required_acceptance_columns(self):
        cols = [col for _, _, mapping in export_mod.queries() for col in mapping.values()]
        self.assertEqual(len(cols), len(set(cols)), "duplicate CSV column names")
        for required in (
            "offer_rps", "start_rps", "get_rps", "finish_rps",
            "current_read_p95_ms", "history_read_p95_ms",
            "activation_rps", "activation_p95_ms",
            "start_claim_lag_p95_ms", "settle_claim_lag_p95_ms",
            "start_attempt_p95_ms", "settle_attempt_p95_ms",
            "http_error_rps", "source_error_rps", "payment_error_rps",
            "profile_fallback_rps", "failed_starts_per_s",
            "starting_orders", "active_orders",
            "pending_tasks", "dead_letters", "oldest_unarchived_s",
            "archived_rows_per_s", "purged_tasks_per_s", "dropped_partitions_per_s",
        ):
            self.assertIn(required, cols)

    def test_fetch_with_pre_minutes_labels_warmup_and_window(self):
        start_ms = export_mod.timestamp("2026-11-24T10:00:00.000Z")
        end_ms = export_mod.timestamp("2026-11-24T10:02:00.000Z")

        class PromHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                qs = parse_qs(urlsplit(self.path).query)
                s = float(qs["start"][0])
                e = float(qs["end"][0])
                step = float(qs["step"][0])
                ts = []
                cur = s
                while cur <= e + 1e-6:
                    ts.append([cur, "1"])
                    cur += step
                expr = qs["query"][0]
                if 'by (route)' in expr:
                    result = [{"metric": {"route": r}, "values": ts} for r in export_mod.ROUTES]
                elif 'by (storage)' in expr:
                    result = [{"metric": {"storage": st}, "values": ts} for st in ("current", "history")]
                elif 'by (kind)' in expr:
                    result = [{"metric": {"kind": k}, "values": ts} for k in ("START", "SETTLE")]
                elif 'by (le,kind)' in expr:
                    result = [{"metric": {"kind": k}, "values": ts} for k in ("START", "SETTLE")]
                elif 'by (le,route)' in expr:
                    result = [{"metric": {"route": r}, "values": ts} for r in export_mod.ROUTES]
                elif 'by (le,storage)' in expr:
                    result = [{"metric": {"storage": st}, "values": ts} for st in ("current", "history")]
                elif 'by(status)' in expr and 'rental_tasks' in expr:
                    result = [{"metric": {"status": st}, "values": ts} for st in ("PENDING", "DEAD_LETTER")]
                elif 'by(status)' in expr and 'rental_orders' in expr:
                    result = [{"metric": {"status": st}, "values": ts} for st in ("STARTING", "ACTIVE")]
                elif 'by (step)' in expr:
                    result = [{"metric": {"step": st}, "values": ts}
                              for st in ("archive_orders_moved", "purge_tasks", "drop_partitions")]
                elif 'by (job)' in expr:
                    result = [{"metric": {"job": j}, "values": ts} for j in export_mod.JOBS]
                elif 'by (reason)' in expr:
                    result = []
                else:
                    result = [{"metric": {}, "values": ts}]
                body = json.dumps({"status": "success", "data": {"result": result}}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        srv = HTTPServer(("127.0.0.1", 0), PromHandler)
        host, port = srv.server_address
        thread = threading.Thread(target=srv.serve_forever, daemon=True)
        thread.start()
        try:
            steps, rows, fields = export_mod.fetch(f"http://{host}:{port}", start_ms, end_ms, pre_minutes=1)
            self.assertEqual(len(steps), 3)
            self.assertEqual(steps[0], start_ms)
            self.assertEqual(steps[1], start_ms + 60_000)
            self.assertEqual(steps[2], end_ms)
            self.assertEqual(rows[0]["offer_rps"], 1.0)
            self.assertEqual(rows[0]["start_claim_lag_p95_ms"], 1.0)
            self.assertEqual(rows[0]["source_error_rps"], 1.0)
            self.assertEqual(rows[0]["payment_error_rps"], 1.0)
            self.assertEqual(rows[0]["starting_orders"], 1.0)
            try:
                import matplotlib  # noqa: F401
            except ImportError:
                pass
            else:
                with tempfile.TemporaryDirectory() as tmp:
                    png = Path(tmp) / "out.png"
                    export_mod.plot(png, steps, rows, start_ms, "test plot")
                    self.assertGreater(png.stat().st_size, 1000)
        finally:
            srv.shutdown()
            srv.server_close()


class SQLInvariantsSyntaxTests(unittest.TestCase):
    def test_sql_scripts_contain_required_checks(self):
        check_sql = (SCRIPTS_DIR / "check-profile.sql").read_text()
        self.assertIn("wrong_retention", check_sql)
        self.assertIn("overlapping_user_rentals", check_sql)
        self.assertIn("distinct_pricing_combos", check_sql)
        self.assertIn("8760 hours", check_sql)
        self.assertIn("max(coalesce(closed_at", check_sql)

        seed_sql = (SCRIPTS_DIR / "seed-history.sql").read_text()
        self.assertIn("START_FAILED", seed_sql)
        self.assertIn("CANCELLED", seed_sql)
        self.assertIn("COMPLETED", seed_sql)
        self.assertIn('ORDER BY order_id::text COLLATE "C"', seed_sql)
        self.assertNotIn("trunc(duration_s", seed_sql)


if __name__ == "__main__":
    unittest.main()
