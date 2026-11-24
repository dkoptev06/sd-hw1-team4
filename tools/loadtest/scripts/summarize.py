#!/usr/bin/env python3
"""Print a loadtest JSON report as a short table: RPS and p95 against targets, failures, verdict."""
import json
import sys
from pathlib import Path

P95_LIMIT_MS = {"offer": 1000, "start": 2000, "finish": 2000, "get_current": 300, "get_history": 300}
ORDER = ("offer", "start", "finish", "get_current", "get_history")


def main():
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} REPORT.json", file=sys.stderr)
        return 2
    path = Path(sys.argv[1])
    raw = path.read_text(encoding="utf-8").strip() if path.exists() else ""
    if not raw:
        print(f"{path}: emulator produced no report, see its stderr above", file=sys.stderr)
        return 2
    r = json.loads(raw)
    rate, reads, hist = r["rate"], r["reads_per_ride"], r["historical_reads_per_ride"]
    targets = {"offer": rate, "start": rate, "finish": rate,
               "get_current": rate * (reads - hist), "get_history": rate * hist}

    print(f"run {r['run_id']}  window {r['window_from']} .. {r['window_to']}")
    print(f"profile: {rate} trips/s, {reads} GET/trip ({hist} archived), "
          f"rides {r['min_ride_seconds']:.0f}-{r['max_ride_seconds']:.0f}s\n")
    print(f"{'operation':<12} {'RPS':>9} {'target':>8} {'p95, ms':>9} {'limit':>7} {'ok/attempts':>17}")
    for name in ORDER:
        if not targets[name]:
            continue
        s = r["window_routes"][name]
        rps_ok = 0.95 * targets[name] <= s["success_rps"] <= 1.05 * targets[name]
        p95_ok = s["p95_ms"] <= P95_LIMIT_MS[name]
        mark = "" if rps_ok and p95_ok else "  <-- out of target"
        print(f"{name:<12} {s['success_rps']:>9.1f} {targets[name]:>8} {s['p95_ms']:>9.2f} "
              f"{P95_LIMIT_MS[name]:>7} {s['success']:>8}/{s['attempts']:<8}{mark}")

    q = r["read_queue"]
    print(f"\ntrips: scheduled {r['scheduled_trips']}, accepted {r['accepted_trips']}, "
          f"completed {r['completed_trips']}, ambiguous {r['ambiguous_trips']}")
    print(f"GET queue: peak {q['peak_queued']}/{q['capacity']}, overflows {q['overflows']}, "
          f"max dispatch delay {q['max_dispatch_delay_ms']:.1f} ms")
    failures = r["all_failures"]
    print("failures: " + (", ".join(f"{k}={v}" for k, v in sorted(failures.items())) if failures else "none"))
    print("\nRESULT: " + ("PASS" if r["http_profile_pass"] else "FAIL"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
