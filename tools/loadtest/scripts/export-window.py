#!/usr/bin/env python3
"""Export aligned one-minute Prometheus samples for a measured Rental window."""
import argparse
import csv
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

ROUTES = {"POST /offers": "offer", "POST /orders": "start", "GET /orders/:order_id": "get",
          "POST /orders/:order_id/finish": "finish"}
API = 'job="rental-api"'
WORKER = 'job="rental-worker"'
MAINT = 'job="rental-maintenance"'
JOBS = ("rental-api", "rental-worker", "rental-maintenance", "source", "payment")


def rate(metric, labels, group=""):
    by = f" by ({group})" if group else ""
    return f"sum{by}(rate({metric}{{{labels}}}[1m]))"


def p95(metric, labels, group=""):
    buckets = rate(metric + "_bucket", labels, "le," + group if group else "le")
    return f"1000 * histogram_quantile(0.95,{buckets})"


def queries():
    # Each selector maps a bounded set of labels to stable CSV columns.
    http = "rental_http_requests_total"
    yield rate(http, API + ',status=~"2.."', "route"), "route", {k: v + "_rps" for k, v in ROUTES.items()}
    yield p95("rental_http_request_duration_seconds", API, "route"), "route", {k: v + "_p95_ms" for k, v in ROUTES.items()}
    read_duration = "rental_order_read_duration_seconds"
    yield rate(read_duration + "_count", API, "storage"), "storage", {
        "current": "current_read_rps", "history": "history_read_rps"}
    yield p95(read_duration, API, "storage"), "storage", {
        "current": "current_read_p95_ms", "history": "history_read_p95_ms"}
    yield rate("rental_order_activation_seconds_count", WORKER), "", {"": "activation_rps"}
    yield p95("rental_order_activation_seconds", WORKER), "", {"": "activation_p95_ms"}
    errors = rate(http, API + ',status!~"2.."')
    yield errors + " or (0 * " + rate(http, API) + ")", "", {"": "http_error_rps"}
    yield f"sum by(status)(max by(kind,status)(rental_tasks{{{MAINT}}}))", "status", {
        "PENDING": "pending_tasks", "DEAD_LETTER": "dead_letters"}
    yield f"max(rental_orders_oldest_closed_age_seconds{{{MAINT}}})", "", {"": "oldest_unarchived_s"}
    yield rate("rental_maintenance_rows_total", MAINT, "step"), "step", {
        "archive_orders_moved": "archived_rows_per_s", "purge_tasks": "purged_tasks_per_s",
        "drop_partitions": "dropped_partitions_per_s"}
    yield "sum by(job)(min_over_time(up[1m]))", "job", {k: k.replace("-", "_") + "_targets_up" for k in JOBS}
    yield "min(min_over_time(up[1m]))", "", {"": "all_observed_targets_up"}
    yield "min(count_over_time(up[1m]))", "", {"": "min_scrapes_per_observed_target"}


def timestamp(value):
    t = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if t.tzinfo is None:
        raise ValueError("provide an RFC3339 time with Z or a UTC offset")
    # Prometheus stores sample timestamps at millisecond precision.
    return round(t.timestamp() * 1000)


def fetch(base, start, end):
    steps = list(range(start + 60000, end + 1, 60000))
    rows, fields = {t: {} for t in steps}, []
    for expr, label, columns in queries():
        fields.extend(columns.values())
        query = urlencode(dict(query=expr, start=steps[0]/1000, end=end/1000, step="60"))
        with urlopen(base.rstrip("/") + "/api/v1/query_range?" + query, timeout=15) as response:
            data = json.load(response)
        if data["status"] != "success":
            raise RuntimeError(data)
        for series in data["data"]["result"]:
            col = columns.get(series["metric"].get(label, ""))
            if not col:
                continue
            for epoch, raw in series["values"]:
                value = float(raw)
                if math.isfinite(value):
                    rows[round(epoch*1000)][col] = round(value, 3)
    return list(rows.values()), fields


def plot(path, rows, title):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import MaxNLocator

    fig, axes = plt.subplots(3, 2, figsize=(12, 10.5), constrained_layout=True)
    minutes = list(range(1, len(rows)+1))
    panels = [
        ("Trips", "successful operations/s", ["offer_rps", "start_rps", "activation_rps", "finish_rps"]),
        ("Order views", "successful reads/s", ["get_rps", "current_read_rps", "history_read_rps"]),
        ("Command latency", "p95, ms", ["offer_p95_ms", "start_p95_ms", "finish_p95_ms", "activation_p95_ms"]),
        ("Read latency", "p95, ms", ["get_p95_ms", "current_read_p95_ms", "history_read_p95_ms"]),
        ("Worker queue", "tasks at minute end", ["pending_tasks", "dead_letters"]),
        ("API errors", "non-2xx responses/s", ["http_error_rps"]),
    ]
    for ax, (heading, units, cols) in zip(axes.flat, panels):
        for i, col in enumerate(cols):
            if any(col in row for row in rows):
                style = ("-", "--", "-.", ":", "-")[i]
                name = col.removesuffix("_rps").removesuffix("_p95_ms").replace("_", " ")
                ax.plot(minutes, [r.get(col, math.nan) for r in rows], style, label=name, linewidth=1.7,
                        marker="o", markersize=2.8)
        ax.set(title=heading, ylabel=units, xlabel="measurement minute")
        ax.set_ylim(bottom=0)
        ax.xaxis.set_major_locator(MaxNLocator(integer=True, nbins=7))
        if len(minutes) == 1:
            ax.set(xlim=(.5, 1.5), xticks=[1])
        ax.grid(alpha=.2)
        handles, _ = ax.get_legend_handles_labels()
        if handles:
            ax.legend(loc="best", fontsize=8, ncols=2, framealpha=.85)
    fig.suptitle(title, fontsize=16)
    fig.savefig(path, dpi=125)
    plt.close(fig)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--prometheus", default="http://localhost:19090")
    p.add_argument("--profile", type=Path, help="JSON produced by run-profile.py or loadtest")
    p.add_argument("--start", help="explicit RFC3339 window start, when no --profile is supplied")
    p.add_argument("--end", help="explicit RFC3339 window end")
    p.add_argument("--out", required=True, type=Path, help="per-minute CSV")
    p.add_argument("--plot", type=Path, help="optional PNG (requires matplotlib)")
    args = p.parse_args()
    if args.profile and not (args.start or args.end):
        profile = json.loads(args.profile.read_text())
        start, end = timestamp(profile["window_from"]), timestamp(profile["window_to"])
    elif not args.profile and args.start and args.end:
        start, end = timestamp(args.start), timestamp(args.end)
    else:
        p.error("provide either --profile or both --start and --end")
    if end <= start or (end-start) % 60000:
        p.error("supply a positive whole number of one-minute intervals")
    if end > datetime.now(timezone.utc).timestamp()*1000:
        p.error("the measurement window has not finished yet")
    rows, fields = fetch(args.prometheus, start, end)
    gaps = [i for i, r in enumerate(rows, 1)
            if r.get("all_observed_targets_up") != 1 or r.get("min_scrapes_per_observed_target", 0) < 10
            or any(not r.get(k.replace("-", "_") + "_targets_up") for k in JOBS)]
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["minute_end_utc", *fields], lineterminator="\n")
        writer.writeheader()
        for i, row in enumerate(rows, 1):
            end_utc = datetime.fromtimestamp(start/1000 + i*60, timezone.utc)
            writer.writerow(dict(minute_end_utc=end_utc.isoformat(timespec="milliseconds"), **row))
    if args.plot:
        args.plot.parent.mkdir(parents=True, exist_ok=True)
        plot(args.plot, rows, f"{args.out.stem} · one-minute server measurements")
    if gaps:
        raise SystemExit(f"Saved evidence with incomplete scrapes in minutes {gaps}")


if __name__ == "__main__":
    main()
