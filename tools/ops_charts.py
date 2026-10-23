"""Render README charts from a workspace's Gold/Silver tables (read over OneLake).

    uv run --no-project --with deltalake --with pyarrow --with matplotlib \\
        python tools/ops_charts.py --workspace-id <prod-id> --out docs/charts

Palette: the dataviz reference instance (categorical slots 1-2 validated: CVD ΔE 24.7, normal ΔE 33.6), light surface.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
from read_onelake import open_table  # noqa: E402

SURFACE, TEXT, TEXT_2, GRID, WET = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df", "#ecebe7"
WET_DAY_MM = 10.0
SERIES = {"member": "#2a78d6", "casual": "#eb6834"}  # categorical slots 1, 2 (fixed order)
BAR = "#2a78d6"


def _style(ax, title: str, subtitle: str) -> None:
    ax.set_facecolor(SURFACE)
    ax.figure.set_facecolor(SURFACE)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.tick_params(colors=TEXT_2, labelsize=9, length=0)
    ax.figure.text(0.012, 0.965, title, fontsize=13, fontweight="bold", color=TEXT, ha="left", va="top")
    ax.figure.text(0.012, 0.905, subtitle, fontsize=9.5, color=TEXT_2, ha="left", va="top")


def daily_trips(ws: str, out: Path) -> None:
    rows = open_table(ws, "lh_gold", "agg_daily_kpis").to_pyarrow_table(columns=["date_key", "member_type", "trips"])
    series: dict[str, dict[datetime, int]] = {"member": {}, "casual": {}}
    for r in rows.to_pylist():
        day = datetime.strptime(str(r["date_key"]), "%Y%m%d")
        if r["member_type"] in series and day >= datetime(2026, 8, 1):  # complete months only (no boundary spill-over)
            series[r["member_type"]][day] = series[r["member_type"]].get(day, 0) + r["trips"]

    weather = open_table(ws, "lh_gold", "dim_weather").to_pyarrow_table(columns=["date_key", "precipitation_sum"])
    wet = [datetime.strptime(str(r["date_key"]), "%Y%m%d") for r in weather.to_pylist()
           if (r["precipitation_sum"] or 0) >= WET_DAY_MM and r["date_key"] >= 20260801]  # fmt: skip

    fig, ax = plt.subplots(figsize=(10, 4.2), dpi=160)
    fig.subplots_adjust(left=0.07, right=0.9, top=0.76, bottom=0.12)
    for i, day in enumerate(wet):  # the weather join in Gold explains the dips
        ax.axvspan(day - timedelta(hours=12), day + timedelta(hours=12), color=WET, linewidth=0,
                   label=f"rain ≥ {WET_DAY_MM:.0f} mm" if i == 0 else None)  # fmt: skip
    for name, points in series.items():
        days = sorted(points)
        values = [points[d] for d in days]
        ax.plot(days, values, color=SERIES[name], linewidth=2, solid_capstyle="round", label=name)
        ax.annotate(f"{name}  {values[-1]:,}", (days[-1], values[-1]), xytext=(6, 0), textcoords="offset points",
                    va="center", fontsize=9, color=TEXT)  # fmt: skip
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.set_ylim(bottom=0)
    ax.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(5000))
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v / 1000:.0f}k" if v else "0"))
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=mdates.MO))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.legend(loc="lower left", bbox_to_anchor=(-0.01, 1.0), ncol=3, frameon=False, fontsize=9, labelcolor=TEXT_2)
    total = sum(sum(p.values()) for p in series.values())
    _style(ax, "Divvy trips per day by rider type",
           f"lh_gold.agg_daily_kpis joined with dim_weather · Aug–Sep 2026 · {total:,} valid trips")  # fmt: skip
    fig.savefig(out / "daily-trips.png")
    plt.close(fig)


def quarantine_reasons(ws: str, out: Path) -> None:
    reasons = open_table(ws, "lh_silver", "trips_quarantine").to_pyarrow_table(columns=["dq_reasons"])
    counts = Counter(r for row in reasons.column("dq_reasons").to_pylist() for r in row)
    labels, values = zip(*sorted(counts.items(), key=lambda kv: kv[1]), strict=True)

    fig, ax = plt.subplots(figsize=(10, 0.55 * len(labels) + 1.6), dpi=160)
    fig.subplots_adjust(left=0.27, right=0.93, top=0.74, bottom=0.08)
    bars = ax.barh(labels, values, color=BAR, height=0.6)
    for bar, value in zip(bars, values, strict=True):
        ax.annotate(f"{value:,}", (bar.get_width(), bar.get_y() + bar.get_height() / 2), xytext=(4, 0),
                    textcoords="offset points", va="center", fontsize=9, color=TEXT)  # fmt: skip
    ax.xaxis.set_visible(False)
    ax.tick_params(axis="y", labelcolor=TEXT, labelsize=9.5)
    total = len(reasons)
    _style(ax, "Why rows were quarantined",
           f"lh_silver.trips_quarantine · {total:,} rows kept with reason codes, never dropped")  # fmt: skip
    fig.savefig(out / "quarantine-reasons.png")
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--workspace-id", required=True)
    parser.add_argument("--out", type=Path, default=Path("docs/charts"))
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    daily_trips(args.workspace_id, args.out)
    quarantine_reasons(args.workspace_id, args.out)
    print(f"charts written to {args.out}")


if __name__ == "__main__":
    main()
