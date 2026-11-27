#!/usr/bin/env python3
"""Regenerate docs/hero.png and print the numbers quoted in the README.

    uv run python examples/make_hero.py

Everything here runs on the seeded generator in clockbars.synthetic, so the
figure and the table below are reproducible without a data licence. The data
is synthetic and labelled as such; the point is to show the mechanical effect
of sampling by activity instead of by clock.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import polars as pl

from clockbars import resample, synthetic_ticks

DOCS = Path(__file__).resolve().parents[1] / "docs"

# One palette for every figure in this repo: burnt orange for the clock
# sampler, deep teal for the activity sampler, ink for everything else.
INK = "#1f2937"
TIME_COLOR = "#c2410c"
DOLLAR_COLOR = "#0f766e"
GRID_ALPHA = 0.3


def coefficient_of_variation(x: pl.Series) -> float:
    arr = x.to_numpy()
    return float(arr.std(ddof=1) / arr.mean())


def excess_kurtosis(x: pl.Series) -> float:
    arr = x.to_numpy()
    centred = arr - arr.mean()
    m2 = (centred**2).mean()
    return float((centred**4).mean() / m2**2 - 3.0)


def one_session(bars: pl.DataFrame) -> pl.DataFrame:
    """Restrict to the session date carrying the most bars."""
    date = (
        bars.group_by(bars["start_ts"].dt.date())
        .len()
        .sort("len", descending=True)["start_ts"][0]
    )
    return bars.filter(pl.col("start_ts").dt.date() == date)


def strip_chartjunk(ax: plt.Axes) -> None:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(INK)
    ax.spines["bottom"].set_color(INK)
    ax.tick_params(colors=INK, labelsize=9)
    ax.grid(alpha=GRID_ALPHA, linewidth=0.6)
    ax.set_axisbelow(True)


def main() -> None:
    trades = synthetic_ticks(n_trades=400_000, n_sessions=20, seed=7)

    time_bars = resample(trades, kind="time", every="1m")
    # Match the dollar threshold to the median 1-minute notional so that both
    # schemes draw roughly the same amount of information out of the stream.
    median_notional = float(time_bars["notional"].median())
    dollar_bars = resample(trades, kind="dollar", threshold=median_notional)

    time_ret = time_bars["close"].pct_change().drop_nulls()
    dollar_ret = dollar_bars["close"].pct_change().drop_nulls()

    rows = {
        "time (1m)": {
            "bars": time_bars.height,
            "volume_cv": coefficient_of_variation(time_bars["volume"]),
            "return_kurtosis": excess_kurtosis(time_ret),
        },
        "dollar": {
            "bars": dollar_bars.height,
            "volume_cv": coefficient_of_variation(dollar_bars["volume"]),
            "return_kurtosis": excess_kurtosis(dollar_ret),
        },
    }

    print(f"{'bar type':<12}{'bars':>8}{'volume CV':>12}{'excess kurt':>14}")
    for name, row in rows.items():
        print(
            f"{name:<12}{row['bars']:>8}{row['volume_cv']:>12.4f}"
            f"{row['return_kurtosis']:>14.3f}"
        )
    print(f"\ndollar threshold = median 1m notional = {median_notional:,.0f}")

    time_day = one_session(time_bars)
    dollar_day = one_session(dollar_bars)

    fig = plt.figure(figsize=(11.5, 7.2), dpi=150)
    grid = fig.add_gridspec(2, 2, height_ratios=[1.0, 1.15], hspace=0.42, wspace=0.22)
    ax_time = fig.add_subplot(grid[0, 0])
    ax_dollar = fig.add_subplot(grid[0, 1])
    ax_session = fig.add_subplot(grid[1, :])

    ax_time.hist(time_bars["volume"].to_numpy(), bins=60, color=TIME_COLOR, alpha=0.9)
    ax_time.set_title("Time bars (1m): volume per bar", fontsize=11, color=INK)
    ax_time.set_xlabel("volume per bar", fontsize=9, color=INK)
    ax_time.set_ylabel("count", fontsize=9, color=INK)
    strip_chartjunk(ax_time)

    ax_dollar.hist(dollar_bars["volume"].to_numpy(), bins=60, color=DOLLAR_COLOR, alpha=0.9)
    ax_dollar.set_title("Dollar bars: volume per bar", fontsize=11, color=INK)
    ax_dollar.set_xlabel("volume per bar", fontsize=9, color=INK)
    ax_dollar.set_ylabel("count", fontsize=9, color=INK)
    strip_chartjunk(ax_dollar)

    def minutes_since_midnight(bars: pl.DataFrame) -> pl.Series:
        return (
            bars["start_ts"].dt.hour().cast(pl.Int32) * 60
            + bars["start_ts"].dt.minute().cast(pl.Int32)
        )

    ax_session.plot(
        minutes_since_midnight(time_day).to_numpy(),
        time_day["volume"].to_numpy(),
        color=TIME_COLOR,
        linewidth=1.3,
        label="time bars (1m)",
    )
    ax_session.plot(
        minutes_since_midnight(dollar_day).to_numpy(),
        dollar_day["volume"].to_numpy(),
        color=DOLLAR_COLOR,
        linewidth=1.3,
        label="dollar bars",
    )
    ax_session.set_title(
        "One session, both clocks: volume per bar across the day",
        fontsize=11,
        color=INK,
    )
    ax_session.set_xlabel("minutes since midnight (UTC)", fontsize=9, color=INK)
    ax_session.set_ylabel("volume per bar", fontsize=9, color=INK)
    ax_session.legend(frameon=False, fontsize=9, labelcolor=INK)
    strip_chartjunk(ax_session)

    fig.suptitle(
        "Same trade stream, two sampling clocks",
        fontsize=13,
        color=INK,
        y=0.97,
    )
    fig.text(
        0.5,
        0.935,
        "synthetic, seed 7, 400,000 trades, 20 sessions",
        ha="center",
        fontsize=9,
        color="#6b7280",
    )

    DOCS.mkdir(parents=True, exist_ok=True)
    out = DOCS / "hero.png"
    fig.savefig(out)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
