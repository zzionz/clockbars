#!/usr/bin/env python3
"""Regenerate every figure in docs/ and print the tables quoted in the README.

    uv run python examples/make_figures.py

Everything here runs on the seeded generator in clockbars.synthetic, so every
figure and every number below is reproducible without a data licence. The
data is synthetic and labelled as such; the point is to show the mechanical
effect of sampling on the market's activity instead of on the clock.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import polars as pl

from clockbars import resample, synthetic_ticks

DOCS = Path(__file__).resolve().parents[1] / "docs"

# One palette for every figure in this repo: near-black ground, ink text,
# crimson for the activity clock, silver for the wall clock.
GROUND = "#0d0b0c"
INK = "#efe6e4"
MUTED = "#8a7f7d"
CRIMSON = "#e63946"
CRIMSON_DEEP = "#891826"
SILVER = "#b8b3b1"
GRID = "#2a2224"


def style_axes(ax: plt.Axes) -> None:
    ax.set_facecolor(GROUND)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=MUTED, labelsize=9)
    for label in ax.get_xticklabels() + ax.get_yticklabels():
        label.set_color(MUTED)
    ax.grid(alpha=0.35, linewidth=0.6, color=GRID)
    ax.set_axisbelow(True)


def coefficient_of_variation(x: pl.Series) -> float:
    arr = x.to_numpy()
    return float(arr.std(ddof=1) / arr.mean())


def excess_kurtosis(x: pl.Series) -> float:
    arr = x.to_numpy()
    centred = arr - arr.mean()
    m2 = (centred**2).mean()
    return float((centred**4).mean() / m2**2 - 3.0)


def busiest_date(frame: pl.DataFrame, column: str = "ts") -> object:
    """The date carrying the most rows, for cutting one session out."""
    return (
        frame.group_by(frame[column].dt.date())
        .len()
        .sort("len", descending=True)[column][0]
    )


def one_session(bars: pl.DataFrame) -> pl.DataFrame:
    """Restrict to the session date carrying the most bars."""
    return bars.filter(pl.col("start_ts").dt.date() == busiest_date(bars, "start_ts"))


def minutes_of(frame: pl.DataFrame, column: str) -> pl.Series:
    return (
        frame[column].dt.hour().cast(pl.Int32) * 60
        + frame[column].dt.minute().cast(pl.Int32)
    )


def minutes_since_midnight(bars: pl.DataFrame) -> pl.Series:
    return minutes_of(bars, "start_ts")


def make_hero(trades: pl.DataFrame) -> dict[str, dict[str, float]]:
    time_bars = resample(trades, kind="time", every="1m")
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

    fig = plt.figure(figsize=(11.5, 7.6), dpi=150, facecolor=GROUND)
    grid = fig.add_gridspec(2, 2, height_ratios=[1.0, 1.2], hspace=0.46, wspace=0.24)
    ax_time = fig.add_subplot(grid[0, 0])
    ax_dollar = fig.add_subplot(grid[0, 1])
    ax_session = fig.add_subplot(grid[1, :])

    for ax, bars, title, color in (
        (ax_time, time_bars, "Time bars (1m): volume per bar", SILVER),
        (ax_dollar, dollar_bars, "Dollar bars: volume per bar", CRIMSON),
    ):
        ax.hist(bars["volume"].to_numpy(), bins=60, color=color, alpha=0.92)
        ax.set_title(title, fontsize=11, color=INK)
        ax.set_xlabel("volume per bar", fontsize=9, color=MUTED)
        ax.set_ylabel("count", fontsize=9, color=MUTED)
        style_axes(ax)

    ax_session.plot(
        minutes_since_midnight(time_day).to_numpy(),
        time_day["volume"].to_numpy(),
        color=SILVER,
        linewidth=1.4,
        label="time bars (1m)",
    )
    ax_session.plot(
        minutes_since_midnight(dollar_day).to_numpy(),
        dollar_day["volume"].to_numpy(),
        color=CRIMSON,
        linewidth=1.4,
        label="dollar bars",
    )
    ax_session.set_title(
        "One session, both clocks: volume per bar across the day",
        fontsize=11,
        color=INK,
    )
    ax_session.set_xlabel("minutes since midnight (UTC)", fontsize=9, color=MUTED)
    ax_session.set_ylabel("volume per bar", fontsize=9, color=MUTED)
    legend = ax_session.legend(frameon=False, fontsize=9)
    for text in legend.get_texts():
        text.set_color(INK)
    style_axes(ax_session)

    fig.suptitle("Same trade stream, two sampling clocks", fontsize=13, color=INK, y=0.97)
    fig.text(
        0.5,
        0.935,
        "synthetic, seed 7, 400,000 trades, 20 sessions",
        ha="center",
        fontsize=9,
        color=MUTED,
    )
    fig.savefig(DOCS / "hero.png", facecolor=GROUND)
    print(f"wrote {DOCS / 'hero.png'}")
    return rows


def make_cusum(trades: pl.DataFrame) -> None:
    """The flow-sampled panel: CUSUM bars cutting on price movement."""
    session = trades.filter(pl.col("ts").dt.date() == busiest_date(trades))
    minute = resample(session, kind="time", every="1m")
    cusum = resample(session, kind="cusum", h=3.0)

    print(f"\ncusum (h=3.0) bars in the busiest session: {cusum.height}")
    print(f"time (1m) bars in the same session: {minute.height}")

    fig, axes = plt.subplots(2, 1, figsize=(11.5, 7.0), dpi=150, sharex=True)
    fig.subplots_adjust(hspace=0.12)
    (ax_price, ax_volume) = axes

    minutes = minutes_of(session, "ts").to_numpy()
    ax_price.plot(minutes, session["price"].to_numpy(), color=MUTED, linewidth=1.0)
    ax_price.set_ylabel("price", fontsize=9, color=MUTED)
    style_axes(ax_price)

    for boundary in cusum["start_ts"]:
        minute_mark = boundary.hour * 60 + boundary.minute
        for ax in axes:
            ax.axvline(minute_mark, color=CRIMSON_DEEP, linewidth=0.7, alpha=0.65)

    ax_price.set_title(
        "CUSUM bars cut where price moves, not where the clock ticks",
        fontsize=11,
        color=INK,
        pad=12,
    )

    ax_volume.plot(
        minutes_since_midnight(minute).to_numpy(),
        minute["volume"].to_numpy(),
        color=SILVER,
        linewidth=1.2,
        label="time bars (1m)",
    )
    ax_volume.plot(
        minutes_since_midnight(cusum).to_numpy(),
        cusum["volume"].to_numpy(),
        color=CRIMSON,
        linewidth=1.2,
        label="cusum bars",
    )
    ax_volume.set_xlabel("minutes since midnight (UTC)", fontsize=9, color=MUTED)
    ax_volume.set_ylabel("volume per bar", fontsize=9, color=MUTED)
    legend = ax_volume.legend(frameon=False, fontsize=9)
    for text in legend.get_texts():
        text.set_color(INK)
    style_axes(ax_volume)

    fig.text(
        0.5,
        0.955,
        "vertical lines: cusum bar boundaries; synthetic, seed 7, busiest session",
        ha="center",
        fontsize=9,
        color=MUTED,
    )
    fig.savefig(DOCS / "cusum.png", facecolor=GROUND)
    print(f"wrote {DOCS / 'cusum.png'}")


def make_overshoot() -> None:
    """The worked example as a figure: restart at zero vs global cumsum."""
    fig, ax = plt.subplots(figsize=(10.5, 3.2), dpi=150, facecolor=GROUND)
    ax.set_facecolor(GROUND)
    ax.set_xlim(-0.5, 3.5)
    ax.set_ylim(0, 4)
    ax.axis("off")

    header_y = 3.2
    row1_y = 2.0
    row2_y = 0.8

    headers = ["", "bar 0", "bar 1", "bar 2"]
    bars_good = ["clockbars\n(restart at zero)", "300\nclosed", "250\nclosed", "20\npartial"]
    bars_bad = ["global\ncumulative sum", "300\nclosed", "200\nBELOW\nTHRESHOLD", "70\npartial"]

    colors_bad = [MUTED, MUTED, CRIMSON, MUTED]

    for _j, (hx, ht) in enumerate(zip([0.0, 1.0, 2.0, 3.0], headers, strict=True)):
        ax.text(hx + 0.5, header_y, ht, ha="center", va="center",
                fontsize=10, fontweight="bold", color=INK)

    for j in range(4):
        text = bars_good[j]
        color = INK if j > 0 else SILVER
        weight = "normal"
        box = dict(boxstyle="round,pad=0.4", facecolor="#1a0e10",
                   edgecolor=CRIMSON, linewidth=1.2)
        if j == 3:
            color = MUTED
        ax.text(j * 1.0 + 0.5, row1_y, text, ha="center", va="center",
                fontsize=9, color=color, fontweight=weight, bbox=box)

    for j in range(4):
        text = bars_bad[j]
        color = colors_bad[j]
        weight = "normal"
        if j == 2:
            weight = "bold"
        box = dict(boxstyle="round,pad=0.4", facecolor="#241418",
                   edgecolor=CRIMSON if j == 2 else GRID, linewidth=1.2)
        ax.text(j * 1.0 + 0.5, row2_y, text, ha="center", va="center",
                fontsize=9, color=color, fontweight=weight, bbox=box)

    ax.annotate("", xy=(0.5, header_y - 0.35), xytext=(0.5, row1_y + 0.45),
                arrowprops=dict(arrowstyle="-", color=GRID, linewidth=0.8))
    ax.annotate("", xy=(0.5, row1_y - 0.35), xytext=(0.5, row2_y + 0.45),
                arrowprops=dict(arrowstyle="-", color=GRID, linewidth=0.8))

    ax.set_title(
        "threshold 250, trades: 200, 100, then 27 x 10",
        fontsize=10, color=MUTED, pad=8,
    )

    fig.text(
        0.5, 0.02,
        "the global cumulative sum charges bar 0's overshoot to bar 1, which closes below threshold",
        ha="center", fontsize=8, color=MUTED, style="italic",
    )

    fig.savefig(DOCS / "overshoot.png", facecolor=GROUND, bbox_inches="tight")
    print(f"wrote {DOCS / 'overshoot.png'}")


def main() -> None:
    trades = synthetic_ticks(n_trades=400_000, n_sessions=20, seed=7)
    DOCS.mkdir(parents=True, exist_ok=True)
    make_hero(trades)
    make_cusum(trades)
    make_overshoot()


if __name__ == "__main__":
    main()
