"""Invariants of threshold bars, and the conventions that make them true.

The invariants are the contract: conservation of every trade, no overlap, a
closed bar always reaches its threshold, only the last bar can fall short.
The convention tests are narrower — they pin the decisions an implementation
could silently get wrong, like whether overshoot leaks into the next bar.
"""

from __future__ import annotations

import polars as pl
import pytest

from clockbars import resample, synthetic_ticks

# Sums of thousands of floats are not exact, so "reached the threshold" is
# checked with a relative tolerance rather than bit equality.
TOL = 1e-9

CASES = [
    ("tick", 250, "n_trades"),
    ("volume", 500.0, "volume"),
    ("dollar", 2_500_000.0, "notional"),
]


@pytest.mark.parametrize(("kind", "threshold", "metric"), CASES)
def test_no_trade_is_lost_or_duplicated(trades, kind, threshold, metric):
    bars = resample(trades, kind=kind, threshold=threshold)
    assert bars["n_trades"].sum() == trades.height


@pytest.mark.parametrize(("kind", "threshold", "metric"), CASES)
def test_volume_is_conserved(trades, kind, threshold, metric):
    bars = resample(trades, kind=kind, threshold=threshold)
    assert bars["volume"].sum() == pytest.approx(trades["size"].sum(), rel=1e-12)


@pytest.mark.parametrize(("kind", "threshold", "metric"), CASES)
def test_notional_is_conserved(trades, kind, threshold, metric):
    bars = resample(trades, kind=kind, threshold=threshold)
    expected = (trades["size"] * trades["price"]).sum()
    assert bars["notional"].sum() == pytest.approx(expected, rel=1e-12)


@pytest.mark.parametrize(("kind", "threshold", "metric"), CASES)
def test_every_closed_bar_meets_its_threshold(trades, kind, threshold, metric):
    bars = resample(trades, kind=kind, threshold=threshold)
    assert bars.height > 1
    closed = bars.head(bars.height - 1)
    assert (closed[metric] >= threshold * (1.0 - TOL)).all()


@pytest.mark.parametrize(("kind", "threshold", "metric"), CASES)
def test_bars_overshoot_by_at_most_one_trade(trades, kind, threshold, metric):
    bars = resample(trades, kind=kind, threshold=threshold)
    closed = bars.head(bars.height - 1)
    if metric == "n_trades":
        largest_step = 1.0
    elif metric == "volume":
        largest_step = trades["size"].max()
    else:
        largest_step = (trades["size"] * trades["price"]).max()
    assert (closed[metric] < threshold + largest_step).all()


@pytest.mark.parametrize(("kind", "threshold", "metric"), CASES)
def test_only_the_final_bar_can_be_partial(trades, kind, threshold, metric):
    bars = resample(trades, kind=kind, threshold=threshold)
    assert bars["is_partial"].sum() <= 1
    if bars["is_partial"].sum() == 1:
        assert bars["is_partial"][-1] is True


@pytest.mark.parametrize(("kind", "threshold", "metric"), CASES)
def test_partial_flag_agrees_with_the_metric(trades, kind, threshold, metric):
    bars = resample(trades, kind=kind, threshold=threshold)
    expected = bars[metric] < threshold * (1.0 - TOL)
    assert bars["is_partial"].to_list() == expected.to_list()


@pytest.mark.parametrize(("kind", "threshold", "metric"), CASES)
def test_bars_never_overlap_and_are_chronological(trades, kind, threshold, metric):
    bars = resample(trades, kind=kind, threshold=threshold)
    assert bars["start_ts"].is_sorted()
    starts = bars["start_ts"].to_list()[1:]
    ends = bars["end_ts"].to_list()[:-1]
    assert all(start >= end for start, end in zip(starts, ends, strict=True))


@pytest.mark.parametrize(("kind", "threshold", "metric"), CASES)
def test_bar_ids_are_contiguous_from_zero(trades, kind, threshold, metric):
    bars = resample(trades, kind=kind, threshold=threshold)
    assert bars["bar_id"].to_list() == list(range(bars.height))


@pytest.mark.parametrize(("kind", "threshold", "metric"), CASES)
def test_ohlc_is_internally_consistent(trades, kind, threshold, metric):
    bars = resample(trades, kind=kind, threshold=threshold)
    assert (bars["high"] >= bars["low"]).all()
    assert (bars["high"] >= bars["open"]).all()
    assert (bars["high"] >= bars["close"]).all()
    assert (bars["low"] <= bars["open"]).all()
    assert (bars["low"] <= bars["close"]).all()


@pytest.mark.parametrize(("kind", "threshold", "metric"), CASES)
def test_vwap_sits_inside_the_bar_range(trades, kind, threshold, metric):
    bars = resample(trades, kind=kind, threshold=threshold)
    assert (bars["vwap"] >= bars["low"]).all()
    assert (bars["vwap"] <= bars["high"]).all()


@pytest.mark.parametrize(("kind", "threshold", "metric"), CASES)
def test_output_is_deterministic(trades, kind, threshold, metric):
    first = resample(trades, kind=kind, threshold=threshold)
    second = resample(trades, kind=kind, threshold=threshold)
    assert first.equals(second)


@pytest.mark.parametrize(("kind", "threshold", "metric"), CASES)
def test_schema_is_identical_across_bar_types(trades, kind, threshold, metric):
    bars = resample(trades, kind=kind, threshold=threshold)
    assert bars.columns == [
        "bar_id",
        "start_ts",
        "end_ts",
        "duration",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "notional",
        "vwap",
        "n_trades",
        "is_partial",
    ]


# --- conventions ------------------------------------------------------------


def test_overshoot_is_not_carried_into_the_next_bar(make_trades):
    # 200 + 100 closes bar 0 at 300, overshooting a 250 threshold by 50.
    # The next bar must restart from zero (25 x 10 = 250), not be docked 50
    # and close at 200. This is what separates a per-bar accumulator from a
    # naive global cumulative sum.
    sizes = [200.0, 100.0] + [10.0] * 27
    trades = make_trades([10.0] * len(sizes), sizes)
    bars = resample(trades, kind="volume", threshold=250.0)
    assert bars["volume"].to_list() == [300.0, 250.0, 20.0]
    assert bars["is_partial"].to_list() == [False, False, True]


def test_the_crossing_trade_closes_the_bar(make_trades):
    # 60 + 60 crosses 100, so both trades belong to bar 0: an overshoot to 120.
    trades = make_trades([10.0, 10.0, 10.0], [60.0, 60.0, 60.0])
    bars = resample(trades, kind="volume", threshold=100.0)
    assert bars.height == 2
    assert bars["volume"].to_list() == [120.0, 60.0]
    assert bars["n_trades"].to_list() == [2, 1]
    assert bars["is_partial"].to_list() == [False, True]


def test_exact_multiple_leaves_no_partial_bar(make_trades):
    trades = make_trades([10.0] * 30, [1.0] * 30)
    bars = resample(trades, kind="volume", threshold=10.0)
    assert bars.height == 3
    assert not bars["is_partial"].any()


def test_a_single_trade_can_open_and_close_a_bar(make_trades):
    trades = make_trades([10.0, 10.0], [500.0, 1.0])
    bars = resample(trades, kind="volume", threshold=100.0)
    assert bars["volume"].to_list() == [500.0, 1.0]
    assert bars["n_trades"].to_list() == [1, 1]


def test_a_single_trade_is_still_one_bar(make_trades):
    bars = resample(make_trades([10.0], [1.0]), kind="volume", threshold=100.0)
    assert bars.height == 1
    assert bars["is_partial"].to_list() == [True]


def test_threshold_above_the_total_leaves_one_partial_bar(make_trades):
    trades = make_trades([10.0, 10.0, 10.0], [5.0, 5.0, 5.0])
    bars = resample(trades, kind="volume", threshold=1_000.0)
    assert bars.height == 1
    assert bars["is_partial"].to_list() == [True]
    assert bars["volume"][0] == 15.0


def test_tick_threshold_rounds_up(make_trades):
    # A bar closes when the count *reaches* the threshold, so a fractional
    # tick threshold of 2.5 yields bars of 3 trades.
    trades = make_trades([10.0] * 6, [1.0] * 6)
    bars = resample(trades, kind="tick", threshold=2.5)
    assert bars["n_trades"].to_list() == [3, 3]


def test_tolerance_absorbs_float_accumulation_at_the_threshold(make_trades):
    # Ten 0.1s sum to 0.9999999999999999 in float64, not 1.0. Mathematically
    # the bar filled; a strict `< threshold` comparison would flag a phantom
    # partial bar. The 1e-9 relative tolerance exists for exactly this value,
    # and this test fails if anyone replaces the comparison with a strict one.
    trades = make_trades([10.0] * 10, [0.1] * 10)
    bars = resample(trades, kind="volume", threshold=1.0)
    assert bars.height == 1
    assert bars["n_trades"].to_list() == [10]
    assert bars["is_partial"].to_list() == [False]


def test_tick_threshold_of_one_yields_one_trade_per_bar(trades):
    bars = resample(trades, kind="tick", threshold=1)
    assert bars.height == trades.height
    assert (bars["n_trades"] == 1).all()


def test_integer_prices_and_sizes_are_accepted(make_trades):
    trades = make_trades([10.0, 11.0], [2.0, 3.0]).with_columns(
        pl.col("price").cast(pl.Int64), pl.col("size").cast(pl.Int64)
    )
    bars = resample(trades, kind="volume", threshold=4.0)
    assert bars["volume"].to_list() == [5.0]
    assert bars["vwap"][0] == pytest.approx((10 * 2 + 11 * 3) / 5)


def test_timestamp_precision_is_preserved(trades):
    bars = resample(trades, kind="tick", threshold=500)
    assert bars.schema["start_ts"] == pl.Datetime("ns", "UTC")
    assert bars.schema["end_ts"] == pl.Datetime("ns", "UTC")


def test_metric_helper_rejects_unknown_kind():
    # Defensive branch, unreachable through resample() because dispatch
    # validates `kind` first. Tested directly so the guard stays honest.
    from clockbars.core import _metric_expr

    with pytest.raises(ValueError, match="unknown bar kind"):
        _metric_expr("dollarish")


# --- scale: the invariants must survive a large stream ---------------------


def test_invariants_hold_at_scale():
    stream = synthetic_ticks(n_trades=200_000, n_sessions=10, seed=3)
    bars = resample(stream, kind="dollar", threshold=2_500_000.0)
    assert bars.height > 100
    assert bars["n_trades"].sum() == stream.height
    assert bars["volume"].sum() == pytest.approx(stream["size"].sum(), rel=1e-12)
    closed = bars.head(bars.height - 1)
    assert (closed["notional"] >= 2_500_000.0 * (1.0 - TOL)).all()
    assert bars["is_partial"].sum() <= 1
