"""Flow-sampled kinds: signs, imbalance bars, runs bars, and CUSUM bars."""

from __future__ import annotations

from datetime import UTC, datetime

import numpy as np
import polars as pl
import pytest

from clockbars import resample, synthetic_ticks
from clockbars.adaptive import tick_rule_signs
from clockbars.builder import _chunk_signs, _sign_under_tick_rule


def frame_of(prices, sizes, start_minute=30):
    n = len(prices)
    raw = [
        int(datetime(2020, 1, 2, 14, start_minute, i % 60, tzinfo=UTC).timestamp()) * 10**9
        for i in range(n)
    ]
    ts = pl.Series(raw, dtype=pl.Int64).cast(pl.Datetime("ns")).dt.replace_time_zone("UTC")
    return pl.DataFrame(
        {
            "ts": ts,
            "price": pl.Series(prices, dtype=pl.Float64),
            "size": pl.Series(sizes, dtype=pl.Float64),
        }
    )


# --- the tick rule ---------------------------------------------------------


def test_signs_follow_the_tick_rule():
    prices = np.array([10.0, 11.0, 11.0, 10.0, 12.0, 12.0, 12.0, 9.0])
    # rise +1, fall -1, flat carries the previous sign, first trade +1.
    assert tick_rule_signs(prices).tolist() == [1.0, 1.0, 1.0, -1.0, 1.0, 1.0, 1.0, -1.0]


def test_first_trade_is_plus_one_and_flat_streams_inherit_it():
    assert tick_rule_signs(np.array([5.0])).tolist() == [1.0]
    assert tick_rule_signs(np.array([5.0, 5.0, 5.0])).tolist() == [1.0, 1.0, 1.0]


def test_a_fall_before_a_flat_run_carries_the_minus():
    prices = np.array([10.0, 9.0, 9.0, 9.0])
    assert tick_rule_signs(prices).tolist() == [1.0, -1.0, -1.0, -1.0]


def test_chunk_signs_continue_across_boundaries_including_flats():
    prices = np.array([10.0, 11.0, 11.0, 10.0, 10.0, 10.0, 12.0])
    full = tick_rule_signs(prices)
    collected = []
    prev_price, prev_sign = None, 1.0
    for lo, hi in [(0, 2), (2, 4), (4, 7)]:
        part = _chunk_signs(prices[lo:hi], prev_price, prev_sign)
        collected.append(part)
        prev_price = float(prices[hi - 1])
        prev_sign = float(part[-1])
    assert np.concatenate(collected).tolist() == full.tolist()

    # The boundary trade is flat and must inherit a carried -1, not a +1.
    carried = _chunk_signs(np.array([9.0]), 10.0, -1.0)
    assert carried.tolist() == [-1.0]


def test_scalar_sign_rule_matches_the_vectorised_one():
    rng = np.random.default_rng(7)
    prices = 5000 + np.round(rng.normal(0, 2, 2000).cumsum(), 2)
    full = tick_rule_signs(prices)
    scalar, prev_price, prev_sign = [], None, 1.0
    for price in prices:
        sign = _sign_under_tick_rule(float(price), prev_price, prev_sign)
        scalar.append(sign)
        prev_price, prev_sign = float(price), sign
    assert np.array_equal(np.array(scalar), full)


# --- imbalance bars --------------------------------------------------------


def test_imbalance_bars_close_at_the_target_with_a_clean_hand_case():
    # Strictly rising prices sign every trade +1, so the signed metric is
    # just the size. With sizes of 1, a target of E[T]*|e| = 4*1 = 4 closes
    # a bar every 4 trades, and both EWMA estimates are already at their
    # fixed points, so every bar is 4 trades.
    trades = frame_of([10.0 + i for i in range(10)], [1.0] * 10)
    bars = resample(
        trades,
        kind="tick_imbalance",
        expected_trades=4,
        expected_imbalance=1,
        window=50,
    )
    assert bars["n_trades"].to_list() == [4, 4, 2]
    assert bars["is_partial"].to_list() == [False, False, True]


def test_imbalance_ewma_updates_after_each_closed_bar():
    # Window 2 gives alpha = 2/3. A target of 1 closes every trade as its
    # own bar, and both estimates sit exactly at their fixed points
    # (length 1, signed metric 1), so every bar is one trade. This pins
    trades = frame_of([10.0, 11.0, 10.5, 11.5, 11.0, 12.0], [1.0] * 6)
    bars = resample(
        trades,
        kind="tick_imbalance",
        expected_trades=1,
        expected_imbalance=1,
        window=2,
    )
    assert bars["n_trades"].to_list() == [1, 1, 1, 1, 1, 1]
    assert not bars["is_partial"].any()


def test_imbalance_bars_conserve_everything():
    trades = synthetic_ticks(n_trades=5_000, n_sessions=1, seed=3)
    bars = resample(
        trades,
        kind="dollar_imbalance",
        expected_trades=200,
        expected_imbalance=5_000.0,
        window=50,
    )
    assert bars["n_trades"].sum() == trades.height
    assert bars["volume"].sum() == pytest.approx(trades["size"].sum(), rel=1e-12)
    assert bars["bar_id"].to_list() == list(range(bars.height))


def test_imbalance_output_is_deterministic():
    trades = synthetic_ticks(n_trades=5_000, n_sessions=1, seed=3)
    first = resample(
        trades, kind="volume_imbalance",
        expected_trades=200, expected_imbalance=0.5, window=50,
    )
    second = resample(
        trades, kind="volume_imbalance",
        expected_trades=200, expected_imbalance=0.5, window=50,
    )
    # Structure is deterministic; sums live in the float-summation-order
    # tier of the parity contract, so they are compared with tolerance.
    for column in [
        "bar_id", "start_ts", "end_ts", "duration", "open", "high",
        "low", "close", "n_trades", "is_partial",
    ]:
        assert first[column].equals(second[column])
    for column in ["volume", "notional", "vwap"]:
        assert first[column].to_list() == pytest.approx(second[column].to_list(), rel=1e-12)


def test_imbalance_bars_never_straddle_a_session_cut():
    trades = synthetic_ticks(n_trades=5_000, n_sessions=2, seed=3)
    cut = [datetime(2019, 1, 3, 14, 30, tzinfo=UTC)]
    bars = resample(
        trades, kind="dollar_imbalance",
        expected_trades=200, expected_imbalance=5_000.0, window=50,
        session_starts=cut,
    )
    straddling = bars.filter(
        (pl.col("start_ts") < cut[0]) & (pl.col("end_ts") >= cut[0])
    )
    assert straddling.height == 0
    assert bars["is_partial"].sum() <= 2


# --- runs bars -------------------------------------------------------------


def test_runs_bars_close_on_streaks_with_a_hand_case():
    # Rising prices: every trade is a buy, so the streak is the bar length.
    # target = E[T] * max(P, 1-P); with P seeded at 0.5 and E[T] = 4 the
    # first bar closes at streak 2. alpha = 2/3 then drags P toward 1 and
    # E[T] toward observed lengths, and the hand-computed lengths are
    # [2, 3, 3].
    trades = frame_of([10.0 + i for i in range(8)], [1.0] * 8)
    bars = resample(trades, kind="runs", expected_trades=4, window=2)
    assert bars["n_trades"].to_list() == [2, 3, 3]


def test_runs_bars_never_close_on_alternating_flow():
    # Perfect alternation never builds a streak, so the whole stream is one
    # open bar. The stream must fall first, because a stream's first trade
    # is always +1 and a rise on trade two would build a streak of 2.
    prices = [10.0, 9.0, 10.0, 9.0, 10.0, 9.0, 10.0, 9.0]
    trades = frame_of(prices, [1.0] * 8)
    bars = resample(trades, kind="runs", expected_trades=4, window=50)
    assert bars.height == 1
    assert bars["is_partial"].to_list() == [True]


def test_runs_bars_conserve_everything_and_cut_at_sessions():
    trades = synthetic_ticks(n_trades=5_000, n_sessions=2, seed=3)
    cut = [datetime(2019, 1, 3, 14, 30, tzinfo=UTC)]
    bars = resample(
        trades, kind="runs", expected_trades=60, window=50, session_starts=cut
    )
    assert bars["n_trades"].sum() == trades.height
    straddling = bars.filter(
        (pl.col("start_ts") < cut[0]) & (pl.col("end_ts") >= cut[0])
    )
    assert straddling.height == 0


# --- CUSUM bars ------------------------------------------------------------


def test_cusum_closes_on_an_upside_drift():
    # Prices 100, 101, 102 with h = 1.5: the cumulative change reaches 2,
    # and 2 - 0 >= 1.5, so the third trade closes the bar.
    trades = frame_of([100.0, 101.0, 102.0, 102.0], [1.0] * 4)
    bars = resample(trades, kind="cusum", h=1.5)
    assert bars["n_trades"].to_list() == [3, 1]
    assert bars["is_partial"].to_list() == [False, True]


def test_cusum_closes_on_a_downside_drift():
    trades = frame_of([100.0, 99.0, 98.0, 98.0], [1.0] * 4)
    bars = resample(trades, kind="cusum", h=1.5)
    assert bars["n_trades"].to_list() == [3, 1]


def test_cusum_chop_closes_in_short_bars():
    # 100 -> 101 -> 100 -> 101 with h = 1: each one-point move trips the
    # filter. The second bar closes on its own opening trade, whose price
    # change already reaches the threshold: a bar-opening trade can close
    # its own bar, by design.
    trades = frame_of([100.0, 101.0, 100.0, 101.0], [1.0] * 4)
    bars = resample(trades, kind="cusum", h=1.0)
    assert bars["n_trades"].to_list() == [2, 1, 1]
    assert bars["is_partial"].to_list() == [False, False, False]


def test_cusum_conerves_everything_and_cuts_at_sessions():
    trades = synthetic_ticks(n_trades=5_000, n_sessions=2, seed=3)
    cut = [datetime(2019, 1, 3, 14, 30, tzinfo=UTC)]
    bars = resample(trades, kind="cusum", h=3.0, session_starts=cut)
    assert bars["n_trades"].sum() == trades.height
    straddling = bars.filter(
        (pl.col("start_ts") < cut[0]) & (pl.col("end_ts") >= cut[0])
    )
    assert straddling.height == 0


def test_cusum_output_is_deterministic():
    trades = synthetic_ticks(n_trades=5_000, n_sessions=1, seed=3)
    first = resample(trades, kind="cusum", h=3.0)
    second = resample(trades, kind="cusum", h=3.0)
    assert first.equals(second)


# --- validation ------------------------------------------------------------


def _five() -> pl.DataFrame:
    return frame_of([10.0, 11.0, 10.5, 11.5, 11.0], [1.0] * 5)


def test_imbalance_kinds_require_both_seeds():
    with pytest.raises(ValueError, match="requires expected_trades"):
        resample(_five(), kind="tick_imbalance", expected_trades=4)
    with pytest.raises(ValueError, match="requires expected_trades"):
        resample(_five(), kind="dollar_imbalance", expected_imbalance=1.0)


def test_a_zero_imbalance_seed_is_rejected():
    with pytest.raises(ValueError, match="nonzero"):
        resample(
            _five(), kind="tick_imbalance",
            expected_trades=4, expected_imbalance=0.0,
        )


def test_runs_requires_expected_trades():
    with pytest.raises(ValueError, match="requires expected_trades"):
        resample(_five(), kind="runs")


def test_cusum_requires_h():
    with pytest.raises(ValueError, match="requires h"):
        resample(_five(), kind="cusum")


@pytest.mark.parametrize(
    "bad", [0.0, -1.0, float("nan"), float("inf")]
)
def test_bad_expected_trades_are_rejected(bad):
    with pytest.raises(ValueError):
        resample(
            _five(), kind="runs", expected_trades=bad,
        )


def test_bad_h_is_rejected():
    with pytest.raises(ValueError, match="positive"):
        resample(_five(), kind="cusum", h=0.0)


def test_bad_window_is_rejected():
    with pytest.raises(ValueError, match="at least 1"):
        resample(
            _five(), kind="runs", expected_trades=4, window=0,
        )
    with pytest.raises(TypeError):
        resample(_five(), kind="runs", expected_trades=4, window=2.5)  # type: ignore[arg-type]


def test_adaptive_kinds_reject_threshold_and_every():
    with pytest.raises(ValueError, match="not threshold/every"):
        resample(
            _five(), kind="tick_imbalance",
            expected_trades=4, expected_imbalance=1.0, threshold=10.0,
        )
    with pytest.raises(ValueError, match="not threshold/every"):
        resample(_five(), kind="cusum", h=1.0, every="1m")
    with pytest.raises(ValueError, match="adaptive parameters"):
        resample(_five(), kind="tick", threshold=10.0, expected_trades=4)


def test_unknown_kind_lists_all_nine():
    with pytest.raises(ValueError, match="cusum"):
        resample(_five(), kind="zeitgeist", threshold=1)  # type: ignore[arg-type]


def test_schema_is_shared_with_the_other_kinds():
    columns = [
        "bar_id", "start_ts", "end_ts", "duration", "open", "high", "low",
        "close", "volume", "notional", "vwap", "n_trades", "is_partial",
    ]
    bars = resample(
        _five(), kind="cusum", h=1.0,
    )
    assert bars.columns == columns
    assert bars.schema["start_ts"] == pl.Datetime("ns", "UTC")
