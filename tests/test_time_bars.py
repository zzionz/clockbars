"""Time bars: the baseline that threshold bars are measured against."""


from __future__ import annotations

from datetime import UTC, datetime

import polars as pl
import pytest

from clockbars import resample

BAR_COLUMNS = [
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


def test_one_minute_bars_have_the_shared_schema(trades):
    bars = resample(trades, kind="time", every="1m")
    assert bars.columns == BAR_COLUMNS
    assert bars.schema["start_ts"] == pl.Datetime("ns", "UTC")


def test_time_bars_never_flag_partial(trades):
    bars = resample(trades, kind="time", every="1m")
    assert not bars["is_partial"].any()


def test_time_bars_conserve_volume_and_trades(trades):
    bars = resample(trades, kind="time", every="1m")
    assert bars["volume"].sum() == pytest.approx(trades["size"].sum(), rel=1e-12)
    assert bars["n_trades"].sum() == trades.height
    assert bars["bar_id"].to_list() == list(range(bars.height))


def test_empty_windows_are_not_emitted(trades):
    bars = resample(trades, kind="time", every="1m")
    assert (bars["n_trades"] > 0).all()


def test_finer_windows_produce_more_bars(trades):
    one = resample(trades, kind="time", every="1m")
    five = resample(trades, kind="time", every="5m")
    assert one.height > five.height


def test_time_bar_ohlc_matches_the_underlying_trades(trades):
    bars = resample(trades, kind="time", every="1m")
    assert bars["open"][0] == trades["price"][0]
    assert bars["close"][-1] == trades["price"][-1]


def test_a_trade_lands_in_the_window_it_starts(make_trades):
    # Windows are half-open [start, start + every). A trade at exactly 14:31:00
    # belongs to the 14:31 window, not the tail of the 14:30 window.
    trades = make_trades([10.0, 11.0], [1.0, 1.0], step_ms=60_000)
    bars = resample(trades, kind="time", every="1m")
    assert bars.height == 2
    assert bars["n_trades"].to_list() == [1, 1]
    assert bars["open"].to_list() == [10.0, 11.0]


def test_windows_align_to_the_epoch_not_to_the_first_trade(make_trades_at):
    # The first trade lands at 14:30:30. If windows were anchored to the data,
    # every boundary would sit at :30 and a trade at 14:31:00 would share the
    # first trade's window. Anchored to the clock, 14:31:00 starts a new one.
    # (An earlier version of this test placed its first trade exactly on a
    # boundary by accident, so it passed under both rules and pinned nothing.)
    def at(minute: int, second: int) -> datetime:
        return datetime(2020, 1, 2, 14, minute, second, tzinfo=UTC)

    same = make_trades_at([at(30, 30), at(30, 59)], [10.0, 11.0], [1.0, 1.0])
    assert resample(same, kind="time", every="1m").height == 1

    split = make_trades_at([at(30, 30), at(31, 0)], [10.0, 11.0], [1.0, 1.0])
    assert resample(split, kind="time", every="1m").height == 2


def test_compound_intervals_are_summed(make_trades_at):
    # "1h30m" is a 90-minute window with epoch-aligned boundaries at 13:30
    # and 15:00: 14:00 and 14:59 share a window, while a trade exactly on the
    # 15:00 boundary starts a new one.
    def at(hour: int, minute: int) -> datetime:
        return datetime(2020, 1, 2, hour, minute, tzinfo=UTC)

    same = make_trades_at([at(14, 0), at(14, 59)], [10.0, 11.0], [1.0, 1.0])
    assert resample(same, kind="time", every="1h30m").height == 1

    split = make_trades_at([at(14, 0), at(15, 0)], [10.0, 11.0], [1.0, 1.0])
    assert resample(split, kind="time", every="1h30m").height == 2


def test_dollar_and_time_bars_do_not_share_bar_counts(trades):
    period = resample(trades, kind="time", every="1m")
    dollar = resample(trades, kind="dollar", threshold=2_500_000.0)
    assert period.height != dollar.height
