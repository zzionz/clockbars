"""The generator must be reproducible, because the published figures depend on it."""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from clockbars import synthetic_ticks


def test_schema_and_dtypes():
    trades = synthetic_ticks(n_trades=1_000, n_sessions=2, seed=3)
    assert trades.columns == ["ts", "price", "size"]
    assert trades.schema["ts"] == pl.Datetime("ns", "UTC")
    assert trades.schema["price"] == pl.Float64


def test_output_is_sorted_and_positive():
    trades = synthetic_ticks(n_trades=5_000, n_sessions=2, seed=4)
    assert trades["ts"].is_sorted()
    assert (trades["price"] > 0).all()
    assert (trades["size"] > 0).all()


def test_same_seed_is_bit_identical():
    a = synthetic_ticks(n_trades=2_000, seed=99)
    b = synthetic_ticks(n_trades=2_000, seed=99)
    assert a.equals(b)


def test_different_seeds_differ():
    a = synthetic_ticks(n_trades=2_000, seed=1)
    b = synthetic_ticks(n_trades=2_000, seed=2)
    assert not a.equals(b)


def test_prices_snap_to_the_tick_grid():
    trades = synthetic_ticks(n_trades=2_000, seed=5, tick_size=0.25)
    steps = trades["price"] / 0.25
    assert np.allclose(steps, np.round(steps))


def test_every_session_is_a_unique_business_day():
    # Regression: the first version of the generator hardcoded ten date
    # offsets, so n_sessions > 10 silently interleaved two sessions into
    # the same day. It also landed on weekends while calling them sessions.
    trades = synthetic_ticks(n_trades=30_000, n_sessions=15, seed=2)
    dates = np.asarray(
        trades["ts"].dt.date().unique().sort().to_list(), dtype="datetime64[D]"
    )
    assert dates.size == 15
    assert np.is_busday(dates).all()


def test_second_resolution_produces_duplicate_timestamps():
    # Timestamps are truncated to whole seconds, so duplicates are expected.
    # This exercises the library's duplicate-timestamp path on purpose.
    trades = synthetic_ticks(n_trades=20_000, n_sessions=2, seed=8)
    assert trades.height > trades["ts"].n_unique()


def test_volume_is_u_shaped_across_the_session():
    trades = synthetic_ticks(n_trades=60_000, n_sessions=1, seed=6)
    # dt.hour() is Int8 in Polars; multiplying it by 60 overflows silently.
    minute = (
        trades["ts"].dt.hour().cast(pl.Int32) * 60
        + trades["ts"].dt.minute().cast(pl.Int32)
    )
    session = minute - minute.min()
    total = session.max() + 1
    third = total // 3

    def share(lo: int, hi: int) -> float:
        mask = (session >= lo) & (session < hi)
        return trades.filter(mask)["size"].sum() / trades["size"].sum()

    midday = share(third, 2 * third)
    assert share(0, third) > midday
    assert share(2 * third, total) > midday


def test_n_trades_is_respected():
    trades = synthetic_ticks(n_trades=10_000, n_sessions=5, seed=7)
    assert 9_000 <= trades.height <= 11_000


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"n_trades": 0}, "n_trades"),
        ({"n_trades": -5}, "n_trades"),
        ({"n_sessions": 0}, "n_sessions"),
    ],
)
def test_bad_arguments_are_rejected(kwargs, match):
    with pytest.raises(ValueError, match=match):
        synthetic_ticks(**kwargs)
