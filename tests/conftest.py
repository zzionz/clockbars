from __future__ import annotations

from collections.abc import Callable

import numpy as np
import polars as pl
import pytest

from clockbars import synthetic_ticks

_BASE_NS = np.datetime64("2020-01-02T14:30:00", "ns").astype("int64")


def _frame(prices, sizes, step_ms: int = 1) -> pl.DataFrame:
    n = len(prices)
    if n == 0:
        ts = pl.Series("ts", [], dtype=pl.Datetime("ns", "UTC"))
    else:
        raw = _BASE_NS + np.arange(n, dtype="int64") * step_ms * 1_000_000
        ts = (
            pl.Series("ts", raw)
            .cast(pl.Datetime("ns"))
            .dt.replace_time_zone("UTC")
        )
    return pl.DataFrame(
        {
            "ts": ts,
            "price": pl.Series(prices, dtype=pl.Float64),
            "size": pl.Series(sizes, dtype=pl.Float64),
        }
    )


@pytest.fixture
def make_trades() -> Callable[..., pl.DataFrame]:
    """Build a tiny hand-written trades frame with millisecond spacing."""
    return _frame


@pytest.fixture
def make_trades_at() -> Callable[..., pl.DataFrame]:
    """Build a trades frame at exact, explicit UTC timestamps."""

    def _make(moments, prices, sizes) -> pl.DataFrame:
        raw = [
            int(m.timestamp()) * 1_000_000_000 + m.microsecond * 1_000
            for m in moments
        ]
        ts = (
            pl.Series(raw, dtype=pl.Int64)
            .cast(pl.Datetime("ns"))
            .dt.replace_time_zone("UTC")
        )
        return pl.DataFrame(
            {
                "ts": ts,
                "price": pl.Series(prices, dtype=pl.Float64),
                "size": pl.Series(sizes, dtype=pl.Float64),
            }
        )

    return _make


@pytest.fixture(scope="session")
def trades() -> pl.DataFrame:
    """20k deterministic synthetic trades, shared across read-only tests."""
    return synthetic_ticks(n_trades=20_000, n_sessions=3, seed=11)
