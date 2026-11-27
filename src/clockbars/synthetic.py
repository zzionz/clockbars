"""Deterministic synthetic trade data.

Real trade data is licensed and large, so the examples and tests here run on a
generated stream with a realistic *shape* rather than licensed content:

- one session per business day, starting from ``start_date``;
- a U-shaped arrival pattern across each session;
- trade sizes that cluster at the open and the close;
- per-trade volatility that scales with trade size, so volume and volatility
  move together the way they do in real markets.

Timestamps are truncated to whole seconds, so a generated stream contains
duplicate timestamps by construction — which is also a deliberate exercise of
the library's duplicate-timestamp path. Nothing here predicts or resembles any
actual market; it exists so every figure and number in the documentation can be
reproduced from a seed.
"""

from __future__ import annotations

import numpy as np
import polars as pl

__all__ = ["synthetic_ticks"]

_SESSION_OPEN_UTC_MIN = 14 * 60 + 30  # 09:30 America/New_York, standard time
_SESSION_MINUTES = 390


def _session_dates(start_date: str, n_sessions: int) -> np.ndarray:
    """First `n_sessions` business days on or after `start_date`.

    Dates are unique and never weekend days, whatever `n_sessions` is. An
    earlier version hardcoded ten offsets, which silently interleaved two
    sessions into the same day (with duplicated timestamps) as soon as anyone
    asked for more than ten sessions.
    """
    day = np.datetime64(start_date, "D")
    dates: list[np.datetime64] = []
    while len(dates) < n_sessions:
        if np.is_busday(day):
            dates.append(day)
        day = day + np.timedelta64(1, "D")
    return np.asarray(dates, dtype="datetime64[D]")


def _intraday_fraction(rng: np.random.Generator, n: int) -> np.ndarray:
    """U-shaped position within the session, as a fraction in [0, 1)."""
    weight_open = rng.random(n)
    shape = rng.random(n)
    opening = rng.beta(0.8, 3.2, n)
    closing = 1.0 - rng.beta(0.8, 3.2, n)
    uniform = rng.random(n)

    return np.where(
        weight_open < 0.40,
        opening,
        np.where(shape < 0.60, closing, uniform),
    )


def synthetic_ticks(
    n_trades: int = 200_000,
    *,
    seed: int = 7,
    start_price: float = 5000.0,
    tick_size: float = 0.25,
    n_sessions: int = 10,
    base_sigma: float = 2.0e-5,
    start_date: str = "2019-01-02",
) -> pl.DataFrame:
    """Generate reproducible synthetic trades.

    Parameters
    ----------
    n_trades
        Approximate total number of trades across all sessions.
    seed
        Seed for ``numpy.random.default_rng``; output is bit-for-bit stable.
    start_price
        Price of the first trade.
    tick_size
        All prices are rounded to this grid.
    n_sessions
        Number of trading sessions to spread the trades across (weekdays).
    base_sigma
        Per-trade log-return scale before size scaling.
    start_date
        First session date, ``YYYY-MM-DD``.

    Returns
    -------
    polars.DataFrame
        ``ts`` (``Datetime("ns", "UTC")``), ``price``, ``size``.
    """
    if n_trades <= 0:
        raise ValueError("n_trades must be positive")
    if n_sessions <= 0:
        raise ValueError("n_sessions must be positive")

    rng = np.random.default_rng(seed)
    per_session = int(np.ceil(n_trades / n_sessions))
    dates = _session_dates(start_date, n_sessions)

    ts_parts: list[np.ndarray] = []
    size_parts: list[np.ndarray] = []
    for session in range(n_sessions):
        frac = _intraday_fraction(rng, per_session)
        minutes = _SESSION_OPEN_UTC_MIN + frac * _SESSION_MINUTES
        seconds = (minutes * 60.0).astype("int64")
        ts_parts.append(
            dates[session].astype("datetime64[s]") + seconds.astype("timedelta64[s]")
        )

        # Size clustering: fatter trades at the open and the close.
        edge_weight = 1.0 + 5.0 * np.abs(frac - 0.5)
        size_parts.append(np.exp(rng.normal(0.0, 0.7, per_session)) * edge_weight)

    ts = np.concatenate(ts_parts)
    size = np.concatenate(size_parts)

    order = np.argsort(ts, kind="stable")
    ts, size = ts[order], size[order]

    # Volatility scales with trade size, giving the volume/volatility link that
    # makes dollar bars meaningfully different from time bars.
    scale = size / size.mean()
    sigma = base_sigma * np.sqrt(scale)
    log_returns = rng.normal(0.0, 1.0, ts.size) * sigma
    price = start_price * np.exp(np.cumsum(log_returns))
    price = np.round(price / tick_size) * tick_size
    price = np.maximum(price, tick_size)

    return pl.DataFrame(
        {
            "ts": pl.Series(ts.astype("datetime64[ns]"))
            .cast(pl.Datetime("ns"))
            .dt.replace_time_zone("UTC"),
            "price": pl.Series(price, dtype=pl.Float64),
            "size": pl.Series(np.round(size, 4), dtype=pl.Float64),
        }
    ).sort("ts")
