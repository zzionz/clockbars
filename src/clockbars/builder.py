"""Incremental construction: the streaming twin of `resample()`.

Bar construction runs in two places — research, over a historical file, and
production, one trade at a time. This module is the production path, and the
parity test holds it to agreement with the batch path:

- **Exact** on everything that decides what a bar *is*: bar boundaries, bar_id,
  timestamps, OHLC, n_trades, and the partial flag. Both paths cumulate the
  metric left to right within a segment and close on the same comparison, so
  they make identical closure decisions on identical floats.
- **Within float-summation order** (<= 1e-12 relative in practice) on per-bar
  volume, notional, and vwap. A vectorised group sum and a sequential
  accumulator sum the same numbers in different orders; pretending otherwise
  would be a stronger claim than floating point can support.

Two limits inherited from Python datetimes, stated rather than hidden:
timestamps pushed into a builder carry microsecond resolution, and trades
finer than that belong in the batch path.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import numpy as np
import polars as pl

from clockbars.core import (
    _ATOL,
    _BAR_COLUMNS,
    _check_threshold,
    _interval_ns,
    _utc_ns,
    validate_trades,
)

_KINDS = ("tick", "volume", "dollar", "time")

_BARS_SCHEMA: dict[str, pl.DataType] = {
    "bar_id": pl.Int64(),
    "start_ts": pl.Datetime("ns", "UTC"),
    "end_ts": pl.Datetime("ns", "UTC"),
    "duration": pl.Duration("ns"),
    "open": pl.Float64(),
    "high": pl.Float64(),
    "low": pl.Float64(),
    "close": pl.Float64(),
    "volume": pl.Float64(),
    "notional": pl.Float64(),
    "vwap": pl.Float64(),
    # pl.len() in the batch path counts groups as UInt32; match it exactly.
    "n_trades": pl.UInt32(),
    "is_partial": pl.Boolean(),
}


@dataclass(frozen=True)
class Bar:
    """One completed bar, shaped like a row of `resample()`'s output."""

    bar_id: int
    start_ts: datetime
    end_ts: datetime
    duration: timedelta
    open: float
    high: float
    low: float
    close: float
    volume: float
    notional: float
    vwap: float
    n_trades: int
    is_partial: bool


class BarBuilder:
    """Build bars one trade at a time, under the same rule as `resample()`.

    Parameters mirror `resample`: threshold kinds take `threshold`, time bars
    take `every`. Pushes must arrive in non-decreasing timestamp order; a live
    feed already guarantees that, and silently reordering would be a policy
    decision this library does not make.

    Call `close_session()` at a rollover to end the current accumulation —
    the streaming counterpart of `session_starts` in the batch path. Call
    `flush()` at the end of a stream (or any time you want the trailing bar)
    to emit what is still open.
    """

    def __init__(
        self,
        kind: str,
        *,
        threshold: float | None = None,
        every: str | None = None,
    ) -> None:
        if kind not in _KINDS:
            raise ValueError(f"unknown kind {kind!r}; expected one of {_KINDS}")
        self._kind = kind

        if kind == "time":
            if every is None:
                raise ValueError("kind='time' requires every=..., e.g. every='1m'")
            if threshold is not None:
                raise ValueError("kind='time' takes every=..., not threshold=")
            self._interval = _interval_ns(every)
            self._threshold = 0.0
        else:
            if threshold is None:
                raise ValueError(f"kind={kind!r} requires threshold=")
            if every is not None:
                raise ValueError(f"kind={kind!r} takes threshold=..., not every=")
            _check_threshold(threshold)
            self._threshold = float(threshold)
            self._interval = 0

        # Segment state: the cumulative metric since the last session boundary.
        self._seg_cum = 0.0
        self._seg_fresh = True
        self._bar_open_cum = 0.0

        # Bar state.
        self._bar_id = 0
        self._n = 0
        self._start_ts: datetime | None = None
        self._end_ts: datetime | None = None
        self._open = 0.0
        self._high = 0.0
        self._low = 0.0
        self._close = 0.0
        self._volume = 0.0
        self._notional = 0.0
        self._total = 0.0
        self._window = 0
        self._last_ts_ns: int | None = None

    def push(self, ts: datetime, price: float, size: float) -> Bar | None:
        """Add one trade; returns the bar it closed, if it closed one."""
        moment = self._check_push(ts, price, size)
        ts_ns = _utc_ns(moment)
        if self._kind == "time":
            return self._push_time(moment, ts_ns, price, size)
        return self._push_threshold(moment, price, size)

    def push_frame(self, trades: pl.DataFrame) -> list[Bar]:
        """Push a whole frame in order. Convenience, not a fast path.

        The frame goes through the same validation as `resample`, so a bad
        column fails here too. Sub-microsecond timestamps are refused rather
        than truncated, because a Python datetime cannot carry them.
        """
        frame = validate_trades(trades)
        stamps = frame["ts"].dt.cast_time_unit("ns").cast(pl.Int64).to_numpy()
        if stamps.size and np.any(stamps % 1_000 != 0):
            raise ValueError(
                "push_frame refuses sub-microsecond timestamps: a Python datetime "
                "cannot carry them. Use the batch path (resample) for such data."
            )
        prices = frame["price"].to_numpy().astype(np.float64, copy=False)
        sizes = frame["size"].to_numpy().astype(np.float64, copy=False)

        bars: list[Bar] = []
        epoch = datetime(1970, 1, 1, tzinfo=UTC)
        for ts_ns, price, size in zip(
            stamps.tolist(), prices.tolist(), sizes.tolist(), strict=True
        ):
            moment = epoch + timedelta(microseconds=ts_ns // 1_000)
            if self._kind == "time":
                bar = self._push_time(moment, int(ts_ns), price, size)
            else:
                self._check_push(moment, price, size)
                bar = self._push_threshold(moment, price, size)
            if bar is not None:
                bars.append(bar)
        return bars

    def close_session(self) -> Bar | None:
        """End the current accumulation, as a session rollover does.

        The trailing bar is emitted, flagged partial if it fell short of the
        threshold, and the next trade starts a fresh segment at zero.
        """
        if self._kind == "time":
            raise ValueError(
                "time bars are cut by the clock; close_session applies to threshold bars"
            )
        bar = None
        if self._n > 0:
            bar = self._emit(partial=self._total < self._threshold * (1.0 - _ATOL))
        self._seg_cum = 0.0
        self._seg_fresh = True
        return bar

    def flush(self) -> Bar | None:
        """Emit the bar still open at the end of a stream, if there is one."""
        if self._n == 0:
            return None
        if self._kind == "time":
            return self._emit(partial=False)
        return self._emit(partial=self._total < self._threshold * (1.0 - _ATOL))

    # -- internals ----------------------------------------------------------

    def _check_push(self, ts: datetime, price: float, size: float) -> datetime:
        if not isinstance(ts, datetime):
            raise TypeError(f"ts must be a datetime, got {type(ts).__name__}")
        if ts.tzinfo is None:
            raise ValueError("ts is timezone-naive; declare the zone before pushing")
        if not math.isfinite(price) or not math.isfinite(size):
            raise ValueError("price and size must be finite (no NaN or inf)")
        if price <= 0 or size <= 0:
            raise ValueError("price and size must be strictly positive")

        moment = ts.astimezone(UTC)
        ts_ns = _utc_ns(moment)
        if self._last_ts_ns is not None and ts_ns < self._last_ts_ns:
            raise ValueError("pushes must be non-decreasing in ts")
        self._last_ts_ns = ts_ns
        return moment

    def _open_bar(self, moment: datetime, price: float, base_cum: float) -> None:
        """Initialise an empty bar; the caller's update block adds the trade.

        `high`/`low` start at infinities so the first max/min land correctly
        for any positive price, and `_n` starts at zero so the increment in
        the update block counts the opening trade exactly once.
        """
        self._n = 0
        self._start_ts = moment
        self._open = price
        self._high = -math.inf
        self._low = math.inf
        self._volume = 0.0
        self._notional = 0.0
        self._bar_open_cum = base_cum

    def _push_threshold(self, moment: datetime, price: float, size: float) -> Bar | None:
        if self._kind == "tick":
            metric = 1.0
        elif self._kind == "volume":
            metric = size
        else:
            metric = size * price

        if self._seg_fresh:
            self._seg_cum = metric
            self._seg_fresh = False
        else:
            self._seg_cum += metric

        if self._n == 0:
            self._open_bar(moment, price, self._seg_cum - metric)

        self._high = max(self._high, price)
        self._low = min(self._low, price)
        self._close = price
        self._end_ts = moment
        self._volume += size
        self._notional += size * price
        self._n += 1
        self._total += metric

        if self._seg_cum >= self._bar_open_cum + self._threshold:
            return self._emit(partial=self._total < self._threshold * (1.0 - _ATOL))
        return None

    def _push_time(self, moment: datetime, ts_ns: int, price: float, size: float) -> Bar | None:
        window = (ts_ns // self._interval) * self._interval
        emitted: Bar | None = None
        if self._n > 0 and window != self._window:
            emitted = self._emit(partial=False)
        if self._n == 0:
            self._open_bar(moment, price, 0.0)
            self._window = window
        self._high = max(self._high, price)
        self._low = min(self._low, price)
        self._close = price
        self._end_ts = moment
        self._volume += size
        self._notional += size * price
        self._n += 1
        return emitted

    def _emit(self, *, partial: bool) -> Bar:
        assert self._start_ts is not None and self._end_ts is not None
        bar = Bar(
            bar_id=self._bar_id,
            start_ts=self._start_ts,
            end_ts=self._end_ts,
            duration=self._end_ts - self._start_ts,
            open=self._open,
            high=self._high,
            low=self._low,
            close=self._close,
            volume=self._volume,
            notional=self._notional,
            vwap=self._notional / self._volume,
            n_trades=self._n,
            is_partial=partial,
        )
        self._bar_id += 1
        self._n = 0
        self._total = 0.0
        self._start_ts = None
        self._end_ts = None
        return bar


def bars_to_frame(bars: Sequence[Bar]) -> pl.DataFrame:
    """Collect bars into a frame with exactly `resample()`'s schema and dtypes."""
    if not bars:
        return pl.DataFrame(schema=_BARS_SCHEMA)
    frame = pl.DataFrame(
        {
            "bar_id": pl.Series([b.bar_id for b in bars], dtype=pl.Int64),
            "start_ts": pl.Series([b.start_ts for b in bars]).cast(pl.Datetime("ns", "UTC")),
            "end_ts": pl.Series([b.end_ts for b in bars]).cast(pl.Datetime("ns", "UTC")),
            "duration": pl.Series([b.duration for b in bars]).cast(pl.Duration("ns")),
            "open": pl.Series([b.open for b in bars], dtype=pl.Float64),
            "high": pl.Series([b.high for b in bars], dtype=pl.Float64),
            "low": pl.Series([b.low for b in bars], dtype=pl.Float64),
            "close": pl.Series([b.close for b in bars], dtype=pl.Float64),
            "volume": pl.Series([b.volume for b in bars], dtype=pl.Float64),
            "notional": pl.Series([b.notional for b in bars], dtype=pl.Float64),
            "vwap": pl.Series([b.vwap for b in bars], dtype=pl.Float64),
            "n_trades": pl.Series([b.n_trades for b in bars], dtype=pl.UInt32),
            "is_partial": pl.Series([b.is_partial for b in bars], dtype=pl.Boolean),
        }
    )
    return frame.select(_BAR_COLUMNS)
