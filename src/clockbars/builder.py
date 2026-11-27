"""Incremental construction: the streaming twin of `resample()`.

Bar construction runs in two places, research over a historical file and
production one trade at a time. This module is the production path, and the
parity tests hold it to agreement with the batch path:

- **Exact** on everything that decides what a bar *is*: bar boundaries, bar_id,
  timestamps, OHLC, n_trades, and the partial flag. Both paths cumulate the
  metric left to right within a bar and close on the same comparison, so
  they make identical closure decisions on identical floats. Chunked pushes
  use a prepend-carry: the bar's partial sum leads the chunk, and cumsum
  over the pair reproduces sequential addition exactly.
- **Within float-summation order** (at most 1e-12 relative in practice) on
  per-bar volume, notional, and vwap. A vectorised group sum and a
  sequential accumulator sum the same numbers in different orders, and
  pretending otherwise would be a claim floating point cannot support.

Two limits inherited from Python datetimes, stated rather than hidden:
timestamps pushed into a builder carry microsecond resolution, and trades
finer than that belong in the batch path.

`push_chunk` is the out-of-core entry point: scan a Parquet file in
slices, push each slice, concatenate the returned frames. A bar open at a
slice boundary continues across it. `close_session()` between slices is the
streaming counterpart of `session_starts` in the batch path.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import numpy as np
import polars as pl

from clockbars.adaptive import (
    _check_imbalance_seed,
    _check_positive_float,
    _check_window,
    cusum_bounds,
    imbalance_bounds,
    runs_bounds,
    tick_rule_signs,
)
from clockbars.core import (
    _ATOL,
    _BAR_COLUMNS,
    _check_threshold,
    _interval_ns,
    _metric_values,
    _utc_ns,
    validate_trades,
)

_KINDS = (
    "tick",
    "volume",
    "dollar",
    "time",
    "tick_imbalance",
    "volume_imbalance",
    "dollar_imbalance",
    "runs",
    "cusum",
)
_THRESHOLD_KINDS = ("tick", "volume", "dollar")
_IMBALANCE_KINDS = ("tick_imbalance", "volume_imbalance", "dollar_imbalance")

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


def _sign_under_tick_rule(
    price: float, prev_price: float | None, prev_sign: float
) -> float:
    """The scalar tick rule: rise +1, fall -1, flat carries, first trade +1."""
    if prev_price is None:
        return 1.0
    if price > prev_price:
        return 1.0
    if price < prev_price:
        return -1.0
    return prev_sign


def _chunk_signs(
    prices: np.ndarray, prev_price: float | None, prev_sign: float
) -> np.ndarray:
    """Tick-rule signs for a chunk, continuing the stream's carry state."""
    if prev_price is None:
        return tick_rule_signs(prices)
    step = np.sign(np.diff(np.concatenate(([prev_price], prices))))
    filled = np.where(step != 0, step, np.nan)
    # Seed the fill with the carried sign so a flat open inherits it.
    seeded = np.concatenate(([prev_sign], filled))
    known = np.where(np.isnan(seeded), 0, np.arange(seeded.size))
    np.maximum.accumulate(known, out=known)
    out = seeded[known]
    return np.where(np.isnan(out), 1.0, out)[1:]


class BarBuilder:
    """Build bars one trade, one frame, or one chunk at a time.

    Parameters mirror `resample`: threshold kinds take `threshold`, time bars
    take `every`, imbalance kinds take `expected_trades` with
    `expected_imbalance`, runs takes `expected_trades`, and cusum takes `h`,
    each with an optional EWMA `window` where it applies. Pushes must arrive
    in non-decreasing timestamp order; a live feed already guarantees that,
    and silently reordering would be a policy decision this library does
    not make.

    Call `close_session()` at a rollover to end the current accumulation,
    the streaming counterpart of `session_starts` in the batch path. Call
    `flush()` at the end of a stream to emit the bar still open, if any.
    """

    def __init__(
        self,
        kind: str,
        *,
        threshold: float | None = None,
        every: str | None = None,
        expected_trades: float | None = None,
        expected_imbalance: float | None = None,
        window: int | None = None,
        h: float | None = None,
    ) -> None:
        if kind not in _KINDS:
            raise ValueError(f"unknown kind {kind!r}; expected one of {_KINDS}")
        self._kind = kind

        self._threshold = 0.0
        self._interval = 0
        self._h = 0.0
        self._alpha = 0.0
        self._ewma_window = 100

        if kind == "time":
            if every is None:
                raise ValueError("kind='time' requires every=..., e.g. every='1m'")
            if threshold is not None:
                raise ValueError("kind='time' takes every=..., not threshold=")
            self._interval = _interval_ns(every)
        elif kind in _THRESHOLD_KINDS:
            if threshold is None:
                raise ValueError(f"kind={kind!r} requires threshold=")
            if every is not None:
                raise ValueError(f"kind={kind!r} takes threshold=..., not every=")
            if (
                expected_trades is not None
                or expected_imbalance is not None
                or h is not None
            ):
                raise ValueError(
                    f"kind={kind!r} takes threshold=..., not the adaptive parameters"
                )
            _check_threshold(threshold)
            self._threshold = float(threshold)
        else:
            if every is not None or threshold is not None:
                raise ValueError(
                    f"kind={kind!r} takes its own parameters, not threshold/every"
                )
            if kind == "cusum":
                if h is None:
                    raise ValueError(
                        "kind='cusum' requires h=..., the price move that closes a bar"
                    )
                if (
                    expected_trades is not None
                    or expected_imbalance is not None
                    or window is not None
                ):
                    raise ValueError(
                        "kind='cusum' takes h=..., not the adaptive parameters"
                    )
                self._h = _check_positive_float(h, "h")
                self._est_trades = 0.0
                self._est_imb = 0.0
                self._buy_frac = 0.5
            else:
                if kind == "runs":
                    if expected_trades is None:
                        raise ValueError("kind='runs' requires expected_trades=")
                    if expected_imbalance is not None or h is not None:
                        raise ValueError("kind='runs' takes no expected_imbalance or h")
                    self._est_trades = _check_positive_float(
                        expected_trades, "expected_trades"
                    )
                    self._est_imb = 0.0
                    self._buy_frac = 0.5
                else:
                    if expected_trades is None or expected_imbalance is None:
                        raise ValueError(
                            f"kind={kind!r} requires expected_trades= and "
                            "expected_imbalance="
                        )
                    if h is not None:
                        raise ValueError(f"kind={kind!r} takes its seeds, not h")
                    self._est_trades = _check_positive_float(
                        expected_trades, "expected_trades"
                    )
                    self._est_imb = _check_imbalance_seed(expected_imbalance)
                    self._buy_frac = 0.5
                self._ewma_window = _check_window(window if window is not None else 100)
                self._alpha = 2.0 / (self._ewma_window + 1.0)

        # Segment state for threshold kinds: cumulative metric since the
        # last session boundary.
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

        # Flow state: signs, signed metric, streaks, and the CUSUM walk.
        self._prev_price: float | None = None
        self._prev_sign = 1.0
        self._last_sign = 1.0
        self._signed = 0.0
        self._streak = 0
        self._buys = 0
        self._d_cum = 0.0
        self._d_min = 0.0
        self._d_max = 0.0

    # -- scalar path --------------------------------------------------------

    def push(self, ts: datetime, price: float, size: float) -> Bar | None:
        """Add one trade; returns the bar it closed, if it closed one."""
        moment = self._check_push(ts, price, size)
        ts_ns = _utc_ns(moment)
        if self._kind == "time":
            return self._push_time(moment, ts_ns, price, size)
        if self._kind in _THRESHOLD_KINDS:
            return self._push_threshold(moment, price, size)
        return self._push_flow(moment, price, size)

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
            elif self._kind in _THRESHOLD_KINDS:
                self._check_push(moment, price, size)
                bar = self._push_threshold(moment, price, size)
            else:
                self._check_push(moment, price, size)
                bar = self._push_flow(moment, price, size)
            if bar is not None:
                bars.append(bar)
        return bars

    # -- chunked path -------------------------------------------------------

    def push_chunk(self, trades: pl.DataFrame) -> pl.DataFrame:
        """Push a chunk of trades and return the bars that completed in it.

        Vectorised per chunk, honouring the builder's carried state: a bar
        open at a chunk boundary continues across it, and per-bar running
        sums are built with a prepend-carry that reproduces the
        one-trade-at-a-time order of addition exactly. This is the
        out-of-core entry point: scan a Parquet file in slices, push each
        slice, concatenate the returned frames, and call `close_session()`
        between sessions.
        """
        if trades.height == 0:
            return bars_to_frame([])
        frame = validate_trades(trades)
        stamps = frame["ts"].dt.cast_time_unit("ns").cast(pl.Int64).to_numpy()
        if self._last_ts_ns is not None and int(stamps[0]) < self._last_ts_ns:
            raise ValueError("pushes must be non-decreasing in ts across chunks")

        emitted = self._scan_chunk(frame)
        self._last_ts_ns = int(stamps[-1])
        self._prev_price = float(frame["price"][-1])
        return emitted

    def close_session(self) -> Bar | None:
        """End the current accumulation, as a session rollover does.

        The trailing bar is emitted, flagged partial, and the next trade
        starts a fresh segment at zero. Adaptive estimates are properties
        of the stream and are left alone.
        """
        if self._kind == "time":
            raise ValueError(
                "time bars are cut by the clock; close_session applies to "
                "threshold and flow-sampled bars"
            )
        bar = None
        if self._n > 0:
            bar = self._emit(partial=True)
        self._reset_segment()
        return bar

    def flush(self) -> Bar | None:
        """Emit the bar still open at the end of a stream, if there is one."""
        if self._n == 0:
            return None
        if self._kind == "time":
            return self._emit(partial=False)
        if self._kind in _THRESHOLD_KINDS:
            return self._emit(partial=self._total < self._threshold * (1.0 - _ATOL))
        return self._emit(partial=True)

    # -- internals: validation and shared state -----------------------------

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

    def _accumulate(self, moment: datetime, price: float, size: float) -> None:
        self._high = max(self._high, price)
        self._low = min(self._low, price)
        self._close = price
        self._end_ts = moment
        self._volume += size
        self._notional += size * price
        self._n += 1

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
        self._reset_bar()
        return bar

    def _reset_bar(self) -> None:
        self._n = 0
        self._total = 0.0
        self._signed = 0.0
        self._streak = 0
        self._buys = 0
        self._d_cum = 0.0
        self._d_min = 0.0
        self._d_max = 0.0
        self._start_ts = None
        self._end_ts = None

    def _reset_segment(self) -> None:
        self._reset_bar()
        self._seg_cum = 0.0
        self._seg_fresh = True

    # -- internals: per-kind scalar pushes ----------------------------------

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

        self._accumulate(moment, price, size)
        self._total += metric

        if self._seg_cum >= self._bar_open_cum + self._threshold:
            return self._emit(partial=self._total < self._threshold * (1.0 - _ATOL))
        return None

    def _push_flow(self, moment: datetime, price: float, size: float) -> Bar | None:
        previous = self._prev_price
        sign = _sign_under_tick_rule(price, self._prev_price, self._prev_sign)
        self._prev_price = price
        self._prev_sign = sign
        if sign > 0:
            self._buys += 1
        if self._n == 0:
            self._open_bar(moment, price, 0.0)

        if self._kind in _IMBALANCE_KINDS:
            if self._kind == "tick_imbalance":
                metric = sign
            elif self._kind == "volume_imbalance":
                metric = sign * size
            else:
                metric = sign * size * price
            self._signed += metric
            self._accumulate(moment, price, size)
            if abs(self._signed) >= self._est_trades * abs(self._est_imb):
                length = self._n
                self._est_trades += self._alpha * (length - self._est_trades)
                self._est_imb += self._alpha * (self._signed / length - self._est_imb)
                return self._emit(partial=False)
            return None

        if self._kind == "runs":
            if self._n == 0:
                self._streak = 1
            elif sign == self._last_sign:
                self._streak += 1
            else:
                self._streak = 1
            self._last_sign = sign
            self._accumulate(moment, price, size)
            target = self._est_trades * max(self._buy_frac, 1.0 - self._buy_frac)
            if self._streak >= max(math.ceil(target), 1):
                length = self._n
                self._est_trades += self._alpha * (length - self._est_trades)
                self._buy_frac += self._alpha * (self._buys / length - self._buy_frac)
                return self._emit(partial=False)
            return None

        # kind == "cusum": every trade's price change feeds the filter, and
        # the trade that trips the threshold closes its own bar.
        step = 0.0 if previous is None else price - previous
        self._d_cum += step
        self._d_min = min(self._d_min, self._d_cum)
        self._d_max = max(self._d_max, self._d_cum)
        self._accumulate(moment, price, size)
        if self._d_cum - self._d_min >= self._h or self._d_max - self._d_cum >= self._h:
            return self._emit(partial=False)
        return None

    def _push_time(self, moment: datetime, ts_ns: int, price: float, size: float) -> Bar | None:
        window = (ts_ns // self._interval) * self._interval
        emitted: Bar | None = None
        if self._n > 0 and window != self._window:
            emitted = self._emit(partial=False)
        if self._n == 0:
            self._open_bar(moment, price, 0.0)
            self._window = window
        self._accumulate(moment, price, size)
        return emitted

    # -- internals: chunked pushes -------------------------------------------

    def _scan_chunk(self, frame: pl.DataFrame) -> pl.DataFrame:
        if self._kind == "time":
            return self._time_chunk(frame)

        prices = frame["price"].to_numpy().astype(np.float64, copy=False)
        signs = _chunk_signs(prices, self._prev_price, self._prev_sign)
        prev_price_before_chunk = self._prev_price
        self._prev_sign = float(signs[-1])
        carried_open = self._n > 0

        if self._kind in _THRESHOLD_KINDS:
            ranges, trailing = self._threshold_ranges(frame)
        else:
            segment = np.array([0], dtype=np.int64)
            if self._kind in _IMBALANCE_KINDS:
                # The bounds functions apply signs internally, so the
                # metric arrives unsigned, exactly as the batch path
                # supplies it.
                base = {
                    "tick_imbalance": "tick",
                    "volume_imbalance": "volume",
                    "dollar_imbalance": "dollar",
                }[self._kind]
                metric = _metric_values(frame, base)
                starts, partials, out = imbalance_bounds(
                    metric,
                    signs,
                    segment,
                    expected_trades=self._est_trades,
                    expected_imbalance=self._est_imb,
                    window=self._ewma_window,
                    carry=self._imbalance_carry(carried_open),
                )
                self._est_trades = out["est_trades"]
                self._est_imb = out["est_imb"]
            elif self._kind == "runs":
                starts, partials, out = runs_bounds(
                    signs,
                    segment,
                    expected_trades=self._est_trades,
                    window=self._ewma_window,
                    carry=self._runs_carry(carried_open),
                )
                self._est_trades = out["est_trades"]
                self._buy_frac = out["buy_frac"]
            else:
                starts, partials, out = cusum_bounds(
                    prices,
                    segment,
                    h=self._h,
                    carry=self._cusum_carry(carried_open),
                    prev_price=prev_price_before_chunk,
                )
            ranges, trailing = self._ranges_from_bounds(starts, partials, frame.height)

        emitted = self._aggregate_ranges(frame, ranges, trailing, carried_open)

        # Trailing flow state is set AFTER aggregation, since the closed-bar
        # loop inside resets per-bar state.
        if trailing is not None:
            if self._kind in _IMBALANCE_KINDS:
                self._signed = float(out["partial"])
            elif self._kind == "runs":
                self._streak = int(out["streak"])
                self._last_sign = float(signs[-1])
                tail_buys = int(np.count_nonzero(signs[trailing:] > 0))
                self._buys = (self._buys if carried_open else 0) + tail_buys
            elif self._kind == "cusum":
                self._d_cum = float(out["d_cum"])
                self._d_min = float(out["d_min"])
                self._d_max = float(out["d_max"])
            else:
                # Threshold kinds: the trailing bar's metric total, for
                # flush()'s tolerance flag. A vectorised tail sum feeding a
                # flag compared with a 1e-9 relative tolerance is the one
                # place order of addition can differ, and the tolerance
                # exists to absorb exactly that.
                tail = self._tail_metric(frame, trailing)
                self._total = (self._total if carried_open else 0.0) + tail
        return emitted

    def _tail_metric(self, frame: pl.DataFrame, trailing: int) -> float:
        base = {
            "tick": "tick",
            "volume": "volume",
        }.get(self._kind, "dollar")
        metric = _metric_values(frame, base)
        return float(np.sum(metric[trailing:]))

    def _imbalance_carry(self, carried_open: bool) -> dict[str, float] | None:
        if not carried_open:
            return None
        return {
            "est_trades": self._est_trades,
            "est_imb": self._est_imb,
            "partial": self._signed,
            "seen": float(self._n),
            "open": 1.0,
        }

    def _runs_carry(self, carried_open: bool) -> dict[str, float] | None:
        if not carried_open:
            return None
        return {
            "est_trades": self._est_trades,
            "buy_frac": self._buy_frac,
            "streak": float(self._streak),
            "last_sign": self._last_sign,
            "seen": float(self._n),
            "buys": float(self._buys),
            "open": 1.0,
        }

    def _cusum_carry(self, carried_open: bool) -> dict[str, float] | None:
        if not carried_open:
            return None
        return {
            "d_cum": self._d_cum,
            "d_min": self._d_min,
            "d_max": self._d_max,
            "open": 1.0,
        }

    def _threshold_ranges(
        self, frame: pl.DataFrame
    ) -> tuple[list[tuple[int, int]], int | None]:
        base = {
            "tick": "tick",
            "volume": "volume",
        }.get(self._kind, "dollar")
        metric = _metric_values(frame, base)
        offset = 0.0 if self._seg_fresh else self._seg_cum
        # Prepend the carried segment partial so cumsum reproduces the
        # streaming order of addition exactly.
        running = np.cumsum(np.concatenate(([offset], metric)))
        n = metric.size
        ranges: list[tuple[int, int]] = []
        trailing: int | None = None
        start = 0
        base_cum = self._bar_open_cum if self._n > 0 else float(running[0])
        while start < n:
            target = base_cum + self._threshold
            k = int(np.searchsorted(running, target, side="left"))
            if k > n:
                trailing = start
                break
            ranges.append((start, k - 1))
            start = k
            base_cum = float(running[k])
        self._seg_cum = float(running[n])
        self._seg_fresh = False
        if trailing is not None:
            self._bar_open_cum = base_cum
        return ranges, trailing

    def _ranges_from_bounds(
        self, starts: list[int], partials: list[bool], n: int
    ) -> tuple[list[tuple[int, int]], int | None]:
        """Closed-bar ranges plus the trailing open start, if a bar is open."""
        ranges: list[tuple[int, int]] = []
        trailing: int | None = None
        for k in range(len(starts)):
            if partials[k]:
                trailing = starts[k]
                break
            end = starts[k + 1] - 1 if k + 1 < len(starts) else n - 1
            ranges.append((starts[k], end))
        return ranges, trailing

    def _time_chunk(self, frame: pl.DataFrame) -> pl.DataFrame:
        stamps = frame["ts"].dt.cast_time_unit("ns").cast(pl.Int64).to_numpy()
        windows = ((stamps // self._interval) * self._interval).astype(np.int64)
        unique = np.unique(windows)
        labels = np.searchsorted(unique, windows)
        # Every window but the last is complete within this chunk; the last
        # may continue in the next, so it stays open.
        closed = unique.size - 1 if unique.size else 0
        remapped = np.where(labels < closed, labels, closed)
        emitted = self._aggregate_labels(frame, remapped, self._n > 0, closed)
        if unique.size:
            self._window = int(unique[-1])
        return emitted

    def _aggregate_ranges(
        self,
        frame: pl.DataFrame,
        ranges: list[tuple[int, int]],
        trailing: int | None,
        carried_open: bool,
    ) -> pl.DataFrame:
        n = frame.height
        closed = len(ranges)
        labels = np.full(n, closed, dtype=np.int64)
        for k, (begin, end) in enumerate(ranges):
            labels[begin : end + 1] = k
        if trailing is not None:
            labels[trailing:n] = closed
        else:
            # No trailing bar: every trade belongs to a closed range.
            pass
        return self._aggregate_labels(frame, labels, carried_open, closed)

    def _aggregate_labels(
        self,
        frame: pl.DataFrame,
        labels: np.ndarray,
        carried_open: bool,
        trailing_label: int,
    ) -> pl.DataFrame:
        labelled = frame.with_columns(pl.Series("_label", labels, dtype=pl.Int64))
        grouped = (
            labelled.group_by("_label", maintain_order=True)
            .agg(
                pl.col("ts").first().alias("start_ts"),
                pl.col("ts").last().alias("end_ts"),
                pl.col("price").first().alias("open"),
                pl.col("price").max().alias("high"),
                pl.col("price").min().alias("low"),
                pl.col("price").last().alias("close"),
                pl.col("size").sum().alias("volume"),
                (pl.col("size").cast(pl.Float64) * pl.col("price"))
                .sum()
                .alias("notional"),
                pl.len().alias("n_trades"),
            )
            .sort("_label")
        )

        bars: list[Bar] = []
        for row in grouped.iter_rows(named=True):
            label = int(row["_label"])
            merged = self._merge_carried(row) if carried_open else self._row_to_state(row)
            if label < trailing_label:
                bars.append(
                    self._state_to_bar(merged, self._bar_id + label, partial=False)
                )
                self._reset_bar()
                carried_open = False
            else:
                # The trailing open bar: adopt the merged aggregates.
                self._adopt_state(merged)

        self._bar_id += trailing_label
        return bars_to_frame(bars)

    def _state_to_bar(
        self, state: dict[str, Any], bar_id: int, *, partial: bool
    ) -> Bar:
        return Bar(
            bar_id=bar_id,
            start_ts=state["start_ts"],
            end_ts=state["end_ts"],
            duration=state["end_ts"] - state["start_ts"],
            open=state["open"],
            high=state["high"],
            low=state["low"],
            close=state["close"],
            volume=state["volume"],
            notional=state["notional"],
            vwap=state["notional"] / state["volume"],
            n_trades=state["n_trades"],
            is_partial=partial,
        )

    def _row_to_state(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "start_ts": row["start_ts"],
            "end_ts": row["end_ts"],
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "volume": float(row["volume"]),
            "notional": float(row["notional"]),
            "n_trades": int(row["n_trades"]),
        }

    def _merge_carried(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "start_ts": self._start_ts,
            "end_ts": row["end_ts"],
            "open": self._open,
            "high": max(self._high, row["high"]),
            "low": min(self._low, row["low"]),
            "close": row["close"],
            "volume": self._volume + float(row["volume"]),
            "notional": self._notional + float(row["notional"]),
            "n_trades": self._n + int(row["n_trades"]),
        }

    def _adopt_state(self, state: dict[str, Any]) -> None:
        self._start_ts = state["start_ts"]
        self._end_ts = state["end_ts"]
        self._open = float(state["open"])
        self._high = float(state["high"])
        self._low = float(state["low"])
        self._close = float(state["close"])
        self._volume = float(state["volume"])
        self._notional = float(state["notional"])
        self._n = int(state["n_trades"])


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
