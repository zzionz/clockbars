"""Core bar-construction engine.

The rule for every threshold bar type:

    A bar closes on the trade that makes its running total reach or exceed the
    threshold, and that trade is included in the closing bar. The next bar then
    restarts from zero.

Consequences of that choice, both deliberate:

* A closed bar's total is always **at least** the threshold; it can exceed it
  only by the size of its own final trade.
* Carry is *not* propagated between bars. If a bar overshoots, the next bar is
  not short-changed to "make up" the difference — it starts fresh. (A global
  cumulative-sum implementation would silently do exactly that, producing bars
  below threshold in the middle of the series.)

Session cuts generalise the same idea one level up: pass `session_starts` and
accumulation restarts at each boundary, so a bar never straddles a rollover.
Time bars are cut by the clock instead, with windows aligned to the epoch via
integer floor division — the same arithmetic the streaming BarBuilder uses,
so the two paths cannot drift.
"""

from __future__ import annotations

import math
import re
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Literal

import numpy as np
import polars as pl

BarKind = Literal["tick", "volume", "dollar", "time"]

_REQUIRED = ("ts", "price", "size")

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

_BAR_COLUMNS = [
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

# Relative tolerance for float accumulation when deciding whether a bar hit its
# target. Sums of thousands of floats are not exact, and pretending otherwise
# produces phantom partial bars.
_ATOL = 1e-9

_INTERVAL_UNITS: dict[str, int] = {
    "ns": 1,
    "us": 1_000,
    "ms": 1_000_000,
    "s": 1_000_000_000,
    "m": 60 * 1_000_000_000,
    "h": 3_600 * 1_000_000_000,
    "d": 86_400 * 1_000_000_000,
    "w": 604_800 * 1_000_000_000,
}

_INTERVAL_RE = re.compile(r"(\d+)(ns|us|ms|s|m|h|d|w)")


def _coerce(trades: object) -> pl.DataFrame:
    """Accept a Polars frame, or a pandas frame if pandas happens to be installed."""
    if isinstance(trades, pl.DataFrame):
        return trades
    try:  # pragma: no cover - only exercised when pandas is present
        import pandas as pd
    except ImportError:
        raise TypeError(
            f"expected a polars.DataFrame, got {type(trades).__name__}"
        ) from None
    if isinstance(trades, pd.DataFrame):
        return pl.from_pandas(trades)
    raise TypeError(f"expected a polars.DataFrame, got {type(trades).__name__}")


def validate_trades(trades: pl.DataFrame) -> pl.DataFrame:
    """Validate and normalise a trades frame.

    Requires columns ``ts``, ``price``, ``size``. ``ts`` must be a timezone-aware
    datetime and is normalised to ``Datetime("ns", "UTC")``. Rows must be sorted
    ascending by ``ts``; duplicate timestamps are allowed and keep input order.
    """
    trades = _coerce(trades)

    missing = [name for name in _REQUIRED if name not in trades.columns]
    if missing:
        raise ValueError(f"trades is missing required column(s): {', '.join(missing)}")
    if trades.height == 0:
        raise ValueError("trades is empty")

    dtype = trades.schema["ts"]
    if not isinstance(dtype, pl.Datetime):
        raise TypeError(f"'ts' must be a Datetime column, got {dtype}")
    if dtype.time_zone is None:
        raise ValueError(
            "'ts' is timezone-naive; trading timestamps must be explicit. "
            'Use .dt.replace_time_zone("UTC") on a known-UTC column.'
        )

    if dtype.time_unit != "ns":
        trades = trades.with_columns(pl.col("ts").dt.cast_time_unit("ns"))
    if dtype.time_zone != "UTC":
        trades = trades.with_columns(pl.col("ts").dt.convert_time_zone("UTC"))

    if trades.select(list(_REQUIRED)).null_count().to_numpy().sum() > 0:
        raise ValueError("trades contains null values in ts/price/size")
    if not trades["ts"].is_sorted():
        raise ValueError("'ts' must be sorted ascending")

    # NaN and inf compare False against everything, so `column <= 0` alone
    # would let them through. Check finiteness first, then sign.
    for name in ("price", "size"):
        column = trades[name]
        if trades.schema[name].is_float() and not column.is_finite().all():
            raise ValueError(f"'{name}' must be finite (no NaN or inf)")
        if (column <= 0).any():
            raise ValueError(f"'{name}' must be strictly positive")

    return trades


def _check_threshold(threshold: float) -> None:
    if isinstance(threshold, bool) or not isinstance(threshold, (int, float)):
        raise TypeError(f"threshold must be a number, got {type(threshold).__name__}")
    if not math.isfinite(threshold):
        raise ValueError(f"threshold must be finite, got {threshold}")
    if threshold <= 0:
        raise ValueError(f"threshold must be positive, got {threshold}")


def _interval_ns(every: str) -> int:
    """Parse a duration string into nanoseconds.

    Accepts single units (``"5m"``, ``"1h"``) and compounds (``"1h30m"``), summed
    left to right. Index windows (``"1i"``) are meaningless for a datetime
    column and are rejected here rather than by a downstream library whose
    grammar may differ.
    """
    if not isinstance(every, str):
        raise TypeError(
            f"'every' must be a duration string like '5m', got {type(every).__name__}"
        )
    position, total = 0, 0
    while position < len(every):
        match = _INTERVAL_RE.match(every, position)
        if match is None:
            raise ValueError(
                f"could not parse interval {every!r}; expected a form like '5m' or '1h30m'"
            )
        total += int(match.group(1)) * _INTERVAL_UNITS[match.group(2)]
        position = match.end()
    if total <= 0:
        raise ValueError(f"interval must be positive, got {every!r}")
    return total


def _utc_ns(moment: datetime) -> int:
    """Whole nanoseconds since the epoch, in exact integer arithmetic."""
    delta = moment - _EPOCH
    return ((delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds) * 1_000


def _session_starts_ns(session_starts: pl.Series | Sequence[datetime]) -> np.ndarray:
    """Normalise user-supplied session boundaries to sorted, deduped UTC nanoseconds."""
    if isinstance(session_starts, pl.Series):
        dtype = session_starts.dtype
        if not isinstance(dtype, pl.Datetime):
            raise TypeError(f"session_starts must be a Datetime series, got {dtype}")
        if dtype.time_zone is None:
            raise ValueError(
                "session_starts is timezone-naive; boundaries must carry an explicit zone"
            )
        values = (
            session_starts.dt.convert_time_zone("UTC")
            .dt.cast_time_unit("ns")
            .cast(pl.Int64)
            .to_numpy()
        )
    elif isinstance(session_starts, str):
        raise TypeError("session_starts must be a Series or a sequence of datetimes")
    else:
        moments = list(session_starts)
        values = np.empty(len(moments), dtype=np.int64)
        for i, moment in enumerate(moments):
            if not isinstance(moment, datetime):
                raise TypeError(
                    f"session_starts entries must be datetime, got {type(moment).__name__}"
                )
            if moment.tzinfo is None:
                raise ValueError(
                    "session_starts entries must be timezone-aware, got a naive datetime"
                )
            values[i] = _utc_ns(moment.astimezone(UTC))

    if values.size == 0:
        raise ValueError("session_starts is empty; pass None when no cuts are wanted")
    if values.size > 1 and not np.all(np.diff(values) >= 0):
        raise ValueError("session_starts must be sorted ascending")
    return np.unique(values)


def _segment_start_indices(ts_ns: np.ndarray, starts_ns: np.ndarray) -> np.ndarray:
    """Indices where a new session begins; index 0 always starts a segment.

    A trade exactly on a boundary belongs to the new session.
    """
    segment_ids = np.searchsorted(starts_ns, ts_ns, side="right")
    flags = np.empty(ts_ns.size, dtype=bool)
    flags[0] = True
    flags[1:] = segment_ids[1:] != segment_ids[:-1]
    return np.flatnonzero(flags)


def _metric_expr(kind: str) -> pl.Expr:
    """The quantity accumulated toward the threshold, as a Polars expression."""
    if kind == "tick":
        return pl.lit(1.0).alias("_metric")
    if kind == "volume":
        return pl.col("size").cast(pl.Float64).alias("_metric")
    if kind == "dollar":
        return (pl.col("size").cast(pl.Float64) * pl.col("price")).alias("_metric")
    raise ValueError(f"unknown bar kind: {kind!r}")


def _metric_values(df: pl.DataFrame, kind: str) -> np.ndarray:
    """The same quantity, as a numpy array, for the segmentation pass."""
    if kind == "tick":
        return np.ones(df.height, dtype=np.float64)
    size = df["size"].to_numpy().astype(np.float64, copy=False)
    if kind == "volume":
        return size
    price = df["price"].to_numpy().astype(np.float64, copy=False)
    return size * price


def _bar_bounds(metric: np.ndarray, threshold: float) -> list[int]:
    """Start index of each bar under the restart-from-zero rule.

    Each bar runs from its start index up to and including the first trade whose
    running total (measured from that start) reaches ``threshold``.

    This is a linear scan over *bars*, not trades: ``O(n_bars * log n_trades)``.
    With a realistic threshold there are far fewer bars than trades, and a tick
    threshold of 1 degrades to one trade per bar, which is the expected answer.
    """
    cumulative = np.cumsum(metric)
    total = float(cumulative[-1])
    bounds = [0]
    start = 0

    while start < metric.size:
        base = float(cumulative[start - 1]) if start else 0.0
        target = base + threshold
        if target > total:
            break
        # First index at or after `start` whose cumulative total reaches target.
        end = int(np.searchsorted(cumulative, target, side="left"))
        start = end + 1
        if start < metric.size:
            bounds.append(start)

    return bounds


def _aggregate(df: pl.DataFrame, threshold: float | None) -> pl.DataFrame:
    bars = (
        df.group_by("bar_id", maintain_order=True)
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
            pl.col("_metric").sum().alias("_target"),
            pl.len().alias("n_trades"),
        )
        .sort("bar_id")
    )

    partial = (
        pl.lit(False)
        if threshold is None
        else pl.col("_target") < threshold * (1.0 - _ATOL)
    )

    bars = bars.with_columns(
        (pl.col("end_ts") - pl.col("start_ts")).alias("duration"),
        (pl.col("notional") / pl.col("volume")).alias("vwap"),
        partial.alias("is_partial"),
    )

    return bars.select(_BAR_COLUMNS)


def _threshold_bars(
    trades: pl.DataFrame,
    kind: str,
    threshold: float,
    session_starts: pl.Series | Sequence[datetime] | None,
) -> pl.DataFrame:
    _check_threshold(threshold)
    trades = validate_trades(trades)

    metric = _metric_values(trades, kind)
    if session_starts is None:
        segment_indices = np.array([0], dtype=np.int64)
    else:
        starts_ns = _session_starts_ns(session_starts)
        ts_ns = trades["ts"].dt.cast_time_unit("ns").cast(pl.Int64).to_numpy()
        segment_indices = _segment_start_indices(ts_ns, starts_ns)

    n = metric.size
    bar_starts: list[int] = []
    for i in range(segment_indices.size):
        begin = int(segment_indices[i])
        end = int(segment_indices[i + 1]) if i + 1 < segment_indices.size else n
        bar_starts.extend(begin + b for b in _bar_bounds(metric[begin:end], float(threshold)))

    lengths = np.diff(np.append(np.asarray(bar_starts, dtype=np.int64), n))
    bar_ids = np.repeat(np.arange(len(bar_starts), dtype=np.int64), lengths)

    labelled = trades.with_columns(
        _metric_expr(kind),
        pl.Series("bar_id", bar_ids, dtype=pl.Int64),
    )
    return _aggregate(labelled, float(threshold))


def _time_bars(trades: pl.DataFrame, every: str) -> pl.DataFrame:
    trades = validate_trades(trades)
    interval = _interval_ns(every)

    # Epoch-aligned windows via integer floor division: the same arithmetic
    # BarBuilder uses one trade at a time, so the two paths cannot disagree
    # about where a window starts. A window with no trades never exists.
    window = (pl.col("ts").dt.cast_time_unit("ns").cast(pl.Int64) // interval) * interval
    bars = (
        trades.with_columns(window.alias("_window"))
        .group_by("_window", maintain_order=True)
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
        .sort("_window")
    )

    bars = bars.with_columns(
        pl.int_range(pl.len()).alias("bar_id"),
        (pl.col("end_ts") - pl.col("start_ts")).alias("duration"),
        (pl.col("notional") / pl.col("volume")).alias("vwap"),
        pl.lit(False).alias("is_partial"),
    )

    return bars.select(_BAR_COLUMNS)


def resample(
    trades: pl.DataFrame,
    kind: BarKind = "dollar",
    *,
    threshold: float | None = None,
    every: str | None = None,
    session_starts: pl.Series | Sequence[datetime] | None = None,
) -> pl.DataFrame:
    """Build bars from a trades frame.

    Parameters
    ----------
    trades
        Frame with ``ts`` (timezone-aware datetime), ``price``, ``size``.
        Must be sorted ascending by ``ts``.
    kind
        ``"tick"``, ``"volume"``, ``"dollar"``, or ``"time"``.
    threshold
        Accumulated quantity that closes a bar. Required for ``tick`` (number of
        trades), ``volume`` (units), and ``dollar`` (quote-currency notional).
        For ``tick`` the threshold is effectively rounded up: a threshold of
        2.5 closes bars of 3 trades, because a bar closes when the count
        *reaches* the threshold.
    every
        Window for ``kind="time"``, e.g. ``"5m"``, ``"1h"``, ``"1h30m"``.
        Windows are half-open ``[start, start + every)`` and aligned to
        wall-clock boundaries, so a 1-minute window starting at 09:30:30 still
        begins at 09:30:00.
    session_starts
        Session boundary timestamps (timezone-aware, ascending). Threshold bars
        restart accumulation at each boundary, so a bar never straddles a
        rollover, and a trade exactly on a boundary belongs to the new session.
        Each session's final bar carries ``is_partial=True`` if it fell short of
        the threshold. Time bars need no cuts — the clock already makes them —
        and passing both raises rather than guessing a precedence.

    Returns
    -------
    polars.DataFrame
        Columns: ``bar_id``, ``start_ts``, ``end_ts``, ``duration``, ``open``,
        ``high``, ``low``, ``close``, ``volume``, ``notional``, ``vwap``,
        ``n_trades``, ``is_partial``.

    Notes
    -----
    ``is_partial`` is ``True`` for a bar whose segment ended before it filled —
    the end of the stream, or the end of a session. Time bars never set it,
    because a time window has no volume target to miss.
    """
    if kind == "time":
        if every is None:
            raise ValueError("kind='time' requires every=..., e.g. every='1m'")
        if threshold is not None:
            raise ValueError("kind='time' takes every=..., not threshold=")
        if session_starts is not None:
            raise ValueError(
                "kind='time' takes no session_starts: the clock already cuts time bars"
            )
        return _time_bars(trades, every)

    if kind not in ("tick", "volume", "dollar"):
        raise ValueError(
            f"unknown kind {kind!r}; expected 'tick', 'volume', 'dollar', or 'time'"
        )
    if threshold is None:
        raise ValueError(f"kind={kind!r} requires threshold=")
    if every is not None:
        raise ValueError(f"kind={kind!r} takes threshold=..., not every=")

    return _threshold_bars(trades, kind, threshold, session_starts)
