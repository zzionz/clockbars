"""Adaptive bar kinds: imbalance, runs, and CUSUM-filtered bars.

All three sample on order flow rather than on a fixed quantity of it, and
all three use the same sign convention, the tick rule: a trade is signed
+1 when its price rises against the previous trade, -1 when it falls, and
a flat price carries the previous sign forward. The first trade of a
stream is signed +1. Signs are a property of the stream, so they carry
across bar and session boundaries.

Imbalance bars (Lopez de Prado, ch. 2) accumulate the signed metric
``s = sum(b_t * m_t)`` and close when ``|s|`` reaches ``E[T] * |e|``, where
``E[T]`` is an EWMA estimate of completed bar lengths and ``e`` an EWMA
estimate of the per-trade signed metric. Both estimates update after each
completed bar, ``alpha = 2 / (window + 1)``. The user seeds both; the data
refines them; nothing but completed bars ever updates them, so the rule is
causal by construction.

Runs bars close when the current streak of consecutive same-signed trades
reaches ``E[T] * max(P, 1 - P)``, with ``P`` an EWMA of the buy fraction
per completed bar. A bar's first trade starts a fresh streak. Alternating
flow produces no runs, so a bar can stay open until a session cut or the
stream ends; that is the rule's honest output, and it is also a diagnostic.

CUSUM bars (Page, 1954) are the purest activity clock here: the bar closes
when the cumulative price change since the last reset, measured one-sided,
reaches ``+h`` or ``-h``. Every trade's price change from the previous
trade is consumed by the bar that contains it; only the stream's first
trade has none. Both accumulators reset when a bar closes and when a
session cuts. ``h`` is in price units.

Every bounds function loops over bars and searches vectorised chunks of
trades, at ``O(n)`` amortised cost. Per-bar running sums are built with a
prepend-carry: the bar's partial sum leads the chunk, and ``np.cumsum``
over the pair reproduces the exact left-to-right order of sequential
addition, so batch and streaming make identical closure decisions on
identical floats.
"""

from __future__ import annotations

import math

import numpy as np

# Chunk granularity for vectorised crossing searches. Bars are expected to
# be far shorter than this, so a bar usually closes inside the first chunk
# and the amortised cost stays linear in trades.
_CHUNK = 2048


def _check_positive_float(value: float, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a number, got {type(value).__name__}")
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite, got {value}")
    if value <= 0:
        raise ValueError(f"{name} must be positive, got {value}")
    return float(value)


def _check_window(window: int) -> int:
    if isinstance(window, bool) or not isinstance(window, int):
        raise TypeError(f"window must be an int, got {type(window).__name__}")
    if window < 1:
        raise ValueError(f"window must be at least 1, got {window}")
    return window


def _check_imbalance_seed(expected_imbalance: float) -> float:
    if isinstance(expected_imbalance, bool) or not isinstance(
        expected_imbalance, (int, float)
    ):
        raise TypeError(
            f"expected_imbalance must be a number, got {type(expected_imbalance).__name__}"
        )
    if not math.isfinite(expected_imbalance):
        raise ValueError(f"expected_imbalance must be finite, got {expected_imbalance}")
    if expected_imbalance == 0:
        raise ValueError(
            "expected_imbalance must be nonzero: a zero seed collapses every bar "
            "to a single trade"
        )
    return float(expected_imbalance)


def tick_rule_signs(prices: np.ndarray) -> np.ndarray:
    """Signs under the tick rule, vectorised.

    +1 on a rise, -1 on a fall, carry the previous sign through flat
    prices, +1 for the first trade of the stream.
    """
    signs = np.ones(prices.size, dtype=np.float64)
    if prices.size < 2:
        return signs

    step = np.sign(np.diff(prices))
    # Forward-fill nonzero steps so flat prices inherit the last sign.
    filled = np.where(step != 0, step, np.nan)
    known = np.where(np.isnan(filled), 0, np.arange(filled.size))
    np.maximum.accumulate(known, out=known)
    filled = filled[known]
    # A stream that never moves inherits the +1 of its first trade.
    filled = np.where(np.isnan(filled), 1.0, filled)
    signs[1:] = filled
    return signs


def imbalance_bounds(
    metric: np.ndarray,
    signs: np.ndarray,
    segment_indices: np.ndarray,
    *,
    expected_trades: float,
    expected_imbalance: float,
    window: int,
    carry: dict[str, float] | None = None,
) -> tuple[list[int], list[bool], dict[str, float]]:
    """Start index, partial flag, and carry state of every imbalance bar.

    The EWMA state is a property of the stream, so it carries across
    session cuts; a cut force-closes the open bar and leaves the estimates
    alone. ``carry`` continues a bar that a previous chunk left open, and
    the returned carry describes the bar still open at the end, if any.
    """
    alpha = 2.0 / (window + 1.0)
    if carry is None:
        est_trades = expected_trades
        est_imb = expected_imbalance
        open_partial = 0.0
        open_seen = 0.0
        has_open = False
    else:
        est_trades = carry["est_trades"]
        est_imb = carry["est_imb"]
        open_partial = carry["partial"]
        open_seen = carry["seen"]
        has_open = bool(carry["open"])
    signed = signs * metric

    bar_starts: list[int] = []
    partials: list[bool] = []
    carry_out = {
        "est_trades": est_trades,
        "est_imb": est_imb,
        "partial": 0.0,
        "seen": 0.0,
        "open": 0.0,
    }
    for i in range(segment_indices.size):
        begin = int(segment_indices[i])
        end = int(segment_indices[i + 1]) if i + 1 < segment_indices.size else metric.size
        start = begin
        while start < end:
            bar_starts.append(start)
            target = est_trades * abs(est_imb)
            closing = -1
            partial = open_partial if has_open else 0.0
            position = start
            while position < end:
                stop = min(position + _CHUNK, end)
                # Prepend the bar's partial sum so cumsum reproduces the
                # streaming order of addition exactly.
                carried = np.concatenate(([partial], signed[position:stop]))
                running = np.cumsum(carried)
                absolute = np.abs(running[1:])
                hits = np.flatnonzero(absolute >= target)
                if hits.size:
                    closing = position + int(hits[0])
                    bar_total = float(running[hits[0] + 1])
                    break
                partial = float(running[-1])
                position = stop
            if closing < 0:
                partials.append(True)  # segment ended with the bar still open
                carry_out = {
                    "est_trades": est_trades,
                    "est_imb": est_imb,
                    "partial": partial,
                    "seen": float(open_seen if has_open else 0.0) + float(end - start),
                    "open": 1.0,
                }
                break
            partials.append(False)
            # A bar carried over a chunk boundary closes with its true
            # length, not the length seen inside this call.
            length = (int(open_seen) if has_open else 0) + (closing - start + 1)
            est_trades += alpha * (length - est_trades)
            est_imb += alpha * (bar_total / length - est_imb)
            has_open = False
            carry_out = {
                "est_trades": est_trades,
                "est_imb": est_imb,
                "partial": 0.0,
                "seen": 0.0,
                "open": 0.0,
            }
            start = closing + 1
    return bar_starts, partials, carry_out


def runs_bounds(
    signs: np.ndarray,
    segment_indices: np.ndarray,
    *,
    expected_trades: float,
    window: int,
    carry: dict[str, float] | None = None,
) -> tuple[list[int], list[bool], dict[str, float]]:
    """Start index, partial flag, and carry state of every runs bar."""
    alpha = 2.0 / (window + 1.0)
    if carry is None:
        est_trades = expected_trades
        buy_frac = 0.5
        carried_streak = 0.0
        carried_sign = 0.0
        open_seen = 0.0
        open_buys = 0.0
        has_open = False
    else:
        est_trades = carry["est_trades"]
        buy_frac = carry["buy_frac"]
        carried_streak = carry["streak"]
        carried_sign = carry["last_sign"]
        open_seen = carry["seen"]
        open_buys = carry["buys"]
        has_open = bool(carry["open"])

    bar_starts: list[int] = []
    partials: list[bool] = []
    carry_out = {
        "est_trades": est_trades,
        "buy_frac": buy_frac,
        "streak": 0.0,
        "last_sign": 0.0,
        "seen": 0.0,
        "buys": 0.0,
        "open": 0.0,
    }
    for i in range(segment_indices.size):
        begin = int(segment_indices[i])
        end = int(segment_indices[i + 1]) if i + 1 < segment_indices.size else signs.size
        start = begin
        while start < end:
            bar_starts.append(start)
            target = est_trades * max(buy_frac, 1.0 - buy_frac)
            needed = max(math.ceil(target), 1)
            carried_this = has_open and start == begin and i == 0
            if carried_this:
                # Continue the carried streak through the first same-sign run.
                # Trade j of the run has streak opening + j, so the run closes
                # the bar at j = needed - opening when that lands inside it.
                first_sign = signs[start]
                opening = carried_streak + 1 if first_sign == carried_sign else 1
                if end - start > 1:
                    flips = np.flatnonzero(signs[start + 1 : end] != signs[start : end - 1])
                else:
                    flips = np.array([], dtype=np.int64)
                # Trades [start, start + prefix] share the opening sign.
                prefix = int(flips[0]) if flips.size else end - start - 1
                if opening >= needed:
                    # Defensive: the scalar path closes a bar the moment
                    # its streak reaches the target, so a carried bar can
                    # never re-open with a streak that already qualifies.
                    closing = start  # pragma: no cover
                elif needed - opening <= prefix:
                    closing = start + int(needed - opening)
                else:
                    closing = -1
                    if flips.size:
                        rest = np.concatenate(
                            ([start + prefix + 1], start + 1 + flips[1:])
                        )
                        rest_lengths = np.diff(np.append(rest, end))
                        for k in range(rest.size):
                            if rest_lengths[k] >= needed:
                                closing = int(rest[k]) + needed - 1
                                break
                has_open = False
            else:
                # Streak starts are the bar start plus every sign change.
                if end - start > 1:
                    flips = np.flatnonzero(signs[start + 1 : end] != signs[start : end - 1])
                    streak_starts = np.concatenate(([start], start + 1 + flips))
                else:
                    streak_starts = np.array([start])
                lengths = np.diff(np.append(streak_starts, end))
                closing = -1
                for k in range(streak_starts.size):
                    if lengths[k] >= needed:
                        closing = int(streak_starts[k]) + needed - 1
                        break
            if closing < 0:
                partials.append(True)  # no streak reached the target
                # Streak at the segment's last trade, for the next chunk.
                if end - start > 1:
                    tail_flips = np.flatnonzero(
                        signs[start + 1 : end] != signs[start : end - 1]
                    )
                else:
                    tail_flips = np.array([], dtype=np.int64)
                last_run = int(tail_flips[-1]) + start + 1 if tail_flips.size else start
                # A bar carried over a chunk boundary keeps its true counts,
                # so the next chunk closes it with the right EWMA inputs.
                tail_buys = int(np.count_nonzero(signs[start:end] > 0))
                seen_now = (open_seen if carried_this else 0.0) + float(end - start)
                buys_now = (open_buys if carried_this else 0.0) + float(tail_buys)
                carry_out = {
                    "est_trades": est_trades,
                    "buy_frac": buy_frac,
                    "streak": float(end - last_run),
                    "last_sign": float(signs[end - 1]),
                    "seen": seen_now,
                    "buys": buys_now,
                    "open": 1.0,
                }
                break
            partials.append(False)
            length = (int(open_seen) if carried_this else 0) + (closing - start + 1)
            local_buys = int(np.count_nonzero(signs[start : closing + 1] > 0))
            buys = (int(open_buys) if carried_this else 0) + local_buys
            est_trades += alpha * (length - est_trades)
            buy_frac += alpha * (buys / length - buy_frac)
            carry_out = {
                "est_trades": est_trades,
                "buy_frac": buy_frac,
                "streak": 0.0,
                "last_sign": float(signs[closing]),
                "seen": 0.0,
                "buys": 0.0,
                "open": 0.0,
            }
            start = closing + 1
    return bar_starts, partials, carry_out


def cusum_bounds(
    prices: np.ndarray,
    segment_indices: np.ndarray,
    *,
    h: float,
    carry: dict[str, float] | None = None,
    prev_price: float | None = None,
) -> tuple[list[int], list[bool], dict[str, float]]:
    """Start index, partial flag, and carry state of every CUSUM bar.

    Filter state per bar: the running sum of price changes, its running
    minimum and maximum. A trade closes the bar when the sum drifts ``h``
    above that minimum or ``h`` below that maximum. Reset on close and on
    a session cut. ``prev_price`` supplies the price before the first trade
    of the call, so a chunk continuing a stream consumes the boundary
    trade's own price change; without it the stream's first trade has none.
    """
    diff = np.zeros(prices.size, dtype=np.float64)
    if prices.size > 1:
        diff[1:] = prices[1:] - prices[:-1]
    if prev_price is not None and prices.size:
        diff[0] = prices[0] - prev_price

    if carry is None:
        open_cum = 0.0
        open_min = 0.0
        open_max = 0.0
        has_open = False
    else:
        open_cum = carry["d_cum"]
        open_min = carry["d_min"]
        open_max = carry["d_max"]
        has_open = bool(carry["open"])

    bar_starts: list[int] = []
    partials: list[bool] = []
    carry_out = {"d_cum": 0.0, "d_min": 0.0, "d_max": 0.0, "open": 0.0}
    for i in range(segment_indices.size):
        begin = int(segment_indices[i])
        end = int(segment_indices[i + 1]) if i + 1 < segment_indices.size else prices.size
        start = begin
        while start < end:
            bar_starts.append(start)
            closing = -1
            d_cum = open_cum if has_open else 0.0
            d_min = open_min if has_open else 0.0
            d_max = open_max if has_open else 0.0
            has_open = False
            position = start
            while position < end:
                stop = min(position + _CHUNK, end)
                carried = np.concatenate(([d_cum], diff[position:stop]))
                running = np.cumsum(carried)
                body = running[1:]
                mins = np.minimum(d_min, np.minimum.accumulate(body))
                maxs = np.maximum(d_max, np.maximum.accumulate(body))
                up_hits = np.flatnonzero(body - mins >= h)
                down_hits = np.flatnonzero(maxs - body >= h)
                if up_hits.size and (not down_hits.size or up_hits[0] <= down_hits[0]):
                    closing = position + int(up_hits[0])
                    break
                if down_hits.size:
                    closing = position + int(down_hits[0])
                    break
                d_cum = float(running[-1])
                d_min = float(mins[-1])
                d_max = float(maxs[-1])
                position = stop
            if closing < 0:
                partials.append(True)  # no trigger before the segment ended
                carry_out = {
                    "d_cum": d_cum,
                    "d_min": d_min,
                    "d_max": d_max,
                    "open": 1.0,
                }
                break
            partials.append(False)
            carry_out = {"d_cum": 0.0, "d_min": 0.0, "d_max": 0.0, "open": 0.0}
            start = closing + 1
    return bar_starts, partials, carry_out
