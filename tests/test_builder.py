"""The streaming path must agree with the batch path — or one of them is wrong.

Exact agreement is required on everything that decides what a bar *is*:
boundaries, ids, timestamps, OHLC, counts, partial flags. Per-bar volume,
notional, and vwap are compared to 1e-12 relative, because a vectorised group
sum and a sequential accumulator sum the same floats in different orders.
"""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl
import pytest

from clockbars import Bar, BarBuilder, bars_to_frame, resample, synthetic_ticks

EXACT = [
    "bar_id",
    "start_ts",
    "end_ts",
    "duration",
    "open",
    "high",
    "low",
    "close",
    "n_trades",
    "is_partial",
]
APPROX = ["volume", "notional", "vwap"]

SESSION_OPEN = [datetime(2019, 1, d, 14, 30, tzinfo=UTC) for d in (2, 3, 4)]

CASES = [
    ("tick", {"threshold": 250}),
    ("volume", {"threshold": 500.0}),
    ("dollar", {"threshold": 2_500_000.0}),
    ("time", {"every": "1m"}),
]


def stream_frame(builder: BarBuilder, frame: pl.DataFrame) -> pl.DataFrame:
    bars = builder.push_frame(frame)
    tail = builder.flush()
    if tail is not None:
        bars.append(tail)
    return bars_to_frame(bars)


def assert_parity(streamed: pl.DataFrame, batch: pl.DataFrame) -> None:
    assert streamed.columns == batch.columns
    for column in EXACT:
        assert streamed[column].equals(batch[column]), f"exact mismatch on {column}"
    for column in APPROX:
        assert streamed[column].to_list() == pytest.approx(
            batch[column].to_list(), rel=1e-12
        ), f"sum mismatch on {column}"


@pytest.mark.parametrize(("kind", "kwargs"), CASES)
def test_streaming_matches_batch(trades, kind, kwargs):
    batch = resample(trades, kind=kind, **kwargs)
    streamed = stream_frame(BarBuilder(kind, **kwargs), trades)
    assert_parity(streamed, batch)


@pytest.mark.parametrize(("kind", "kwargs"), [case for case in CASES if case[0] != "time"])
def test_streaming_close_session_matches_batch_session_starts(trades, kind, kwargs):
    batch = resample(trades, kind=kind, session_starts=SESSION_OPEN, **kwargs)

    builder = BarBuilder(kind, **kwargs)
    bars: list[Bar] = []
    for i, boundary in enumerate(SESSION_OPEN):
        if i + 1 < len(SESSION_OPEN):
            chunk = trades.filter(
                (pl.col("ts") >= boundary) & (pl.col("ts") < SESSION_OPEN[i + 1])
            )
        else:
            chunk = trades.filter(pl.col("ts") >= boundary)
        bars.extend(builder.push_frame(chunk))
        if i + 1 < len(SESSION_OPEN):
            closing = builder.close_session()
            if closing is not None:
                bars.append(closing)
    tail = builder.flush()
    if tail is not None:
        bars.append(tail)

    assert_parity(bars_to_frame(bars), batch)


def test_streaming_matches_batch_at_scale():
    stream = synthetic_ticks(n_trades=200_000, n_sessions=10, seed=3)
    kwargs = {"threshold": 2_500_000.0}
    batch = resample(stream, kind="dollar", **kwargs)
    assert batch.height > 1_000
    streamed = stream_frame(BarBuilder("dollar", **kwargs), stream)
    assert_parity(streamed, batch)


# --- builder semantics -----------------------------------------------------


def test_push_returns_none_until_a_bar_closes():
    builder = BarBuilder("volume", threshold=100.0)
    moment = datetime(2020, 1, 2, 14, 30, tzinfo=UTC)
    assert builder.push(moment, 10.0, 60.0) is None
    bar = builder.push(moment.replace(minute=31), 10.0, 60.0)
    assert bar is not None
    assert bar.n_trades == 2
    assert not bar.is_partial
    assert bar.volume == 120.0


def test_flush_emits_the_trailing_partial():
    builder = BarBuilder("volume", threshold=100.0)
    builder.push(datetime(2020, 1, 2, 14, 30, tzinfo=UTC), 10.0, 40.0)
    tail = builder.flush()
    assert tail is not None
    assert tail.is_partial
    assert tail.volume == 40.0
    assert builder.flush() is None


def test_close_session_resets_accumulation():
    builder = BarBuilder("volume", threshold=100.0)
    builder.push(datetime(2020, 1, 2, 14, 30, tzinfo=UTC), 10.0, 60.0)
    partial = builder.close_session()
    assert partial is not None
    assert partial.is_partial
    assert partial.volume == 60.0

    # The new segment starts from zero, so a single 100 closes a bar alone.
    bar = builder.push(datetime(2020, 1, 3, 14, 30, tzinfo=UTC), 10.0, 100.0)
    assert bar is not None
    assert not bar.is_partial
    assert bar.n_trades == 1


def test_close_session_with_nothing_open_is_a_noop():
    builder = BarBuilder("volume", threshold=100.0)
    assert builder.close_session() is None


def test_time_builder_emits_windows_and_flushes_the_trailing_one():
    builder = BarBuilder("time", every="1m")
    assert builder.push(datetime(2020, 1, 2, 14, 30, 20, tzinfo=UTC), 10.0, 1.0) is None
    bar = builder.push(datetime(2020, 1, 2, 14, 31, 10, tzinfo=UTC), 11.0, 1.0)
    assert bar is not None
    assert bar.n_trades == 1
    assert not bar.is_partial
    tail = builder.flush()
    assert tail is not None
    assert tail.n_trades == 1


def test_empty_bars_to_frame_has_the_shared_schema():
    frame = bars_to_frame([])
    assert frame.height == 0
    assert frame.columns == [
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


# --- builder validation ----------------------------------------------------


def _builder() -> BarBuilder:
    return BarBuilder("volume", threshold=100.0)


def test_naive_pushes_are_rejected():
    with pytest.raises(ValueError, match="timezone-naive"):
        _builder().push(datetime(2020, 1, 2, 14, 30), 10.0, 1.0)  # type: ignore[arg-type]


def test_non_datetime_ts_is_rejected():
    with pytest.raises(TypeError, match="datetime"):
        _builder().push(1_577_953_800_000, 10.0, 1.0)  # type: ignore[arg-type]


def test_out_of_order_pushes_are_rejected():
    builder = _builder()
    builder.push(datetime(2020, 1, 2, 14, 31, tzinfo=UTC), 10.0, 1.0)
    with pytest.raises(ValueError, match="non-decreasing"):
        builder.push(datetime(2020, 1, 2, 14, 30, tzinfo=UTC), 10.0, 1.0)


@pytest.mark.parametrize("bad_price", [float("nan"), float("inf"), 0.0, -1.0])
def test_bad_prices_are_rejected(bad_price):
    with pytest.raises(ValueError):
        _builder().push(datetime(2020, 1, 2, 14, 30, tzinfo=UTC), bad_price, 1.0)


@pytest.mark.parametrize("bad_size", [float("nan"), float("inf"), 0.0, -1.0])
def test_bad_sizes_are_rejected(bad_size):
    with pytest.raises(ValueError):
        _builder().push(datetime(2020, 1, 2, 14, 30, tzinfo=UTC), 10.0, bad_size)


def test_unknown_kind_is_rejected():
    with pytest.raises(ValueError, match="unknown kind"):
        BarBuilder("zeitgeist", threshold=1)  # type: ignore[arg-type]


def test_time_builder_rejects_threshold():
    with pytest.raises(ValueError, match="not threshold"):
        BarBuilder("time", every="1m", threshold=1)


def test_time_builder_requires_every():
    with pytest.raises(ValueError, match="requires every"):
        BarBuilder("time")


def test_threshold_builder_rejects_every():
    with pytest.raises(ValueError, match="not every"):
        BarBuilder("volume", threshold=1, every="1m")


def test_threshold_builder_requires_threshold():
    with pytest.raises(ValueError, match="requires threshold"):
        BarBuilder("volume")


def test_threshold_builder_rejects_bad_thresholds():
    with pytest.raises(ValueError, match="finite"):
        BarBuilder("volume", threshold=float("nan"))
    with pytest.raises(ValueError, match="positive"):
        BarBuilder("volume", threshold=0.0)


def test_time_builder_close_session_raises():
    builder = BarBuilder("time", every="1m")
    with pytest.raises(ValueError, match="cut by the clock"):
        builder.close_session()


def test_push_frame_validates_like_resample(make_trades):
    with pytest.raises(ValueError, match="missing required column"):
        BarBuilder("volume", threshold=1.0).push_frame(make_trades([10.0], [1.0]).drop("size"))


def test_push_frame_refuses_sub_microsecond_timestamps():
    raw = [int(datetime(2020, 1, 2, 14, 30, tzinfo=UTC).timestamp()) * 10**9 + 500]
    ts = pl.Series(raw, dtype=pl.Int64).cast(pl.Datetime("ns")).dt.replace_time_zone("UTC")
    frame = pl.DataFrame({"ts": ts, "price": [10.0], "size": [1.0]})
    with pytest.raises(ValueError, match="sub-microsecond"):
        BarBuilder("volume", threshold=1.0).push_frame(frame)
