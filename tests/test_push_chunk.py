"""Out-of-core chunked construction must agree with batch and streaming."""

from __future__ import annotations

import polars as pl
import pytest

from clockbars import BarBuilder, bars_to_frame, resample, synthetic_ticks

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

CASES = [
    ("dollar", dict(threshold=2_500_000.0)),
    ("tick", dict(threshold=250)),
    ("volume", dict(threshold=500.0)),
    ("time", dict(every="1m")),
    ("tick_imbalance", dict(expected_trades=200, expected_imbalance=0.05, window=50)),
    ("volume_imbalance", dict(expected_trades=200, expected_imbalance=0.5, window=50)),
    ("dollar_imbalance", dict(expected_trades=200, expected_imbalance=5_000.0, window=50)),
    ("runs", dict(expected_trades=60, window=50)),
    ("cusum", dict(h=3.0)),
]

UNEVEN_CUTS = [0, 1300, 5700, 5701, 9000, 15000, 19999, 20_000]


def assert_parity(streamed: pl.DataFrame, batch: pl.DataFrame) -> None:
    assert streamed.height == batch.height
    for column in EXACT:
        assert streamed[column].equals(batch[column]), f"exact mismatch on {column}"
    for column in APPROX:
        assert streamed[column].to_list() == pytest.approx(
            batch[column].to_list(), rel=1e-12
        ), f"sum mismatch on {column}"


@pytest.fixture(scope="module")
def stream() -> pl.DataFrame:
    return synthetic_ticks(n_trades=20_000, n_sessions=2, seed=11)


def run_chunked(frame: pl.DataFrame, kind: str, kwargs: dict, cuts: list[int]) -> pl.DataFrame:
    builder = BarBuilder(kind, **kwargs)
    frames = [
        builder.push_chunk(frame[cuts[i] : cuts[i + 1]])
        for i in range(len(cuts) - 1)
    ]
    tail = builder.flush()
    if tail is not None:
        frames.append(bars_to_frame([tail]))
    return pl.concat(frames)


@pytest.mark.parametrize(("kind", "kwargs"), CASES)
def test_chunked_matches_batch(stream, kind, kwargs):
    batch = resample(stream, kind=kind, **kwargs)
    assert_parity(run_chunked(stream, kind, kwargs, UNEVEN_CUTS), batch)


def test_single_trade_chunks_match_batch(stream):
    # The most brutal slicing: one trade per chunk for the first 500 trades,
    # then the rest as one chunk.
    cuts = [*range(500), 20_000]
    batch = resample(stream, kind="dollar", threshold=2_500_000.0)
    assert_parity(
        run_chunked(stream, "dollar", dict(threshold=2_500_000.0), cuts), batch
    )


def test_chunk_spanning_a_bar_uses_its_true_length(stream):
    # The regression that motivated the carried trade count: a bar open at a
    # chunk boundary must close with its true length, or every EWMA update
    # after it diverges. dollar_imbalance bars here span the cut at 5700.
    kwargs = dict(expected_trades=200, expected_imbalance=5_000.0, window=50)
    batch = resample(stream, kind="dollar_imbalance", **kwargs)
    assert_parity(run_chunked(stream, "dollar_imbalance", kwargs, UNEVEN_CUTS), batch)


def test_chunks_split_by_session_match_batch_session_starts(stream):
    # The out-of-core recipe: slice by session, close_session between slices.
    cut_ts = stream.filter(
        pl.col("ts") >= pl.datetime(2019, 1, 3, 14, 30, time_zone="UTC")
    )["ts"][0]
    session_a = stream.filter(pl.col("ts") < cut_ts)
    session_b = stream.filter(pl.col("ts") >= cut_ts)

    batch = resample(
        stream,
        kind="dollar",
        threshold=2_500_000.0,
        session_starts=[cut_ts],
    )
    builder = BarBuilder(kind="dollar", threshold=2_500_000.0)
    frames = [builder.push_chunk(session_a)]
    closing = builder.close_session()
    if closing is not None:
        frames.append(bars_to_frame([closing]))
    frames.append(builder.push_chunk(session_b))
    tail = builder.flush()
    if tail is not None:
        frames.append(bars_to_frame([tail]))
    chunked = pl.concat(frames)
    assert_parity(chunked, batch)


def test_adaptive_chunks_split_by_session_match_batch(stream):
    cut_ts = stream.filter(
        pl.col("ts") >= pl.datetime(2019, 1, 3, 14, 30, time_zone="UTC")
    )["ts"][0]
    session_a = stream.filter(pl.col("ts") < cut_ts)
    session_b = stream.filter(pl.col("ts") >= cut_ts)

    kwargs = dict(expected_trades=200, expected_imbalance=5_000.0, window=50)
    batch = resample(
        stream,
        kind="dollar_imbalance",
        session_starts=[cut_ts],
        **kwargs,
    )
    builder = BarBuilder("dollar_imbalance", **kwargs)
    frames = [builder.push_chunk(session_a)]
    closing = builder.close_session()
    if closing is not None:
        frames.append(bars_to_frame([closing]))
    frames.append(builder.push_chunk(session_b))
    tail = builder.flush()
    if tail is not None:
        frames.append(bars_to_frame([tail]))
    assert_parity(pl.concat(frames), batch)


def test_chunked_then_scalar_pushes_continue_the_stream(stream):
    # A builder is resumable in any mode: chunk the first half, push the
    # second half one trade at a time, and the result still matches batch.
    kwargs = dict(threshold=2_500_000.0)
    batch = resample(stream, kind="dollar", **kwargs)
    builder = BarBuilder("dollar", **kwargs)
    frames = [builder.push_chunk(stream[:10_000])]
    for row in stream[10_000:].iter_rows(named=True):
        bar = builder.push(row["ts"], float(row["price"]), float(row["size"]))
        if bar is not None:
            frames.append(bars_to_frame([bar]))
    tail = builder.flush()
    if tail is not None:
        frames.append(bars_to_frame([tail]))
    assert_parity(pl.concat(frames), batch)


def test_empty_chunks_return_the_schema_and_change_nothing(stream):
    builder = BarBuilder("dollar", threshold=2_500_000.0)
    empty = pl.DataFrame(
        {
            "ts": pl.Series([], dtype=pl.Datetime("ns", "UTC")),
            "price": pl.Series([], dtype=pl.Float64),
            "size": pl.Series([], dtype=pl.Float64),
        }
    )
    assert builder.push_chunk(empty).height == 0
    frames = [builder.push_chunk(stream)]
    tail = builder.flush()
    if tail is not None:
        frames.append(bars_to_frame([tail]))
    batch = resample(stream, kind="dollar", threshold=2_500_000.0)
    assert_parity(pl.concat(frames), batch)


def test_out_of_order_chunks_are_rejected(stream):
    builder = BarBuilder("dollar", threshold=2_500_000.0)
    builder.push_chunk(stream[:100])
    with pytest.raises(ValueError, match="non-decreasing"):
        builder.push_chunk(stream[50:150])


def test_chunked_parity_holds_at_scale():
    big = synthetic_ticks(n_trades=100_000, n_sessions=10, seed=3)
    batch = resample(big, kind="cusum", h=3.0)
    builder = BarBuilder("cusum", h=3.0)
    frames = [builder.push_chunk(big[i : i + 8_192]) for i in range(0, big.height, 8_192)]
    tail = builder.flush()
    if tail is not None:
        frames.append(bars_to_frame([tail]))
    assert_parity(pl.concat(frames), batch)
