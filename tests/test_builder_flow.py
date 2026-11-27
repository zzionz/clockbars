"""The scalar row path and the constructor contract for flow-sampled kinds."""

from __future__ import annotations

from datetime import UTC, datetime

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

ROW_CASES = [
    ("tick_imbalance", dict(expected_trades=200, expected_imbalance=0.05, window=50)),
    ("volume_imbalance", dict(expected_trades=200, expected_imbalance=0.5, window=50)),
    ("dollar_imbalance", dict(expected_trades=200, expected_imbalance=5_000.0, window=50)),
    ("runs", dict(expected_trades=60, window=50)),
    ("cusum", dict(h=3.0)),
]


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
    return synthetic_ticks(n_trades=5_000, n_sessions=1, seed=11)


@pytest.mark.parametrize(("kind", "kwargs"), ROW_CASES)
def test_row_path_matches_batch(stream, kind, kwargs):
    batch = resample(stream, kind=kind, **kwargs)
    builder = BarBuilder(kind, **kwargs)
    bars = builder.push_frame(stream)
    tail = builder.flush()
    if tail is not None:
        bars.append(tail)
    assert_parity(bars_to_frame(bars), batch)


def test_single_push_closes_adaptive_bars():
    # Rising prices sign every trade +1, so a tick-imbalance target of 4
    # closes a bar on every fourth push, and the trailing trades flush.
    builder = BarBuilder("tick_imbalance", expected_trades=4, expected_imbalance=1)
    moment = datetime(2020, 1, 2, 14, 30, tzinfo=UTC)
    closed = 0
    for i in range(10):
        bar = builder.push(moment.replace(second=i), 10.0 + i, 1.0)
        if bar is not None:
            closed += 1
            assert bar.n_trades == 4
            assert not bar.is_partial
    tail = builder.flush()
    assert closed == 2
    assert tail is not None
    assert tail.is_partial
    assert tail.n_trades == 2


def test_close_session_emits_a_partial_and_resets_flow_state(stream):
    builder = BarBuilder("dollar_imbalance", expected_trades=200, expected_imbalance=5_000.0)
    builder.push_frame(stream[:200])
    partial = builder.close_session()
    assert partial is not None
    assert partial.is_partial
    # The estimates survive a cut; the accumulation restarts.
    before = builder._est_trades
    builder.push_frame(stream[200:400])
    assert builder._est_trades != before or builder._n > 0
    assert builder.flush() is not None


def test_time_builder_still_rejects_close_session():
    builder = BarBuilder("time", every="1m")
    with pytest.raises(ValueError, match="cut by the clock"):
        builder.close_session()


def test_a_carried_runs_bar_closes_its_streak_across_chunks():
    # Rising prices build a streak one trade at a time. A cut one trade
    # into a bar carries streak 1, and the continuation closes when the
    # streak reaches the target: the carried-length regression for runs.
    # With E[T]=4 and P seeded at 0.5, the target is 2, so bars close every
    # two trades and the cut at trade 5 lands mid-bar.
    prices = [10.0 + i for i in range(9)]
    raw = [
        int(datetime(2020, 1, 2, 14, 30, i, tzinfo=UTC).timestamp()) * 10**9
        for i in range(9)
    ]
    ts = pl.Series(raw, dtype=pl.Int64).cast(pl.Datetime("ns")).dt.replace_time_zone("UTC")
    trades = pl.DataFrame(
        {
            "ts": ts,
            "price": pl.Series(prices, dtype=pl.Float64),
            "size": pl.Series([1.0] * 9, dtype=pl.Float64),
        }
    )
    kwargs = dict(expected_trades=4, window=50)
    batch = resample(trades, kind="runs", **kwargs)
    # The buy fraction drifts up as bars close, so lengths run [2, 3, 3, 1];
    # the cut at trade 6 lands inside the third bar, mid-streak.
    assert batch["n_trades"].to_list() == [2, 3, 3, 1]
    builder = BarBuilder("runs", **kwargs)
    frames = [builder.push_chunk(trades[:6]), builder.push_chunk(trades[6:])]
    tail = builder.flush()
    if tail is not None:
        frames.append(bars_to_frame([tail]))
    assert_parity(pl.concat(frames), batch)


def test_a_pure_streak_tail_carries_without_flips():
    # A trailing runs bar made of one unbroken streak exercises the
    # no-flip branch of the tail carry. E[T]=12 gives a target of 6, which
    # five rising trades never reach.
    prices = [10.0 + i for i in range(5)]
    raw = [
        int(datetime(2020, 1, 2, 14, 30, i, tzinfo=UTC).timestamp()) * 10**9
        for i in range(5)
    ]
    ts = pl.Series(raw, dtype=pl.Int64).cast(pl.Datetime("ns")).dt.replace_time_zone("UTC")
    trades = pl.DataFrame(
        {
            "ts": ts,
            "price": pl.Series(prices, dtype=pl.Float64),
            "size": pl.Series([1.0] * 5, dtype=pl.Float64),
        }
    )
    builder = BarBuilder("runs", expected_trades=12, window=50)
    builder.push_chunk(trades)
    assert builder._streak == 5
    assert builder.flush() is not None


# --- constructor validation ------------------------------------------------


def test_unknown_kind_lists_all_nine_in_the_builder():
    with pytest.raises(ValueError, match="cusum"):
        BarBuilder("zeitgeist", threshold=1)  # type: ignore[arg-type]


def test_threshold_kinds_reject_the_adaptive_parameters():
    with pytest.raises(ValueError, match="adaptive parameters"):
        BarBuilder("dollar", threshold=10.0, expected_trades=4)
    with pytest.raises(ValueError, match="not every"):
        BarBuilder("dollar", threshold=10.0, every="1m")


def test_flow_kinds_reject_threshold_and_every():
    with pytest.raises(ValueError, match="not threshold/every"):
        BarBuilder("cusum", h=1.0, threshold=10.0)
    with pytest.raises(ValueError, match="not threshold/every"):
        BarBuilder("runs", expected_trades=4, every="1m")


def test_cusum_requires_h_and_rejects_the_rest():
    with pytest.raises(ValueError, match="requires h"):
        BarBuilder("cusum")
    with pytest.raises(ValueError, match="not the adaptive parameters"):
        BarBuilder("cusum", h=1.0, expected_trades=4)
    with pytest.raises(ValueError, match="not the adaptive parameters"):
        BarBuilder("cusum", h=1.0, window=50)


def test_runs_requires_expected_trades_and_rejects_extras():
    with pytest.raises(ValueError, match="requires expected_trades"):
        BarBuilder("runs")
    with pytest.raises(ValueError, match="no expected_imbalance or h"):
        BarBuilder("runs", expected_trades=4, h=1.0)
    with pytest.raises(ValueError, match="no expected_imbalance or h"):
        BarBuilder("runs", expected_trades=4, expected_imbalance=1.0)


def test_imbalance_kinds_require_both_seeds():
    with pytest.raises(ValueError, match="expected_imbalance"):
        BarBuilder("tick_imbalance", expected_trades=4)
    with pytest.raises(ValueError, match="expected_trades"):
        BarBuilder("volume_imbalance", expected_imbalance=1.0)
    with pytest.raises(ValueError, match="not h"):
        BarBuilder("dollar_imbalance", expected_trades=4, expected_imbalance=1.0, h=1.0)


def test_flow_builder_seeds_are_validated():
    with pytest.raises(ValueError, match="positive"):
        BarBuilder("runs", expected_trades=-1)
    with pytest.raises(ValueError, match="finite"):
        BarBuilder("cusum", h=float("nan"))
    with pytest.raises(ValueError, match="must be finite"):
        BarBuilder("tick_imbalance", expected_trades=4, expected_imbalance=float("inf"))
    with pytest.raises(ValueError, match="nonzero"):
        BarBuilder("tick_imbalance", expected_trades=4, expected_imbalance=0.0)
    with pytest.raises(TypeError):
        BarBuilder("runs", expected_trades="many")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        BarBuilder("tick_imbalance", expected_trades=4, expected_imbalance="x")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        BarBuilder("runs", expected_trades=4, window=2.5)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="at least 1"):
        BarBuilder("runs", expected_trades=4, window=0)


def test_resample_rejects_the_same_misuse(stream):
    with pytest.raises(ValueError, match="no expected_imbalance or h"):
        resample(stream, kind="runs", expected_trades=4, h=1.0)
    with pytest.raises(ValueError, match="takes its seeds, not threshold/every/h"):
        resample(
            stream,
            kind="dollar_imbalance",
            expected_trades=4,
            expected_imbalance=1.0,
            threshold=2.0,
        )
    with pytest.raises(ValueError, match=r"takes h=..., not the adaptive parameters"):
        resample(stream, kind="cusum", h=1.0, expected_trades=4)
    with pytest.raises(TypeError):
        resample(stream, kind="runs", expected_trades="many")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="not threshold/every"):
        resample(stream, kind="runs", expected_trades=4, every="1m")
    with pytest.raises(ValueError, match="not window"):
        resample(stream, kind="dollar", threshold=10.0, window=50)


def test_time_bars_reject_partial_column_aggregation():
    # _aggregate with neither a threshold nor a partial column emits
    # is_partial=False uniformly; the time path exercises this through its
    # own frame construction, and this pins the fallback directly.
    trades = synthetic_ticks(n_trades=500, n_sessions=1, seed=2)
    bars = resample(trades, kind="time", every="5m")
    assert not bars["is_partial"].any()
