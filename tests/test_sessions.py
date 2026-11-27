"""Session cuts: accumulation restarts at each boundary, bars never straddle."""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl
import pytest

from clockbars import resample

# The shared fixture streams over business days 2019-01-02..04, each session
# opening at 14:30 UTC.
SESSION_OPEN = [datetime(2019, 1, d, 14, 30, tzinfo=UTC) for d in (2, 3, 4)]


def test_a_cut_in_the_middle_of_a_bar_splits_it(make_trades):
    # Six trades of 100 against a 300 threshold: without a cut that is two
    # complete bars of 300. Cutting after the second trade ends the first
    # segment short at 200, lets the second build a complete 300, and leaves
    # the final 100 partial.
    trades = make_trades([10.0] * 6, [100.0] * 6)
    cut = [trades["ts"][2]]

    plain = resample(trades, kind="volume", threshold=300.0)
    assert plain["volume"].to_list() == [300.0, 300.0]
    assert plain["is_partial"].to_list() == [False, False]

    split = resample(trades, kind="volume", threshold=300.0, session_starts=cut)
    assert split["volume"].to_list() == [200.0, 300.0, 100.0]
    assert split["is_partial"].to_list() == [True, False, True]


def test_a_trade_exactly_on_a_boundary_starts_the_new_session(make_trades_at):
    def at(minute: int, second: int) -> datetime:
        return datetime(2020, 1, 2, 14, minute, second, tzinfo=UTC)

    boundary = at(30, 0)
    trades = make_trades_at([at(29, 50), boundary], [10.0, 11.0], [50.0, 50.0])
    bars = resample(trades, kind="volume", threshold=80.0, session_starts=[boundary])
    # Under the documented rule the boundary trade belongs to the new
    # session, so each segment ends short and both bars are partial. If the
    # boundary trade had joined the old session, this would be one complete
    # bar of 100 instead.
    assert bars["n_trades"].to_list() == [1, 1]
    assert bars["is_partial"].to_list() == [True, True]
    assert bars["open"].to_list() == [10.0, 11.0]


def test_no_bar_straddles_a_boundary(trades):
    bars = resample(trades, kind="dollar", threshold=2_500_000.0, session_starts=SESSION_OPEN)
    for boundary in SESSION_OPEN:
        straddling = bars.filter(
            (pl.col("start_ts") < boundary) & (pl.col("end_ts") >= boundary)
        )
        assert straddling.height == 0


def test_sessions_conserve_everything(trades):
    bars = resample(trades, kind="volume", threshold=500.0, session_starts=SESSION_OPEN)
    assert bars["n_trades"].sum() == trades.height
    assert bars["volume"].sum() == pytest.approx(trades["size"].sum(), rel=1e-12)
    assert bars["bar_id"].to_list() == list(range(bars.height))


def test_partials_only_at_segment_ends(trades):
    bars = resample(trades, kind="volume", threshold=500.0, session_starts=SESSION_OPEN)
    partials = bars.filter(pl.col("is_partial"))

    # Each session may end short; nothing mid-session may be partial.
    ends: set[datetime] = {trades["ts"].max()}
    for i, boundary in enumerate(SESSION_OPEN):
        in_session = trades.filter(pl.col("ts") >= boundary)
        if i + 1 < len(SESSION_OPEN):
            in_session = in_session.filter(pl.col("ts") < SESSION_OPEN[i + 1])
        if in_session.height:
            ends.add(in_session["ts"].max())
    for ts in partials["end_ts"]:
        assert ts in ends


def test_every_closed_bar_still_meets_its_threshold(trades):
    bars = resample(trades, kind="volume", threshold=500.0, session_starts=SESSION_OPEN)
    closed = bars.filter(~pl.col("is_partial"))
    assert (closed["volume"] >= 500.0 * (1.0 - 1e-9)).all()


def test_session_cuts_are_deterministic(trades):
    first = resample(trades, kind="dollar", threshold=2_500_000.0, session_starts=SESSION_OPEN)
    second = resample(trades, kind="dollar", threshold=2_500_000.0, session_starts=SESSION_OPEN)
    assert first.equals(second)


def test_cuts_before_the_first_trade_are_harmless(trades):
    # A boundary earlier than every trade merely numbers segments; it cannot
    # create an empty bar.
    early = [datetime(2018, 12, 25, tzinfo=UTC), *SESSION_OPEN]
    bars = resample(trades, kind="volume", threshold=500.0, session_starts=early)
    assert (bars["n_trades"] > 0).all()


# --- validation ------------------------------------------------------------


def test_time_bars_reject_session_cuts(trades):
    with pytest.raises(ValueError, match="takes no session_starts"):
        resample(trades, kind="time", every="1m", session_starts=SESSION_OPEN)


def test_naive_boundaries_are_rejected(make_trades):
    with pytest.raises(ValueError, match="timezone-aware"):
        resample(
            make_trades([10.0], [1.0]),
            kind="volume",
            threshold=1.0,
            session_starts=[datetime(2020, 1, 2, 14, 30)],  # naive
        )


def test_unsorted_boundaries_are_rejected(make_trades):
    with pytest.raises(ValueError, match="sorted ascending"):
        resample(
            make_trades([10.0], [1.0]),
            kind="volume",
            threshold=1.0,
            session_starts=[
                datetime(2020, 1, 3, tzinfo=UTC),
                datetime(2020, 1, 2, tzinfo=UTC),
            ],
        )


def test_empty_boundaries_are_rejected(make_trades):
    with pytest.raises(ValueError, match="empty"):
        resample(
            make_trades([10.0], [1.0]),
            kind="volume",
            threshold=1.0,
            session_starts=[],
        )


def test_non_datetime_boundaries_are_rejected(make_trades):
    with pytest.raises(TypeError, match="datetime"):
        resample(
            make_trades([10.0], [1.0]),
            kind="volume",
            threshold=1.0,
            session_starts=["2020-01-02"],  # type: ignore[list-item]
        )


def test_string_boundaries_are_rejected(make_trades):
    with pytest.raises(TypeError, match="Series or a sequence"):
        resample(
            make_trades([10.0], [1.0]),
            kind="volume",
            threshold=1.0,
            session_starts="2020-01-02",  # type: ignore[arg-type]
        )


def test_boundaries_as_a_series_match_the_sequence_form(trades):
    series = pl.Series("starts", SESSION_OPEN, dtype=pl.Datetime("ns", "UTC"))
    from_sequence = resample(trades, kind="volume", threshold=500.0, session_starts=series)
    from_list = resample(trades, kind="volume", threshold=500.0, session_starts=SESSION_OPEN)
    assert from_sequence.equals(from_list)


def test_coarser_resolution_boundaries_are_normalised(trades):
    micro = pl.Series("starts", SESSION_OPEN, dtype=pl.Datetime("us", "UTC"))
    from_micro = resample(trades, kind="volume", threshold=500.0, session_starts=micro)
    from_nano = resample(trades, kind="volume", threshold=500.0, session_starts=SESSION_OPEN)
    assert from_micro.equals(from_nano)


def test_non_datetime_series_boundaries_are_rejected(make_trades):
    with pytest.raises(TypeError, match="Datetime series"):
        resample(
            make_trades([10.0], [1.0]),
            kind="volume",
            threshold=1.0,
            session_starts=pl.Series("starts", ["2020-01-02"]),
        )


def test_naive_series_boundaries_are_rejected(make_trades):
    stamp = pl.Series("starts", [datetime(2020, 1, 2, 14, 30)], dtype=pl.Datetime("ns"))
    with pytest.raises(ValueError, match="timezone-naive"):
        resample(
            make_trades([10.0], [1.0]),
            kind="volume",
            threshold=1.0,
            session_starts=stamp,
        )
