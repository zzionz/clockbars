"""Input validation: bad input must fail loudly, never produce quiet garbage.

Every rule here exists because the alternative is a wrong answer that looks
right. NaN is the motivating case — it compares False against everything, so a
sign check alone waves it through, and the resulting bars look plausible while
meaning nothing.
"""

from __future__ import annotations

import polars as pl
import pytest

from clockbars import resample, validate_trades

NAN = float("nan")
INF = float("inf")


# --- frame-level validation ------------------------------------------------


def test_empty_frame_is_rejected():
    empty = pl.DataFrame(
        {
            "ts": pl.Series([], dtype=pl.Datetime("ns", "UTC")),
            "price": pl.Series([], dtype=pl.Float64),
            "size": pl.Series([], dtype=pl.Float64),
        }
    )
    with pytest.raises(ValueError, match="empty"):
        resample(empty, kind="volume", threshold=1.0)


def test_missing_column_is_named_in_the_error(make_trades):
    trades = make_trades([10.0], [1.0]).drop("size")
    with pytest.raises(ValueError, match="size"):
        resample(trades, kind="volume", threshold=1.0)


def test_unsorted_timestamps_are_rejected(make_trades):
    trades = make_trades([10.0, 11.0, 12.0], [1.0, 1.0, 1.0]).reverse()
    with pytest.raises(ValueError, match="sorted ascending"):
        resample(trades, kind="tick", threshold=1)


def test_naive_timestamps_are_rejected(make_trades):
    trades = make_trades([10.0], [1.0]).with_columns(
        pl.col("ts").dt.replace_time_zone(None)
    )
    with pytest.raises(ValueError, match="timezone-naive"):
        resample(trades, kind="tick", threshold=1)


def test_non_datetime_ts_is_rejected(make_trades):
    trades = make_trades([10.0], [1.0]).with_columns(
        pl.col("ts").dt.cast_time_unit("ns").cast(pl.Int64)
    )
    with pytest.raises(TypeError, match="Datetime"):
        resample(trades, kind="tick", threshold=1)


def test_non_utc_timestamps_are_converted_not_shifted(make_trades):
    base = make_trades([10.0, 10.0], [1.0, 1.0])
    ny = base.with_columns(pl.col("ts").dt.convert_time_zone("America/New_York"))
    bars = resample(ny, kind="volume", threshold=1.0)
    assert bars.schema["start_ts"] == pl.Datetime("ns", "UTC")
    assert bars["start_ts"].to_list() == base["ts"].to_list()


def test_microsecond_input_is_promoted_to_nanoseconds(make_trades):
    base = make_trades([10.0, 10.0], [1.0, 1.0]).with_columns(
        pl.col("ts").dt.cast_time_unit("us")
    )
    assert validate_trades(base).schema["ts"] == pl.Datetime("ns", "UTC")


def test_nulls_are_rejected(make_trades):
    trades = make_trades([10.0, 10.0], [1.0, 1.0]).with_columns(
        pl.when(pl.int_range(pl.len()) == 1)
        .then(None)
        .otherwise(pl.col("price"))
        .alias("price")
    )
    with pytest.raises(ValueError, match="null"):
        resample(trades, kind="volume", threshold=1.0)


# --- NaN and infinity: the checks that a sign test cannot catch ------------


@pytest.mark.parametrize("bad_price", [NAN, INF, -INF])
def test_non_finite_price_is_rejected(make_trades, bad_price):
    with pytest.raises(ValueError, match="finite"):
        resample(make_trades([bad_price, 10.0], [1.0, 1.0]), kind="volume", threshold=1.0)


@pytest.mark.parametrize("bad_size", [NAN, INF, -INF])
def test_non_finite_size_is_rejected(make_trades, bad_size):
    with pytest.raises(ValueError, match="finite"):
        resample(make_trades([10.0, 10.0], [1.0, bad_size]), kind="volume", threshold=1.0)


def test_nan_row_is_rejected_even_though_it_is_not_negative():
    # NaN <= 0 evaluates to False, which is exactly why finiteness is checked
    # separately from sign. This test is the regression guard for that hole.
    trades = pl.DataFrame(
        {
            "ts": (
                pl.Series(
                    [0, 1_000_000], dtype=pl.Int64
                ).cast(pl.Datetime("ns")).dt.replace_time_zone("UTC")
            ),
            "price": pl.Series([10.0, NAN], dtype=pl.Float64),
            "size": pl.Series([1.0, 1.0], dtype=pl.Float64),
        }
    )
    with pytest.raises(ValueError, match="finite"):
        resample(trades, kind="volume", threshold=1.0)


@pytest.mark.parametrize("bad_size", [0.0, -1.0])
def test_non_positive_size_is_rejected(make_trades, bad_size):
    with pytest.raises(ValueError, match="size"):
        resample(make_trades([10.0, 10.0], [1.0, bad_size]), kind="volume", threshold=1.0)


def test_non_positive_price_is_rejected(make_trades):
    with pytest.raises(ValueError, match=r"price"):
        resample(make_trades([0.0, 10.0], [1.0, 1.0]), kind="volume", threshold=1.0)


# --- threshold validation ---------------------------------------------------


@pytest.mark.parametrize("bad_threshold", [0, -5.0, -0.1])
def test_non_positive_threshold_is_rejected(make_trades, bad_threshold):
    with pytest.raises(ValueError, match="positive"):
        resample(make_trades([10.0], [1.0]), kind="volume", threshold=bad_threshold)


@pytest.mark.parametrize("bad_threshold", [NAN, INF, -INF])
def test_non_finite_threshold_is_rejected(make_trades, bad_threshold):
    # NaN passes `threshold <= 0` because every comparison with NaN is False.
    with pytest.raises(ValueError, match="finite"):
        resample(make_trades([10.0], [1.0]), kind="volume", threshold=bad_threshold)


def test_bool_threshold_is_rejected(make_trades):
    with pytest.raises(TypeError, match="threshold"):
        resample(make_trades([10.0], [1.0]), kind="volume", threshold=True)  # type: ignore[arg-type]


def test_non_numeric_threshold_is_rejected(make_trades):
    with pytest.raises(TypeError, match="threshold"):
        resample(make_trades([10.0], [1.0]), kind="volume", threshold="100")  # type: ignore[arg-type]


# --- dispatch validation ----------------------------------------------------


def test_unknown_kind_is_rejected(make_trades):
    with pytest.raises(ValueError, match="unknown kind"):
        resample(make_trades([10.0], [1.0]), kind="zeitgeist", threshold=1)  # type: ignore[arg-type]


def test_missing_threshold_is_rejected(make_trades):
    with pytest.raises(ValueError, match="requires threshold"):
        resample(make_trades([10.0], [1.0]), kind="volume")


def test_threshold_bars_reject_every(make_trades):
    with pytest.raises(ValueError, match="not every"):
        resample(make_trades([10.0], [1.0]), kind="volume", threshold=1, every="1m")


def test_time_bars_reject_threshold(make_trades):
    with pytest.raises(ValueError, match="not threshold"):
        resample(make_trades([10.0], [1.0]), kind="time", every="1m", threshold=1)


def test_time_bars_require_every(make_trades):
    with pytest.raises(ValueError, match="requires every"):
        resample(make_trades([10.0], [1.0]), kind="time")


# --- accepted input that only looks invalid ---------------------------------


def test_duplicate_timestamps_are_allowed_and_keep_input_order(make_trades):
    base = make_trades([10.0, 11.0, 12.0], [1.0, 1.0, 1.0])
    stamps = base["ts"].to_list()
    duplicated = base.with_columns(
        pl.Series("ts", [stamps[0], stamps[1], stamps[1]], dtype=pl.Datetime("ns", "UTC"))
    )
    bars = resample(duplicated, kind="volume", threshold=3.0)
    assert bars.height == 1
    assert bars["close"][0] == 12.0


def test_list_input_is_rejected_with_a_clear_type_error():
    with pytest.raises(TypeError, match=r"polars\.DataFrame"):
        resample([{"ts": 1, "price": 1.0, "size": 1.0}], kind="tick", threshold=1)  # type: ignore[arg-type]


# --- interval validation ----------------------------------------------------


@pytest.mark.parametrize(
    "bad_every",
    ["5x", "1i", "", "m5", "0m", "00s", "5 m"],
)
def test_unparseable_intervals_are_rejected(make_trades, bad_every):
    with pytest.raises(ValueError):
        resample(make_trades([10.0], [1.0]), kind="time", every=bad_every)


def test_non_string_interval_is_rejected(make_trades):
    with pytest.raises(TypeError, match="duration string"):
        resample(make_trades([10.0], [1.0]), kind="time", every=5)  # type: ignore[arg-type]
