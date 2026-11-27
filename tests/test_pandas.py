"""The pandas adapter earns its keep by passing the same validation as Polars.

`_coerce` accepts pandas frames when pandas is installed. Untested adapters
are worse than no adapter, so this file holds it to the identical contract:
same bars as the Polars path, same rejection of naive timestamps.
"""

from __future__ import annotations

import pandas as pd
import polars as pl
import pytest

from clockbars import resample


def _to_pandas(frame: pl.DataFrame) -> pd.DataFrame:
    """Rebuild the frame in pandas without needing pyarrow for the reverse path."""
    stamps = frame["ts"].dt.cast_time_unit("ns").cast(pl.Int64).to_list()
    return pd.DataFrame(
        {
            "ts": pd.to_datetime(stamps, unit="ns", utc=True),
            "price": frame["price"].to_list(),
            "size": frame["size"].to_list(),
        }
    )


def test_tz_aware_pandas_input_matches_the_polars_result(make_trades):
    base = make_trades([10.0, 11.0, 12.0, 13.0], [5.0, 5.0, 5.0, 5.0])
    bars = resample(_to_pandas(base), kind="volume", threshold=10.0)
    expected = resample(base, kind="volume", threshold=10.0)
    assert bars.equals(expected)


def test_pandas_input_survives_validation(make_trades):
    base = make_trades([10.0, 10.0], [1.0, 1.0])
    bars = resample(_to_pandas(base), kind="tick", threshold=1)
    assert bars.schema["start_ts"] == pl.Datetime("ns", "UTC")
    assert bars["n_trades"].sum() == base.height


def test_naive_pandas_input_is_rejected(make_trades):
    pdf = _to_pandas(make_trades([10.0], [1.0]))
    pdf["ts"] = pdf["ts"].dt.tz_localize(None)
    with pytest.raises(ValueError, match="timezone-naive"):
        resample(pdf, kind="volume", threshold=1.0)


def test_missing_pandas_raises_a_type_error(monkeypatch, make_trades):
    # The adapter's ImportError branch: same clear message, no pandas needed.
    # Setting the sys.modules entry to None makes `import pandas` raise.
    import sys

    monkeypatch.setitem(sys.modules, "pandas", None)
    with pytest.raises(TypeError, match=r"polars\.DataFrame"):
        resample([{"ts": 1, "price": 1.0, "size": 1.0}], kind="tick", threshold=1)  # type: ignore[arg-type]


def test_non_frame_objects_still_fail_with_a_clear_type_error():
    with pytest.raises(TypeError, match=r"polars\.DataFrame"):
        resample([{"ts": 1, "price": 1.0, "size": 1.0}], kind="tick", threshold=1)  # type: ignore[arg-type]
