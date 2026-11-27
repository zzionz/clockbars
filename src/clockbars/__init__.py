"""clockbars — bars sampled on the market's own clock.

Converts a stream of trades into tick, volume, dollar, or time bars, using a
deterministic restart-from-zero rule with vectorised aggregation in Polars.

    >>> import clockbars
    >>> bars = clockbars.resample(trades, kind="dollar", threshold=1_000_000)

Two execution paths share that one rule: `resample` builds bars in batch
from a Polars frame, and `BarBuilder` builds the same bars one trade at a
time for live streams. A parity test asserts the two agree byte for byte,
session cuts included.
"""

from __future__ import annotations

from clockbars.builder import Bar, BarBuilder, bars_to_frame
from clockbars.core import resample, validate_trades
from clockbars.synthetic import synthetic_ticks

__all__ = [
    "Bar",
    "BarBuilder",
    "__version__",
    "bars_to_frame",
    "resample",
    "synthetic_ticks",
    "validate_trades",
]

__version__ = "1.1.0"
