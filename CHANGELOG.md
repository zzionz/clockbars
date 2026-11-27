# Changelog

Notable changes to clockbars are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[SemVer](https://semver.org/).

## [1.0.0] - 2026-11-27

Initial public release.

### Added

- `resample()` builds tick, volume, dollar, and time bars from a trade
  stream, all returning one shared 13-column schema. Threshold bars accept
  `session_starts`: accumulation restarts at each boundary, a bar never
  straddles a rollover, and a trade exactly on a boundary belongs to the
  new session.
- `BarBuilder` builds the same bars one trade at a time for live streams,
  with `close_session()` as the streaming counterpart of `session_starts`
  and `flush()` for the trailing bar. `bars_to_frame()` collects bars with
  exactly `resample()`'s schema and dtypes.
- Restart-from-zero threshold rule: the crossing trade closes its bar, a
  closed bar's total always meets its threshold, and only the final bar
  of a segment can be partial: flagged, never dropped.
- Strict input validation: timezone-aware timestamps normalized to
  `Datetime("ns", "UTC")`, sortedness required, and finiteness checked
  separately from sign so NaN and inf cannot pass a sign check.
- Deterministic seeded trade generator (`synthetic_ticks`) with one
  session per business day, U-shaped arrivals, size clustering at the open
  and close, and volatility scaling with trade size.
- pandas input adapter, held to the same validation as Polars input.
- 172 tests: conservation, contiguity, threshold, partial-flag, OHLC and
  VWAP consistency, determinism, validation, the pandas path, session
  cuts, float-accumulation tolerance, streaming parity with the batch
  path, and 200,000-trade scale runs.
- CI on Ubuntu and Windows across Python 3.11 through 3.14: ruff, strict
  mypy, and pytest with statement coverage required at 100 percent.
- `py.typed` marker, so downstream type checkers see the package's types.
- `examples/make_hero.py` regenerates the README figure and its table
  from a seed.
- `docs/paper.md`: the fifty-nine-year history of activity sampling, its
  one genuinely contested claim, and the failure modes the test suite
  pins, with published measurements cited alongside the library's own
  reproducible numbers.
- `CITATION.cff`, `CONTRIBUTING.md`, issue and PR templates, and a
  dependabot schedule.

### Design decisions worth knowing before depending on this

- Overshoot is not carried into the next bar. A global cumulative sum
  would dock the following bar for the previous bar's overshoot and close
  it below threshold mid-series; clockbars restarts each bar at zero.
- Threshold comparisons use a `1e-9` relative tolerance, because summed
  floats land on `499.9999999999999` and a strict comparison would flag
  phantom partial bars.
- A fractional tick threshold rounds up: 2.5 closes bars of 3 trades.
- Time-bar windows are half-open `[start, start + every)`, compound
  intervals like `1h30m` are summed, and alignment is to the epoch, not
  to the first trade.
- Batch and streaming must agree: exactly on bar boundaries, timestamps,
  OHLC, counts, and partial flags; per-bar volume, notional, and vwap to
  within float-summation order (at most `1e-12` relative), because a
  vectorised group sum and a sequential accumulator sum the same floats
  in different orders.
- A trade exactly on a session boundary belongs to the new session, and a
  session's final short bar is flagged partial, never merged into the next
  day.

[1.0.0]: https://github.com/zzionz/clockbars/releases/tag/v1.0.0
