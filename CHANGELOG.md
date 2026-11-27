# Changelog

Notable changes to clockbars are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[SemVer](https://semver.org/).

## [1.1.0] - 2026-11-27

### Added

- Imbalance bars, EWMA-adaptive, for tick, volume, and dollar metrics.
  Signs follow the tick rule (+1 rise, -1 fall, flat carries, first trade
  +1), the metric accumulates left to right from zero, and the bar closes
  when the signed metric reaches E[T] * |e|, both estimates updated only on
  completed bars. The user seeds both; the data refines them.
- Runs bars: a bar closes when the streak of consecutive same-signed trades
  reaches E[T] * max(P, 1 - P), with P an EWMA of the buy fraction. A
  bar's first trade starts a fresh streak. Alternating flow never builds a
  streak, so a bar can stay open until a session cut, which is the rule's
  honest output and a diagnostic.
- CUSUM-filtered bars (Page, 1954): the bar closes when the cumulative
  price change since the last reset drifts h above its running minimum or
  h below its running maximum. Both accumulators reset on close and on a
  session cut. h is in price units.
- `push_chunk`: the out-of-core entry point. Vectorised per chunk,
  honouring carried state, with the prepend-carry trick that reproduces the
  one-trade-at-a-time order of addition exactly. A bar open at a chunk
  boundary continues across it and closes with its true counts, so every
  adaptive estimate stays on the batch trajectory. 239 tests at 100 percent
  statement coverage, strict mypy, CI on Ubuntu and Windows across Python
  3.11 through 3.14.
- Dark figures in one palette: near-black ground, crimson for the activity
  clock, silver for the wall clock. `examples/make_figures.py` regenerates
  both `docs/hero.png` and `docs/cusum.png` from a seed.
- Three real bugs found and fixed in the process, each now pinned by a
  regression test: double-signing in the chunked path (the metric arrived
  already signed), carried bars closing with their chunk-local length
  instead of their true length (which corrupted every adaptive estimate
  after a chunk-spanning closure), and a stale has_open reset that
  silently disabled the true-length fix.

### Changed

- README rewritten around the nine kinds and three execution paths, with
  real terminal output in the quickstart, the overshoot worked example as
  a table, the flowchart updated for the new kinds, and no roadmap. The
  paper's related-work section drops its opening line, per a reader's
  suggestion.

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
