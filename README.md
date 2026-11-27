# clockbars

**Bars on the market's clock.** Every bar closes when a set amount of trading
has actually happened. Quiet minutes produce nothing, busy ones produce
plenty. Tick, volume, and dollar bars in Polars, built the same way in batch
and in a live stream.

[![CI](https://github.com/zzionz/clockbars/actions/workflows/ci.yml/badge.svg)](https://github.com/zzionz/clockbars/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![Typed](https://img.shields.io/badge/typing-mypy_strict-blue)
![License](https://img.shields.io/badge/license-MIT-green)

---

Most pipelines build bars by clock: take all the trades in each minute, call it
a row, repeat. Markets do not produce information at a constant rate, so the
result is uneven by construction. The first and last half hour of a session
carry several times the volume of midday, which means a 1-minute bar at 09:31
and a 1-minute bar at 12:31 agree in duration while differing by an order of
magnitude in content. Downstream code treats them as equal rows anyway, and
every statistic computed on the series inherits the unevenness.

Sampling by activity is the old fix. Mandelbrot and Taylor noted in 1967 that
price changes over a fixed number of transactions behave far better than price
changes over fixed time. Clark's 1973 model treated volume as the clock.
Easley, López de Prado, and O'Hara built a microstructure framework on
equal-volume buckets and named the idea: the volume clock.

The comparison has also been measured on real data, which is worth more than
any argument. Oomen (2006) compared calendar, business, and transaction-time
sampling on IBM transaction data from 2000 to 2004. Sampling in transaction
time cut the mean squared error of the realized variance estimator by about
5 percent on average, and by as much as 40 percent on days with irregular
trading. The clock choice is a measurable property of your estimator.

This problem has existing software; see [Related work](#related-work). What
none of it offers is a Polars-native path: one call from `DataFrame` to
`DataFrame` with a fixed schema, ready to drop into a pipeline that already
speaks Polars. That is the gap this fills. Two properties come with it that
the free ecosystem currently lacks: a streaming builder that provably
produces the same bars as the batch path, and session-aware construction so a
bar never straddles an exchange close.

![Time bars vs dollar bars](docs/hero.png)

Same 400,000-trade synthetic stream, 20 sessions. Top row: volume per bar is
a long right tail for time bars and a spike for dollar bars. Bottom row: both
schemes across one session, where time-bar volume traces the intraday U and
dollar bars hug a flat line. The stream is synthetic (seeded, reproducible)
because trade data is licensed; the mechanism is real.

## Install

```bash
pip install clockbars
```

For development:

```bash
git clone https://github.com/zzionz/clockbars.git
cd clockbars
uv sync --all-extras
uv run pytest -q   # 172 passed, 100% coverage enforced
```

CI runs the full gate set on every push: ruff, strict mypy, and the test
suite with coverage required at 100 percent, on Ubuntu and Windows across
Python 3.11 through 3.14. The package ships a `py.typed` marker, so if you
use clockbars under mypy you get its types, not `Any`.

## Quickstart

```python
import polars as pl
from clockbars import resample

trades = pl.read_parquet("trades.parquet")  # ts, price, size (sorted by ts)

dollar = resample(trades, kind="dollar", threshold=1_000_000)
volume = resample(trades, kind="volume", threshold=500)
ticks  = resample(trades, kind="tick",   threshold=250)
minute = resample(trades, kind="time",   every="1m")
```

The same stream can be cut so that no bar straddles a session boundary. Pass
the rollover times and accumulation restarts at each one:

```python
from datetime import datetime, timezone

rollovers = pl.Series([datetime(2026, 1, 6, 23, 0, tzinfo=timezone.utc)])

# A dollar bar never spans the overnight close, and every session's final
# bar is flagged partial if it fell short of the threshold.
daily = resample(trades, kind="dollar", threshold=1_000_000,
                 session_starts=rollovers)
```

The same rule runs one trade at a time, for live streams:

```python
from clockbars import BarBuilder

builder = BarBuilder(kind="dollar", threshold=1_000_000)
for ts, price, size in feed:
    bar = builder.push(ts, price, size)   # completed bar, or None
    if bar is not None:
        emit(bar)

trailing = builder.flush()                # the final partial bar
```

A parity test asserts that `BarBuilder` over the batch input agrees with
`resample` on everything that decides what a bar is: boundaries, ids,
timestamps, OHLC, counts, partial flags, at 200,000 trades, for all four
kinds, session cuts included. Per-bar volume and notional agree to within
float-summation order (at most 1e-12 relative). A vectorised group sum and a
sequential accumulator sum the same numbers in different orders, so claiming
bit equality there would be a claim floating point cannot support. The bar
your backtest computed and the bar your live process computed are the same
object, or you find out in a test.

All four kinds return the same schema, so nothing downstream has to branch on
how the bars were made:

| Column | Meaning |
| :--- | :--- |
| `bar_id` | zero-based bar index |
| `start_ts`, `end_ts` | first and last trade timestamp in the bar |
| `duration` | `end_ts - start_ts` |
| `open`, `high`, `low`, `close` | first price, max, min, last price |
| `volume` | units traded |
| `notional` | price × size summed |
| `vwap` | `notional / volume` |
| `n_trades` | trade count |
| `is_partial` | `True` only if the bar fell short of its threshold |

## The rule, and the decisions inside it

A bar closes on the trade that pushes its running total to the threshold, and
that trade is included in the closing bar. Each bar then restarts from zero.
Everything else follows:

1. **A closed bar's total is always at least its threshold.** It can overshoot
   only by the size of its own final trade.
2. **Overshoot stays in the bar that earned it.** The obvious vectorised
   implementation is a global cumulative sum, and it has a defect: it charges
   each bar's overshoot to the following bar, which then closes below
   threshold in the middle of the series. In one 128-bar run built that way,
   62 bars came out flagged partial. Restarting from zero keeps every bar
   answerable on its own. `test_overshoot_is_not_carried_into_the_next_bar`
   pins this.
3. **The final bar of any segment is kept, flagged, and never dropped.**
   `is_partial=True` means "the segment ended before this bar filled": the end
   of the stream, or the end of a session. Without session cuts the whole
   stream is one segment, so only the last bar can be partial. Throwing a
   partial bar away would silently rewrite history.
4. **Naive timestamps are rejected.** If your data says 14:30 with no time
   zone, you get an error telling you to declare the zone. Silently assuming
   UTC is how a DST bug gets born.
5. **NaN and inf are rejected before they can look like data.** In
   floating-point comparison, `NaN <= 0` is `False`, so a sign check alone
   accepts NaN, and one NaN price poisons every bar after it. Finiteness is
   checked separately from sign.
6. **Threshold comparisons carry a `1e-9` relative tolerance.** Summing a few
   thousand floats lands on `499.9999999999999` more often than anyone wants
   to admit. Without the tolerance, those become phantom partial bars.
7. **Duplicate timestamps are fine.** Trades share timestamps in real data.
   Input order is preserved within a bar, so `open`/`close` are stable.
8. **Input must be sorted.** Unsorted input is an error, not a silent
   `sort()`. Sorting implicitly is a policy decision about your data, and it
   is not this library's to make.

Time bars follow the same schema with `is_partial` always `False`. Windows
are half-open `[start, start + every)` and align to wall-clock boundaries: a
frame that begins at 09:30:30 still produces windows that begin on the
minute. Compound intervals like `1h30m` are summed. Empty windows are never
emitted. Session cuts do not apply to time bars, since the clock already cuts
them; passing both raises instead of guessing a precedence.

## What the tests actually check

Not "the code runs." The properties that make the output trustworthy:

- volume, notional, and trade counts are conserved exactly across bars;
- bars are chronological and non-overlapping, with `bar_id` contiguous from zero;
- every closed bar meets its threshold, and overshoots by at most one trade;
- only the final bar of a segment can be partial, and the flag agrees with the
  bar's own total;
- OHLC and VWAP are internally consistent (`low ≤ vwap ≤ high`);
- identical input yields identical output;
- bars never straddle a session boundary, with the boundary trade going to
  the new session;
- the streaming `BarBuilder` matches `resample` exactly on boundaries,
  timestamps, OHLC, counts, and flags, and to 1e-12 relative on sums;
- the whole suite of invariants also runs at 200,000-trade scale, streaming
  parity included.

Plus every validation rule above, each with the input that breaks it.

## What the evidence says

Three results from three sources. The first is ours and reproducible from a
seed. The other two are published studies on real data, and they point in
opposite directions. All three belong here.

`examples/make_hero.py` builds the figure and prints this table (seed 7,
dollar threshold matched to the median 1-minute notional so the comparison is
fair):

| Sampling | Bars | Volume CV | Excess kurtosis |
| :--- | ---: | ---: | ---: |
| time (1m) | 7,800 | 0.7734 | 1.422 |
| dollar | 10,232 | 0.0255 | −0.007 |

Volume CV is the coefficient of variation of per-bar volume: near zero means
each bar carries about the same amount of trading. On this stream, dollar
bars hold per-bar volume roughly 30 times steadier than time bars.

On real data, the stakes are higher than shape. Oomen (2006) measured
realized variance under calendar, business, and transaction-time sampling on
IBM transaction data, 2000 to 2004. Transaction time won on every single day
in the sample: the MSE of the estimator fell by about 5 percent on average
and by as much as 40 percent on days with irregular trading. That is the
cost of the old way, priced by someone else's measurement.

The newest study goes the other way, and citing it is the difference between
documentation and marketing. Grądzki, Wójcik, and Lessmann (2025) compared
sampling schemes for BTC and ETH algorithmic trading over 2018 to 2023 with
transaction costs, out of sample. CUSUM-filtered sampling was the only scheme
with consistently positive performance. Dollar bars were the weakest of
everything they tested. Their diagnosis: crypto exchange volume is inflated
by wash trading, so a dollar bar accumulates a partly fabricated signal.

Read together, the lesson is precise. The mechanical claim is settled:
sampling on an honest activity signal gives bars of near-constant information
content, and clock sampling does not. Whether a given activity signal is
honest, and whether an honest one is enough to help a given strategy, are
empirical questions with published answers on both sides. Clockbars makes
the choice cheap to test, correct by construction, and identical between
backtest and production. It does not promise the market will cooperate.

## pandas input

If pandas is installed, a `pandas.DataFrame` is accepted and held to the same
validation, with one caveat: Polars converts pandas' timezone-aware datetime
columns through pyarrow, so pandas users need pyarrow too. Naive pandas
timestamps are rejected, same as naive Polars ones.

## Limitations

- Thresholds are absolute. Choosing one is still your job; a defensible
  default is the median per-minute notional over your sample, which is what
  the example does.
- Threshold bars are not uniform in time. Anything that assumes a fixed bar
  duration (annualisation factors, some indicator definitions) needs
  rethinking before it runs on them.
- Imbalance and run bars are not implemented. Fixed-threshold versions exist
  in bar-forge and EWMA-adaptive versions in mlfinpy; a Polars-native
  implementation is on the roadmap.
- Session awareness means *your* boundaries: the library ships no exchange
  calendars. You provide the rollover times; `resample` guarantees the cut.
  Rollover contract-adjustment is still your problem.
- Bar segmentation scans over bars, at `O(n_bars · log n_trades)`. With a
  realistic threshold there are far fewer bars than trades. A tick threshold
  of 1 on a million trades is inherently one bar per trade and will not be
  fast; nothing makes that a good idea.

## Roadmap

- [ ] Imbalance bars, EWMA-adaptive (fixed-threshold versions exist in
      bar-forge; adaptive ones in mlfinpy; a Polars port needs a reason
      beyond porting)
- [ ] Run bars
- [ ] CUSUM-filtered sampling, the scheme that won in Grądzki et al. (2025)
- [ ] Out-of-core chunked batch construction

## Related work

Three implementations matter, and pretending otherwise would be the first
sign of a promotional README.

**[bar-forge](https://github.com/bmouler/bar-forge)** (Mouler, 2026) is the
closest contemporary: tick, volume, dollar, and fixed-threshold imbalance
bars over NumPy trade records, plus strictly causal normalization
transforms, with 100 percent coverage, strict mypy, and a mutation-testing
run (1,208 mutants, 92.96 percent killed). Its bar conventions agree with
ours where they overlap, which we read as both libraries deriving from the
same source. It does not do sessions, streaming, or DataFrames, and it cites
no literature. We do the first three and ship [a paper](docs/paper.md).

**mlfinpy** is a free port of the mlfinlab data structures, including the
EWMA-adaptive imbalance and run bars from López de Prado ch. 2, which we do
not yet implement.

**[mlfinlab](https://github.com/hudson-and-thames/mlfinlab)** (Hudson &
Thames) implements the full López de Prado catalogue; it began open source
and is now a commercial product.

Practitioner tooling has followed the same trail: Databento's 2024 guest
series from Double River Investments builds volume and dollar bars in Polars
with a Rust kernel, which is a good sign for the Polars-native path and a
complement to this library.

## Paper

A short paper covering why bar construction matters, the history behind it,
the implementation errors this library exists to prevent, and why batch and
streaming must provably agree, is in
[docs/paper.md](docs/paper.md).

If you use clockbars in research, cite it:

```bibtex
@software{clockbars,
  author = {zzionz},
  title  = {clockbars: bars sampled on the market's own clock},
  url    = {https://github.com/zzionz/clockbars},
  year   = {2026}
}
```

## License

MIT. See [LICENSE](LICENSE).
