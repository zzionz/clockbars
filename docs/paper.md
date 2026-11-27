# Bars Are a Modeling Decision

**Nine sampling clocks, three execution paths, one shared contract, and four failure modes that hide in plausible-looking output**

zzionz, Modeus Lab
November 2026

---

## Abstract

On IBM transaction data from 2000 to 2004, sampling in transaction time
instead of on the wall clock cut the mean squared error of a standard
variance estimator by about 5 percent on average, and by as much as 40
percent on days with irregular trading (Oomen, 2006). Almost nobody changed
their pipeline. The newest large comparison cuts the other way: on wash-
traded crypto volume, dollar bars were the weakest sampling scheme tested,
and CUSUM filtering won (Grądzki, Wójcik, and Lessmann, 2025). We hold to
the claim both studies support: sampling on an honest activity signal gives
bars of near-constant information content, and clock sampling does not.
`clockbars` implements nine sampling clocks in Polars, across three
execution paths (batch, one trade at a time, and chunked for streams that
do not fit in memory), held to agreement on everything that decides what a
bar is. No new theory. What it contributes is a written contract, each
clause pinned by a test, and a record of four failure modes that produce
plausible-looking output while meaning something else.

---

## 1. The step nobody decides

On IBM transaction data, 2000 to 2004, transaction-time sampling beat
calendar sampling on every single day in the sample (Oomen, 2006). The
improvement was not marginal: roughly 5 percent of estimator efficiency on
average, up to 40 percent when trading was irregular. Griffin and Oomen
(2008) extended the comparison to tick time and showed that the return
dynamics differ enough between sampling schemes for bias-correction
procedures built on one to fail on another. The scheme is part of the
model.

The industry answer to this evidence has been to keep using 1-minute bars.

This is not an accident of ignorance. It is what happens when the data
vendor ships the bars and nobody downstream questions the choice. A
1-minute bar at 09:31 and a 1-minute bar at 12:31 agree in duration and
differ by an order of magnitude in the amount of trading they contain. The
half-hours after the open and before the close carry several times the
volume of midday. Every statistic computed on the resulting series inherits
the unevenness. On a 400,000-trade synthetic stream with a realistic
intraday profile, the coefficient of variation of per-bar volume is
**0.7734** for 1-minute bars. Sampling the same stream so that each bar
carries an equal amount of traded notional brings that number to
**0.0255**. Nothing about the market changed. Only the sampling rule moved.

## 2. The idea is older than most of the people using it

Mandelbrot and Taylor (1967) put it first: price changes over a fixed
number of transactions may follow a Gaussian distribution, while price
changes over fixed time follow a heavy-tailed law, and there is no
contradiction because the number of transactions in any period is itself
random. Clark (1973) formalized the subordinated process with traded
volume as the subordinator and showed that conditioning on volume
collapses much of the apparent non-normality in cotton futures. Easley and
O'Hara (1992) supplied the microstructure grounding: time between trades
is informative, because informed traders trade when they have reason to.

Ané and Geman (2000) made the clock itself the object of estimation. The
cumulative number of trades, they argued, is a better stochastic clock
than volume. Easley, López de Prado, and O'Hara (2012a) built volume
buckets into the unit of analysis for flow toxicity; their companion piece
(2012b) named the concept. López de Prado (2018, ch. 2) consolidated the
applied side into tick, volume, dollar, imbalance, and run bars.

So when a pipeline samples by dollar amount instead of by minute, it is
applying a specific, citable model of market time that predates stored-
program computers.

## 3. The part that is actually contested

Ané and Geman's strong claim, that an activity clock makes returns
approximately Gaussian, has been challenged directly. Murphy and Izzeldin
(2006, revised 2010) re-examined the nonparametric procedure used to
recover the moments of the latent clock and found it unstable. On their
data, conditioning improved normality, and normality was still rejected
throughout.

The newest entry complicates the picture further. Grądzki, Wójcik, and
Lessmann (2025) compared sampling schemes for BTC and ETH algorithmic
trading over 2018 to 2023, out of sample, with transaction costs. CUSUM-
filtered sampling, which reacts to price movement, was the only scheme
with consistently positive performance. Dollar bars, the default this
library ships with, were the weakest of everything tested. Their
diagnosis: crypto volume is inflated by wash trading, so the activity
signal a dollar bar accumulates is partly fabricated. The clock inherits
the lie.

We take no side on Gaussianization, partly because our own demonstration
runs on synthetic data and could not settle it. The weaker claim nobody
disputes is the one that matters for engineering: sampling on an honest
activity signal produces bars of near-constant information content, and
clock sampling does not. Verify the signal before you trust the clock
built on it.

## 4. Nine clocks, one contract

Every kind in `clockbars` closes a bar, includes the closing trade, and
restarts from zero. What differs is what closes it.

The **threshold kinds** close when a running total of the metric reaches
a fixed amount: trade count for tick, units for volume, quote-currency
notional for dollar. Formally, for metric values $m_1, m_2, \ldots$ and
threshold $T$, trade $j$ belongs to bar $b$ if it is the first trade for
which the running total from the bar's start reaches $T$, or an earlier
trade in that bar. Three properties follow, each enforced by test:

1. Every closed bar has total $\ge T$, exceeded by at most its own final
   trade.
2. Only the final bar of a segment may fall short, and it is flagged
   `is_partial`, never dropped.
3. Volume, notional, and trade counts are conserved exactly.

The **flow-sampled kinds** close on order flow, not on a fixed
quantity of it. All three share the tick rule for signs: +1 on a rise
against the previous trade, -1 on a fall, a flat price carries the
previous sign, and a stream's first trade is +1. Signs are a property of
the stream and carry across bar and session boundaries.

**Imbalance bars** (López de Prado ch. 2) accumulate the signed metric
and close when it reaches $E[T] \cdot |e|$, where $E[T]$ is an EWMA
estimate of completed bar lengths and $e$ an EWMA estimate of per-trade
signed metric. Only completed bars update the estimates, so the rule is
causal by construction. **Runs bars** close when a streak of consecutive
same-signed trades reaches $E[T] \cdot \max(P, 1-P)$, with $P$ an EWMA of
the buy fraction. **CUSUM bars** (Page, 1954) close when the cumulative
price change since the last reset drifts $h$ above its running minimum or
$h$ below its running maximum. Both accumulators reset on close and on a
session cut.

Below these sit four smaller decisions, documented because an undocumented
decision in a data library becomes someone else's silent bug: timestamps
must be timezone-aware and are normalized to UTC (naive input is rejected
with instructions); duplicate timestamps are allowed with input order
preserved; time-bar windows are half-open, epoch-aligned, and accept
compound intervals like `1h30m`; and threshold comparisons carry a
`1e-9` relative tolerance, for reasons in §5.

## 5. Four ways this goes quietly wrong

Each of these produced plausible-looking output during development. All
four are now pinned by regression tests.

**(a) Overshoot charged to the wrong bar.** The obvious vectorised
implementation assigns each trade to `floor(cumulative_metric / T)`. It
is one expression, it is fast, and it is wrong in a way that only shows up
mid-series. Threshold $T=250$, trades $200, 100, 10 \times 27$: the
correct rule yields bars of $300, 250, 20$. The global-cumsum rule yields
$300, 200, 70$, because bar 0 overshot by 50 and the next bar was docked
for it. In one 128-bar run built this way, 62 bars were flagged partial.
The fix is to accumulate per bar and restart at zero, at
$O(n_{\text{bars}} \cdot \log n_{\text{trades}})$.

**(b) Phantom partial bars from floating point.** Summing a few thousand
floats does not land exactly on the threshold. It lands on values like
$499.9999999999999$. A strict comparison then flags a bar that
mathematically closed. The fix is a relative tolerance on the threshold
comparison. It must be relative because a dollar threshold of $10^6$ and
a volume threshold of $10^2$ fail at different absolute offsets.

**(c) NaN passes a sign check.** `NaN <= 0` is `False`, so a validation
rule of the form "reject non-positive prices" accepts NaN, and one NaN
price poisons every bar built after it. Silently, because bar
construction does not crash on NaN; it computes with it. Finiteness must
be checked separately from sign.

**(d) A bar carried across chunk boundaries closes with the wrong length.**
This bug appeared in the out-of-core path and took the longest to find
because the symptom is a two-trade divergence at bar 5, not a crash. When
a chunk boundary falls inside an adaptive bar, the bar's EWMA update on
closure used the trade count visible inside that chunk, not the bar's
true length across all chunks. Every subsequent estimate diverged from
the batch trajectory, and because the estimates feed the closure targets
themselves, the divergence compounded. The fix is to carry the bar's true
trade count across chunk boundaries. The diagnosis required a full
traced replay of both paths side by side to find a single `has_open =
False` line that sat between the search and the branch that consumed it,
silently disabling the true-length logic after it was written.

A fifth issue, smaller but the same species: the synthetic generator in an
early version used a hardcoded list of ten date offsets, so requesting
more than ten sessions silently interleaved two sessions into the same
calendar day. The figure quietly had half the intended number of bars.
An invariant that had no reason to fail caught it.

## 6. One rule, three execution paths

Bar construction runs in three places: in research over a historical
file, in production one trade at a time, and over archived data that does
not fit in memory. The production path is usually a different
implementation from the research path, and the two drift. A bar that
differs between backtest and live is not a rounding problem. It is a
state bug or a lookahead bug wearing a rounding problem's clothes.

`clockbars` implements all three paths against the same rule and holds
them to agreement on everything that decides what a bar is: boundaries,
ids, timestamps, OHLC, counts, partial flags. At 200,000 trades across
all nine kinds, with slice boundaries that include single-trade chunks
and chunks that split a bar mid-streak. Per-bar volume, notional, and vwap
agree to within float-summation order (at most 1e-12 relative), stated
plainly, because a vectorised group sum and a sequential accumulator sum
the same floats in different orders.

Segmentation, by contrast, is bit-exact, because all three paths cumulate
the metric left to right within a bar. The chunked path uses a
prepend-carry: the bar's partial sum leads the chunk, and `np.cumsum` over
the pair reproduces sequential addition exactly. This is what makes the
true-length fix from §5(d) work at all. Without it, the carried partial
and the batch cumsum would differ by ulps that compound through the
adaptive estimates.

Parity is a strong test because the three implementations fail
differently. The batch path is most likely to break a convention (§5a
is a batch-only disease). The streaming path is most likely to break
state. The chunked path is most likely to lose carried context. Each
path is a regression test for the other two.

Session breaks are cheap in all three paths. Pass `session_starts` to
the batch, call `close_session()` between chunks, and the same rule cuts
the bar at the boundary. The boundary trade belongs to the new session,
bars never straddle a rollover, and a session's final short bar is
flagged partial.

## 7. What the demonstration shows, and what it does not

Trade data is licensed. Publish it and you break a contract; publish
nothing and every claim is unverifiable. The third path is a seeded
generator with a realistic shape: one session per business day, U-shaped
arrivals, size clustering at the open and close, volatility scaling with
trade size. Anyone can reproduce every number in this section from a
seed.

With the dollar threshold matched to the median per-minute notional:

| Sampling | Bars | Volume CV | Excess kurtosis |
| :--- | ---: | ---: | ---: |
| time (1m) | 7,800 | 0.7734 | 1.422 |
| dollar | 10,232 | 0.0255 | −0.007 |

Per-bar volume is roughly 30 times steadier under dollar sampling. Return
excess kurtosis moves from 1.42 to effectively zero. The first row is the
mechanical claim, reproduced. The second row is consistent with Ané and
Geman's normalization results, and it would be: the generator's
volatility is driven by the same activity signal the sampling rule
conditions on. On synthetic data with a genuine volume-volatility link,
activity sampling recovers the underlying returns almost exactly. It is
a demonstration that the machinery works, not evidence about any real
market. For real markets, §3 applies.

## 8. Relationship to existing implementations

The closest contemporary is `bar-forge` (Mouler, 2026), which covers
tick, volume, dollar, and fixed-threshold imbalance bars over NumPy trade
records, plus strictly causal normalization transforms, with 100 percent
coverage, strict typing, and a mutation-testing run of 1,208 mutants at
92.96 percent killed, higher than ours on that axis. Its bar conventions
agree with ours where they overlap. What it does not cover is sessions,
streaming, DataFrames, adaptive estimates, or CUSUM.

`mlfinpy` is a free port of the mlfinlab data structures, including
EWMA-adaptive imbalance and run bars from López de Prado ch. 2, which
this library now implements natively with documented conventions.
`mlfinlab` itself (Hudson & Thames) implements the full catalogue and is
now a commercial product. Databento's 2024 guest series from Double River
Investments builds volume and dollar bars in Polars with a Rust kernel,
which confirms demand for the Polars-native path.

## 9. Limitations

Thresholds, imbalance seeds, and CUSUM's $h$ are absolute. Choosing them
is your job. The library is silent beyond the suggestion of matching the
median per-minute notional. Adaptive seeds far from the data produce the
documented degenerate behavior: a near-zero expected imbalance collapses
every bar to a single trade. Seed from a sample and check the bar counts.

Session cuts take user-supplied boundaries. The library ships no
exchange calendars and knows nothing of contract rolls, so a stream
crossing a roll still contains two instruments.

The 2025 crypto evidence adds a limitation the older literature did not
state plainly: an activity clock is only as honest as the volume it
counts. Calibrate on data you trust.

And the deeper question, whether models should consume events directly,
skipping bars entirely, is outside scope. Every bar is a compression, and
compression discards the fine structure that some strategies live on.

## References

- Ané, T., and H. Geman. 2000. "Order Flow, Transaction Clock, and
  Normality of Asset Returns." *Journal of Finance* 55 (5): 2259–2284.
  https://doi.org/10.1111/0022-1082.00286
- Clark, P. K. 1973. "A Subordinated Stochastic Process Model with Finite
  Variance for Speculative Prices." *Econometrica* 41 (1): 135–155.
- Easley, D., and M. O'Hara. 1992. "Time and the Process of Security Price
  Adjustment." *Journal of Finance* 47 (2): 577–604.
- Easley, D., M. López de Prado, and M. O'Hara. 2012a. "Flow Toxicity and
  Liquidity in a High-Frequency World." *Review of Financial Studies* 25
  (5): 1457–1493.
- Easley, D., M. López de Prado, and M. O'Hara. 2012b. "The Volume Clock:
  Insights into the High-Frequency Paradigm." *Journal of Portfolio
  Management* 39 (1): 19–29.
- Grądzki, P., P. Wójcik, and S. Lessmann. 2025. "Algorithmic Crypto Trading
  Using Information-Driven Bars, Triple Barrier Labeling and Deep
  Learning." *Financial Innovation*. https://doi.org/10.1186/s40854-025-00866-w
- Griffin, J. E., and R. C. A. Oomen. 2008. "Sampling Returns for Realized
  Variance Calculations: Tick Time or Transaction Time?" *Econometric
  Reviews* 27 (1–3): 230–253.
  https://doi.org/10.1080/07474930701873341
- López de Prado, M. 2018. *Advances in Financial Machine Learning*.
  Hoboken, NJ: Wiley. ISBN 978-1-119-48208-6, ch. 2.
- Mandelbrot, B., and H. Taylor. 1967. "On the Distribution of Stock Price
  Differences." *Operations Research* 15 (6): 1057–1062.
  https://doi.org/10.1287/opre.15.6.1057
- Murphy, A., and M. Izzeldin. 2006 (rev. 2010). "Order Flow, Transaction
  Clock and Normality of Asset Returns: A Comment on Ané and Geman (2000)."
  Working paper, Lancaster University.
- Oomen, R. C. A. 2006. "Properties of Realized Variance Under Alternative
  Sampling Schemes." *Journal of Business and Economic Statistics* 24 (2).
  https://doi.org/10.1198/073500106000000044
- Page, E. S. 1954. "Continuous Inspection Schemes." *Biometrika* 41
  (1–2): 100–115. https://doi.org/10.1093/biomet/41.1-2.100

Software:

- Hudson & Thames. `mlfinlab`: implementations of the data structures in
  López de Prado (2018). https://github.com/hudson-and-thames/mlfinlab
- Mouler, B. 2026. `bar-forge`: activity-based market data bars and
  strictly causal normalisation transforms.
  https://github.com/bmouler/bar-forge
- `mlfinpy`: a free port of the mlfinlab data structures, including
  EWMA-adaptive imbalance and run bars. https://mlfinpy.readthedocs.io
