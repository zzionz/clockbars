# Bars Are a Modeling Decision

**A careful implementation of activity-sampled bars, and the failure modes that hide in plausible-looking output**

zzionz, DEUS Research Lab
November 2026

---

## Abstract

Every quantitative pipeline begins by chopping a stream of trades into rows.
In practice nearly everyone inherits that step: the data
vendor ships 1-minute bars, so 1-minute bars become the unit of analysis. We
argue that bar construction is a modeling decision with a fifty-nine-year
history behind it, summarize that history and its one genuinely contested
claim, and describe `clockbars`, a Polars-native implementation of
activity-sampled bars. The old way has a published price: on IBM transaction
data, 2000 to 2004, transaction-time sampling cut the mean squared error of a
standard variance estimator by about 5 percent on average and by as much as 40
percent on irregular trading days (Oomen, 2006). The newest large comparison
cuts the other way: in crypto, where reported volume is inflated by wash
trading, dollar bars were the weakest sampling scheme tested (Grądzki,
Wójcik, and Lessmann, 2025). Both results sharpen the same mechanical claim,
which nobody disputes: sampling on an honest activity signal gives bars of
near-constant information content, and clock sampling does not. The
implementation contributes no new theory. What it contributes is a written
contract (a set of invariants, each pinned by a test), plus two properties
the free ecosystem currently lacks: a streaming constructor held to exact
parity with the batch path, and session-aware construction so a bar never
straddles an exchange close. Along the way we record three failure modes
that produce plausible-looking output while meaning something else: threshold
overshoot charged to the wrong bar, phantom partial bars from floating-point
accumulation, and NaN values that pass a sign check because `NaN <= 0`
evaluates to `False`. We demonstrate the mechanical effect of activity
sampling on a seeded synthetic stream, and we are explicit about what that
demonstration does and cannot establish.

---

## 1. The step nobody decides

Ask a quant how they chose their bar size and the honest answers cluster:
the data came that way, the platform defaults to it, everyone uses
five-minute bars. This is odd, because the bar is the unit on which every
later statistic is computed. Choose bars by clock and you have chosen to give
every observation equal *time* and unequal *content*. The half-hours after
the open and before the close carry several times the volume of midday, so a
minute bar at 09:31 and a minute bar at 12:31 agree in duration and differ by
an order of magnitude in the amount of trading they contain.

The consequence is easy to reproduce and unpleasant to inherit. On a
400,000-trade synthetic stream with a realistic intraday volume profile, the
coefficient of variation of per-bar volume is **0.7734** for 1-minute bars.
Sampling the same stream so that each bar carries an equal amount of traded
notional brings that number to **0.0255**. Nothing about the market changed
between those two rows. Only the sampling rule moved.

Downstream, uneven content per observation surfaces as fat-tailed returns,
volatility clustering, and serial dependence that are partly artifacts of
the sampling scheme. Features computed on such bars inherit the artifact. So
do backtests.

Calendar sampling has also been measured against the alternative on real
data, which settles the stakes better than any argument. Oomen (2006)
derived the bias and mean squared error of realized variance under calendar,
business, and transaction-time sampling for a pure-jump price process, then
estimated the model on IBM transaction data from 2000 to 2004. Transaction
time won on every day in the sample: sampling there instead of on the clock
lowered the MSE of the estimator by about 5 percent on average, and by as
much as 40 percent on days with irregular trading. Griffin and Oomen (2008)
extended the comparison between transaction time and tick time and showed
that the return dynamics differ materially between them, enough that
bias-correction procedures built for one can fail under the other. The
sampling scheme is part of the model. Choosing it by vendor default is still
a choice, just an unexamined one.

## 2. The idea is older than most of the people using it

The observation that markets run on their own clock, not the wall clock, has
been made repeatedly and forgotten repeatedly.

Mandelbrot and Taylor (1967) put it first, plainly: price changes over a
fixed *number of transactions* may follow a Gaussian distribution, while
price changes over fixed *time* follow a heavy-tailed law, and there is no
contradiction because the number of transactions in any time period is
itself random. Clark (1973) formalized this as a subordinated stochastic
process, a random walk in operational time, with traded volume as the
subordinator, and showed that conditioning on volume collapses much of the
apparent non-normality in cotton futures. Easley and O'Hara (1992) gave the
microstructure grounding: time between trades is itself informative, because
informed traders trade when they have reason to.

Ané and Geman (2000) made the clock itself the object of estimation. Using
high-frequency data, they argued that the cumulative number of trades is a
better stochastic clock than volume, and that conditioning returns on this
clock recovers "virtually perfect" normality. Easley, López de Prado, and
O'Hara (2012a) built the practical machinery: fixed-length volume buckets as
the unit of analysis for flow toxicity, requiring no parameter estimation
and updating in real time. Their companion piece (2012b) named the concept,
the volume clock, and argued that high-frequency trading is better
understood as decisions made in volume time than in chronological time.
López de Prado (2018, ch. 2) consolidated the applied side: tick, volume,
and dollar bars as the sampling primitives, with dollar bars preferred for
cross-asset comparability, and imbalance and run bars as the adaptive
generalizations.

So when a pipeline samples by dollar amount instead of by minute, it is
applying a specific, citable model of market time that has been in the
literature since before stored-program computers were commonplace.

## 3. The part that is actually contested

Here the literature divides, and honest treatments must say so.

The strong claim, that sampling by an activity clock makes returns
approximately Gaussian, comes from Ané and Geman (2000). It has been
challenged directly. Murphy and Izzeldin, in a comment circulated as a
working paper (2006, revised 2010), re-examined the nonparametric procedure
Ané and Geman used to recover the moments of the latent clock and found it
unstable. On their own data, conditioning on transaction counts or volume
made returns more Gaussian than raw returns, and normality was still
decisively rejected throughout. Geman's later summary, that the transactions
clock "recovers a quasi perfect normality," is on this record an
overstatement.

The newest entry in this debate complicates it further, and it is the one
practitioners should read most carefully. Grądzki, Wójcik, and Lessmann
(2025) ran a large out-of-sample comparison of sampling schemes for
algorithmic trading in BTC and ETH, tick-level data from 2018 through 2023,
with transaction costs, across Transformers, classical ML, and hybrid
architectures. Their result inverts the usual ranking. CUSUM-filtered
sampling, which reacts to price movement, was the only scheme with
consistently positive performance. Volume bars were inconsistent. Dollar
bars, the clock this library defaults to, were the weakest of everything
they tested. Their diagnosis matters more than the ranking: crypto exchange
volume is inflated by wash trading, so the activity signal a dollar bar
accumulates is partly fabricated. The clock inherits the lie.

We take no side on whether activity clocks Gaussianize returns, partly
because our own demonstration runs on synthetic data and could not settle
it. What matters for a data-engineering argument is the weaker claim that
nobody disputes: sampling by an honest activity signal produces bars of
near-constant information content, and sampling by clock does not. That
claim is mechanical, checkable, and the one our measurements address. The
2025 study adds the practitioner's corollary: verify that your activity
signal is honest before you trust the clock built on it. Whether the
resulting series is Gaussian enough to matter for your estimator is an
empirical question about your market, and the published evidence says to
check, and to expect answers on both sides.

## 4. The rule, written down

`clockbars` closes a bar on the trade that pushes its running total to the
threshold, includes that trade in the bar it closes, and restarts the next
bar from zero. Formally, for a sequence of trades with metric values
$m_1, m_2, \ldots$ (count, units, or notional) and threshold $T$: trade $j$
belongs to bar $b$ if it is the first trade for which the running total from
the bar's start reaches $T$, or an earlier trade in that bar. Three
properties follow and are enforced by test:

1. Every closed bar has total $\ge T$, exceeded by at most its own final trade.
2. Only the final bar of a segment may fall short, and it is flagged
   `is_partial`, never dropped. Without session cuts the whole stream is one
   segment, so only the last bar can be partial. With them, each session's
   last bar answers for itself.
3. Volume, notional, and trade counts are conserved exactly across bars.

Below that sit four smaller decisions that we document because an
undocumented decision in a data library becomes someone else's silent bug:
timestamps must be timezone-aware and are normalized to UTC (naive input is
rejected with instructions); duplicate timestamps are allowed with input
order preserved; time-bar windows are half-open, epoch-aligned, and accept
compound intervals like `1h30m`; and threshold comparisons carry a `1e-9`
relative tolerance, for reasons in §5.

## 5. Three ways this goes quietly wrong

Each of these produced plausible-looking output in a real implementation.
All three are now pinned by regression tests.

**(a) Overshoot charged to the wrong bar.** The obvious vectorised
implementation assigns each trade to `floor(cumulative_metric / T)`, a
global cumulative sum. It is one expression, it is fast, and it is wrong in
a way that only shows up mid-series. Worked example with threshold $T=250$
and trades $200, 100, 10 \times 27$: the correct rule yields bars of
$300, 250, 20$. The global-cumsum rule yields $300, 200, 70$, because bar 0
overshot by 50 and the next bar was docked 50 for it, closing below
threshold in the middle of the series. The tell in production output is
partial bars far from the end of the stream: in one 128-bar run built this
way, 62 bars were flagged partial. The fix is to accumulate per bar and
restart at zero. The cost is that segmentation scans over bars instead of
being a single expression, at $O(n_{\text{bars}} \cdot \log
n_{\text{trades}})$, which is negligible at any realistic threshold since
there are far fewer bars than trades.

**(b) Phantom partial bars from floating point.** Summing a few thousand
floats in sequence does not land exactly on the threshold. It lands on
values like $499.9999999999999$. A strict comparison then flags a bar that
mathematically closed. Individually harmless, these flags destroy trust in
the `is_partial` column, which exists precisely to be trusted. The fix is a
relative tolerance (`1e-9`) on the threshold comparison. The tolerance must
be *relative* because a dollar-bar threshold of $10^6$ and a volume
threshold of $10^2$ fail at different absolute offsets.

**(c) NaN passes a sign check.** `NaN <= 0` is `False`, so a validation rule
of the form "reject non-positive prices" accepts NaN, and one NaN price
poisons every bar built after it. Silently, because bar construction does
not crash on NaN; it computes with it. Finiteness must be checked
separately from sign, before the sign check, or the sign check is theater.

A fourth issue is smaller but the same species. The synthetic generator in
an early version of this library used a hardcoded list of ten date offsets,
so requesting more than ten sessions silently interleaved two sessions into
the same calendar day, with duplicated timestamps. Nothing crashed. The
figure just quietly had half the intended number of bars. It was caught by
an invariant that had no reason to fail (the bar count of a time-bar
baseline), and it is the reason the test suite now includes a test whose
entire job is to assert that generated sessions are unique business days.

## 6. One rule, two execution paths

Bar construction runs in two places: in research, over a historical file,
and in production, one trade at a time. The production path is usually a
*different implementation*, a loop written later, by someone else, or by the
same person in a different mood, and the two drift. A bar that differs
between backtest and live is not a rounding problem. It is a state bug or a
lookahead bug wearing a rounding problem's clothes. Neither the free
libraries we compared nor the notebook ecology ships a streaming
constructor, which says something about how often the question is asked.

`clockbars` implements both paths against the same rule and holds them to
parity: feeding a historical stream through the incremental `BarBuilder`
must agree with `resample` on the same stream, exactly, on everything that
decides what a bar is (boundaries, ids, timestamps, OHLC, counts, partial
flags), at 200,000 trades, all four kinds. Per-bar volume, notional, and vwap
agree to within float-summation order (at most 1e-12 relative in practice),
and we say that plainly instead of claiming bit-equality: a vectorised group
sum and a sequential accumulator sum the same floats in different orders.
Segmentation, by contrast, is bit-exact, because both paths cumulate the
metric left to right within a segment and close on the same comparison of
the same partials. Parity is a strong test because the two implementations
fail differently. The batch path is most likely to break a *convention*
(§5a's carry-over is a batch-only disease). The streaming path is most
likely to break *state*: what survives a bar boundary, or a session
rollover. Each path is a regression test for the other.

The same mechanism makes session breaks cheap. Futures roll at a known
time; a dollar bar spanning 17:59:59 and 18:00:01 mixes two trading days
into one row. Pass the rollover times as `session_starts` and accumulation
restarts at each boundary. The boundary trade belongs to the new session,
bars never straddle a cut, and a session's final short bar is flagged
partial instead of being silently merged into the next day. Time bars need
no cuts, since the clock already makes them. Passing both raises instead of
inventing a precedence.

The streaming counterpart of a session cut is `BarBuilder.close_session()`.
In production the rollover is an event you observe, and the parity test
covers that form too: batch `session_starts` and streaming `close_session()`
at the same timestamps produce identical frames.

## 7. What the demonstration shows, and what it does not

Trade data is licensed, which makes honest demonstration awkward. Publish
the data and you have broken a contract; publish nothing and every claim is
unverifiable. We take the third path, a seeded generator with a realistic
shape (one session per business day, U-shaped arrivals, size clustering at
the open and close, volatility scaling with trade size), so that anyone can
reproduce every number in this section from a seed.

With the dollar threshold matched to the median per-minute notional, so
that both schemes draw comparable information out of the stream:

| Sampling | Bars | Volume CV | Excess kurtosis |
| :--- | ---: | ---: | ---: |
| time (1m) | 7,800 | 0.7734 | 1.422 |
| dollar | 10,232 | 0.0255 | −0.007 |

Per-bar volume is roughly 30 times steadier under dollar sampling; return
excess kurtosis moves from 1.42 to effectively zero. The first row is the
mechanical claim, reproduced. The second row is *consistent with* the
normalization results of Ané and Geman, and it would be, because the
generator's volatility is driven by the same activity signal that the
sampling rule conditions on. That is the honest reading: on synthetic data
with a genuine volume-volatility link, activity sampling recovers the
underlying returns almost exactly, because the construction was designed to
make the clock informative. It is a demonstration that the machinery
works. It is not evidence about any real market. For real markets, §3
applies: measure on your own data, with Oomen's variance results as the
floor of what better sampling buys, and with the 2025 crypto study as the
reminder that a corrupted activity signal corrupts the clock.

## 8. Relationship to existing implementations

This problem has existing software, and a fair comparison names all of it.

The closest contemporary is `bar-forge` (Mouler, 2026), which appeared while
this library was being written and covers tick, volume, dollar, and
fixed-threshold imbalance bars over NumPy trade records, plus strictly
causal normalization transforms. It holds itself to a high verification bar:
100 percent coverage, strict typing, and a mutation-testing run of 1,208
mutants with 92.96 percent killed, higher than ours on that axis, and we say
so. Its bar conventions agree with ours where they overlap. The
threshold-crossing trade is kept whole, empty intervals produce no bar, the
trailing partial bar is never silently dropped. Two implementations arriving
at the same conventions independently from the same source is worth more
than either arriving alone. What it does not do is sessions, streaming, or
DataFrames, and its documentation cites no literature.

`mlfinpy` is a free port of the mlfinlab data structures, including the
EWMA-adaptive imbalance and run bars from López de Prado ch. 2, which we do
not implement and will not claim as a gap in the free ecosystem, because it
is not one. Hudson & Thames' `mlfinlab` itself implements the full
catalogue; it began open source and is now a commercial product. Notebook
implementations of dollar bars circulate widely, and the gist ecology
contains both careful ones and global-cumsum ones. Practitioner tooling has
followed the same trail: Databento's 2024 guest series from Double River
Investments builds volume and dollar bars in Polars with a Rust kernel,
which confirms demand for a Polars-native path and complements this library.

`clockbars` is deliberately narrower in a different direction: the three
standard activity samplers plus the time baseline, Polars-native, one call
from `DataFrame` to `DataFrame` with a fixed schema, a documented rule, and
the two properties in §6 (streaming parity and session cuts) that the named
alternatives lack. What it offers is auditability. The failure modes in §5
each have a regression test, the streaming path is pinned to the batch path
at scale, and the demonstration data ships with the library.

## 9. Limitations

Thresholds are absolute, and the library is silent on choosing them beyond
the suggestion of matching the median per-minute notional of a baseline.
Imbalance and run bars are future work, and adaptive versions already exist
in `mlfinpy`, so a Polars port needs a reason beyond porting. Session cuts
take user-supplied boundaries: the library ships no exchange calendars and
knows nothing of contract rolls, so a stream crossing a roll still contains
two instruments and should be split first. The 2025 crypto evidence adds a
limitation the older literature did not state plainly: an activity clock is
only as honest as the volume it counts, so calibrate on data you trust.
And the deeper question, whether ML models should consume events directly,
skipping bars entirely, is outside scope but worth stating: every bar is a
compression, and compression discards the fine structure that some
strategies live on.

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
