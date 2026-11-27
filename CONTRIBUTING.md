# Contributing

Small library, short rules.

## Setup

```bash
git clone https://github.com/zzionz/clockbars.git
cd clockbars
uv sync --all-extras
```

The dev environment includes pandas and pyarrow so the pandas adapter is
tested in the same run as everything else.

## The gates

Every PR must pass all three, and CI enforces them:

```bash
uv run ruff check .
uv run mypy src          # strict
uv run pytest -q --cov=clockbars   # coverage stays at 100%
```

100% statement coverage is enforced deliberately, at the current size of the
library (167 statements). If a line cannot be tested, the honest options are
to delete it or to argue for it in a PR. Lowering the threshold quietly is
neither.

## The one rule that matters

Conventions here are load-bearing. Whether overshoot leaks into the next bar,
how a partial final bar is flagged, what happens to naive timestamps: each
is documented in the README, stated in the paper, and pinned by a named test.
If you change one of these behaviors, the PR changes all three artifacts in
the same commit: the test that pins it, the README section that states it,
and the paper passage that explains why. A behavior change that arrives
without its documentation is a regression wearing a feature's clothes.

## Figures

`docs/hero.png` and every number in the README's comparison table come from
`examples/make_hero.py`. If you touch the generator or the engine, regenerate
both and commit the result, so the published numbers stay true.

## Reporting problems

Use the issue templates. For suspected wrong output, include the input, the
output, and the three versions (`python`, `polars`, `clockbars`). Say
whether you believe it is a bug or an undocumented convention, since those
get very different fixes.
