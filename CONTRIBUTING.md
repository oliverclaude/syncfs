# Contributing

Design choices are recorded in [DECISIONS.md](DECISIONS.md).

## Issue tracking

Every issue and enhancement is scored on two axes when it is filed.
Lower is better on both.

- **Effort** (labels `effort:1` … `effort:5`): 1 is the smallest or
  easiest fix, 5 is the largest or hardest.
- **Likelihood** (labels `likelihood:1` … `likelihood:5`): 1 is the
  most likely to happen, or to hit users or CI; 5 is rare.

The issue also gets a short comment of the form `Score: effort N,
likelihood M, total X. Reason: …`. Total is effort + likelihood.

Work is picked by the lowest total. Ties go to the lower effort, then
to the lower issue number.

If you file an issue, propose both scores and a one-line reason (the
[bug](.github/ISSUE_TEMPLATE/bug.md) and
[feature request](.github/ISSUE_TEMPLATE/feature.md) templates ask for
them). The labels and the score comment are applied when the issue is
opened.

Any bug found while testing or fixing another issue, and that is not
part of the issue being worked on, is filed straight away as its own
issue, with full detail and scores. It is not fixed in that change.
