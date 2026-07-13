# HL Genomics IPO Backfill

## Idea Summary

Add the missing 2026 HL Genomics IPO event and reviewed demand-forecast inputs so the app subscription report can resolve its stock detail JSON and display institutional metrics.

## MVP Scope

### Core User Problem

The app knows the HL Genomics subscription schedule from an external feed, but the public IPO data repository has no matching event or stock detail file. Detail resolution therefore returns no institutional values.

### Must-Have Flows

- Publish the July 13-14 subscription schedule and July 24 listing date.
- Publish offer price, institution competition, institution count, lockup, float, and public allocation.
- Regenerate active, index, dashboard, yearly, and stock detail JSON.
- Verify that an app event can resolve the generated stock path by company and subscription dates.

### Out of Scope

- Publishing final retail competition before the July 14 close.
- Guessing equal allocation per account before final application counts are available.
- Changing the Flutter report layout.

### Success Criteria

- `stocks/에이치엘지노믹스_2026-07-13.json` exists.
- Institution competition is 714.52, institution count is 2,148, and lockup is 1.91%.
- Float is 33.04% and public allocation is 641,250 shares.
- The active feed includes the stock detail path.
- JSON validation and Dart analysis pass, and the correction is pushed to `origin/main`.

## Feature Specification

### IPO Schedule Record

Purpose: give the app a stable public identity and detail path for HL Genomics.

User interaction flow: the user opens the July 13-14 event and the app resolves the matching public stock JSON.

Data/state changes: add a discovered IPO event with normalized company identity, dates, market, and lead managers.

Error states: a company-name or subscription-date mismatch prevents fallback path resolution.

Acceptance criteria: the active feed contains the exact company and date tuple used by the app.

### Demand And Supply Metrics

Purpose: populate the subscription report with reviewed institutional and tradable-supply data.

User interaction flow: institution competition, lockup, float, and expected-return analysis appear instead of `-`.

Data/state changes: add a manual fundamentals override and regenerate the stock analysis.

Error states: retail competition and per-account equal allocation remain pending until final subscription results exist.

Acceptance criteria: all reviewed fundamentals are present while unconfirmed retail values remain null.

## Wireframe

```text
HL Genomics subscription report
  Institution competition  714.52 : 1
  Lockup                    1.91%
  Tradable supply           33.04%
  Expected return           model output

  KB Securities             allocation 577,125 shares
  IBK Securities            allocation  64,125 shares
  Final equal estimate      pending close
```
