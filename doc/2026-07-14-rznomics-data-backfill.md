# Rznomics IPO Data Backfill

## Idea Summary

Backfill the completed Rznomics IPO with reviewed demand-forecast, float, retail competition, and broker allocation data so the app can display institution demand, lockup, tradable supply, equal allocation, proportional allocation, and expected-return analysis.

## MVP Scope

### Core User Problem

The Rznomics detail page shows empty IPO metrics because the generated stock has no reviewed fundamentals override or broker snapshot.

### Must-Have Flows

- Add institution competition, participant count, lockup rate, float rate, and public allocation.
- Add Samsung Securities and NH Investment & Securities retail allocation snapshots.
- Regenerate all app-facing JSON through the existing batch.
- Verify the stock detail and index outputs contain the reviewed values.

### Out of Scope

- Replacing the scoring model.
- Treating historical listing-day performance as a predicted return.
- Backfilling unrelated IPOs.

### Success Criteria

- Institution competition is 848.91 and participants are 2,229.
- Demand-stage lockup is 74.29% and float rate is 26%.
- Aggregate retail competition is 1,871.43.
- Samsung equal allocation is about 0.47 share and NH equal allocation is about 0.44 share.
- Generated JSON parses, Dart analysis passes, and the result is pushed to `origin/main`.

## Feature Specification

### Reviewed Fundamentals

Purpose: provide missing demand and supply inputs.

User interaction flow: the user opens Rznomics and sees institution demand, lockup, float, and allocation values.

Data/state changes: add a keyed override in `data/manual_fundamentals.json`.

Error states: malformed percentages or a mismatched IPO ID would leave generated values empty.

Acceptance criteria: generated fundamentals contain the reviewed numeric values.

### Broker Allocation Snapshot

Purpose: show broker-level equal and proportional allocation context.

User interaction flow: the user compares Samsung and NH allocation expectations.

Data/state changes: add a completed snapshot to `data/broker_snapshots/2025.json`.

Error states: a broker name not matching the lead-manager identity is filtered during normalization.

Acceptance criteria: generated broker rows show Samsung about 0.47 share and NH about 0.44 share for equal allocation.

## Wireframe

```text
Rznomics detail
  Institution demand: 848.91 : 1 / 2,229 institutions
  Lockup: 74.29%       Float: 26.00%
  Retail: 1,871.43 : 1

  Broker            Equal estimate    Proportional rate
  Samsung           0.47 share        3,912.70 : 1
  NH Investment     0.44 share        3,572.00 : 1
```
