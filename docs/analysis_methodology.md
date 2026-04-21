# IPO analysis methodology

This repository generates a derived `analysis` block for each IPO stock JSON.
The score is a reference indicator, not investment advice.

## Current method

`methodVersion: ipo-score-v1`

The first version is intentionally rule-based. It prioritizes transparency over
model complexity because the dataset is still sparse and source quality varies.

## Score factors

The overall score is capped to `0..100`.

- `competition`: latest public subscription competition rate.
- `institutionDemand`: institution demand forecast competition rate.
- `lockupCommitment`: institution lock-up commitment ratio.
- `floatRate`: public float ratio on listing day.
- `pricing`: final offer price position inside or above the submitted band.
- `market`: KOSPI/KOSDAQ market context.
- `leadManagers`: number of known lead managers.
- `recency`: whether the subscription date is active/upcoming/recent.
- `dataCompleteness`: availability of snapshots, source URL, market, date, and managers.

## Expected return

Expected return is a coarse scenario estimate:

- `bearCaseListingGainRate`
- `baseCaseListingGainRate`
- `bullCaseListingGainRate`
- `expectedListingGainRate`
- `expectedAllocatedShares`
- `expectedProfitKrw`

When offer price is missing, v1 uses a conservative placeholder assumption and
marks this in `warnings`.

## Extended input blocks

Stock JSON may include:

```json
{
  "fundamentals": {
    "offerPrice": 38000,
    "priceBandMin": 32000,
    "priceBandMax": 38000,
    "institutionCompetitionRate": 615.9,
    "institutionParticipants": 2300,
    "lockupCommitmentRate": 0.451,
    "floatRate": 0.32,
    "marketCapKrw": 3707100000000,
    "publicAllocationShares": 500000
  },
  "outcome": {
    "listingDate": "2025-11-18",
    "openReturnRate": 0.4,
    "highReturnRate": 0.8,
    "closeReturnRate": 0.25
  }
}
```

`outcome` is not used to score future rows. It is stored for backtesting and
calibration.

## How to improve accuracy

The score becomes materially more useful when these fields are collected:

- final offer price
- institution demand forecast competition rate
- institution participation count
- lock-up commitment ratio
- public float ratio on listing day
- market cap at offer price
- public allocation shares by broker
- subscription account count by broker
- equal/proportional allocation split
- broker fee
- first-day open/high/close return for historical backtests

## Backtest plan

After enough rows are collected, replace the static weights with calibrated
weights:

1. Store first-day return outcomes for historical IPOs.
2. Split historical rows into train/test periods.
3. Fit simple interpretable weights first, not a black-box model.
4. Compare score buckets against realized average and median returns.
5. Publish calibration error and confidence with each method version.

Until that is done, `confidence` should be shown prominently in the app.
