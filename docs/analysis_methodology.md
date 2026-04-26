# IPO analysis methodology

This repository generates a derived `analysis` block for each IPO stock JSON.
The score is a reference indicator, not investment advice.

## Current method

`methodVersion: ipo-score-v4`

The current version is intentionally rule-based. It prioritizes transparency
over model complexity because the dataset is still sparse and source quality
varies.

## Score factors

The overall score is capped to `0..100`.

- `competition`: latest public subscription competition rate.
- `institutionDemand`: institution demand forecast competition rate.
- `lockupCommitment`: institution lock-up commitment ratio.
- `floatRate`: public float ratio on listing day.
- `spacMomentum`: SPAC-only listing-day demand signal from retail,
  proportional, and institution competition.
- `spacVolatility`: SPAC-only volatility adjustment for low lock-up and hot
  retail demand.
- `pricing`: final offer price position inside or above the submitted band.
- `market`: KOSPI/KOSDAQ market context.
- `leadManagers`: number of known lead managers.
- `recency`: whether the subscription date is active/upcoming/recent.
- `dataCompleteness`: availability of snapshots, source URL, market, date, and managers.

For operating-company IPOs, `lockupCommitment` and `floatRate` remain direct
quality factors. For SPAC IPOs, those two factors are replaced by
`spacMomentum` and `spacVolatility` because low or absent lock-up commitments
are common and should not be interpreted the same way as a weak operating
company IPO. SPAC scoring therefore treats broker proportional competition,
retail competition, institution demand, fixed low offer price, and volatility as
separate subscription attractiveness signals.

For operating-company IPOs that are still before or during the subscription
window, missing retail competition rate is not treated the same way as a weak
completed deal. In that stage the generator applies a small pending-demand
competition score from institution demand and lock-up strength, and missing
float data is treated as a mild neutral placeholder rather than a full zero.
This keeps pre-subscription judgement usable while still remaining more
conservative than after live retail data arrives.

## Missing-data display policy

The score is meaningful only when enough judgement inputs are present. If a
generated feed item is missing required source context, the Flutter app should
show a neutral information state instead of a low grade:

- score label: `정보 부족`
- grade label: `-`
- visual treatment: white/gray, not red or other warning colors

Missing source context includes absent public subscription competition snapshots,
absent retail competition rate after the subscription has completed, absent
broker-level competition rows for completed IPOs, or an explicit
`decisionLevel: insufficient_data`.

This prevents sparse rows, such as newly discovered or incompletely crawled IPOs,
from being mistaken for objectively poor subscriptions.

## Expected return

Expected return is a coarse scenario estimate:

- `bearCaseListingGainRate`
- `baseCaseListingGainRate`
- `bullCaseListingGainRate`
- `expectedListingGainRate`
- `expectedAllocatedShares`
- `expectedProfitKrw`

When offer price is missing, the generator uses a conservative placeholder
assumption and marks this in `warnings`.

### SPAC expected-return overlay

SPAC IPOs are modeled separately from operating-company IPOs because the listing
day payoff is usually driven by fixed 2,000 KRW offer pricing, retail momentum,
proportional competition intensity, and short-term volatility rather than
fundamental valuation.

For stocks whose company name contains `스팩` or `SPAC`, v4 applies
`ipo_score_v4_spac_listing_day_momentum` to expected returns:

- retail competition rate remains the aggregate demand signal.
- broker proportional competition rate is used as an additional listing-day
  momentum signal when broker-level snapshots exist.
- institution demand forecast competition adds demand confirmation.
- near-zero lock-up commitment is treated as volatility: it raises bull-case
  upside but also widens the bear case.
- fixed low offer price and public allocation size add SPAC-specific momentum
  adjustments.

This overlay affects `expectedListingGainRate`, `bearCaseListingGainRate`,
`baseCaseListingGainRate`, and `bullCaseListingGainRate`. SPAC grades also use
the related `spacMomentum` and `spacVolatility` score factors, so strong SPAC
subscription demand is no longer suppressed by missing operating-company float
or lock-up fields.

### SPAC calibration overlay

When historical SPAC outcomes exist, the generator also writes
`ipo_competition_data/calibration_report.json` and applies a lightweight
sample-size-damped adjustment to future SPAC expected returns.

- calibration uses a SPAC-specific reference return, not close only:
  `0.45 * open + 0.35 * high + 0.20 * close` when those prices exist.
- the model compares that historical reference return with the raw expected
  listing gain rate and stores the average error by retail competition bucket.
- the applied adjustment is intentionally capped and damped while sample size is
  small.

Applied calibration is exposed in each SPAC item's
`analysis.expectedReturn.assumptions` as:

- `uncalibratedBaseGainRate`
- `calibrationApplied`
- `calibrationSampleCount`
- `calibrationCompetitionBucket`

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

Broker-level snapshots are maintained under `data/broker_snapshots/*.json`.
They are required for meaningful equal/proportional allocation estimates:

```json
{
  "id": "example_ipo_id",
  "capturedAt": "2026-04-22T16:00:00+09:00",
  "brokers": [
    {
      "name": "한국투자증권",
      "offeredShares": 100000,
      "equalAllocationShares": 50000,
      "proportionalAllocationShares": 50000,
      "applicationCount": 80000,
      "competitionRate": 1200.0,
      "proportionalCompetitionRate": 2400.0,
      "depositRate": 0.5,
      "feeKrw": 2000
    }
  ]
}
```

### Secondary fallback sources

When official broker allocation notices or first-party live competition pages
are missing, this repository may attach a secondary broker snapshot from a
screen-parsed public source such as a YouTube live board.

Current source naming:

- `youtube_video_ocr_secondary`

Rules:

- secondary snapshots are fallback-only and should not replace later official
  broker notices for the same broker.
- they are acceptable for filling `applicationCount`,
  `equalAllocationShares`, and broker presence when the official source is
  absent.
- values derived from a screen parse should keep the source explicit so app and
  downstream tooling can lower trust or label them as supplemental if needed.
- GitHub Actions OCR collection reads `data/video_ocr_sources.json`, captures the
  configured latest frame with a Chromium browser session first, and upserts
  snapshots by `id + source`. If browser capture fails, the pipeline can still
  fall back to direct stream extraction. For the same source, a newer
  `capturedAt` replaces the older fallback row.
- each source may define `imagePath` for the latest maintained screenshot. When
  present, OCR uses that image first and skips direct YouTube access. This is
  the preferred operating mode because it avoids anti-bot failures.

Recommended use:

- prefer `official_broker_result*`, direct broker live pages, and public JSON.
- use `youtube_video_ocr_secondary` only to avoid a blank broker card when no
  official broker row exists yet.

When broker-level equal allocation and application-count rows are available,
`expectedAllocatedShares` includes the expected equal shares per account plus the
proportional estimate for each capital scenario. Without broker-level rows,
expected allocation remains a low-confidence aggregate/proportional estimate.

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

The batch writes `ipo_competition_data/backtest_report.json` from rows that have
`outcome.closeReturnRate`.

Historical outcomes can be maintained independently from seed rows under
`data/outcomes/*.json`. Each row may specify prices instead of return rates:

```json
{
  "id": "example_ipo_id",
  "company": "예시기업",
  "listingDate": "2026-04-30",
  "offerPrice": 30000,
  "openPrice": 42000,
  "highPrice": 48000,
  "closePrice": 39000
}
```

After enough rows are collected, replace the static weights with calibrated
weights:

1. Store first-day return outcomes for historical IPOs.
2. Split historical rows into train/test periods.
3. Fit simple interpretable weights first, not a black-box model.
4. Compare score buckets against realized average and median returns.
5. Publish calibration error and confidence with each method version.

Until that is done, `confidence` should be shown prominently in the app.
