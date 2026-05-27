# PublicOfferingShares IPO data

Public static JSON data repository for the PublicOfferingShares app.

The private Flutter app should read generated JSON from this repository instead
of embedding batch jobs or source snapshots in the app repository.

## Layout

```text
ipo_competition_data/
  index.json
  active.json
  upcoming.json
  recent.json
  yearly/
    {year}.json
  stocks/
    {ipoId}.json
tool/
  ipo_competition_batch.dart
data/
  identifiers/
    ipo_identifiers.json
  broker_snapshots/
  discovered/
    ipo_events.json
  manual_fundamentals.json
  ipo_competition_seed.example.json
.github/
  workflows/
    finuts_baseline_discovery_sync.yml
```

## App URLs

After this folder is pushed as a public GitHub repository, the app should read:

```text
https://raw.githubusercontent.com/<owner>/<repo>/main/ipo_competition_data/index.json
https://raw.githubusercontent.com/<owner>/<repo>/main/ipo_competition_data/active.json
https://raw.githubusercontent.com/<owner>/<repo>/main/ipo_competition_data/upcoming.json
https://raw.githubusercontent.com/<owner>/<repo>/main/ipo_competition_data/recent.json
https://raw.githubusercontent.com/<owner>/<repo>/main/ipo_competition_data/yearly/{year}.json
https://raw.githubusercontent.com/<owner>/<repo>/main/ipo_competition_data/stocks/{ipoId}.json
https://raw.githubusercontent.com/<owner>/<repo>/main/ipo_competition_data/backtest_report.json
https://raw.githubusercontent.com/<owner>/<repo>/main/ipo_competition_data/coverage_report.json
```

## Generated schedule fields

Stock detail files and feed rows include app-facing schedule fields when the
upstream input provides them:

- `demandForecastDate`, `demandForecastStart`, `demandForecastEnd`
- `subscriptionStart`, `subscriptionEnd`
- `refundDate`
- `listingDate`
- `lockupReleaseDate`
- `generalSharesDate`
- `cbBwDate`

`demandForecastDate` is the representative start date kept for current app
compatibility; consumers that need a range should read `demandForecastStart`
and `demandForecastEnd`.

Recommended app access pattern:

```text
Home refresh: active.json + upcoming.json + recent.json
History screen: yearly/{year}.json
Detail screen: stocks/{ipoId}.json
Fallback/full sync: index.json
```

## Batch

This repository now runs in a Finuts-only scheduled workflow model.

All mutating workflows follow the same pattern:

1. sync Finuts-derived input files
2. rebuild `ipo_competition_data/` through `tool/ipo_competition_batch.dart`
3. validate generated data
4. commit and push to `main`

The shared rebuild step is:

```bash
dart run tool/ipo_competition_batch.dart \
  --backfill-years 3 \
  --manual-fundamentals-path data/manual_fundamentals.json \
  --no-discover \
  --no-identifier-discover \
  --no-ipo-korea-supplement \
  --no-article-lead-manager-discover \
  --no-public-live-collect
```

The active workflows are:

- `finuts_baseline_discovery_sync`
  - weekday 17:30 KST
  - syncs new IPO baseline rows from Finuts into `data/discovered/ipo_events.json`
  - refreshes listing-date / offer-price style outcome rows under `data/outcomes/`
- `finuts_demand_forecast_fundamentals_sync`
  - weekday 18:00 and 18:20 KST
  - refreshes institution-demand fundamentals into `data/manual_fundamentals.json`
- `finuts_subscription_live_competition_sync`
  - weekday 09:50 to 17:10 KST in repeated runs
  - refreshes Finuts broker snapshot rows into `data/broker_snapshots/`
  - runs `tool/video_ocr_secondary_ingest.py` with `--today-only --ignore-market-hours`
  - broker snapshot rebuild should continue even if the fundamentals refresh step fails
- `finuts_manual_targeted_backfill`
  - manual only
  - targeted repair for one stock id / company
  - can target one broker snapshot source through `tool/video_ocr_secondary_ingest.py --source-id`

The Finuts helper scripts are:

```text
tool/finuts_discovery_sync.py
tool/finuts_fundamentals_sync.py
tool/video_ocr_secondary_ingest.py
```

## Documentation upkeep

Every data-pipeline or app-facing data contract change must update this README
or `docs/analysis_methodology.md` in the same change. In particular, document:

- new external source adapters or GitHub Actions secrets
- new generated JSON fields read by the Flutter app
- score, grade, confidence, or missing-data display behavior
- manual override files under `data/outcomes`, `data/broker_snapshots`,
  `data/identifiers`, or `data/ipo_competition_seed.json`

If a generated stock has missing judgement inputs, the app should not present a
low numeric score as if it were a negative assessment. It should render a neutral
white/gray "정보 부족" state and grade `-` until enough source fields are present.

## Data source policy

Do not fabricate historical competition rates. Seed rows should come from a
verifiable source such as Finuts, broker notices, or manually reviewed public
disclosures.

Scheduled automation should use Finuts as the only live upstream source.
Non-Finuts public scraping workflows have been removed from this repository.

`tool/ipo_competition_batch.dart` is still the shared normalizer and derived
JSON builder, but Finuts workflows should call it with:

- `--no-discover`
- `--no-identifier-discover`
- `--no-ipo-korea-supplement`
- `--no-article-lead-manager-discover`
- `--no-public-live-collect`

## Batch input files

The scheduled Finuts workflows update these mutable inputs before each rebuild:

- manually reviewed final historical rows go into `data/ipo_competition_seed.json`
- auto-discovered upcoming rows are stored in `data/discovered/ipo_events.json`
- Finuts demand-forecast overrides go into `data/manual_fundamentals.json`
- historical listing outcomes go into `data/outcomes/*.json`
- Finuts broker-level allocation and competition rows go into `data/broker_snapshots/*.json`
- durable corpCode/stockCode/kindCode/isin crosswalk rows go into `data/identifiers/ipo_identifiers.json`

When a completed stock has an aggregate retail competition rate, public
allocation shares, and lead managers but no broker-level rows yet, the batch
adds an `estimated_broker_split` snapshot. This splits the public allocation
evenly across lead managers and then splits each broker allocation 50/50 into
equal and proportional buckets. These rows are app-ready fallback data, not
broker-confirmed account-count data; confirmed rows from `data/broker_snapshots`
or broker adapters take precedence.

When public allocation shares are still missing but aggregate retail competition
and lead managers exist, the batch adds an `estimated_broker_rate_only` snapshot.
This lets the app show broker-level proportional-rate context immediately while
leaving equal/proportional allocation volumes empty until confirmed data is
available.

The batch also attempts to backfill missing DART `corpCode` values through the
public DART company search page and caches successful matches in
`data/identifiers/ipo_identifiers.json`. Disable that network backfill with
`--no-identifier-discover` when only local crosswalk data should be used.

## GitHub Actions secrets

The Finuts-only workflows require:

```text
FINUTS_ID
FINUTS_PASSWORD
```

No scheduled workflow in this repository should depend on:

```text
DART_API_KEY
ITICK_API_KEY
KIS_APP_KEY
KIS_APP_SECRET
YOUTUBE_COOKIES_TXT
```

## Analysis output

Each generated stock JSON includes `analysis`:

- `score`: rule-based reference score, grade, confidence, and factor breakdown.
- `expectedReturn`: coarse expected listing gain and allocation/profit scenarios.
- `brokerScores`: broker-level equal/proportional allocation indicators when broker data exists.
- `decision`: app-ready label, reasons, and warnings.
- `inputs`: key fields used by the current method.

The current method is `ipo-score-v2`. It is intentionally transparent and should
be calibrated with historical `outcome` rows before being treated as predictive.

Generated backtest report:

```text
ipo_competition_data/backtest_report.json
```

Generated coverage report:

```text
ipo_competition_data/coverage_report.json
```

Broker metric gap report:

```text
ipo_competition_data/broker_metrics_missing_report.json
```

The coverage report lists discovered IPOs that were not generated, duplicate
subscription candidates, and generated stocks with missing judgement fields such
as institution competition rate, institution participant count, lock-up
commitment rate, retail competition rate, broker-level competition, and external
identifiers. The broker metric gap report focuses only on stocks that still
cannot render broker-level equal/proportional context from generated JSON.
