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
  live_snapshots/
  ipo_competition_seed.example.json
.github/
  workflows/
    ipo_competition_batch.yml
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
```

Recommended app access pattern:

```text
Home refresh: active.json + upcoming.json + recent.json
History screen: yearly/{year}.json
Detail screen: stocks/{ipoId}.json
Fallback/full sync: index.json
```

## Batch

Generate the static JSON once:

```bash
dart run tool/ipo_competition_batch.dart --backfill-years 3
```

Merge reviewed historical seed and local live snapshots:

```bash
dart run tool/ipo_competition_batch.dart --backfill-years 3 --live-dir data/live_snapshots
```

Remote discovery is enabled by default. It uses these optional environment
variables:

```text
DART_API_KEY
ITICK_API_KEY
KIS_APP_KEY
KIS_APP_SECRET
```

If no keys are configured, the batch still runs and only normalizes local seed,
discovered, and live snapshot files.

KIS OpenAPI credentials must be provided only through environment variables or
GitHub Actions repository secrets. Do not commit the app key or app secret into
this public repository. The batch currently detects KIS credentials but does not
call a KIS IPO subscription competition endpoint until a verified endpoint is
added.

Run continuously for active subscription days:

```bash
dart run tool/ipo_competition_batch.dart --backfill-years 3 --watch --interval-minutes 10 --live-dir data/live_snapshots
```

GitHub Actions runs the same generation command every 10 minutes during Korean
weekday market hours.

## Data source policy

Do not fabricate historical competition rates. Seed rows should come from a
verifiable source such as broker notices, final IPO reports, DART attachments,
or manually reviewed public disclosures.

The batch normalizes and republishes source data into app-friendly JSON. Source
adapters can be added incrementally per broker.

During active subscription days, the batch also attempts public live snapshot
collection from Shinhan Securities, Daishin Securities, IPOSTOCK detail pages,
and 38 Communication news pages. When the current day is inside an IPO's
subscription window, adapters try to parse public subscription competition
rates, retail allocation shares, broker allocation tables, offer price, and
deposit rate, then merge the result into the generated stock JSON. If a public
page has not published a rate yet, that adapter skips the stock and the rest of
the batch still succeeds.

## Live snapshot input

Files under `data/live_snapshots/*.json` may contain either a full stock object
or `{ "stocks": [...] }`. The batch merges these snapshots by `id`.

This is the bridge before broker-specific adapters are implemented:

- manually reviewed final historical rows go into `data/ipo_competition_seed.json`
- auto-discovered upcoming rows are stored in `data/discovered/ipo_events.json`
- active subscription snapshots go into `data/live_snapshots/*.json`
- historical listing outcomes go into `data/outcomes/*.json`
- broker-level allocation and competition rows go into `data/broker_snapshots/*.json`
- durable corpCode/stockCode/kindCode/isin crosswalk rows go into `data/identifiers/ipo_identifiers.json`
- future broker adapters can write the same JSON shape

The batch also attempts to backfill missing DART `corpCode` values through the
public DART company search page and caches successful matches in
`data/identifiers/ipo_identifiers.json`. Disable that network backfill with
`--no-identifier-discover` when only local crosswalk data should be used.

## GitHub Actions secrets

For automatic new-IPO discovery, configure either or both repository secrets:

```text
DART_API_KEY
ITICK_API_KEY
```

For future Korea Investment & Securities OpenAPI adapters, configure both:

```text
KIS_APP_KEY
KIS_APP_SECRET
```

The workflow runs every 10 minutes during Korean weekday market hours. It
discovers upcoming IPO rows, regenerates `ipo_competition_data/`, and commits
changes when generated JSON changes.

## Analysis output

Each generated stock JSON includes `analysis`:

- `score`: rule-based reference score, grade, confidence, and factor breakdown.
- `expectedReturn`: coarse expected listing gain and allocation/profit scenarios.
- `brokerScores`: broker-level equal/proportional allocation indicators when broker data exists.
- `decision`: app-ready label, reasons, and warnings.
- `inputs`: key fields used by the current method.

The current method is `ipo-score-v1`. It is intentionally transparent and should
be calibrated with historical `outcome` rows before being treated as predictive.

Generated backtest report:

```text
ipo_competition_data/backtest_report.json
```
