# PublicOfferingShares IPO data

Public static JSON data repository for the PublicOfferingShares app.

The private Flutter app should read generated JSON from this repository instead
of embedding batch jobs or source snapshots in the app repository.

## Layout

```text
ipo_competition_data/
  index.json
  stocks/
    {ipoId}.json
tool/
  ipo_competition_batch.dart
data/
  ipo_competition_seed.example.json
.github/
  workflows/
    ipo_competition_batch.yml
```

## App URLs

After this folder is pushed as a public GitHub repository, the app should read:

```text
https://raw.githubusercontent.com/<owner>/<repo>/main/ipo_competition_data/index.json
https://raw.githubusercontent.com/<owner>/<repo>/main/ipo_competition_data/stocks/{ipoId}.json
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

## Live snapshot input

Files under `data/live_snapshots/*.json` may contain either a full stock object
or `{ "stocks": [...] }`. The batch merges these snapshots by `id`.

This is the bridge before broker-specific adapters are implemented:

- manually reviewed final historical rows go into `data/ipo_competition_seed.json`
- active subscription snapshots go into `data/live_snapshots/*.json`
- future broker adapters can write the same JSON shape
