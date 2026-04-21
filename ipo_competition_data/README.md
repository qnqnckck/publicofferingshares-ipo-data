# IPO competition data format

The app reads `index.json`, then loads per-stock files from `stocks/{ipoId}.json`.

## Files

- `index.json`: compact stock list and latest competition status.
- `stocks/{ipoId}.json`: per-stock competition snapshots.

## Update policy

- Historical backfill: maintain the last 3 years of reviewed IPO rows.
- Active subscriptions: refresh every 10 minutes during Korean weekday market
  hours.
- Source adapters should write broker snapshots; calculated aggregate fields are
  derived by the batch.

## Index example

```json
{
  "schemaVersion": 1,
  "generatedAt": "2026-04-21T09:00:00+09:00",
  "stocks": [
    {
      "id": "example_ipo_id",
      "company": "예시기업",
      "market": "KOSDAQ",
      "subscriptionStart": "2026-04-20",
      "subscriptionEnd": "2026-04-21",
      "latestCompetitionRate": 1523.42,
      "latestSnapshotAt": "2026-04-21T14:20:00+09:00",
      "path": "stocks/example_ipo_id.json"
    }
  ]
}
```

## Stock example

```json
{
  "schemaVersion": 1,
  "id": "example_ipo_id",
  "company": "예시기업",
  "market": "KOSDAQ",
  "subscriptionStart": "2026-04-20",
  "subscriptionEnd": "2026-04-21",
  "leadManagers": ["한국투자증권"],
  "snapshots": [
    {
      "capturedAt": "2026-04-21T14:20:00+09:00",
      "source": "manual",
      "brokers": [
        {
          "name": "한국투자증권",
          "offeredShares": 100000,
          "subscribedShares": 152342000,
          "competitionRate": 1523.42,
          "equalCompetitionRate": 1200.12,
          "proportionalCompetitionRate": 1523.42
        }
      ],
      "aggregate": {
        "offeredShares": 100000,
        "subscribedShares": 152342000,
        "competitionRate": 1523.42
      }
    }
  ]
}
```
