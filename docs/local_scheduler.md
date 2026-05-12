# Local IPO Data Scheduler

This replaces the scheduled GitHub Actions in this repository with local
`launchd` jobs.

## One-time setup

```sh
cd /Users/jerome/project/publicofferingshares-ipo-data
cp scripts/local/ipo_data_batch.env.example scripts/local/ipo_data_batch.env
```

Fill `scripts/local/ipo_data_batch.env` only if Finuts-based jobs are needed.
The default local scheduler does not require Finuts credentials.

```sh
FINUTS_ID=...
FINUTS_PASSWORD=...
```

Install local dependencies:

```sh
scripts/local/setup_local_dependencies.sh
```

## Manual batch commands

신규 종목/기본 일정 발굴:

```sh
scripts/local/run_ipo_data_batch.sh baseline
```

피너츠 없이 공개 소스 기반 신규 종목/일정/파생 데이터 갱신:

```sh
scripts/local/run_ipo_data_batch.sh public-refresh
```

수요예측 fundamentals 갱신:

```sh
scripts/local/run_ipo_data_batch.sh demand
```

청약일 균등/비례 경쟁률 갱신:

```sh
scripts/local/run_ipo_data_batch.sh live
```

피너츠 없이 네이버 IPO 계산기 기반으로 청약일 균등/비례 경쟁률 갱신:

```sh
scripts/local/run_ipo_data_batch.sh naver-live
```

repo 데이터만으로 파생 JSON 재생성:

```sh
scripts/local/run_ipo_data_batch.sh rebuild
```

특정 종목 수동 backfill:

```sh
scripts/local/run_ipo_data_batch.sh targeted --stock-id machinarax_2026 --mode full
```

## Scheduler

Install a 10-minute local scheduler:

```sh
scripts/local/install_ipo_data_scheduler.sh 600
```

Run it immediately:

```sh
launchctl kickstart -k gui/$(id -u)/com.yellowrocket.publicofferingshares.ipo-data-batches
```

Logs:

```sh
tail -f build/local_scheduler/logs/ipo-data-scheduler.out.log
tail -f build/local_scheduler/logs/ipo-data-scheduler.err.log
```

Uninstall:

```sh
scripts/local/uninstall_ipo_data_scheduler.sh
```

## Local schedule

All times are KST.

- `live`: every scheduler tick between `09:00` and `17:30` on weekdays.
  Defaults to `naver-live`, which does not require Finuts credentials and uses
  Naver IPO calculator data for current subscription stocks.
- `demand`: weekdays at `18:00` and `18:20`, only when
  `IPO_DATA_DEMAND_ENABLED=1`.
- `baseline`: weekdays at `17:30`, only when `IPO_DATA_BASELINE_ENABLED=1`.
  Defaults to `public-refresh`, which skips Finuts and uses configured public
  sources such as DART/iTick/IPOKorea/articles/Naver.

For Finuts-free local operation, keep `IPO_DATA_LIVE_BATCH=naver-live`,
`IPO_DATA_BASELINE_BATCH=public-refresh`, `IPO_DATA_DEMAND_ENABLED=0`, and
`IPO_DATA_BASELINE_ENABLED=1`.

The scheduler keeps marker files under `scripts/local/.state/` to avoid running
the same slot twice.
