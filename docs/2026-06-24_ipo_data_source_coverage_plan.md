# IPO Data Source Coverage Plan

## Idea Summary

Improve the IPO data batch so current subscription rows continue to refresh equal/proportional broker data while baseline and demand jobs also attempt full public-source discovery for new listings, institution demand, offer price, listing date, and lock-up fields.

## MVP Scope

Core user problem: active IPO rows can show current broker allocation data, but newly published demand/result fields can lag when scheduled jobs run with Finuts discovery disabled.

Must-have flows:
- Baseline and demand refresh jobs should run the full public discovery path by default.
- Live subscription jobs should remain lightweight and update active broker/equal/proportional data without re-running all discovery sources.
- Missing institution participants, lock-up rate, listing date, or retail competition should remain explicit in generated reports instead of being converted to misleading zero values.
- Manual overrides may be used only for reviewed public data that current adapters do not expose.

Out-of-scope items:
- Authenticated/private upstream integrations.
- Fabricating missing institution participants or lock-up rates from incomplete summaries.
- Replacing the existing generated JSON schema.

Success criteria:
- `baseline`, `demand`, and GitHub public baseline/demand workflows no longer pass `--no-finuts-discover`.
- `live`/`naver-live` still avoid full discovery for frequent intraday updates.
- Current active rows preserve unknown retail competition as `null`, not `0.0`.
- Validation reports identify remaining missing fields for manual or future adapter work.

## Feature Specification

Full public discovery refresh:
- Purpose: maximize new IPO and demand/fundamental coverage.
- User interaction flow: run `scripts/local/run_ipo_data_batch.sh baseline`, `demand`, or `public-refresh`.
- Data/state changes: refresh `data/discovered`, `data/identifiers`, generated `ipo_competition_data`.
- Error states: upstream timeout logs a source failure and the batch continues with other sources.
- Acceptance criteria: command path omits `--no-finuts-discover`.

Live broker refresh:
- Purpose: keep same-day equal/proportional and retail competition fresh.
- User interaction flow: run `live` or `naver-live`.
- Data/state changes: update generated active stock snapshots.
- Error states: if Naver returns null competition fields, generated competition remains null.
- Acceptance criteria: active stocks can have broker allocation rows without fake `0.0` competition.

Missing-field visibility:
- Purpose: make remaining source gaps visible.
- User interaction flow: inspect `ipo_competition_data/service_health_report.json` and `reports/missing_app_fields.json`.
- Data/state changes: report fields list missing institution participants, lock-up, and retail competition.
- Error states: incomplete data is not downgraded into a negative signal.
- Acceptance criteria: Lemon Healthcare shows institution competition when manually verified, while missing lock-up/participants remain null.

## Wireframe

```text
Scheduled / Manual Trigger
        |
        +-- baseline / demand / public-refresh
        |       |
        |       +-- Finuts discovery
        |       +-- DART/iTick discovery
        |       +-- IPOKorea/article supplements
        |       +-- Naver active calculator
        |       +-- generated JSON + missing-field reports
        |
        +-- live / naver-live
                |
                +-- repo data only
                +-- Naver/broker active snapshots
                +-- generated JSON + freshness reports
```
