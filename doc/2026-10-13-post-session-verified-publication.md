# October 13 post-session verified retail publication

## Idea summary

The user explicitly requested publishing newly verified values after the 17:00
scheduled check withheld them. Publish only MS Bio and DTS's observed broker
metrics, using the current E-drive nested data repository. Starting main:
4e362d0726b31f5b209a55010e0b327ce6805e5e.

## MVP scope

Add the source-observed 16:05:31 / 16:05:30 snapshots to the two reviewed issuers.
Preserve previous observations and publish their app-facing summary rows to main.
Retain unknown subscribed shares as null; equal averages are pool/count
indicators, and direct proportional ratios are not doubled total ratios.
Elice's unverified final aggregate and existing local drafts are excluded under
the user's explicit verified-only instruction. This manual exception does not
change future scheduled publication gates.

## Feature specification

The 17:16:03 KST public API recheck confirms the earlier 16:05 observations.
Use a complete contemporaneous snapshot rather than relabeling an older total
ratio under a new time: MS Bio total1538.51, proportional3076.02,286498 accounts;
DTS Daishin total30.25, proportional60.49,83209 accounts; Eugene total13.27,
proportional26.54,9008 accounts. DTS combined28.552 is allocation-weighted from
rounded reported broker totals and is not an exact final aggregate.

Generate in an isolated copy of committed inputs/code plus the new reviewed
input. Merge/stage only these two details and their existing feed rows. Preserve
all other issuers, their membership, source inputs, app code and unrelated local
changes. Validate canonical contracts, histories, feeds, JSON, source selection,
monthly identities and the exact staged bytes. Report the full local Elice audit
error separately from the scoped publication audit. Verify origin/main and exact
GitHub Raw app endpoints after push. Stop on any new error in the selected scope.

## Wireframe

Source recheck -> canonical reviewed snapshot -> isolated batch -> selected
details and feed rows -> scoped audit and staged diff -> main push -> Raw URLs.

## Source evidence

- MS Bio: https://m.stock.naver.com/front-api/ipo/calculator/operands?code=A0013Y0
- DTS: https://m.stock.naver.com/front-api/ipo/calculator/operands?code=A0015M0
- Earlier scheduled observation: doc/2026-10-13-1700-observed-retail.json
- Current October-November candidate ledger: doc/monthly-ipo-audit-2026-10-13.json

Both observations are post-session provisional calculator values, not confirmed
final allotments. DTS is day one of October13-14. Retail pools remain1000000
and557230; API underwriting volumes are not retail allocations. No schedules,
institutional results, company inputs or listing outcomes are changed.

## Validation and publication

### Verified values

| Issuer / broker | Source time KST | Applications | Indicative equal average | Direct proportional |
| --- | --- | ---: | ---: | ---: |
| MS Bio / KB | 16:05:31 | 286498 | 1.7452 | 3076.02 |
| DTS / Daishin | 16:05:30 | 83209 | 3.0135 | 60.49 |
| DTS / Eugene | 16:05:30 | 9008 | 3.0930 | 26.54 |

MS Bio now has7 observations and DTS4; every earlier snapshot is preserved.
The source labels/timestamps remain provisional rather than final allotments.

### Validation

- Target range: October1 through December1 exclusive, covering October/November.
 43 candidate decisions,28 included/matched,15 excluded,0 unresolved identities.
- Exact two-issuer publication candidate audit:0 errors,23 pending-listing-date
  warnings. Full local draft audit:1 existing Elice final aggregate error and23
  warnings. The explicit manual exception excludes the Elice draft and does not
  claim Elice final-result completeness or relax future scheduled gates.
- Both Python and PowerShell parsed573 publication JSON files. All1598 feed
  paths resolve and28 monthly identities match canonical inputs, details and
  yearly/index membership.
- All496 other stock detail files, unrelated feed rows, README, discovery and
  fundamental inputs, identifier bytes, and pre-existing Elice/tool/test drafts
  are preserved. Only the new broker input,2 stock details,4 changed feed indexes
  and this report are intended for publication. The recent/upcoming feeds have
  no semantic change and are excluded from staging.
- Dart static analysis of the isolated committed tool directory passed; both
  source-priority and equal-freshness regressions passed.
- Original Finuts live validation returned1 only for the existing exact SK SPAC14
  name alias. The primary-confirmed alias diagnostic returned0 against live
  Finuts. No validator was modified or listing placeholder imported.
- The CRLF-aware working-tree whitespace check passed. Staged bytes receive a
  separate default whitespace check before commit. No repository/global Git
  settings were changed.

Commands run from the nested E-drive repository:

```powershell
py -X utf8 build/refresh_20261013_post_session.py prepare
E:/harness/flutter/bin/cache/dart-sdk/bin/dart.exe run build/refresh-20261013-post-session/publication/tool/ipo_competition_batch.dart --backfill-years 3 --manual-fundamentals-path build/refresh-20261013-post-session/publication/data/manual_fundamentals.json --no-discover --no-public-live-collect --no-identifier-discover --no-ipo-korea-supplement --no-article-lead-manager-discover --broker-snapshot-dir build/refresh-20261013-post-session/publication/data/broker_snapshots --discovered build/refresh-20261013-post-session/publication/data/discovered/ipo_events.json --identifier-path build/refresh-20261013-post-session/publication/data/identifiers/ipo_identifiers.json --out build/refresh-20261013-post-session/output
py -X utf8 build/refresh_20261013_post_session.py merge
py -X utf8 build/refresh_20261013_post_session.py docs
py -X utf8 build/refresh_20261013_post_session.py validate
E:/harness/flutter/bin/cache/dart-sdk/bin/dart.exe analyze build/refresh-20261013-post-session/publication/tool
E:/harness/flutter/bin/cache/dart-sdk/bin/dart.exe run build/refresh-20261013-post-session/publication/tool/ipo_snapshot_priority_test.dart
E:/harness/flutter/bin/cache/dart-sdk/bin/dart.exe run build/refresh-20261013-post-session/publication/tool/ipo_equal_freshness_test.dart
& C:/Users/김용성/.codex/skills/refresh-ipo-monthly-data/scripts/audit_monthly_ipo_data.ps1 -RepoPath E:/harness/projects/publicofferingshares/ipo-data/build/refresh-20261013-post-session/publication -CandidateLedgerPath doc/monthly-ipo-audit-2026-10-13.json -AsOfDate 2026-10-13
& C:/Users/김용성/.codex/skills/refresh-ipo-monthly-data/scripts/audit_monthly_ipo_data.ps1 -RepoPath E:/harness/projects/publicofferingshares/ipo-data -CandidateLedgerPath doc/monthly-ipo-audit-2026-10-13.json -AsOfDate 2026-10-13
py -X utf8 tool/validate_finuts_schedule_sync.py --warn-on-analysis-issues
py -X utf8 build/check_schedule_alias_20261013.py
py -X utf8 build/refresh_20261013_post_session.py stage
git diff --cached --check
```

### Publication

The selected candidate is validated. Commit, push and exact public-endpoint
verification follow; their results will be recorded after verification.
