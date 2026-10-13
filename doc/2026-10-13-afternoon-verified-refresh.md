# October 13 afternoon verified-only refresh

## 1. Idea summary

Manual user refresh started 2026-10-13 14:33:38 KST, targeting October and
November. Data repository: E:/harness/projects/publicofferingshares/ipo-data.
Starting main: e7494780379e92e0c2288ca638f64344c421983a.

## 2. MVP scope

Problem: today's MS Bio and DTS broker counts/ratios changed since the 13:00
local draft; the app still reads the morning published version. Must-have:
source-reviewed observations, preserved history, scoped publication and exact
remote verification. Out of scope: app release/code changes, Elice partial-final
publication, unrelated documents or records. Success: only the two approved
issuers are updated on main and exact public app URLs match the committed data.

## 3. Feature specification

Recheck three independent schedule views and all 28 existing issuer DART indexes.
No new effective schedule or filing was found. There are still 43 candidate
decisions: 28 included (16 October/12 November), 15 excluded, 0 unresolved.
Elice's final aggregate remains undisclosed in checked sources, so the full
working-tree monthly audit cannot pass. The user explicitly approved this
run's exception: commit and push verified MS Bio/DTS only, withholding Elice.
This does not relax future scheduled zero-error gates.

Preserve the previously prepared 13:09 observations and add the latest actual
source observations, labeled intraday/provisional. Keep unknown subscribed
shares null and equal averages derived from published pools/account counts.
Run an isolated batch, merge only the two reviewed details into working feeds,
and stage shared feeds from HEAD plus only those two rows. Preserve Elice's
unpublished working-tree changes and all unrelated edits. Validate both the full
local draft and the exact staged publication scope, then verify remote SHA/raw
stock and active-feed JSON. Stop if any additional error appears.

## 4. Wireframe

Public calendars/filings -> verified live observations -> isolated regeneration
-> two-issuer scoped staging -> validation -> main push -> exact app URLs.

## Evidence, validation and publication

### Sources and verified values

Public calculators: [MS Bio](https://m.stock.naver.com/front-api/ipo/calculator/operands?code=A0013Y0)
and [DTS](https://m.stock.naver.com/front-api/ipo/calculator/operands?code=A0015M0).
Both use source baseTime **2026-10-13 14:34:35 KST**; Daishin's underlying
update time is 14:34:34 and Eugene's is 14:34:35. These are observations before
the subscription closes, not final allotments.

| Issuer / broker | Applications | Total competition | Direct proportional | Indicative equal average |
| --- | ---: | ---: | ---: | ---: |
| MS Bio / KB | 233993 | 1028.96 | 2056.91 | 2.1368 |
| DTS / Daishin | 71815 | 25.23 | 50.46 | 3.4917 |
| DTS / Eugene | 7672 | 10.64 | 21.28 | 3.6316 |

DTS aggregate 23.771 is allocation-weighted from rounded broker total ratios,
not an exact final aggregate. MS Bio's direct proportional 2056.91 is preserved
instead of doubling its total ratio. Retail pools are 1000000 for MS Bio and
557230 for DTS; no underwriting volumes are substituted. Unknown subscribed
shares stay null. Equal averages derive from published pool/application counts,
not a promise of individual allotment. The 13:09:31 observations and all earlier
history are preserved: MS Bio now has 6 snapshots and DTS has 3.

Calendars rechecked: [38](https://www.38.co.kr/html/fund/index.htm?o=k),
[Todaystk](https://todaystk.kr/ipo),
[Mainpy October](https://ipo.mainpy.dev/2026-10) and
[Mainpy November](https://ipo.mainpy.dev/2026-11). Mainpy now reflects the AL Robot
and Adel changes published this morning. All 28 issuer DART indexes still show
the same latest filings as the morning review. No unrelated date, price,
institutional, identity or listing-outcome input is changed in this publication.

Elice's [Mirae final notice](https://securities.miraeasset.com/bbs/board/message/view.do?messageId=2343308&categoryId=41)
and [Samsung final notice](https://www.samsungpop.com/customer/guide.do?cmd=notice_view&menuNo=020306&MenuSeqNo=24603)
have verified broker corrections, but the final aggregate and Mirae numeric
final proportional ratio remain undisclosed in checked sources. Their local
draft, the new official-source priority code and its tests are **not staged**.
Public Elice detail/rows remain byte-for-byte/equivalent to starting HEAD.

### Validation and scoped staging

The user explicitly approved "검증된 두 종목만 커밋·푸시" in this run.
No inference from a previous exception is needed. Isolated generation used
the existing committed batch implementation and committed inputs plus only
the two approved broker-input files; no pending Elice code/data was consumed.
The batch normalized its temporary discovery/identifier copies; those scratch
changes were excluded and original committed input bytes retained.

- Exact publication candidate monthly audit: **0 errors, 23 warnings**.
- Full working-tree monthly audit: **1 error, 23 warnings**. The Elice final
  aggregate error is still present and not suppressed. Candidate pass does not
  mean Elice's final results were resolved; its old provisional public data is
  unchanged under the user's explicit two-issuer exception.
- 43 candidate decisions: 28 included, 15 excluded, 0 unresolved; 16 October and
  12 November. Listing-date warnings remain pending rather than placeholders.
- Publication JSON: **572 parsed**, **1598 feed references** resolved, exact
  monthly identity/yearly membership checks passed. Both Python and PowerShell
  parsing passed. 496 other stock details and unrelated feed rows are unchanged.
- Dart static analysis passed (3 isolated committed files); both source-priority
  and equal-freshness regressions passed. No Dart code is in this publication.
- Original Finuts validator returned1 only for the known exact SK SPAC14 display
  alias. The primary-confirmed alias diagnostic returned0 against Finuts live.
  No validator was changed and the source's October16 listing placeholder was
  not imported.
- Hash checks preserve Elice's local draft, its generator/test changes, the old
  October2 ledger, identifier bytes and other pre-existing review documents.
- Shared feeds are staged from HEAD plus only the two reviewed rows, not by
  staging whole working files. The recent feed/Elice changes remain unstaged.
- Only two canonical snapshot inputs, two stock details, four feed indexes,
  README, the current candidate ledger and this report are intended for commit.

Commands (nested E-drive repository):

```powershell
py -X utf8 build/review_indexes_20261012.py
py -X utf8 build/refresh_20261013_afternoon.py prepare
dart run build/refresh-20261013-afternoon/publication/tool/ipo_competition_batch.dart --backfill-years 3 --manual-fundamentals-path build/refresh-20261013-afternoon/publication/data/manual_fundamentals.json --no-discover --no-public-live-collect --no-identifier-discover --no-ipo-korea-supplement --no-article-lead-manager-discover --broker-snapshot-dir build/refresh-20261013-afternoon/publication/data/broker_snapshots --discovered build/refresh-20261013-afternoon/publication/data/discovered/ipo_events.json --identifier-path build/refresh-20261013-afternoon/publication/data/identifiers/ipo_identifiers.json --out build/refresh-20261013-afternoon/output
py -X utf8 build/refresh_20261013_afternoon.py merge
py -X utf8 build/refresh_20261013_afternoon.py docs
py -X utf8 build/refresh_20261013_afternoon.py validate
& "$env:USERPROFILE/.codex/skills/refresh-ipo-monthly-data/scripts/audit_monthly_ipo_data.ps1" -RepoPath E:/harness/projects/publicofferingshares/ipo-data/build/refresh-20261013-afternoon/publication -CandidateLedgerPath doc/monthly-ipo-audit-2026-10-13.json -AsOfDate 2026-10-13
& "$env:USERPROFILE/.codex/skills/refresh-ipo-monthly-data/scripts/audit_monthly_ipo_data.ps1" -RepoPath E:/harness/projects/publicofferingshares/ipo-data -CandidateLedgerPath doc/monthly-ipo-audit-2026-10-13.json -AsOfDate 2026-10-13
flutter analyze --no-pub build/refresh-20261013-afternoon/publication/tool/ipo_competition_batch.dart build/refresh-20261013-afternoon/publication/tool/ipo_snapshot_priority_test.dart build/refresh-20261013-afternoon/publication/tool/ipo_equal_freshness_test.dart
dart run build/refresh-20261013-afternoon/publication/tool/ipo_snapshot_priority_test.dart
dart run build/refresh-20261013-afternoon/publication/tool/ipo_equal_freshness_test.dart
py -X utf8 tool/validate_finuts_schedule_sync.py --warn-on-analysis-issues
py -X utf8 build/check_schedule_alias_20261013.py
py -X utf8 build/refresh_20261013_afternoon.py stage
py -X utf8 build/refresh_20261013_afternoon.py check_index
git diff --cached --check
```

### Publication

Validation complete; commit, push and exact public-URL verification follow.
