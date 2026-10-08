# 2026-10-08 post-close manual IPO refresh

## 1. Idea summary

The user requested another data refresh after the 13:00 publication. Actual
start time: 2026-10-08 16:18:39 KST. Target October-November 2026, source rules
from `refresh-ipo-monthly-data`. Baseline data main:
`cbef8722d325a87e4e2e183524d6f5124cbe025f`.

## 2. MVP scope

- Update verified post-close Elice broker counts and equal/proportional inputs.
- Recheck all 39 monthly candidate decisions across current public calendars;
  retain 25 issuer identities and 14 exclusions, with no invented new dates.
- Record newly identified correction-demand risks for Ucast and Dabeeo.
- Preserve all history, demand-stage scores, fundamentals and unrelated work.
- Out of scope: application features, store release, invented final allotments,
  removal of an IPO merely because one calendar omits it, or guessed new dates.
- Success: zero audit errors, JSON/identity/feed/analysis tests, scoped main
  publication, exact remote JSON verification.

## 3. Feature specification

### Elice closing observation

https://m.stock.naver.com/front-api/ipo/calculator/operands?code=A0158S0

Source baseTime **2026-10-08 16:06:39 KST**. Mirae row updated16:06:38;
Samsung row updated16:06:39. The single snapshot uses the source baseTime, not
the review time. The October6 prospectus confirms subscription hours08:00-16:00:
https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20261006000451

| Broker | Applications | Retail pool | Equal/proportional pools | Total ratio | Proportional ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| Mirae Asset | 229,879 | 444,400 | 222,200 each | 408.76 | 817.52 |
| Samsung | 59,539 | 111,100 | 55,550 each | 406.28 | 811.55 |

Weighted aggregate **408.264** uses rounded broker ratios. Exact subscribed
shares remain null. Samsung proportional811.55 is directly reported, not a
mechanically doubled total406.28. Equal averages are indicative pool/count
calculations, not final individual awards. Source remains `naver_calculator_live`;
post-close does not mean officially finalized. Keep the three earlier snapshots.

After the verified16:00 close, include Elice in the generated recent feed,
respecting its existing60-row cap; the oldest row leaves that feed only, not
the full index, yearly feed or stock archive. Active feed retains the existing
date-inclusive same-day contract; no application filtering changes here.

### Monthly reconciliation and pending terms

Rechecked38, MainpyOctober/November and Sonnimlab; FreshStock used only to
discover filing links. Current38 now also lists GenonNovember4-5. No new
subscription window was verified. MainpyNovember web cache still showsOct7;
calendars may lag results and do not override primary filings.

- https://www.38.co.kr/html/fund/index.htm?o=k
- https://ipo.mainpy.dev/2026-10
- https://ipo.mainpy.dev/2026-11
- https://www.sonnimlab.com/schedule/
- https://stock.startuprecipe.co.kr/

Ucast's October1 correction demand suspends the filing's effectiveness and
explicitly says issue schedules may change. It is **not a withdrawal notice**;
no replacement subscription dates or subsequent correction were verified.
Dabeeo's DART index similarly contains an October6 correction demand. The last
filed October19-20 windows are retained as historical announced plans, **not
reconfirmed effective schedules**. Do not invent replacement dates or delete
these issuer identities. Record both risks in the candidate ledger and report.

- Ucast: https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20261001100050
- Dabeeo: https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20261006100034
- Corroboration: https://newspim.com/news/view/20261007000822

Jincostech's DART index was checked successfully after one transient connection
reset; no new final allotment filing was verified. Keep issuer closing685.83
separate from the provisional broker observation. DTS's latest prospectus is
stillOctober8; demand closes17:00 and final pricing is scheduledOctober12.
Unpublished offer/demand/allotment facts remain null. New listing outcomes are
not applicable to the October-start subscription candidates yet.

## 4. Wireframe

Public sources -> refreshed candidate ledger -> source snapshot -> isolated
batch -> scoped Elice stock/feed merge -> validations -> main push -> Raw check.
Home/detail: updated timestamp, count and indicative allocation metrics.
No UI or app build changes.

## Validation and publication

### Additional correctness gate found during generated-diff review

The existing `bestEqualExpectedSharesPerAccount` scanned every historical
snapshot and selected the maximum, retaining Elice's first-morning68.79-share
average in the portfolio expectation even after current broker averages fell
below1. This directly corrupts the refreshed derived data. Fix only this helper
to use the same source-priority/current-per-broker observations as broker scores,
then take the best current broker average. Preserve historical snapshots and
all demand grades. Add regression cases for declining counts/averages, input
order, final-over-provisional precedence, incomplete current data and aggregate
rows. Regenerate only the currently reviewed Elice output; no bulk historical
score rewrite or app release is authorized here.

Final local validation:

- Candidate identities:39, matched25 (October18/November7), excluded14,
  unresolved0. Ucast/Dabeeo schedule effectiveness risks are separate explicit
  notes, not claims that their old dates have been reconfirmed.
- Monthly audit: **0 errors,22 warnings** (21 pending listing dates and the
  Jincostech issuer aggregate's unpublished account count).
- JSON:562 documents parsed by Python and independently by PowerShell.
- Feed/identity checks:1,587 references valid;25 monthly identities each once;
  494 unrelated stock details unchanged. All four Elice snapshots retained.
- Latest equal averages: Mirae0.9666, Samsung0.9330; demand gradeC+/53 unchanged.
  The derived model's minimum-subscription expectation is now0.991 rather than
  the erroneous historical68.817. This is still a model scenario, not an award.
- New equal-freshness regression suite passed all cases. Existing official
  final/closing/provisional priority tests passed. Flutter analyze: no issues.
  Dart formatter emitted an unresolved flutter_lints include warning when run
  standalone; the subsequent Flutter analysis passed cleanly.
- Schedule synchronization: local seed fallback passed; live Finuts timed out
  (WinError10060). Live synchronization is not claimed. Three independent
  calendar checks and the primary filing checks above supplement that gap.
- Full generated diff reviewed, unchanged upcoming feed restored, diff check
  passed. No demand fundamental, source identifier or unrelated file changed.

Executed commands, from the nested E-drive data repository:

```powershell
py -X utf8 build/refresh_20261008_manual.py prepare
& E:\harness\flutter\bin\cache\dart-sdk\bin\dart.exe run tool/ipo_competition_batch.dart --backfill-years 3 --manual-fundamentals-path data/manual_fundamentals.json --no-discover --no-public-live-collect --no-identifier-discover --no-ipo-korea-supplement --no-article-lead-manager-discover --discovered build/refresh-20261008-manual/reviewed_discovered.json --identifier-path build/refresh-20261008-manual/identifiers.json --out build/refresh-20261008-manual/output
py -X utf8 build/refresh_20261008_manual.py merge
py -X utf8 build/refresh_20261008_manual.py validate
& E:\harness\flutter\bin\cache\dart-sdk\bin\dart.exe run tool/ipo_equal_freshness_test.dart
& E:\harness\flutter\bin\cache\dart-sdk\bin\dart.exe run tool/ipo_snapshot_priority_test.dart
& E:\harness\flutter\bin\flutter.bat analyze --no-pub tool/ipo_competition_batch.dart tool/ipo_snapshot_priority_test.dart tool/ipo_equal_freshness_test.dart
py -X utf8 tool/validate_finuts_schedule_sync.py --warn-on-analysis-issues
& "$env:USERPROFILE\.codex\skills\refresh-ipo-monthly-data\scripts\audit_monthly_ipo_data.ps1" -RepoPath E:\harness\projects\publicofferingshares\ipo-data -CandidateLedgerPath doc/monthly-ipo-audit-2026-10-08.json -AsOfDate 2026-10-08
git -c safe.directory=E:/harness/projects/publicofferingshares/ipo-data diff --check
```

Published commit: `a7fc7d47372c224352174f85aea8e5eea96119ee` on `origin/main`
in `qnqnckck/publicofferingshares-ipo-data`. Remote main SHA matched local HEAD.
`py -X utf8 build/verify_remote_20261008.py` confirmed semantic equality for
eight exact GitHub Raw URLs: index, active, upcoming, recent, yearly/2026,
dashboard, Elice detail and unchanged DTS detail. No alternate branch or
cache-busting URL was used. Only the pre-existing unrelated dirty files remain.
This follow-up changes publication evidence only.
