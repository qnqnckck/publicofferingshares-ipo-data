# October 13 morning IPO refresh

## 1. Idea summary

The user requests updated data. First clock:2026-10-13 08:58:25 KST.
Review October1 through December1 exclusive using refresh-ipo-monthly-data.
Start from main db35fad73c58748b03fe62145e231b0ed106ee5e in the E-drive
nested data repository; preserve unrelated identifier/audit/document edits.

## 2. MVP scope

Resolve published demand results and refresh timestamped retail metrics.
Reconcile October/November calendars against primary filings for new issuers
and changed schedules. Current leads: DTS final demand, MS Bio morning data,
AL Robot revised window and Adel new November IPO. Classify the Simwon America
calendar entry before including it. Do not insert zero/pre-opening placeholders
or fabricate final retail/allotment results. Successful validation and scoped
publication are required for app reflection, as requested in this conversation.

## 3. Feature specification

Use at least three current calendars for discovery and latest primary filings
for disputed dates/values. Maintain a dated candidate ledger with exact issuer
identity and exclusion reasons. Prefer quantity-basis lockup computed from its
official numerator/denominator when press summaries disagree. Migrate changed
subscription IDs without duplicate stock files. Rebuild in isolation and merge
only reviewed records and necessary index memberships. Preserve history and
unrelated numerical fields. Run JSON, monthly, schedule, static and regression
checks; report unsolved issues explicitly rather than manufacturing values.

## 4. Wireframe

Current calendars -> exact issuer/filing reconciliation -> reviewed canonical
inputs -> isolated generation -> scoped feeds/detail -> validation -> authorized
main commit/push -> exact app URL verification.

## Evidence and validation

### Scope and sources

Target:2026-10-01 through2026-12-01 exclusive. Ledger43 decisions:
27 matched +1 added =28 included (16 October/12 November),15 excluded,
0 unresolved identities. Discovery compared38,Todaystk,Mainpy October/November
and StayRich; cached calendars are not authoritative for corrected schedules.
All27 previously included issuers' DART filing indexes were rechecked.
Unchanged facts and previously resolved exclusions were retained, not all
re-extracted. See [candidate ledger](monthly-ipo-audit-2026-10-13.json).

### Reviewed changes

| Issuer | Verified update |
| --- | --- |
| DTS | Offer18500KRW; institutional983.98:1;2381 participants; quantity lockup13.30593168%; float15.12%; market cap275312097500KRW; announced listing targetOctober23 |
| MS Bio | SourceOctober13 08:57:24 KST:KB106172 applications; total14.39; direct proportional27.78; indicative equal4.7093 shares |
| DTS retail | SourceOctober13 08:57:23 KST:Daishin8584 applications/proportional5.83; Eugene534 applications/proportional0.56. Estimated equal values derive from explicit pools, not final allocation |
| AL Robot | Corrected subscriptionNovember6-9; demandOctober28-November3; refundNovember11; migrate the oldOctober26 ID |
| Adel | NewNovember23-24 subscription; demandNovember12-18; refundNovember26; Samsung/Mirae Asset; band18000-25000; retail542500 baseline; float36.29%; no putback |

DTS primary source: [October12 corrected prospectus](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20261012000323).
Its quantity commitment table has218871000 committed shares out of1644913000,
stored as fraction0.133059, not percentage13.3059. Press summaries reporting
12.27% conflict with the primary quantity table and were not copied. There are
292 committed applications out of2381; application and quantity bases must not
be interchanged. The market cap uses14881735 post-offering shares, including
22290 additional underwriter shares. Floating2250787 shares is disclosed15.12%,
superseding old15.15%. [Eugene's official notice](https://m.eugenefn.com/no20r.do?msgId=523324&topFlag=0)
confirms the18500 price,55723 Eugene retail shares andOctober23 listing target.
The target is a future schedule, not a completed listing outcome.

[AL Robot corrected filing](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20261012000490)
overrides lagging calendars. Price band15700-17600,retail600000 and float35.01%
remain unchanged. Removed only the obsolete generated stock file
`에이엘로봇_2026-10-26.json`; its history is recoverable in Git. Canonical input,
manual key, upcoming/index/dashboard/year feed now point to the single new ID.

[Adel initial filing](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20261012000445)
confirms the baseline. Final price, institutions, lockup, market cap and listing
date remain null. [Simwon America filing](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20261012000149)
explicitly excludes Korean residents from its Nasdaq offering; the foreign
offering and illustrative reference dates are excluded, not imported as a
domestic IPO. Ucast's October19-20 historical schedule still awaits a corrected
effective filing after the correction demand; do not describe it as reconfirmed.

Retail sources: [MS Bio calculator](https://m.stock.naver.com/front-api/ipo/calculator/operands?code=A0013Y0)
and [DTS calculator](https://m.stock.naver.com/front-api/ipo/calculator/operands?code=A0015M0).
Both are provisional source observations with exact baseTime, not final results.
MS Bio's prior3 snapshots remain intact; its ratios were slightly revised down
without a new account count. DTS retail pools501507+55723 reconcile to557230;
the API's underwriting allocation is not retail allocation. DTS aggregate2.656
is weighted from rounded broker total ratios; proportional ratios are the
direct broker figures, not twice the aggregate. No subscribed-share counts
were invented. Equal estimates are pool/applications, not guaranteed allotment.

### Validation

- Monthly audit:43 candidates,28 local matches,15 exclusions,0 unresolved;
  **0 errors,23 warnings**, all unannounced listing dates. Future demand-result
  fields stay null. No publication exception or audit bypass was used.
- JSON:570 documents parsed using Python and PowerShell;1598 feed references
  resolve;28 monthly identities match inputs/details; each occurs once yearly.
- Public details498; index498,active2,upcoming23,yearly2026 85,dashboard504.
  Recent60 unchanged.494 unrelated existing stock details remain unchanged.
- Identifier file SHA256 remains
  `B389739C910688045D7E18E34ED1F9CD1C123CD8E85A7AD8E0A43B7766B1344A`.
  Existing unrelated audit/identifier/document edits are not staged.
- Dart static analysis:3 files,no issues. Both snapshot-priority and
  equal-freshness regressions passed.
- Unmodified Finuts validator returned1 for the known exact display-name alias
  `에스케이증권제14호스팩` versus `SK증권제14호스팩`; this was not hidden or treated
  as a missing IPO. [Primary prospectus20260917000001](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260917000001)
  confirms the exact issuer,SK lead manager andOctober15-16 subscription window.
  A diagnostic with only that source-backed alias returned0 using **Finuts live**.
  No persistent validator code changes and no Finuts listing placeholder copied.
- Full scoped diff checks and publication verification are recorded below.

Commands run from the nested E-drive repository:

```powershell
py -X utf8 build/refresh_20261013_morning.py ledger
py -X utf8 build/refresh_20261013_morning.py prepare
dart run tool/ipo_competition_batch.dart --backfill-years 3 --manual-fundamentals-path data/manual_fundamentals.json --no-discover --no-public-live-collect --no-identifier-discover --no-ipo-korea-supplement --no-article-lead-manager-discover --discovered build/refresh-20261013-morning/reviewed_discovered.json --identifier-path build/refresh-20261013-morning/identifiers.json --out build/refresh-20261013-morning/output
py -X utf8 build/refresh_20261013_morning.py merge
py -X utf8 build/refresh_20261013_morning.py validate
& "$env:USERPROFILE/.codex/skills/refresh-ipo-monthly-data/scripts/audit_monthly_ipo_data.ps1" -RepoPath E:/harness/projects/publicofferingshares/ipo-data -CandidateLedgerPath doc/monthly-ipo-audit-2026-10-13.json -AsOfDate 2026-10-13
flutter analyze --no-pub tool/ipo_competition_batch.dart tool/ipo_snapshot_priority_test.dart tool/ipo_equal_freshness_test.dart
dart run tool/ipo_snapshot_priority_test.dart
dart run tool/ipo_equal_freshness_test.dart
py -X utf8 tool/validate_finuts_schedule_sync.py --warn-on-analysis-issues
py -X utf8 build/check_schedule_alias_20261013.py
git diff --check
```

### Publication

The user previously explicitly approved commit and push in this conversation.
Published only this reviewed update to nested data `origin/main` as
`52664c8303867f1cde3087c9655ceb2a75b15c9f`.
`git ls-remote origin refs/heads/main` matched that local SHA after the push.
Exact GitHub Raw JSON matched all10 checked app endpoints: index,active,
upcoming,recent,yearly2026,dashboard,MS Bio,DTS,new AL Robot and Adel details.
No CDN mismatch or Contents API fallback was needed. The publication-evidence
documentation follow-up changes no app data.

```powershell
git commit -m "Refresh October-November IPO data and October 13 retail metrics"
git push origin main
py -X utf8 build/verify_remote_20261013.py
```
