# October 12 manual IPO refresh

## 1. Idea summary

User requested a fresh data update. First clock:2026-10-12 14:34:57 KST.
Continue from the held13:00 draft without losing its verified12:58 observation.
Repository:E:/harness/projects/publicofferingshares/ipo-data, main baseline
f70d50e4ab35025e96b304d25b8e432292c03c22.

## 2. MVP scope

Review October/November calendars and new primary filings. Update only verified
facts, prioritize DTS final demand results and MS Bio live subscriptions.
Preserve unknown values and old source observations. Exclude unrelated edits,
app releases, UI and historical mass changes. Success:source-backed inputs,
valid regenerated data, scoped diff, monthly audit and publication verification.
If DTS still blocks the monthly audit, no publication without a new explicit
user-approved scoped exception; the scheduled gate remains unchanged.

## 3. Feature specification

Record broker account counts and directly reported competition ratios with real
source time. Derive only the existing explicitly indicative equal average from
verified pool/count inputs. Generate in an isolated directory; merge MS Bio
detail/summaries only. Preserve the prior upcoming-to-active transition and all
unrelated detail rows. Verify JSON, identity, feed references, schedule sync,
Dart analysis and regression tests. Keep requested publication separate from
unverified final-demand data and never call provisional data final.

## 4. Wireframe

Public source evidence -> dated broker observation -> isolated batch -> scoped
detail/current feeds -> validation -> authorized main publication -> app refresh.
No UI changes. Detail exposes source time and indicative broker metrics.

## Results

### Evidence and scoped update

Current38, Todaystk and MainpyOctober/November calendars agree with the reviewed
27 IPO/SPAC identities (17October,10November);41 candidate decisions include
14 exclusions and zero unresolved identities. No newly verified IPO/window
change was found. The existing same-day ledger was updated with the new MS Bio
observation, and every included issuer's DART index was checked again.

[Naver calculator](https://m.stock.naver.com/front-api/ipo/calculator/operands?code=A0013Y0)
sourceTime2026-10-12 14:31:43 KST reports KB90,680 applications, total11.78 and
direct proportional22.55. Confirmed retail pool remains1,000,000 shares,
split500,000 equal/500,000 proportional. These are provisional day-one numbers,
not final subscription competition or allotment. Do not double the total ratio.
The new source input is
`data/broker_snapshots/2026-10-12-afternoon-reviewed-retail.json`; the earlier
`2026-10-12-intraday-reviewed-retail.json` is preserved.

The [October7 MS Bio prospectus](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20261007000328)
remains the latest verified final terms. Fundamentals and gradeA/86 are unchanged.
Only MS Bio detail and five corresponding feed files change against main;
the morning upcoming-to-active transition is retained. Recent and496 unrelated
stock details are preserved, as are pre-existing identifier/audit/doc edits.

### Unresolved publication gate

The [DTS DART index](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20261008000030)
still shows theOctober8 prospectus, without new final terms. The
[Daishin public table](https://www.daishin.com/g.ds?m=194&p=1031&v=681)
shows a zero placeholder price/competition for DTS; these are not results and
are not inserted. The announced price-publication day isOctober12, but the
actual final results remain unverified at this review.

The six pending fields are offerPrice,topBandConfirmation,
institutionCompetitionRate,institutionParticipants,lockupCommitmentRate,
marketCapKrw. They remain null. The scheduled zero-error gate is not changed.
A new scoped-exception approval was requested for MS Bio only; no approval or
publication is assumed. No commit/push is claimed.

### Validation and publication

- 567 JSON documents parsed independently with Python and PowerShell.
- 1594 feed references resolve; all27 monthly identities match source inputs,
  detail and yearly/index feeds. Unique IDs, prior observations, unrelated
 496 detail files and the existing identifier-file hash are preserved.
- MS Bio's latest indicative equal average is5.5139 (=500000/90680), not an
  allotment entitlement. Demand inputs and grade remain unchanged.
- Flutter static analysis: no issues. Snapshot-priority and equal-freshness
  regression suites passed. Full scoped diff and `git diff --check` passed.
- Schedule validation passed using local seed fallback. Finuts live fetch timed
  out (WinError10060); live synchronization is not claimed.
- Monthly audit:6 errors (DTS fields above),23 warnings (future listing dates).
  This is not a fully passing monthly dataset. Errors were not suppressed.
- No staging, commit or push; publication remains subject to explicit approval
  of the requested MS Bio-only exception or resolution of all audit errors.
- Remote main was checked with `git ls-remote origin refs/heads/main` and remains
  `f70d50e4ab35025e96b304d25b8e432292c03c22`. No new Raw publication is claimed.

Commands from the E-drive nested data repository:

```powershell
py -X utf8 build/review_indexes_20261012.py
& E:\harness\flutter\bin\cache\dart-sdk\bin\dart.exe run tool/ipo_competition_batch.dart --backfill-years 3 --manual-fundamentals-path data/manual_fundamentals.json --no-discover --no-public-live-collect --no-identifier-discover --no-ipo-korea-supplement --no-article-lead-manager-discover --discovered build/refresh-20261012-manual/reviewed_discovered.json --identifier-path build/refresh-20261012-manual/identifiers.json --out build/refresh-20261012-manual/output
py -X utf8 build/refresh_20261012_manual.py merge
py -X utf8 build/refresh_20261012_manual.py validate
& C:\Users\김용성\.codex\skills\refresh-ipo-monthly-data\scripts\audit_monthly_ipo_data.ps1 -RepoPath E:\harness\projects\publicofferingshares\ipo-data -CandidateLedgerPath doc/monthly-ipo-audit-2026-10-12.json -AsOfDate 2026-10-12
& E:\harness\flutter\bin\flutter.bat analyze --no-pub tool/ipo_competition_batch.dart tool/ipo_snapshot_priority_test.dart tool/ipo_equal_freshness_test.dart
& E:\harness\flutter\bin\cache\dart-sdk\bin\dart.exe run tool/ipo_snapshot_priority_test.dart
& E:\harness\flutter\bin\cache\dart-sdk\bin\dart.exe run tool/ipo_equal_freshness_test.dart
py -X utf8 tool/validate_finuts_schedule_sync.py --warn-on-analysis-issues
git -c safe.directory=E:/harness/projects/publicofferingshares/ipo-data diff --check
```
