# 2026-09-16 verified local corrections

## Scope and publication

- KST review window: 2026-09-01 <= subscription start < 2026-11-01.
- Candidate ledger: `doc/monthly-ipo-audit-2026-09-16.json`.
- 20 candidates: 19 matched existing events, 1 added, 0 excluded, 0 unresolved identities. This is not a certification of every metric.
- Branch remains `main`, HEAD `4a5444f3e`. No commit, push, release, or remote publication verification was performed.
- User's confirmation was ambiguous; the explicitly announced scope was local verified data corrections and a real-data design preview, without publication.

## Verified changes

1. Bigwave Robotics: `hasPutbackRight=true`, six-month qualified summary. The [underwriter notice](https://m.eugenefn.com/no20r.do?msgId=521106&topFlag=0) specifies 2026-09-29 through 2027-03-28, an offer-price 90% base, index adjustment, and restrictions on sold/transferred/withdrawn shares. Do not describe this as an unconditional 90% guarantee.
2. Global Technology: offer KRW 10,000, top-band confirmation false, institution ratio 123.19, 961 participants. [Issuer announcement reported by Newspim](https://www.newspim.com/news/view/20260915001123), independently matched by [thebell](https://m.thebell.co.kr/m/newsview.asp?newskey=202609151704065520102649&svccode=01). Refund 2026-09-21 cross-checked on [38](https://www.38.co.kr/html/fund/?l=&no=2310&o=v&page=1) and StayRich; secondary-source confidence, not a primary filing certification.
3. Duksan Navcours: offer KRW 14,600, top-band confirmation true, institution ratio 868.64, 2,269 participants, quantity lockup 0.0405 (4.05%). [thebell](https://m.thebell.co.kr/m/newsview.asp?newskey=202609141609580920103091&svccode=00) and [Hankyung syndicated by Nate](https://m.news.nate.com/view/20260915n20247). Market cap is a derived integer: reported post-offering 18,893,889 shares x KRW 14,600 = KRW 275,850,779,400. The share count includes mandatory/extra underwriter acquisition; rounded news market caps differ by this basis.
4. DTS: added missing 2026-10-13 through 10-14 IPO, demand 10-01 through 10-08, refund 10-16, band KRW 17,000-18,500, Daishin/Eugene, retail baseline 557,230 shares. [Issuer announcement reported by Consumertimes](https://www.cstimes.com/news/articleView.html?idxno=720452) and [Dealsite](https://dealsite.co.kr/articles/168940) confirm name, KOSDAQ IPO, dates, band and total offering 2,228,917. Refund, co-underwriter and retail baseline cross-check use 38/ETFShopping. Final offer, demand metrics, listing date, float and exact cap remain null. Do not use the old conflicting Naver offering size or call band low the final offer.

Canonical changes are in `data/manual_fundamentals.json` and `data/discovered/ipo_events.json`. Four affected generated stock records and six feed indexes reflect the reviewed inputs. No retail snapshot was promoted to final.

## Remaining blockers and warnings

Monthly audit: **6 ERROR, 15 WARNING** after corrections, down from 17 ERROR / 14 WARNING before changes (the new DTS adds a pending-listing warning).

- Global Technology: quantity lockup, float, exact cap, putback summary remain unverified/null.
- Duksan Navcours: float and putback summary remain unverified/null.
- Existing generated schema defaults an unknown putback boolean to false. For Global/Duksan/DTS, that boolean is not evidence of absence; the missing summary is the audit gate, and the preview explicitly says '확인 전'. No schema migration was attempted in this data-only pass.
- 12 missing local listing dates, Brills demand-result publication pending, and two SPAC application counts form the 15 warnings. A missing local date does not prove no announcement exists elsewhere.
- Additional manual cautions not caught by the audit: Bigwave float/cap/retail-allocation discrepancies require primary recheck; Ellis refund 10/12 locally vs 10/13 on StayRich; Ucast is omitted by 38 but not proven withdrawn. Preserve prior data pending stronger evidence.
- WisePlanet's 9/15 16:06 broker observation in the pre-existing untracked audit file is provisional, not an approved final allocation. It was not applied in this pass.
- Bigwave 16.94% in thebell is application-count basis; the preserved 18.14% is quantity basis. Never replace one with the other or store 18.14 as a fraction.

## Commands and results

Run from the nested data repository unless stated otherwise. Dart executable: `D:\harness\flutter\bin\dart.bat`; Python executable: `C:\Users\김용성\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe`; Git executable: `C:\Users\김용성\.cache\codex-runtimes\codex-primary-runtime\dependencies\native\git\cmd\git.exe`.

```text
dart run tool/ipo_competition_batch.dart --backfill-years 3 --manual-fundamentals-path data/manual_fundamentals.json --no-discover --no-public-live-collect --no-identifier-discover --no-ipo-korea-supplement --no-article-lead-manager-discover
```

PASS: local-only generation produced 477 records. The time-relative three-year window and current scoring caused unrelated historical deletions/score normalization. Full result retained outside the data repository at `../build/design-qa/ipo-batch-2026-09-16/`. Mechanical scope cleanup restored original unreviewed history, reports and identifier normalization, retaining only reviewed records and applicable feed membership changes. It also preserved original discovered-file number formatting. No historical data was intentionally removed.

```text
powershell: & C:\Users\김용성\.codex\skills\refresh-ipo-monthly-data\scripts\audit_monthly_ipo_data.ps1 -RepoPath D:\harness\projects\publicofferingshares\ipo-data -CandidateLedgerPath doc/monthly-ipo-audit-2026-09-16.json -AsOfDate 2026-09-16
```

BLOCKED: 6 ERROR / 15 WARNING above. All 20 candidates resolve to one local event and stock file; no target feed-path or yearly duplication errors.

```text
python tool/validate_finuts_schedule_sync.py --warn-on-analysis-issues
```

PASS with local-seed fallback only. Finuts network request failed with Windows 10061; no live-provider parity claim.

```text
dart analyze tool/ipo_competition_batch.dart
```

NOT VERIFIED: sandbox initially denied analysis-server process creation; scoped retry outside sandbox encountered the pre-existing Dart perf-witness shutdown error at `%LOCALAPPDATA%/Dart/perf` (errno 1920). No Dart source was modified; do not label this an analysis pass. Do not delete user cache as a workaround.

```text
Get-Content <each changed/new JSON> -Raw | ConvertFrom-Json
git -c core.safecrlf=false diff --check
```

PASS: 13 changed/new JSON files parse and diff has no whitespace errors. Existing untracked 9/15 documents are preserved.

Preview (app repository build directory): `node build/design-qa/check-b-real-0916.cjs` PASS: 320/400px, three active selections, upcoming/past, report/back, bookmark isolation, calendar navigation, alternative cover/accent CSS, no overflow or JavaScript errors. Screenshots visually reviewed. This is not a Flutter/device build or app deployment test.
