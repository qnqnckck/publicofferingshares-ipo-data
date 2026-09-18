# September / October 2026 manual data refresh

## Idea summary

The user requested a fresh data update. Clock verified 2026-09-18 02:18:44 UTC (11:18:44 Asia/Seoul). Review the complete current/next-month window `[2026-09-01, 2026-11-01)`, including pending corrections and newly available demand/retail results.

## MVP scope

- Problem: missing demand fields and stale subscription snapshots have blocked recent publication.
- Must-have flows: independent calendar reconciliation; primary evidence for missing facts; source-input updates; deterministic public-feed rebuild; monthly audit, JSON, schedule-sync and static-analysis checks.
- Out of scope: app UI, store release, unrelated historical changes, fabricated or unpublished values, credentials, and GitHub Actions.
- Success: verified source values appear consistently in regenerated detail and index feeds. Any audit ERROR blocks publication. Prior authorization to publish verified IPO data remains conditional on all gates passing; record commit/remote verification separately from local work.

## Feature specification

1. Candidate ledger: compare at least three current schedule views, match exact company and subscription window, and record additions/exclusions/conflicts. Unknown identity blocks publication.
2. Fundamentals: use filings and issuer/underwriter notices first. Store percentages as fractions and explicitly distinguish quantity-basis lockup from application-count basis. Missing facts remain null.
3. Retail: retrieve latest broker/provider observations with source capture timestamps. Distinguish intraday/day-one/post-close observations from confirmed final allocations. Derive an equal-allocation expectation only from published pool and application counts and label it as an expectation, never guaranteed allotment.
4. Regeneration: preserve all pre-existing dirty work; run the existing local-only batch with isolated output where supported. Apply only target-month outputs and necessary coherent feed indexes after reviewing historical churn.
5. Gates and handoff: run the monthly audit and validation; investigate all errors without weakening the audit. Publish only a reviewed scoped diff when every gate passes, otherwise report remaining specific missing evidence and what was updated locally.

Verification-discovered scope: preserve unknown retail subscribed-share counts as null through parsing, aggregation and serialization. Do not derive exact counts from rounded ratios. Keep explicitly published zero counts and published ratios intact, and add focused regression coverage before rebuilding. This supports the data contract; it does not change investment scoring or app UI.

## Wireframe

```text
Fresh Sep/Oct schedule ledger -> exact identity + official evidence
 -> reviewed canonical inputs -> deterministic generated feeds
 -> JSON / monthly audit / schedule sync / static analysis
    -> pass: scoped publication + remote verification
    -> errors: retain verified local changes; explain blockers
```

## Starting state

Nested repository `main`, HEAD `4a5444f3e`. Existing uncommitted Sep 16 fundamentals, DTS discovery, generated feeds, and audit documents are preserved. The last audit had six missing-demand-field errors; re-evaluate against Sep 18 rather than assuming those counts remain current. No app files are in scope.

## Verified corrections and sources

Ledger: 24 exact company/subscription windows, 19 matched, 5 added relative to the previous publication (DTS was already an unpublished local addition), 0 excluded ledger entries, 0 unresolved identities. Four new discoveries today: Davio, Intellivix, Hanwha Plus No. 6 SPAC, iM No. 1 SPAC. Five independent calendar sources were consulted; calendar prices are not treated as confirmed offers.

### Fundamentals

- Global Technology: quantity lockup 0.27% = 990,000 / 369,558,000 rounded; float 22.19%; post-offering 19,646,144 shares x KRW 10,000 = cap KRW 196,461,440,000; no retail putback. [DART September 15 prospectus](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260915000241), sections I and III. Reject application-count lockup and stale share counts.
- Duksan Navcours: no retail putback; preserve demand quantity lockup 4.05%. [DART prospectus](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260915000084) confirms 18,893,889 post-offering shares and pre-allocation float 40.11%. [September 17 issuer announcement](https://www.newspim.com/news/view/20260917001294) supersedes float with 6,289,515 / 18,893,889 = **33.29%**. Allocated-institution lockup 57.26% is not demand-stage lockup. Exact cap remains KRW 275,850,779,400.
- Brils: KRW 19,500 (top band), 2,181 institutions, demand 1,187.74:1, quantity lockup **21.75%**, float **26.42%**, retail 300,000, no putback. [DART prospectus](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260916000240), I/III: locked demand (9,029,000 + 5,471,000 + 36,284,000 + 135,191,000) / 855,171,000; float 3,002,063; post-offering 11,363,649 shares x 19,500 = cap KRW 221,591,155,500. Reject secondary parser values 27.67% and 60.17%. [Issuer announcement](https://marketin.edaily.co.kr/News/ReadE?newsId=04398486645580776) confirms October 1 listing.
- Bigwave: retail corrected 380,000 -> **400,000**, float **26.76%**, post-offering 10,270,411 shares x 18,000 = cap KRW 184,867,398,000. [DART September 14 prospectus](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260914000385), I/III. Preserve quantity lockup 18.14% and [qualified six-month Eugene putback](https://m.eugenefn.com/no20r.do?msgId=521106&topFlag=0), including index/eligibility restrictions.
- Neosapiens: latest allocated float **23.21%**, [September 16 issuer announcement](https://m.edaily.co.kr/News/Read?mediaCodeNo=257&newsId=02761766645580776). Retain demand lockup 8.98%, not allocated-institution 44.02%. [DART prospectus III](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260909000317): 12,215,845 post-offering shares x 10,000 = cap KRW 122,158,450,000.
- WisePlanet: latest allocated float **14.42%**, 1,445,356 shares, [September 16 issuer announcement](https://marketin.edaily.co.kr/News/ReadE?newsId=02683046645580776). Demand lockup 3.65% is unchanged.

### Schedules

| Event | Demand | Subscription | Refund | Band / retail baseline | Filing |
| --- | --- | --- | --- | --- | --- |
| Davio | Oct 7-14 | Oct 19-20 | Oct 22 | KRW 12,300-16,600 / 250,000 | [DART](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260915000390) |
| Intellivix | Oct 12-16 | Oct 21-22 | Oct 26 | KRW 7,600-9,000 / 625,000 | [DART](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260916000544) |
| Hanwha Plus No. 6 SPAC | Oct 14-15 | Oct 20-21 | Oct 23 | KRW 2,000 / 1,500,000 | [DART](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260917000365) |
| iM No. 1 SPAC | Oct 19-20 | Oct 26-27 | Oct 29 | KRW 2,000 / 1,250,000 | [DART](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260917000220) |

Future retail quantities are filing baselines, not final allocations. New future offers, demand results, listing dates and putback status remain unknown in canonical inputs. Legacy putback serialization defaults unknown booleans to false; missing summary is **not** proof of absence. No putback schema migration is included.

Melcon refund **Oct 6 -> 7**, [prospectus I](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260914000034). Ellis Group refund **Oct 12 -> 13**, [filing I](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260904000517). DTS's previously verified local addition is retained; its evidence is in `2026-09-16_verified-local-update.md`.

### Retail and finality

Canonical evidence is `data/broker_snapshots/2026-09-18-reviewed.json`. Retail offered shares are equal + proportional pools, **not** Npay `orgAllcShares` (includes institutions). Expected equal shares = published pool / applications, not guaranteed allocation. Unknown exact subscribed counts remain null, never inferred from rounded ratios.

| Company / broker | Source time KST | Applications | Equal pool | Expected equal | Proportional |
| --- | --- | ---: | ---: | ---: | ---: |
| WisePlanet / Daishin | Sep 15 16:06:31 | 165,616 | 200,000 | 1.2076 | 2,955.3 |
| Bigwave / Eugene | Sep 16 16:04:22 | 87,303 | 102,000 | 1.1683 | 2,840.94 |
| Bigwave / Mirae | Sep 16 16:04:21 | 161,196 | 98,000 | 0.6080 | 2,655.68 |
| Global / KIS | Sep 17 16:01:17 | 77,278 | 500,000 | 6.4701 | 113.3 |
| Duksan / Daishin | Sep 17 16:01:19 | 132,911 | 375,000 | 2.8214 | 665.64 |
| Brils / IBK | Sep 18 11:29:15 | 63,682 | 150,000 | 2.3555 | 1,299.26 |

Brils is intraday, not final. Global/Duksan/Brils provider responses were fetched today. WisePlanet/Bigwave use preserved Sep 15/16 observations corroborated by fresh news; their old URLs are not described as freshly fetched. WisePlanet 591,060,000 subscribed shares and Duksan 249,614,780 are explicitly reported in issuer articles above.

Bigwave issuer aggregate **1,375.34** ([company announcement](https://www.mimint.co.kr/bbs/view/news/S1N11/5586822)) differs slightly from earlier broker-weighted 1,375.08. [Broker figures and counts](https://biz.newdaily.co.kr/site/data/html/2026/09/16/2026091600326.html) match preserved observations. Keep aggregate and broker source times separate; do not call them final allotments. Global's total 57.15 matches [KIS](https://www.truefriend.com/main/Main.jsp); preserve provider proportional **113.3**, not double the total.

## Validation and limitations

- Monthly audit: **24 events, 0 ERROR, 17 WARNING** (15 unknown October listing dates, missing application counts for KB No. 34 / Korea No. 17 SPAC). Unknowns were not guessed; this is not certification of every historical metric.
- Local batch: **481 generated records** in isolated output. Applied only **13 reviewed stock records and six feeds**, retaining **472 unrelated index rows byte-for-byte** and history outside the moving three-year cutoff. Final index: 485 records. No historical stocks removed.
- Restored unrelated identifier normalization and two embedded unreviewed SPAC analysis changes made by the batch. App files/unrelated local work are untouched.
- Regression executable PASS: pending/mixed counts, explicit zero, published ratios, equal expectation.
- Changed/untracked JSON parsing: PASS (27 documents including prior audit evidence); `git -c core.safecrlf=false diff --check`: PASS. App parser inspection confirms these source names retain Npay priority, and nullable subscribed-share counts are not consumed by the app's broker parser.
- Batch static analysis PASS on isolated retry (`No issues found`). Combined/test-only analyzer attempts hit the pre-existing Windows Dart perf-witness shutdown error (errno 1920); test execution compiles/passes. User caches were not deleted. Do not claim every analyzer invocation passed.
- Finuts schedule validation PASS **local-seed fallback only**: public request timed out (WinError 10060). Live Finuts parity is unverified. Fresh independent calendars and primary filings provide the reconciliation evidence.

### Commands

```text
dart run tool/ipo_competition_batch.dart --backfill-years 3 --manual-fundamentals-path data/manual_fundamentals.json --no-discover --no-public-live-collect --no-identifier-discover --no-ipo-korea-supplement --no-article-lead-manager-discover --out build/refresh-20260918/output
python -X utf8 build/apply_reviewed_20260918.py
powershell: & C:\Users\김용성\.codex\skills\refresh-ipo-monthly-data\scripts\audit_monthly_ipo_data.ps1 -RepoPath D:\harness\projects\publicofferingshares\ipo-data -CandidateLedgerPath doc/monthly-ipo-audit-2026-09-18.json -AsOfDate 2026-09-18
dart run tool/ipo_pending_counts_test.dart
dart analyze tool/ipo_competition_batch.dart
dart analyze tool/ipo_pending_counts_test.dart
python -X utf8 tool/validate_finuts_schedule_sync.py --warn-on-analysis-issues
git -c core.safecrlf=false diff --check
```

The ignored `build/apply_reviewed_20260918.py` mechanically copies reviewed generated records and merges feeds with semantic/path assertions; it is not a new data source. All canonical updates use reviewed input files.

## Publication

Publication gate cleared: zero monthly data errors, passing regression execution and batch analysis, with the analyzer/Finuts infrastructure limitations disclosed above. Target is `main` at `https://github.com/qnqnckck/publicofferingshares-ipo-data.git`. Remote commit and served-feed verification will be recorded after the push. No GitHub Actions, app binaries, or store releases are part of this update.
