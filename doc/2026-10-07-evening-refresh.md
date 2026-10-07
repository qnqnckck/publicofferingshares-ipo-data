# October 7 evening IPO refresh

## 1. Idea summary

Manual update requested at 23:25 KST, outside scheduled runs. Continue the uncommitted morning review without reusing an old publication exception. Target October 1 through December 1, 2026, exclusive, in E:/harness/projects/publicofferingshares/ipo-data; baseline main f02da4bc2a82701b2a3b078e665a5034b4e6d04e.

## 2. MVP scope

- Cross-check October/November candidates and newly filed disclosures, resolve MS Bio's missing final demand results, refresh Elice's day-one broker observation, and incorporate verified final retail results and missing new IPO baselines.
- Preserve unknowns, past snapshots, unrelated working changes and app code. No app release or automated workflow dispatch.
- Publish scoped data to main only after zero monthly audit errors and passing JSON, schedule, static-analysis and regression checks. Verify remote SHA and exact app-facing Raw URLs.

## 3. Feature specification

- Candidate ledger: use 38, Mainpy October/November, Sonnimlab and current disclosure leads. Primary documents decide exact identity, subscription window and announced fields.
- MS Bio: verify October 7 final prospectus, quantity-basis lockup, post-offering share denominator and float; do not retain pre-offering capitalization by accident.
- Elice: retain the morning snapshot, append October 7 16:04:53 broker data; this is day one, not final subscription/allotment. Preserve Samsung's directly reported proportional ratio.
- Melcon: official October 7 issuance report outranks October 2 calculator data; account count and equal/proportional pools must use the same final source.
- Genon: add the newly filed November 4-5 IPO baseline; leave final demand, competition and exact listing date pending.
- Regeneration: use isolated reviewed inputs and merge only reviewed details/feed rows; inspect every changed field and retain unrelated history.

## 4. Wireframe / execution flow

KST clock -> calendars / disclosure leads -> candidate ledger -> primary-source checks
-> canonical inputs -> local deterministic batch -> audits / diff review
-> scoped main push and Raw verification, or report blockers.

## Evidence and verification

### Source decisions

- [MS Bio final prospectus](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20261007000328), also [final terms](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20261007000327): offer7,800, demand1,096.56, participants2,269. Quantity-basis lockup is (81,080,000+47,924,000+283,795,000+540,631,000)/3,114,241,000 =30.615164%, stored0.306152. Rounded aggregator30.8% is not used. Section III says initial tradable6,347,310 shares/27,202,650 post-offering shares =23.33%; capitalization is27,202,650*7,800 =KRW212,180,670,000, replacing the prior pre-offering denominator. Contingent40,000 extra underwriter shares are not asserted as issued. No putback; retail1,000,000. Recalculated gradeA, score86.
- MS Bio's October22 listing is the issuer-announced plan in [NewsPim](https://web3.newspim.com/news/view/20261007001490) and [Yakup](https://www.yakup.com/news/index.html?cat=12&mode=view&nid=333469). Elice's October20 plan is reported by [ETNews](https://www.etnews.com/20261007000385) and corroborated by Mainpy/38. These future dates are announced plans, not completed KRX listing approvals.
- [Elice calculator](https://m.stock.naver.com/front-api/ipo/calculator/operands?code=A0158S0): actual source timestamp October7 16:04:53 KST. Mirae64,602 accounts, total9.27, proportional18.54, retail444,400; Samsung16,670 accounts, total8.88, proportional16.75, retail111,100. Both equal/proportional pools remain half of retail. Generated equal averages3.4395 and3.3323 are indicative, not final allotments. Aggregate9.192 is allocation-weighted from rounded reported totals. No invented subscribed-share count. GradeC+/53 remains based on final demand fundamentals reviewed in the morning.
- [Melcon official issuance report](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20261007000189), section II: final retail625,000, applications254,009, subscribed1,088,422,690, equal312,500 and proportional312,500. Aggregate1,741.48 is rounded from final counts; the explicitly stated residual-deposit proportional ratio is3,481.33, not2*aggregate. Each account receives one equal share and58,491 remaining equal shares are allocated by lottery (312,500-254,009). Summary average1.2303 is not fractional ownership. Source capturedAt is review time23:34:32, not the filing's unknown exact time. Preserve October2 history and demand-stage lockup; do not substitute final allocated-institution lockup.
- [Genon initial filing](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20261007000505): newly discovered via [FreshStock](https://stock.startuprecipe.co.kr/), corroborated by [Pikumo](https://pikumo.net/ipo). DemandOctober26-30, retailNovember4-5, refundNovember9, Samsung, band9,700-12,200, retail baseline550,000 (announced range550,000-660,000), float29.07%, no putback. Final offer/demand/retail results, market cap and exact listing date remain null; no premature numeric rating is presented.
- Jincostech's morning source-separated issuer closing aggregate685.83, calculator account147,227 and indicative equal average0.7234 are retained. Final allotment is not yet verified. [Hanwha SPAC6 October7 correction](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20261007000249) retains October20-21 retail/October23 refund; no unsupported schedule change made.

### Candidate and validation results

- Ledger:39 candidates,24 matched plus1 added,14 excluded,0 unresolved. Included25 IPOs/SPACs (18 October,7 November). All exact subscription windows are reconciled against canonical inputs, manual fundamentals, generated details and yearly feeds. Existing rights/asset-contract/duplicate exclusions remain intact. Prior primary evidence for unchanged fields is retained without claiming every historical filing was freshly extracted.
- Local-only batch: `dart run tool/ipo_competition_batch.dart --backfill-years 3 --manual-fundamentals-path data/manual_fundamentals.json --no-discover --no-public-live-collect --no-identifier-discover --no-ipo-korea-supplement --no-article-lead-manager-discover --discovered build/refresh-20261007-evening/reviewed_discovered.json --identifier-path build/refresh-20261007-evening/identifiers.json --out build/refresh-20261007-evening/output` passed. Isolated cache/crosswalk copies protect canonical inputs. Scoped reconciliation preserves490 unrelated detail files and historical feed rows. Public index495, active1, upcoming22, recent60, yearly2026=82, dashboard501.
- `py -X utf8 build/validate_refresh_20261007.py`: passed560 JSON documents,1,587 feed references,25 monthly identities, source-specific metrics/priority and unchanged unrelated rows. Equal indicators are computed from the corresponding verified pools/counts, never invented individual allotments.
- `audit_monthly_ipo_data.ps1 -RepoPath E:\harness\projects\publicofferingshares\ipo-data -CandidateLedgerPath doc/monthly-ipo-audit-2026-10-07.json -AsOfDate 2026-10-07`: passed,0 errors/22 warnings. Accepted pending warnings:21 unannounced future listing dates, plus Jincostech issuer aggregate has no account count (separate calculator observation carries the published account count). No gate was disabled and no one-off exception is needed.
- `flutter analyze --no-pub tool/ipo_competition_batch.dart tool/ipo_snapshot_priority_test.dart`: passed, no issues.
- `dart run tool/ipo_snapshot_priority_test.dart`: passed final/closing priority, corrected filings, preserved histories and provisional fallback.
- `git diff --check`: passed. Pre-existing identifier/October2 ledger changes and older untracked notes are excluded from the intended commit.
- `py -X utf8 tool/validate_finuts_schedule_sync.py --warn-on-analysis-issues`: exit0, local seed fallback. Finuts timed out (WinError10060), so this is not a live-Finuts verification claim; current independent calendars and changed primary schedules were checked separately.
- Independent PowerShell `ConvertFrom-Json` parsing: passed all560 input/output/ledger JSON documents. Final staged `git diff --cached --check`: passed.

### Publication verification

- Data commit `8de5c1a630c6a796ed45fbde8ade6502789aa72a` was pushed to the nested data repository `origin/main`; `git ls-remote origin refs/heads/main` matched local HEAD exactly.
- `py -X utf8 build/verify_remote_20261007.py`: passed all11 exact GitHub Raw documents on main: five reviewed stock details, index, active, upcoming, recent, yearly2026 and dashboard. Every fetched JSON matched its local reviewed value. No CDN fallback or stale-content exception was needed.
- This publication uses the normal zero-error gate. Unrelated local identifier/October2 audit work and older notes remain outside the commit. App binaries and workflows were not changed or dispatched.

The morning report remains historical evidence. Its five MS Bio errors were resolved by the new official results; no earlier exception approval was reused.
