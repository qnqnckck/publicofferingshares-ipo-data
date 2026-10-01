# October 2, 2026 reviewed refresh

## 1. Idea summary

Refresh the public IPO dataset for October and November 2026, checked from 06:16 KST on October 2. The configured D: checkout has moved to the existing E:/harness/projects/publicofferingshares/ipo-data repository. Use refresh-ipo-monthly-data and retain public source evidence.

## 2. MVP scope

- Problem: month rollover leaves November candidates and current subscription feeds incomplete.
- Required: research three independent calendars, reconcile every October/November identity, verify announced baselines and due results, and collect Melcon's prior-day observation if available.
- Also inspect recently completed Duksan Neocore/Brils listing outcomes, without mixing intraday, regular-session and after-hours closes.
- Out of scope: app code, store releases, unrelated user edits and guessed unpublished values.
- Success: canonical inputs and public feeds agree; all audit errors resolved before a scoped main commit/push and exact remote read-back. Current-day pre-opening figures must not be fabricated.

## 3. Feature specification

- Candidate period: 2026-10-01 through 2026-12-01 exclusive. Record duplicates, listed-company rights offerings and investment-contract offerings separately as excluded, not IPOs.
- Baselines: exact company/window/market/broker identity; primary prospectus or issuer/broker notice first. Percentages are fractions and lockup is requested-share quantity based.
- Retail: source timestamp and status preserved; October 1 observations are not October 2 final results. Jincostech has not opened as of this morning.
- Generation: local-only scratch batch; preserve historical members and unrelated scores. Regenerate all necessary date-sensitive feeds.
- Verification: JSON/path/identity validation, monthly audit, schedule synchronization, Dart analysis, regression tests and complete diff. Any ERROR blocks publication; no prior exception is assumed.
- Publication: data repository main only; preserve unrelated identifier state and old local notes. Verify remote SHA and the exact changed stock and active/upcoming feeds.

## 4. Wireframe / execution flow

KST clock -> calendars -> identity ledger -> primary evidence -> canonical edits
-> scratch generation -> scoped feed reconciliation -> validation -> main push -> remote read-back.

## Evidence and results

### Candidate reconciliation

Compared [38](https://www.38.co.kr/html/fund/index.htm?o=k), [Mainpy October](https://ipo.mainpy.dev/2026-10), [Mainpy November](https://ipo.mainpy.dev/2026-11) and [Sonnimlab](https://www.sonnimlab.com/schedule/). All 18 existing October identities/windows remain. Add four November IPOs; exclude 14 rights/offering, investment-contract or duplicate-alias rows. Ledger total: 36; included 22, excluded 14, unresolved 0. Old September ledger exclusions of November candidates were period-based, not invalid-company findings.

### Primary November baselines

- Baropharm: [October 1 corrected filing](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20261001000582), demand October 22-28, retail November 2-3, refund November 5; Mirae, band 16,400-20,200, retail 445,000 shares, initial float 30.38%, three-month putback. Risk section III: 3,629,716/11,947,817 shares. Do not persist 38's uncorroborated 952366 identifier.
- Nex-I: [September 28 filing](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260928000260), demand October 22-28, retail November 3-4, refund November 6; Korea Investment, band 10,500-13,000, retail 1,250,000 shares, float 37.03%, no putback. Risk table III shows initial 16,947,525 shares and 37.03%.
- Optonics: [September 29 filing](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260929000347), demand October 21-27, retail November 5-6, refund November 10; Korea Investment, band 10,000-12,000, retail 550,000 shares, initial float 22.71%, no putback. Risk section III lines 5195-5218: initial 2,172,890/9,566,000 shares. The 33.91% secondary summary describes cumulative float after six months; do not use as initial float.
- Hana Aerodynamics: [September 21 filing](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260921000405), demand November 2-6, retail November 10-11, refund November 13; IBK, band 4,700-5,700, retail 1,100,000 shares, initial float 30.83%, no putback. Risk section III: 5,465,381/17,729,964 shares.
- These are announced pre-demand baselines, not final allocations or final valuation. All final offer prices, institutional competition/lockup/participants, final market caps and listing dates remain null. New generated grades remain neutral `-`.

### Retail observation

[Naver public calculator](https://m.stock.naver.com/front-api/ipo/calculator/operands?code=A179880) returns baseTime/updtTm October 1 16:02:50, 95,884 applications, total competition 43.97, proportional 87.93, equal/proportional pools 312,500 each. The total IPO offering field is 2,500,000 and is not the retail allocation; use 625,000, consistent with the reviewed prospectus and the two pools. Subscribed shares are not published by this endpoint and remain null, not inferred from a rounded ratio. The normal generator derives equal average 3.2591 from 312,500/95,884. This is provisional day-one information, not final allocation. Source timestamp is retained with +09:00. [Jincostech calculator](https://m.stock.naver.com/front-api/ipo/calculator/operands?code=A250030) has null rates/counts before opening; no zero or invented snapshot is inserted.

### Listing outcomes and source conflicts

- Duksan Neocore September 30: regular open 27,100, high 28,700, close 17,980. KIND and next-day MK prior-close field support 17,980. Naver daily high agrees with the high observed during the regular session. The daily endpoint's differing-scope 16,830 close is not substituted.
- Brils October 1: regular open 61,800, high 62,100, close 30,450. [EBN completed-session report at 15:39](https://www.ebn.co.kr/news/articleView.html?idxno=1726440) explicitly reports all three; KRX-labelled Kokstock close corroborates 30,450. Do not use the stale KIND intraday 49,100, intraday 33,450/33,900, after-hours 31,250 or daily endpoint 31,100 as regular close.

### Generation and verification

- Local-only `dart run tool/ipo_competition_batch.dart --backfill-years 3 --manual-fundamentals-path data/manual_fundamentals.json --no-discover --no-public-live-collect --no-identifier-discover --no-ipo-korea-supplement --no-article-lead-manager-discover --identifier-path build/refresh-20261002/identifiers.json --out build/refresh-20261002/output`: PASS, 478 scratch stock records.
- `py -X utf8 build/reconcile_refresh_20261002.py --apply`: merge only seven reviewed details, retain all 488 pre-existing published identities and add four (492 total). Preserve 485 unrelated details/analysis and raw unrelated feed rows. Restore the batch's incidental discovered-analysis rewrite, retaining only the four intended new canonical rows. Reclassify current-date feed membership using existing batch rules; active has Melcon/Jincostech, upcoming has 20. Existing recent-feed rule includes subscriptions ending today; a recent-feed row is not evidence of a final result.
- JSON parsing: PASS, 552 data/generated files, plus today's ledger separately. Monthly audit `-AsOfDate 2026-10-02`: PASS, 36 candidates/22 included/14 excluded/0 unresolved, 0 errors, 21 warnings. Twenty unannounced listing dates and Elis demand results (next-weekday publication grace through October 2) remain pending; no zeros are supplied.
- `flutter analyze --no-pub tool/ipo_competition_batch.dart tool/ipo_snapshot_priority_test.dart`: PASS. `dart run tool/ipo_snapshot_priority_test.dart`: PASS.
- `py -X utf8 tool/validate_finuts_schedule_sync.py --warn-on-analysis-issues`: exit 0 using local seed fallback. Live Finuts timed out (WinError 10060); do not claim live synchronization. Current calendar and primary filing comparisons were independently completed.
- `py -X utf8 build/validate_refresh_20261002.py`: PASS, 553 JSON files including the ledger, 1,577 unique feed path/identity rows, 22 monthly identities; all 485 unrelated details, existing scores and identifier bytes preserved.
- Final scope: 20 reviewed source, generated-feed and documentation files only. Existing identifier-file state and earlier untracked audit notes are excluded. Publish to data repository `main` after the staged whitespace check; report the resulting commit SHA and GitHub Raw read-back separately.
