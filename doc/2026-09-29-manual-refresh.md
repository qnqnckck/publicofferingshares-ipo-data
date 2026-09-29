# September 29, 2026 manual IPO refresh

## Idea summary

User requested current data verification and update. Start: 2026-09-29 09:15 KST. Follow refresh-ipo-monthly-data in the migrated E:/harness/projects/publicofferingshares/ipo-data repository (configured D: location absent).

## MVP scope

- Problem: newly published demand results and corrected retail results must reach app data without fabricated pending figures.
- Required flow: three current calendar views, exact September/October candidate reconciliation, primary filing checks, canonical corrections, scoped batch regeneration and validation.
- Out of scope: UI/application releases, automation settings, incomplete listing-day outcomes, unrelated worktree changes.
- Success: publish only reviewed factual changes if every mandatory validation passes. A monthly audit ERROR remains a publication blocker; prior manual exceptions are not assumed.

## Feature specification

- Range: September 1 to November 1, 2026 exclusive, based on current Asia/Seoul date.
- Prioritize September 28 Jincostech final terms and Melcon's announced September 29 result. Verify quantity-basis lockup, final offer, participation and capitalization directly; band endpoints are not final results.
- Keep today's Bigwave Robotics/Global Technology intraday quotes out of final listing outcome fields until the session is complete.
- Preserve existing Korea SPAC 17 verified local snapshot and earlier documentation; revalidate it before any publication. Preserve the pre-existing identifier bytes and unrelated files.
- Prepare a new candidate ledger before canonical edits. Regenerate to scratch with copied mutable discovery/identifier inputs; merge only reviewed changes, preserving all historical stock/feed identities.
- Validate JSON, feed paths/uniqueness, monthly phase gates, schedule synchronization, Dart analysis and diff scope. Unknown remains null and percentage fields use fractions.

## Wireframe

Current KST -> calendars + primary disclosures -> exact candidate ledger -> canonical correction
 -> scratch batch -> scoped public feed merge -> checks -> zero errors: main commit/push and remote verification
 -> unavailable facts/audit errors: preserve null and report local-only status.

## Results

Baseline main: 0f5095e8f6f8ba578789a2c858d94d41451f113c. New ledger: `doc/monthly-ipo-audit-2026-09-29.json`.

### Coverage and fresh source checks

- Reviewed [38](https://www.38.co.kr/html/fund/index.htm?o=k), [Mainpy September](https://ipo.mainpy.dev/2026-09), [Mainpy October](https://ipo.mainpy.dev/2026-10), and [Sonnimlab](https://www.sonnimlab.com/schedule/). Mainpy September's retrieved page has a six-day crawl age and was used only as corroboration. The newer primary Jincostech disclosure takes precedence over stale calendar price placeholders.
- September has nine eligible retail windows and October eighteen. Ledger: **47 candidates, 27 matched, 20 excluded, zero unresolved**. Newly observed Nex-I (November 3-4 subscription) is excluded from the September/October subscription range; its October demand forecast does not make it an October retail IPO. Previously excluded listed-company offerings, investment contracts and duplicate identities remain excluded.
- Read fourteen DART filing-chain references from the previous ledger. New changes include Jincostech final conditions and Creates' September 28 amendment. Other chains retained their previously reviewed latest filing. Historical evidence not fetched again in full is explicitly retained as historical, not represented as a new exhaustive source extraction.
- Creates' [September 28 amendment](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260928000196) adds risk/governance/valuation disclosure; reviewed offering summary retains 1,400,000 shares, KRW 15,000-18,000 band, October 20-21 subscription and October 23 payment. No app-field change is warranted by the reviewed facts.
- Melcon's [prospectus filing chain](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260914000034) still ends at September 14 prospectus / September 9 registration in this morning's check. Current search/calendar evidence does not establish a final price or institutional result. September 29 is the announced publication date, not proof of publication. The six pending values remain null.
- No active retail subscription today. Today's Bigwave Robotics and Global Technology final listing outcomes remain unfilled because this is a morning, in-session review.

### Jincostech verified correction

Primary result: [September 28 final-condition filing](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260928000402). Direct final-result table: https://dart.fss.or.kr/report/viewer.do?rcpNo=20260928000402&dcmNo=11593050&eleId=1&offset=654&length=80231&dtd=dart4.xsd . This later 18:54 filing contains the demand table; the previous price-only filing and prospectus body are not sufficient by themselves for all demand metrics.

- `offerPrice`: **23,500 KRW**, `topBandConfirmation`: true.
- `institutionCompetitionRate`: **1097.62**, `institutionParticipants`: **2006**. Requested 701,380,000 shares / institutional allocation 639,000 shares agrees with the filed ratio. Do not adopt a conflicting media headline's 1068 ratio.
- Quantity lockup: (4,992,000 + 17,169,000 + 6,064,000 + 8,865,000) / 701,380,000 = 0.05288146226011577. Store **0.052881** (six-decimal fractional precision), displayed approximately **5.29%**. Do not substitute application-count 90/2006 or 5.29 as the stored fraction.
- `marketCapKrw`: **88,983,525,500**, from the [September 29 prospectus](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260928000394) stated post-offer 3,786,533 shares times KRW 23,500. Conditional extra underwriter acquisition is not assumed to have happened.
- Existing float 58.44%, public allocation 213,000 and no-putback policy were rechecked against that prospectus and preserved. Subscription October 2-6, refund October 8 and unannounced listing date remain unchanged.
- [ATS View's direct DART extraction](https://atsview.com/ipo/01158632) independently corroborates price, 2006 participation, 1097.62 competition and quantity-basis lockup. Rounded 5.3% there is consistent with the precise table-derived 5.29% stored here.
- Canonical correction is exactly six fields in `data/manual_fundamentals.json`. Existing model recalculation yields **75 / B+**; this is the app's existing reference score, not an investment recommendation. No score-model code changed, no retail snapshot/result was fabricated, and no listing outcome was inserted.

### Generation and validation

All commands ran in E:/harness/projects/publicofferingshares/ipo-data.

1. `py -X utf8 build/review_sources_20260928.py --dart`: read-only filing-chain checks. `py -X utf8 build/research_dart_20260918.py <receipt> --section <name>`: primary table/terms extraction. Extended this ignored local helper with bounded line output after an overly broad excerpt was truncated; reread all result-specific rows without truncation. A final-condition amendment has only a `증권발행조건확정` section, so the initial ordinary-prospectus section lookup was corrected without changing any data.
2. `dart run tool/ipo_competition_batch.dart --backfill-years 3 --manual-fundamentals-path data/manual_fundamentals.json --no-discover --no-public-live-collect --no-identifier-discover --no-ipo-korea-supplement --no-article-lead-manager-discover --discovered build/refresh-20260929-manual/inputs/ipo_events.json --identifier-path build/refresh-20260929-manual/inputs/ipo_identifiers.json --out build/refresh-20260929-manual/output`: PASS, generated 474 scratch records at 09:21:23 KST. Both mutable inputs copied to scratch first. No canonical discovery/identifier rewrite.
3. `py -X utf8 build/reconcile_refresh_20260929.py` then `--apply`: PASS. Merge only Jincostech stock and its index/upcoming/dashboard/yearly-2026 rows, keeping all 488 index members and 487 other local rows intact. Active/recent unchanged this turn. The prior Korea SPAC 17 correction remains intact. Scratch rolling-window count was not used to delete historical records.
4. `py -X utf8 build/validate_refresh_20260929.py`: PASS, **547 JSONs**, **1,561 unique per-feed identities/path references**. All 27 exact canonical/ledger/generated/yearly identities and manual facts agree. Against HEAD, only the two reviewed stocks differ; 486 unrelated details are unchanged. Canonical schedules/outcomes unchanged, and identifier SHA256 remains B389739C910688045D7E18E34ED1F9CD1C123CD8E85A7AD8E0A43B7766B1344A.
5. `flutter analyze --no-pub tool/ipo_competition_batch.dart`: PASS, no issues. `dart run tool/ipo_pending_counts_test.dart`: PASS (pending, mixed, published-rate, known-count and explicit-zero cases). Only process-local Git safe-directory settings were used; no global Git configuration edits.
6. `py -X utf8 tool/validate_finuts_schedule_sync.py --warn-on-analysis-issues`: exit 0, **local seed fallback only**. Finuts live timed out with WinError 10060; do not report this as a successful live Finuts sync. Public calendar checks independently corroborate the retained windows.
7. `audit_monthly_ipo_data.ps1 -RepoPath E:\harness\projects\publicofferingshares\ipo-data -CandidateLedgerPath doc\monthly-ipo-audit-2026-09-29.json -AsOfDate 2026-09-29`: FAIL, **6 errors / 18 warnings**, down from 12 errors. Errors are only Melcon's missing offerPrice, topBandConfirmation, institutionCompetitionRate, institutionParticipants, lockupCommitmentRate and marketCapKrw. Warnings are unannounced October listing dates. No errors are suppressed.
8. `git diff --check`: PASS. Full Jincostech source/detail diff reviewed; generated analysis changes correspond to six verified inputs. Only intended feed row/timestamp changes accepted. `git ls-remote origin refs/heads/main`: unchanged **0f5095e8f6f8ba578789a2c858d94d41451f113c**.

### Publication status

At the end of preparation, the ordinary publication gate remained blocked by Melcon's six errors. The user then explicitly selected **"검증된 두 종목만 푸시"** in response to the scoped-exception question. This authorizes committing and pushing only the verified Jincostech and Korea SPAC 17 corrections and their supporting evidence, leaving all Melcon pending fields null.

The monthly audit remains FAIL (6 errors), not silently waived or reported green. This manual exception does not transfer to future heartbeats. No audit rules, schedules, workflows or source placeholders are changed. Prior unrelated identifier state and historical local notes are excluded from staging.

Publication sequence under this approval: review explicit staged paths and JSON, commit the two-stock correction to main, push main, compare local/remote SHA, then read exact GitHub Raw Jincostech/Korea SPAC 17 details and upcoming/recent feed rows. Record verified commit and remote results in the task completion report; do not equate local generation or a push attempt with successful app-facing publication.
