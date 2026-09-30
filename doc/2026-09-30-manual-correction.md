# September 30 manual IPO correction

## Idea summary

User requested another factual correction after the 13:00 refresh. Start 14:06 KST; baseline main 18fda654ad7daaa8a287c068aaad0705fef3513e. Use refresh-ipo-monthly-data and preserve the existing worktree changes.

## MVP scope

Recheck September/October (September 1 through November 1 exclusive). Reuse today's reviewed 48-candidate ledger, refreshing source evidence before corrections. Prioritize unresolved listing-session highs and post-allocation vs demand-stage share figures. Do not insert current-day final outcomes before trading closes or replace differing metric bases without primary evidence.

## Feature specification

Cross-check current calendars, exact identities, public daily price data and DART issuance reports. Modify canonical inputs only for verified facts, generate in a separate scratch directory, preserve all historical feed members and dashboard-specific fields. Run JSON/scope checks, monthly audit, schedule synchronization, Dart analysis and diff checks. Scoped main publication follows only passing checks; verify exact remote stock/feed values.

## Wireframe

Fresh sources -> reconcile ledger and prior pending facts -> verified canonical corrections -> scratch generation -> scoped merge -> audit/tests -> main push -> Raw readback.

## Results

Calendar set remains 48 candidates, 27 matched, 21 excluded and zero unresolved based on current 38, Mainpy and Sonnimlab views. Exact company/date comparison covers all 27 target events and all public indexes. Prior morning evidence is retained by reference, not claimed to have been entirely re-extracted.

### Final allotment corrections

The [Bigwave September18 issuance report](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260918000412), section II, reports 248338 valid applications and 523112490 subscribed shares for 400000 retail shares. This makes final aggregate competition **1307.78**, not the earlier press-release/post-close **1375.34**. Final counts exclude duplicates and subscriptions exceeding permitted limits.

| Broker | Valid applications | Subscribed shares | Equal pool | Proportional pool | Total competition | Proportional ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Yujin | 87216 | 289777660 | 102000 | 102000 | 1420.48 | 2839.96 |
| Mirae | 161122 | 233334830 | 98000 | 98000 | 1190.48 | 2380.97 |

Ratios are arithmetic from published final counts, not guessed demand. Yujin's disclosed residual-demand method gives `(289777660-102000)/102000`. Mirae's [official notice](https://securities.miraeasset.com/bbs/board/message/view.do?categoryId=41&messageId=2342819) confirms the final applications and pools and warns that final proportional competition differs from the subscription-screen observation. Its [original refund table](https://securities.miraeasset.com/bbs/board/message/file.do?attachmentId=2147358), sheet `환불조견표`, rows5-7, states that equal shares use additional payment and excess subscriptions are excluded. Its proportional ratio is therefore `233334830/98000` rather than subtracting the equal pool. The workbook was read-only; no exported/modified workbook was produced.

The generator computes average equal shares from exact inputs: Yujin1.1695 and Mirae0.6082. These are not guaranteed fractional allotments. Official actual results are Yujin1 share each plus14784 lottery shares, Mirae0or1share. No estimated `expectedEqualShares` override was inserted.

The [Global September21 issuance report](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260921000349), section II, confirms77278 applications and57149700 subscribed shares. Equal/proportional pools are500000each. Aggregate57.15 and residual-demand proportional113.30 remain unchanged; the previously missing exact subscribed-share count is now available. Actual equal allotment is6shares each plus36332 lottery shares.

New snapshot timestamps `2026-09-30T14:17:31+09:00` are review times, not filing/subscription times. All prior snapshots remain intact.

### Final-result selection fix

Testing revealed `snapshotSourcePriority` ranked Naver90 and Finuts100 while reviewed DART/broker final allotment sources defaulted to0. App feeds therefore continued to select the old provisional Bigwave figures after adding the official report. Exact reviewed DART and KB final-source labels now rank110. Generic secondary labels containing `final` are not elevated. Tests cover later provisional refetches, input-order independence, subsequent official corrections, history preservation and no-final fallback.

KB34 and Korea17 were also regenerated and compared because they use the same reviewed source labels. Their existing final figures were already selected over same-rank secondary data, and both detail files remain unchanged. No unrelated August records or scores are republished.

### Listing-day highs

| Company | Listing date | Verified high | Existing regular close retained |
| --- | --- | ---: | ---: |
| Wiseplanet | September23 | 48000 | 46300 |
| Bigwave | September29 | 60000 | 29900 |
| Global | September29 | 29750 | 18220 |

Completed Naver daily data (`api.finance.naver.com/siseJson.naver`, codes0010S0/0035S0/486510) and its chart endpoint agree on the maxima. The endpoints are the same provider, not counted as independent sources. Session-specific corroboration is [Wiseplanet's15:35 close report](https://www.seoul.co.kr/news/economy/securities/2026/09/23/20260923500253), [Bigwave's regular-session quotation](https://www.mt.co.kr/search/stock?keyword=%EB%B9%85%EC%9B%A8%EC%9D%B4%EB%B8%8C%EB%A1%9C%EB%B3%B4%ED%8B%B1%EC%8A%A4) and [Global's15:35 close report](https://www.seoul.co.kr/news/economy/securities/2026/09/29/20260929500195) with [MK market data](https://stock.mk.co.kr/price/home/KR7486510001). The AI-assisted close reports are corroboration, not the sole evidence. Bigwave's60000 occurred during the regular session and equals the completed full-day maximum, resolving the previously missing high.

The Naver daily closes45700/27050/14500 differ from verified regular-session closes. Do not replace the existing regular closes with these differing-scope values. Neosapiens's daily high40000 still lacks sufficient regular-session corroboration and remains null. Duksan'sSeptember30 session is ongoing, so no final high/close is inserted.

### Basis and preserved fields

Final institutional allocation ratios differ from demand-request lockup ratios: Bigwave864590/1102926=78.39%, Global990000/3000000=33%. Neither replaces the demand-stage18.14%/0.27% inputs. Existing prospectus float fields are retained at their documented demand-stage basis. Separate actual-float stage support is not introduced by this correction. Schedules, offer prices, demand results, market caps, putback information, identifiers and unrelated records are unchanged.

### Generation and validation

Generate with `dart run tool/ipo_competition_batch.dart --backfill-years 3 --manual-fundamentals-path data/manual_fundamentals.json --no-discover --no-public-live-collect --no-identifier-discover --no-ipo-korea-supplement --no-article-lead-manager-discover --discovered build/refresh-20260930-manual/inputs/ipo_events.json --identifier-path build/refresh-20260930-manual/inputs/ipo_identifiers.json --out build/refresh-20260930-manual/output`.

The scratch batch has474 records; scoped merge preserves all488 published stock records and every historical feed member. Only reviewed records enter the public files. Wiseplanet's unrelated calibration sample-count drift is discarded; only its generated high return is merged. Dashboard-specific broker/putback fields are preserved, with Bigwave's actual dependent best-broker rate updated.

Validation completed:

- `py -X utf8 build/validate_manual_20260930.py`:548 JSON files,1561 feed rows,488 stock identities,27 ledger/canonical/manual/yearly matches.3 corrected details and485 unchanged. Exact final share/application totals, unchanged scores and preserved snapshot histories verified.
- `audit_monthly_ipo_data.ps1 -RepoPath E:\harness\projects\publicofferingshares\ipo-data -CandidateLedgerPath doc\monthly-ipo-audit-2026-09-30.json -AsOfDate 2026-09-30`:48 candidates,27 included,21 excluded,0 unresolved,0 errors,16 warnings. Warnings are unannounced October listing dates and remain null.
- `flutter analyze --no-pub tool/ipo_competition_batch.dart tool/ipo_pending_counts_test.dart tool/ipo_snapshot_priority_test.dart`: no issues.
- `dart run tool/ipo_pending_counts_test.dart` and `dart run tool/ipo_snapshot_priority_test.dart`: pass.
- `py -X utf8 tool/validate_finuts_schedule_sync.py --warn-on-analysis-issues`: exit0 using local seed fallback. Live Finuts retrieval timed out with WinError10060, so no claim of successful live Finuts synchronization. Three independent calendar views were checked separately.
- `git diff --check`: pass after normalizing only newly added blocks in mixed-line-ending files. Existing identifier file bytes and unrelated user files remain intact.

Publication targets nested data `main` only. Exact remote commit and Raw stock/recent/index/dashboard/yearly readback must be checked after push and reported in the turn result.

### App compatibility limitation

Read-only follow-up found that the app's `lib/main.dart::_brokerMetricSourcePriority` independently ranks Naver90 and reviewed DART0. Thus the corrected public feeds/analysis are valid, but existing app binaries can still select old broker details from preserved history. A separate app fix and release are needed; the user was asked whether to include that work. No app code, build/version or store state is changed by this data-only publication. Do not relabel DART sources as Naver or delete genuine history to bypass this consumer bug.
