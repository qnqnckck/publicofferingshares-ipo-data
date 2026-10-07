# October 7, 2026 manual IPO data refresh

Morning review only. The [evening continuation](2026-10-07-evening-refresh.md)
subsequently verified MS Bio's newly published results and resolves the five-error
publication blocker recorded below. Retain this report as historical evidence.

## 1. Idea summary

Review October-November IPOs in the relocated E:/harness/projects/publicofferingshares/ipo-data repository. Start time: October 7 08:46:10 Asia/Seoul. Baseline main: f02da4bc2a82701b2a3b078e665a5034b4e6d04e. Apply refresh-ipo-monthly-data source, identity and data contracts.

## 2. MVP scope

- Problem: recently closed retail subscriptions and newly due demand results may be stale in app-facing feeds.
- Required: cross-check three calendars, exact identities and primary disclosures; update verified inputs; regenerate affected feeds; run JSON, monthly audit, schedule and Dart checks; publish to main only with zero audit errors.
- Out of scope: unrelated app code/releases, guessing unpublished results, changing source timestamps, modifying earlier local work, or reusing prior one-off publication exceptions.
- Success: verified current facts are visible through exact public stock/feed URLs; unavailable fields remain null and publishing blockers are reported truthfully.

## 3. Feature specification

- Candidate reconciliation: compare every October-November candidate with canonical events, manual fundamentals, generated details and yearly/feed membership. Exclude rights offerings and duplicate aliases with evidence.
- Institutional results: use latest primary filing, quantity-basis lockup and confirmed final terms. Keep uncertainty explicit rather than entering zeros or provisional final metrics.
- Retail results: compare source timestamps and official final allotment priority; preserve historical observations. A closing calculator observation is not automatically a confirmed final allotment.
- Publication: review scoped changes and preserve unrelated working files; block on every audit error. Confirm remote main SHA and exact Raw stock/feed contents after an authorized push.

## 4. Wireframe / execution flow

KST clock -> calendars and candidate ledger -> primary demand/retail evidence
-> reviewed canonical updates -> deterministic local regeneration
-> JSON/monthly/schedule/Dart validation -> scoped main publication or documented blocker.

## Evidence and outcome

### Schedule and identity review

- Reopened [38](https://www.38.co.kr/html/fund/index.htm?o=k), [Mainpy October](https://ipo.mainpy.dev/2026-10), [Mainpy November](https://ipo.mainpy.dev/2026-11), [Sonnimlab](https://www.sonnimlab.com/schedule/) and [Todaystk](https://todaystk.kr/ipo). Calendar pages can lag result announcements; use them for candidate discovery and confirm changed results from primary documents.
- Ledger: 38 candidates, 24 matched (18 October, six November), 14 excluded, zero unresolved. No new verified IPO or changed subscription window. Existing listed-company rights offerings, asset-contract offerings and the duplicate SK SPAC alias remain excluded. Preserve their documented reasons.
- Public feed membership is refreshed to October 7: Elice active, Jincostech recent, 21 upcoming. The recent feed still holds 60 rows; its oldest dropped entry remains in index/yearly/detail history. No company history is deleted.

### Reviewed factual changes

- [Elice October 6 final prospectus](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20261006000451), with [final-terms notice](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20261006000447): offer KRW90,500, institutional competition388.84, participants2367, quantity-basis lockup (8698200+5487400+8179800+66391400)/647998700 =13.6970645% (stored0.136971). Do not substitute institution-count percentages. Initial float is20.77%, not prior20.81%; post-offering11128864 shares includes the additional underwriter acquisition, giving KRW1007162192000 at the final offer. Retail555500; no putback. Primary sources reviewed here still say October listing without a confirmed exact day, so secondary October19 leads are not silently promoted to a firm listing date.
- [Elice calculator](https://m.stock.naver.com/front-api/ipo/calculator/operands?code=A0158S0), actual source timestamp October7 08:46:59 KST: Mirae3230 applications, total0.38, proportional0.76, equal/proportional pools222200 each; Samsung883 applications, total0.61, proportional0.22, pools55550 each. Preserve the reported proportional ratios rather than doubling all totals. Aggregate0.426 is the allocation-weighted average of rounded broker ratios, not exact subscribed shares. This is intraday, not closing/final data. Unpublished subscribed-share counts remain null. Recalculated rule-based gradeC+, score53; not investment advice.
- [Jincostech October6 issuer closing disclosure](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20261006600712): retail213000, subscribed146081630, reported aggregate685.83. Its capturedAt records the October7 08:51:01 review, not a fabricated filing time. Preserve the separate [October1 prospectus](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20261001000594) minimum equal-pool106500 baseline. Final account count/allotment is not disclosed in that closing notice.
- [Jincostech calculator](https://m.stock.naver.com/front-api/ipo/calculator/operands?code=A250030), source timestamp October6 16:14:51 KST: applications147227, total685.33, proportional1370.66, pools106500 each. Derived equal average0.7234 is indicative, not a final entitlement. Keep this broker observation separate from the issuer aggregate. The new `dart_subscription_close_report` source ranks105: below official final allotments110 but above provisional collectors. Regression tests cover both precedence boundaries.
- [MS Bio October1 prospectus](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20261001000006): confirmed no putback and filled the missing explanatory field. Latest document index still has September29 correction/October1 prospectus. Final-price announcement is scheduled for October7; no final terms were available in this morning review. Leave offerPrice, topBandConfirmation, institutionCompetitionRate, institutionParticipants and lockupCommitmentRate null.
- Melcon calculator now returns404, so do not erase its earlier verified observation or accidentally reuse another request's response. No new final-allotment facts were verified; its published record is unchanged.

### Generation and validation

- Regenerated with `dart run tool/ipo_competition_batch.dart --backfill-years 3 --manual-fundamentals-path data/manual_fundamentals.json --no-discover --no-public-live-collect --no-identifier-discover --no-ipo-korea-supplement --no-article-lead-manager-discover --discovered build/refresh-20261007/reviewed_discovered.json --identifier-path build/refresh-20261007/identifiers.json --out build/refresh-20261007/output`. Used isolated source/crosswalk copies to avoid incidental canonical-cache churn. Merged only the three reviewed detail files and affected feed rows. Kept unrelated historical rows and MS Bio's unchanged analysis.
- `flutter analyze --no-pub tool/ipo_competition_batch.dart tool/ipo_snapshot_priority_test.dart`: passed, no issues.
- `dart run tool/ipo_snapshot_priority_test.dart`: passed final/closing precedence, corrected filings, historical preservation and provisional fallback.
- `py -X utf8 tool/validate_finuts_schedule_sync.py --warn-on-analysis-issues`: exit0 using local seed fallback. Live Finuts timed out with WinError10060; this is not a claim that live Finuts was verified. Independent public calendars were checked separately.
- Monthly audit (October1 through December1 exclusive): exit1, five errors and23 warnings. All errors are the five unpublished MS Bio fields above. Warnings are22 unannounced listing dates and the absent account count in Jincostech's issuer aggregate; the separate calculator account count remains available. The earlier intermediate aggregate-row equal-pool omission was resolved using the prospectus baseline, not invented allotment data.
- `git diff --check`: passed after normalizing only newly inserted line endings. Pre-existing identifier and October2 ledger working changes, plus earlier local notes, are preserved.
- `py -X utf8 build/validate_refresh_20261007.py`: passed558 JSON documents,1583 feed references,24 exact monthly identities and491 unchanged existing detail records. Confirmed Elice gradeC+/score53, Jincostech source-separated aggregate/broker indicators, five MS Bio null fields, and current active/upcoming/recent membership. Independent PowerShell `ConvertFrom-Json` parsing passed all558 documents. No staged files.

### Publication status

Remote main was checked and remains f02da4bc2a82701b2a3b078e665a5034b4e6d04e. No commit/push has been performed under the ordinary five-error gate. A one-run user decision was requested about publishing only these verified changes while keeping MS Bio final results null; absent a new explicit approval, do not reuse earlier exceptions.
