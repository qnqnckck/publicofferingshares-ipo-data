# September and October IPO refresh (2026-09-20 KST)

## 1. Idea summary

Recheck the app's current and next-month public IPO schedule and reported metrics, correcting only evidence-backed input data and publishing verified changes to the app-facing main branch when validation gates pass.

## 2. MVP scope

- User problem: missing or stale schedules and subscription metrics can mislead the app's home/report views.
- Range: 2026-09-01 inclusive through 2026-11-01 exclusive, determined from current KST date, not record dates.
- Must-have: three independent current schedule views, candidate identity ledger, full input/output/feed comparison, verified demand/retail data, deterministic local regeneration, monthly audit and remote verification.
- Out of scope: app UI/binary release, investment advice, fabricated final values, unrelated local identifier edits, new scheduled workflows.
- Success: verified corrections become readable in main stock/feed URLs; unresolved facts and publication blockers are disclosed instead of reported as complete.
- Monetization: existing app advertising remains unchanged; data changes introduce no paid service or tracking dependency.

## 3. Feature specification

- Discovery: compare exact company plus subscription window across schedule providers and primary notices; distinguish new IPOs from rights offerings, mergers and listed companies. Unknown identity remains unresolved.
- Metrics: prefer official final results; store percentages as fractions and ratios as ratios. Quantity-basis lockup must not be confused with application-basis lockup. Preserve null for unpublished facts and label estimates.
- Data flow: modify canonical discovered/manual/broker/outcome inputs only, regenerate locally, review all generated changes. Preserve unrelated dirty files.
- Errors: every monthly-audit ERROR or unresolved candidate blocks publication. Remote source conflicts are documented and never silently guessed.
- Acceptance: repository validation, Dart analysis, changed-JSON parse and diff check pass; main SHA and exact raw stock/feed output verified after authorized publication.

## 4. Wireframe / data flow

```text
Current schedules + primary notices
 -> candidate ledger -> canonical source inputs
 -> deterministic batch -> monthly audit + validation
 -> scoped commit/main push -> stock + active/upcoming/recent feeds -> app refresh
```

## Results

### Coverage and scope

Freshly compared five schedule views (38 Communications, StayRich, Sonnimlab, Seohak, Mainpy) and every included row against discovered inputs, manual overrides, stock detail, yearly/index and lightweight feeds. Candidate ledger: **33 rows, 24 matched, 9 excluded, 0 unresolved identities**. Four existing October events intentionally have no separate manual override because their reviewed baseline already resides in discovered inputs. No newly missing eligible IPO was identified.

Exclusions: six already-listed issuers, one art investment-contract subscription, one duplicate SK SPAC alias, and Baropharm's November subscription outside the target window. Mainpy's headline count is not a count of unique new equity IPOs. [K-Auction's official filing](https://kind.krx.co.kr/external/2026/03/23/000754/20260323003585/11011.htm) confirms Together Art's investment-contract business.

### Applied correction: MBD

Primary evidence: [September 18 corrected securities filing](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260918000439), section I (schedule/price) and III (float/shares), fetched directly from DART. Secondary corroboration: [38 change notice](https://www.38.co.kr/html/board/?code=380060&no=3448&o=v).

| Field | Before | Corrected |
| --- | --- | --- |
| Demand forecast | Sep 21-29 | Oct 16-22 |
| Subscription | Oct 6-7 | Oct 28-29 |
| Refund/payment | Oct 12 | Nov 2 |
| Indicative price band | KRW 9,400-11,500 | KRW 9,600-11,700 |
| Pre-allocation float | 32.90% | 34.06% |
| Upper-band indicative cap | KRW 164,700,000,000 | KRW 167,570,184,600 |

Float is 4,878,263 / 14,322,238 shares, rounded to the disclosed 34.06%. Indicative capitalization follows the existing pre-offer upper-band convention: 14,322,238 x 11,700, **not a final-price capitalization**. Final offer, institution demand/participants/quantity lockup and listing date remain null. Retail baseline remains 500,000 (25% of 2,000,000 offered shares), and filing I confirms no retail putback right.

Migrated the old October 6 canonical ID to October 28 in both canonical inputs and generated detail/index/yearly/upcoming. Removed only the superseded generated stock path; it remains recoverable in Git history. No duplicate MBD event is published.

### Retail and pending-source checks

- Fresh Npay queries for Brils, Global Technology and Duksan Navcours exactly match their already-published post-close observations. Do not create a new September 20 market timestamp for unchanged observations.
- Brils: 2026-09-18 16:06:02 KST, total 1676.26, proportional 3352.52, equal pool 150,000, applications 113,800. Equal expectation 1.3181 is an estimate, not a final allotment promise.
- Global: 2026-09-17 16:01:17 KST, total 57.15, proportional 113.3, applications 77,278, equal pool 500,000. Duksan: 16:01:19, total 332.82, proportional 665.64, applications 132,911, equal pool 375,000.
- WisePlanet, Bigwave and Neosapiens calculator URLs now return HTTP 404. Retain previously verified snapshots and source timestamps; their aggregate ratios still agree with the fresh 38 schedule. This is not a new verification of final broker allotment tables.
- Brils is no longer an active subscription on Sunday September 20: regenerated active feed is empty; its detail/recent history remains available.
- Full dashboard link verification additionally found three pre-existing stale paths (Bigwave Sep 16, Neosapiens Sep 8, Brils Sep 8) and ten missing current September/October rows. Rebuilt the 24 target-month dashboard rows from the local generator, removed only those three superseded links, and preserved unrelated historical rows byte-for-byte. This repairs feed membership without deleting any valid historical stock.
- [Eu-Cast's filing](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260909000378) freshly confirms Oct 19-20/refund Oct 22 and band 21,000-26,000. A provider labels the lower band as a final price; reject that label and retain null final offer.
- [Lablup correction request](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260908100002) exists from Sep 8. No replacement subscription date was found; retain the [last issuer-announced Oct 12-13 window](https://www.lablup.com/ko/press/2026-08-lablup-kosdaq-ipo) as tentative, not confirmed. Existing schema has no dedicated schedule-status display field; do not misuse unrelated financial fields for a warning.

### Validation

- Local-only batch: PASS, 479 isolated generated records. Published history retains all 485 records; 484 unrelated index entries preserved byte-for-byte. Discovery/identifier normalization ran only on copies under build, not user input files.
- Monthly audit: **0 errors / 17 warnings**. Accepted pending warnings: 15 October listing dates not announced, plus missing application counts for KB34 and Korea17 SPAC. Never substitute zero. Lablup schedule risk is separately disclosed above.
- Pending-count regression: PASS (pending, mixed, published-rate, known-count, explicit-zero).
- Schedule-sync validator: PASS using **local seed fallback only**; Finuts live endpoint timed out. Fresh external reconciliation is evidenced by the calendar/filing ledger, not this fallback.
- Static analysis was attempted: sandbox process launch denied; elevated attempts encountered the existing Windows Dart perf-witness shutdown failure, errno 1920. Do not report those runs as passing. No Dart code changed; the same generator ran successfully and its regression compiled/passed. User caches were not deleted or modified to hide the issue.
- Final JSON parse: PASS for all 10 changed/new JSON files. Scoped semantic checks: PASS; canonical inputs changed only for MBD; six public feed families resolve to stock files with matching IDs. Index 485, active 0, upcoming 15, recent 60, dashboard 491 (including six additional valid historical rows), yearly 2026 72. All 467 unrelated dashboard rows preserved byte-for-byte. `git diff --check`: PASS.

Commands (from nested data repository; Dart resolves to D:/harness/flutter/bin/dart.bat):

```text
dart run tool/ipo_competition_batch.dart --backfill-years 3 --manual-fundamentals-path data/manual_fundamentals.json --no-discover --no-public-live-collect --no-identifier-discover --no-ipo-korea-supplement --no-article-lead-manager-discover --out build/refresh-20260920/output --discovered build/refresh-20260920/ipo_events.json --identifier-path build/refresh-20260920/ipo_identifiers.json
py -3 build/apply_reviewed_20260920.py
py -3 build/verify_refresh_20260920.py
audit_monthly_ipo_data.ps1 -RepoPath D:/harness/projects/publicofferingshares/ipo-data -CandidateLedgerPath doc/monthly-ipo-audit-2026-09-20.json -AsOfDate 2026-09-20
py -3 tool/validate_finuts_schedule_sync.py --warn-on-analysis-issues
dart analyze tool/ipo_competition_batch.dart
dart run tool/ipo_pending_counts_test.dart
git diff --check
```

Existing dirty `data/identifiers/ipo_identifiers.json` is preserved byte-for-byte (SHA256 B389739C910688045D7E18E34ED1F9CD1C123CD8E85A7AD8E0A43B7766B1344A). Prior untracked audit notes remain outside this change. No app code, binaries, GitHub Actions or store operations were requested or performed in this refresh.
