# ALRobot listing-date correction — 2026-11-12

The issuer's securities issuance results report (DART receipt `20261111000281`) was fetched at 2026-11-12 14:13:15.034–14:13:15.430 KST. Section IV.3 explicitly states the listing/trading start date as **2026-11-18 (scheduled)**. This source replaces the earlier unresolved secondary-calendar disagreement; it is not claimed as an actual trade date.

- Target: `에이엘로봇_2026-11-06`, stock code `471800`, lead manager Daishin Securities.
- Source: https://dart.fss.or.kr/report/viewer.do?rcpNo=20261111000281&dcmNo=11649528&eleId=1&offset=0&length=0&dtd=dart4.xsd
- Raw bytes: 155042; SHA-256: `8e24848d7865112e1550a3c53a0b231c7c66afa2c58878b3c62bbd1014959c37`.
- Stored source: `neoni/pixel-office/workspace/companies/app-studio/apps/ipo-app/runs/2026-11-12-alrobot-listingdate-fix/raw/dart_alrobot_issuance_results.html` (app operation run).
- Updated canonical and generated public listing-date fields: discovered event, stock detail, index, dashboard, and 2026 yearly feed. The target recent-feed row includes final aggregate competition 834.73035:1, direct proportional 1,669.46:1, planned listing date 2026-11-18, and the DART observation time 14:13:15.430 KST.
- The first candidate omitted the target from `recent.json`; v2 incorrectly preserved all 60 old rows and made the feed 61 rows. Independent Ops #218 and QA #219 rejected that revision because the canonical feed limit is 60. Neither rejected candidate was published.
- Current candidate reconciles `recent.json` to the canonical generator’s 60-row membership/order. It adds the target plus Hana, Optonics, and YKMC rows from the isolated canonical generator and rolls four older rows out of this rolling feed only. Their detail/history records remain in the repository. The exact generator, candidate/reference order, retained-row immutability, additions/removals, and 60-row limit are now checked by the new fail-closed `canonical_window` gate contract.

This candidate remains unpublished until fresh full audits, exact scope gate, same-fingerprint independent operations/QA review, normal fast-forward push, and immutable public parity all pass.

## 2026-11-12 official final retail snapshot

DART securities issuance report 20261111000281 was fetched 2026-11-12 14:13:15.034–14:13:15.430 KST; raw SHA-256 `8e24848d7865112e1550a3c53a0b231c7c66afa2c58878b3c62bbd1014959c37`. It reports 600,000 general shares, 178,689 applications, 500,838,210 applied shares, 300,000 each in equal/proportional pools, and explicitly states proportional competition 1,669.46:1. Aggregate 834.73035:1 is applied shares divided by the 600,000 general pool. No guaranteed equal allocation per applicant is reported; `expectedEqualShares` remains null. Canonical Dart generator was run in an isolated copy; exact ALRobot target outputs were merged.


### Generation provenance follow-up (2026-11-12)

The recent feed candidate was regenerated in an isolated working tree with public live collection disabled. The command, source/runtime hashes, UTC execution interval, generator-reported Asia/Seoul time, canonical output SHA and final merged output SHA are recorded in the app operations run artifact `generation-v5-provenance.json`; the candidate manifest pins that artifact hash. The validator checks the record hash, success exit code, time interval, generator hash and exact recent/reference output hashes. This records producer execution evidence; it does not resolve unrelated audit errors or imply publication.
