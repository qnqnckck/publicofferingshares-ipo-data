# 엠비디 verified demand and offer delta — 2026-11-12

## Confirmed source values

Target: `엠비디_2026-11-12`, existing event stock code `498150`, Hana Securities. The event code is carried forward as stored; this update does not approve the separate live-roster code-mapping blocker.

Primary source is the DART corrected investment prospectus, report `20261110000261`, captured successfully at 2026-11-12 08:09:50.816203 KST (SHA-256 `98ae39bf0c755f957978e6d977ca940b95ed6959c2ce4c00fceb03f4ab690926`). The current DART issuer filing search was captured 08:14:52.748481 KST (SHA-256 `8f22865afa013139c020c79afb492532b213ef3440233919a5c48343e1ac2e4f`) and listed this corrected prospectus and final-conditions filing as the newest entries. The final-conditions body was captured 08:43:12.689227 KST (SHA-256 `e74c01b9d4e20fbb51d8131254ae8324ef0115dc608afdcee511e8a8de01a92c`). All three raw artifacts and exact request/response times are retained in the app run's `source-observation.json`.

- Fixed offer price: KRW 7,000. The filing states this is the issuer and lead manager's final price, below the stored KRW 9,600–11,700 band; therefore `topBandConfirmation=false`.
- Institutional demand: 925 participating institutions, 280,648,000 requested shares, and issuer-reported 187.10:1 competition using the 1,500,000-share institutional allocation pool. This is institutional demand, not retail competition.
- Institutional requested-share lockup commitment: 0.0. Domestic and overseas tables show all 925 institutions and all 280,648,000 requested shares under “미확약”; each 15-day, one-, three-, and six-month commitment row is explicitly empty.
- Schedule: general subscription 2026-11-12 through 2026-11-13, refund 2026-11-17. The active/upcoming membership change reflects the saved prospectus date in Asia/Seoul, not a live application-count observation.

## Preserved unknowns

`marketCapKrw` remains null. The prospectus gives a post-offer share count, so multiplying that count by KRW 7,000 would be a derived capitalization, but this feed contract has no separate observed-versus-calculated provenance field. Listing date remains unannounced. No live retail count, equal-allocation pool/result, proportional competition, or proportional allocation is added; snapshots remain unchanged and empty. The local 10-minute collector was not started or duplicated.

## Publication scope

Only this event, its manual fundamentals, its public detail, and the applicable index/dashboard/yearly/active/upcoming/recent feeds are in scope. All non-target feed rows and snapshots are preserved. The exact changed path list and permitted leaf paths are in `verified-delta-manifests/2026-11-12-mbd.json`; full audit deltas and independent same-candidate review are retained in the accompanying app run record.
