# Verified-delta publication policy and Hana update — 2026-11-10

## Policy change

The user directed that unavailable data remain missing while independently verified values continue to be updated and published. This is a standing policy: nulls do not by themselves hold an unrelated verified data delta. The full phase and completeness audits still run and their residual errors remain visible. Publication is allowed only when an exact manifest and the fail-closed `check_verified_delta.py` pass, no new audit error appears, no in-scope integrity/finality/source conflict exists, and independent operations and QA reviews approve the same candidate hash. A changed value still requires source URL, actual observation time, identity and source/hash provenance. A review annotation or missing value does not prove that a number is zero or official-none.

The scope validator blocks unexpected paths or target fields, changed non-target feed rows, history loss, invalid source provenance, identity/date conflicts, stale target membership, non-null values where the observation says unknown, and any new audit error. Unchanged out-of-scope issues of any category are carried forward by ID/category as open work; they are not waived or marked complete. Full audit counts are reported as nonzero whenever they are nonzero.

## Hana Aerodynamics verified delta

Target `하나에어로다이내믹스_2026-11-10` (stock code `271160`; IBK Securities) has five source-backed updates from the DART final terms receipt `20261109000163`, viewer `dcmNo=11646220`, and IBK subscription notice:

- Confirmed offer price: KRW 5,700; top of KRW 4,700–5,700 band.
- Institution demand: 2,152 participants and 1,051.38:1.
- Requested-share lock-up commitment: 144,401,000 / 3,469,561,000 = 0.04161938642958. This is request intent, **not final allocation commitment**.

The actual source responses were observed on 2026-11-10 between 11:21:59 and 11:21:59.9 KST. Exact HTTPS URLs, response timestamps, hashes and source raw files are retained in the linked official observation record in the app workspace. Retail application counts, equal/proportional pools and ratios, market capitalization, and actual listing date remain unknown and null; snapshot history remains empty. No value is synthesized from a total competition ratio or from a later outcome. The official subscription window places this target on an active subscription day, so the candidate moves it from `upcoming.json` into `active.json` once, matching the canonical index row. This membership correction does not fabricate or refresh a `generatedAt` timestamp.

Task141 previously validated the isolated candidate but did not publish it. Its baseline-to-candidate full audit counts were phase errors 16→11 and completeness errors 152→147; the residual errors are not a green full audit. The applicable run manifest is `doc/verified-delta-manifests/2026-11-10-hana.json`. At candidate preparation, publication was pending the new scope gate, same-hash independent service/QA review, commit/push, and exact remote stock/feed parity. Those steps are now recorded below.

## Deferred items

The remaining 11 monthly phase errors and 147 completeness errors from the candidate audit remain listed in `staff/app-ops-reliability/task141/final-phase.log` and `final-completeness.json`. Those totals overlap and must not be added together. In-scope HANA omissions include market capitalization and actual listing date; they remain null. Unchanged out-of-scope issues, including source conflicts or stale membership on other issuers, remain attached to their existing IDs/categories for future review and are not resolved by this publication. Any new issue or HANA in-scope integrity error blocks the publication.


## Publication completion — 2026-11-10 12:46 KST

The exact scoped candidate passed the fail-closed delta validator and same-candidate independent service/QA reviews (#158/#159). Data and policy commit `a72a47b6a97e85160e0d0892052229046d94da29` is on GitHub `main`. A post-push read of the immutable commit checked SHA256 equality for all 14 manifest paths plus `ipo_competition_data/recent.json` (15/15). The public detail contains the five verified values above; active membership is exactly one and identical to the index row; upcoming and recent membership are zero; yearly/index/dashboard memberships are one. The history remains empty and undisclosed market cap/listing/retail values remain null.

Remote verification is data/feed parity only; the private app screen/cache was not inspected. The full audits still report 11 phase errors and 147 completeness errors, with no new errors in the scoped candidate. Existing out-of-scope issues remain open.
