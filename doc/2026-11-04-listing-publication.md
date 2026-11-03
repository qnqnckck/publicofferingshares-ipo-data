# November4 verified listing-date publication

## Idea / MVP / flow
Publish only 이에스티_2026-10-22 and 아이엠스팩1호_2026-10-26 listingDate2026-11-04 and necessary feed membership. Clean HEAD -> canonical date corrections -> isolated batch -> semantic date-only validation -> scoped main push -> exact remote check.

## Authorization and evidence
User requested push immediately after being informed that both verified dates are locally corrected but withheld due to audit errors. This is a manual scoped exception for these two listing dates only, not permission to publish other held values or waive future heartbeat gates. Known blockers: IM SPAC1 missing quantity lockup, supplementary unreviewed fields and external SPAC schedule alias. No financial data changed.
Sources: https://www.yna.co.kr/amp/view/AKR20261102156400008 (KRX approval, November4 trading commencement); https://newspim.com/news/view/20261030000313 (EST November4 listing). Compared live public index and both stock details before publication: listingDate null; local both2026-11-04.
All other held corrections including YKMC demand results and November3 broker observations remain excluded. Preserve unrelated staged and unstaged edits.

## Verification
Isolated batch regenerated468stock files. Candidate restricted to10paths, including8public JSON artifacts. Semantic assertions verify only the two listing dates and necessary active/upcoming/recent membership change; unrelated issuer rows and numerical fields unchanged. JSON parsing and staged whitespace check pass. Dart analysis passes. Full phase audit remains1error27warnings; external schedule alias mismatch persists. Full completeness report retained at build/completeness-20261104-listings.json. This scoped manual publication does not claim full audit success.
