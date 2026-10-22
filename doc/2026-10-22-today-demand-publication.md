# October22 verified current subscription results

## Idea summary
User explicitly approved push of verified corrections despite unrelated monthly blockers, and requested review of today's missing scores.
## MVP scope
Publish verified demand metrics for Intellivix, Yeonggwang, Lablup and EST only. Preserve unrelated drafts and unresolved schedules. This one-off exception does not change automation gates.
## Feature specification
Competition and participants from issuer-result reporting; quantity-basis lockup fractions: Intellivix0.0614, Lablup0.0209, EST0.0024. Yeonggwang lockup conflicting secondary values remain pending.
Sources: https://m.coinlab.edaily.co.kr/News/Read?mediaCodeNo=257&newsId=02532166645615544 ; https://m.coinlab.edaily.co.kr/News/Read?mediaCodeNo=257&newsId=02597766645615544 ; https://kkigit.com/ipo/래블업 ; https://sdr1982.tistory.com/1004 ; https://seohak.kr/ipo ; Intellivix and Yeonggwang sources recorded in prior correction/review documents. No claim of directly retrieving primary filings.
## Wireframe
Evidence -> isolated HEAD-based canonical generation -> scoped JSON staging -> verification -> main push -> Raw verification.
Full monthly audit and schedule checks remain blocked by unrelated/pending fields; no claim that entire dataset is complete.

## Verification
HEAD-based isolated generator completed476 stocks. Scoped staged JSON, unique feed IDs, all referenced files, no unrelated row changes, preserved snapshots, non-insufficient decisions and staged whitespace pass. Scores: Intellivix75/B+, Yeonggwang70/B, Lablup36/C-, EST51/C+. Full audit45 errors23warnings, ledger47 candidates26included15excluded6unresolved. Existing Finuts failures CNM/JNTG missing and MBD dates remain. Dart analyzer exits with its existing perf-file deletion OS1920 failure; no generator source changes included. User-approved verified-only exception applies to this publication, not subsequent automation runs.
App hides score when public decision is insufficient_data; all four generated decisions are now non-insufficient. Remote Raw previously had null Intellivix institution data, confirming missing publication rather than absence of available results. No app code change or binary release is necessary for this data update.
