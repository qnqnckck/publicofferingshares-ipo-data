# IPO withdrawal dispositions — 2026-11-12

## Verified withdrawals

The 2026-11-11 DART withdrawal filings for `씨앤엠_2026-11-12` and `제이앤티지_2026-11-12` cancel the remaining offering schedule. The isolated candidate removes their active canonical event rows and public feed/detail memberships. Complete original canonical events and generated details (including any snapshots) remain in `data/withdrawn_ipo_events.json` with the official withdrawal URL, raw SHA-256 and observed request/response timestamps.

| Event | DART filing | Raw SHA-256 | Observed response (KST) |
|---|---|---|---|
| 씨앤엠_2026-11-12 | [rcpNo 20261111000246](https://dart.fss.or.kr/report/viewer.do?rcpNo=20261111000246&dcmNo=11649349&eleId=1&offset=0&length=0&dtd=dart4.xsd) | `0770c17e645719969a3b8726b2e257e5a2b214f3a8afa2cfdde1943a8cc3e6c4` | 2026-11-12 08:09:50.946470 +09:00 |
| 제이앤티지_2026-11-12 | [rcpNo 20261111000314](https://dart.fss.or.kr/report/viewer.do?rcpNo=20261111000314&dcmNo=11649667&eleId=1&offset=0&length=0&dtd=dart4.xsd) | `2b09e739377e53941f9932cc260a8dbdd19e9ae9a7b07404d05a5eccedf9093c` | 2026-11-12 08:09:51.022190 +09:00 |

## Disposition contract

- A withdrawn offering is absent from `data/discovered/ipo_events.json`, every public feed, and `ipo_competition_data/stocks/<id>.json`.
- Preserve the original canonical event and generated detail exactly in the withdrawal archive. Do not erase past observations.
- Keep every non-target feed row and its ordering unchanged. Update `generatedAt` only from an actual deterministic generator run.
- Validate with `check_withdrawn_delta.py` and an exact manifest. The gate checks official response status/hash/times, archive equality to base, complete feed/detail removal, unchanged non-target rows, exact changed paths and new full-audit errors.
- The regular one-target active-data gate remains unchanged. Never use this withdrawal gate for numeric/fundamental updates.
- Publication still requires the same candidate hash to pass operations and QA review, full audits, and post-push public feed parity. A local candidate is not published.

The current candidate and its audit state are recorded in the app workspace run `2026-11-12-morning-lifecycle-audit`.
