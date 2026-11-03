# Lablup listing-date scoped publication

## Idea summary
Publish the verified listing date for `래블업_2026-10-22`: 2026-11-03. Local data already contains it; public main did not. User explicitly requested push after being told audit errors blocked publication. This is a manual date-only exception, not a future scheduled-run waiver.

## MVP scope
Canonical event listingDate, generated stock listingDate, and applicable public feed listingDate. Remove this closed subscription from stale active membership and add it to recent, as confirmed by the regenerated feeds. Preserve other issuers, metrics, snapshot histories and timestamps. Success requires main push and exact public JSON verification.

## Feature specification
Prepare an isolated HEAD archive, set the single canonical date, run local-only batch, extract only the generated listingDate delta into publication artifacts, validate JSON and identity/feed consistency, commit through a separate index preserving existing staging, push main and compare remote artifacts. On failed validation stop; never force push.

Known full-review blockers remain: November3 morning phase audit 1 error/28 warnings (IM SPAC1 lockup missing), completeness 169 review errors, and Shinyoung SPAC12 schedule alias mismatch. These are not certified resolved by this date-only publication. Other local corrections and November3 broker observations remain withheld.

## Wireframe / data flow
NH official notice -> canonical event -> isolated batch -> stock and applicable feed dates -> main -> exact public comparison. No UI changes.

## Evidence
NH Investment notice dated 2026-10-27 explicitly identifies Lablup subscription 2026-10-22 through 2026-10-23 and listing/trading date 2026-11-03:
https://www.nhsec.com/wooriwmBoard/boardView.action?board_id=1&main_no=13140
Checked 2026-11-03 KST. This is a listing date, not a fabricated observation capture timestamp.

## Validation
Clean HEAD local-only Dart batch generated470stocks; Dart analysis passed. Repeated full local phase audit:45candidates/30included/15excluded/0unresolved,1error/28warnings; completeness631cells/169errors. Schedule alias mismatch remains. Exact candidate assertions restrict all data deltas to the identified issuer's listing date and active/recent membership; unrelated metrics/history and issuer rows remain unchanged. JSON parsing and alternate-index diff checks passed. Publication status is established by the subsequent commit and remote verification, not by this preparation record.
