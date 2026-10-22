# Past subscription completeness review

## Idea summary
User requests missing equal allocation data for past subscriptions. Review September1 through October21 closed subscriptions, distinguish absent data from stale drafts and UI display.
## MVP scope
Publish verified TNE/Creates post-close observations; retain other issuer drafts. Do not claim official final individual allocation. Explicit prior user request authorizes verified data push; unrelated audit errors remain outside this scoped exception.
## Feature specification
Source Naver calculator operands A0053F0/A439270, checked October22. Actual source times October21 16:07:21/16:06:50. Equal estimates1.4394 and8.7291 respectively. No ratio doubling: use direct proportional2505.79/12.84. Creates gross allocation field invalid, use reported two pools only.
September-October local inventory19 ended events: three without broker data (Ucast, HanwhaSPAC6, SKSPAC14); TNE/Creates only unpublished day-one data before this repair. Other14 have local expectedEqualShares but source finality varies. Elice official partial allotment correction is still unpublished. These existing values are not proof of final complete app data.
SKSPAC14 lead: https://gamesgo.kr/sk스팩14호-청약-경쟁률-균등-비례-배정결과/ reports69844 applications,656.02 total,1312.03 proportional; requires primary corroboration. Ucast has unresolved postponed identity/schedule and must not be assigned a fabricated result.
## Wireframe
Inventory -> source comparison -> canonical observations -> isolated generation -> scoped publication -> remote check.

## Verification
Isolated committed generator produced476 stock files. TNE expectedEqualShares1.4394 and Creates8.7291 confirmed; unchanged fundamentals and preserved committed snapshot histories asserted. Local day-one drafts retained. Monthly audit45errors23warnings remains, covered only by this scoped verified-data publication exception. App main.dart16129 reads expectedEqualShares/equal pool plus applications; no app binary edit required. SKSPAC14 secondary source ATS reports1:1 against other656.02 reports, so do not import uncertain ratios. HanwhaSPAC6 count unverified. No claim of complete past-data backfill.
