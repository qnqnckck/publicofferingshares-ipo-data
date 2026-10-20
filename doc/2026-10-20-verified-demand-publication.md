# October 20 verified demand publication

## Idea summary
The user explicitly requested committing and pushing verified facts after the scheduled run reported missing-field audit errors. Publish a narrowly scoped factual correction for TNE Korea and Creates, while leaving all unknowns and unrelated drafts unchanged. This manual exception does not relax future scheduled audit gates.

## MVP scope
Update offer price, upper-band confirmation, institutional competition and participant count for the two October20-21 subscriptions. Preserve uncertain lockup, listing dates, retail snapshots and other issuers. Success means verified canonical changes, deterministic regeneration, scoped checks and public main-feed verification.

## Feature specification
TNE Korea: offer15300, upper-band true, institution ratio1041.02, participants2274. Sources: NewsPim and IB Tomato both October16. Creates: offer15000, upper-band false, ratio240.46, participants831, issuer announcement reported by Prime Economy October19. The Bell independently confirms lower-band price. Do not infer quantity-basis lockup from application counts or rounded zeroes. Preserve missing facts as missing.

Sources checked October20:
- https://www.newspim.com/news/view/20261016001301
- https://www.ibtomato.com/mobile/mExternalView.aspx?no=19843&type=1
- https://www.newsprime.co.kr/news/article/?no=751503
- https://search.newspim.com/news/view/20261019001243 (independent indexed article confirms Creates price15000, ratio240.46, participants831)
- https://www.thebell.co.kr/front/newsview.asp?code=0302&key=202610191456254200102184 (search result; direct retrieval failed)

## Wireframe
Verified source facts -> manual fundamentals -> isolated committed generator -> selected details/feed rows -> diff/JSON/audit checks -> main push -> exact public URLs.

## Validation scope
Initial full local audit:38 errors,25 warnings against October13 ledger (43 candidates,28 included,15 excluded). This is not a complete new monthly reconciliation. Only the eight explicitly reviewed fields and their derived analysis may change. Other stock details and pre-existing local drafts must be preserved. No new retail observation is claimed.

Validation completed:572 publication JSON files parse,1598 feed references resolve, all nonselected details/feed rows and protected local drafts are preserved. Canonical diff is exactly8 reviewed fields. Selected details retain identical snapshot histories and schedules. The two selected October20-21 subscriptions move from upcoming to active; other issuers' memberships are preserved. Dart analysis passes; Finuts live schedule validation passes. After correction, full local audit retains30 errors and25 warnings (8 missing fields resolved). The user's explicit verified-only publication instruction is the scoped exception; completeness is not claimed for either issuer. No generator code is included in this commit.

Commands: build/publish_verified_20261020.py prepare/merge/validate/docs/stage; isolated ipo_competition_batch.dart with remote collectors disabled; dart analyze; audit_monthly_ipo_data.ps1 -AsOfDate2026-10-20 with the existing October13 ledger; validate_finuts_schedule_sync.py --warn-on-analysis-issues. Remote main matched starting ddeeef5cf6105e4db19f0e3191097760f61d614a before staging.
