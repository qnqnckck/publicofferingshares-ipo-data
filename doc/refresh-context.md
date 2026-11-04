# IPO refresh context — read before every refresh

## Stable execution contract

- Data repository: `E:\harness\projects\publicofferingshares\ipo-data`; verify Git root/origin on entry. App repository is its parent, not the publication target.
- Public data: `https://raw.githubusercontent.com/qnqnckck/publicofferingshares-ipo-data/main/ipo_competition_data`.
- User instruction on 2026-11-02: always finish data updates through scoped commit, main push and public-feed verification; do not repeatedly ask for routine push approval. This is NOT permission to fabricate values, bypass explicit audit gates, publish unrelated changes or force-push.
- If a current heartbeat explicitly prohibits publishing with audit errors, honor it. A manual exception must identify its precise blockers and scope. Resolve actionable review gaps instead of indefinitely repeating the same error count; never label missing research as confirmed unpublished.
- Scheduled runs: Asia/Seoul weekdays09:00–19:00; 13h full review + Dart analysis; 17h broker-only. Manual refresh requests are not restricted to these hours.

## Coverage: not subscription month alone

Default current and next month; use all explicitly requested months. Include subscription overlap, demand dates, refund dates AND listing dates in range. Always carry prior-run held IDs and user-reported IDs until verified published or explicitly excluded with evidence. Search official KRX/KIND and issuer/broker listing announcements independently of DART filing indexes. An unchanged filing index does not mean no new listing announcement.

For unknown listing dates, review closed subscriptions from the two preceding calendar months as a minimum safety net; retain older unresolved events explicitly in the ledger. The helper's date selection cannot discover an unknown event on the internet. Build a bidirectional ledger from official listing notices, three calendar views, canonical inputs and public feeds.

Regression example: `영광_2026-10-22` subscribed in October and listed November2; it belongs in November review. `영광와이케이엠씨_2026-11-04` is a different issuer. Never copy dates between them by name similarity.

## Completion and evidence

User reinforcement2026-11-04: every check/update must reconcile the reviewed local result with the app's public data, not stop at local edits. Standing routine push authorization is already granted; do not ask again merely to push. Report completion only after scoped commit, main push, exact public detail/feed comparison and membership checks. If the live app is not inspected, say public-feed verification, not verified app-screen refresh. If a mismatch remains, identify the affected IDs/fields and fix the cause within scope; never invent timestamps to force cache refresh. A blocked publication remains incomplete and must be reported as unpublished in manual responses. Heartbeat notification rules still apply. This does not authorize unverified dirty changes or override explicit audit gates.

Latest verified publication2026-11-04: `0d4e87854fb5dfeee5bd0a39917f519eaa72b823` publishes only 이에스티_2026-10-22 and 아이엠스팩1호_2026-10-26 listingDate2026-11-04 with applicable membership. Both stock files and all six affected feeds matched Raw exactly. Earlier Lablup publicationa8288d0 also completed. Historical status below is superseded for these published corrections; YKMC demand results, November3 broker snapshots and other held changes are NOT certified published. Reconcile them explicitly on subsequent work.

Read the latest run doc and current Git status, including staged changes. Research → canonical inputs → isolated generation → scoped merge → JSON / phase / completeness / schedule checks (Dart for full/code runs). Read every error category: numerical conflict, missing evidence annotation, external stale calendar and publication mismatch are different causes.

Record per-run requested/reviewed/changed/held IDs, evidence URLs and actual source times, audit counts, committed paths/SHA, pushed SHA, remote verification time/result. Every verified-but-unpublished correction must retain an ID, field, reason and next action. Do not discard it at a month boundary. Existing `build/` helpers are scratch, not permanent authority; inspect them before reuse.

Completion requires matching exact committed stock JSON AND applicable index/yearly/active/upcoming/recent/dashboard rows on remote main. A Git push alone is not proof of app data delivery. If Raw is delayed, compare GitHub Contents at the pushed SHA and report public verification still pending. If remote is correct but UI remains stale, inspect app cache invalidation without inventing a new observation timestamp.

For dirty repositories use an isolated publication candidate from HEAD plus verified deltas. Never stage all data or include historical unverified edits. Keep local, committed, pushed and publicly verified states distinct. A blocked run is incomplete, not a successful update; quiet heartbeat rules only suppress duplicate notifications, not the obligation to track and work on gaps.

## Known unfinished state (2026-11-02; recheck, do not assume current)

Publication update: manual scoped push `6c091b6abaad2865ae6ab705b674b52c75d1d2f8` supersedes the historical SHA/state below for 영광 listingDate and Nov2 Dongwon/Baropharm observations. All9changed public JSON artifacts match GitHub Contents at this exact commit. Raw recent/dashboard/yearly match, but stock3 + index/active/upcoming still return cached prior content at verification; public rollout is pending, not complete. Retry exact Raw comparison on next run. Other historical dirty corrections remain withheld. Context and portable skill also pushed; app handoff doc pushed as77c9400.

- Last confirmed public SHA: `290a113c1839dca12ba74d641029c6822a94cbf6`.
- 영광 listingDate locally `2026-11-02`, but remote stock/index/yearly were null when checked this evening. Pending publication, not pending announcement.
- Dongwon/Baropharm first-day observations and other dirty data remain local-only. They continue subscriptions November3; do not call November2 observations final.
- Latest manual run: `doc/2026-11-02-2246-manual-review.md`; supplementary audit had156missing-review errors, NOT156proven wrong numbers. Finuts schedule now passes. Expanded lifecycle audit may expose additional real gaps; do not weaken it to preserve the previous count.
- Next work: reconcile carried listing corrections (영광 first), obtain primary evidence for missing-field reviews, validate scoped publication and complete remote verification. Do not claim this context-hardening task published the data.
