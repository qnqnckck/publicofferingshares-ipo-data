# IPO refresh context — read before every refresh

## Current publication policy — verified deltas with deferred missing values (2026-11-10)

The user explicitly changed the operating policy: leave unsupported/missing values `null`, keep researching them, and continue publishing independently verified updates without waiting for every omission to be filled. This supersedes the full-zero-errors-only rule for **scoped verified deltas**. Every run still executes and records the full monthly phase and completeness audits. A scoped delta may proceed only through `scripts/check_verified_delta.py` with an exact manifest, same-candidate independent service and QA review, verified provenance, unchanged snapshots, null preservation, exact feed membership/parity, no changed paths outside the manifest, and no new audit errors. Any target-scope hard error, target source conflict, stale/wrong target membership, malformed value, or feed mismatch blocks publication. Unchanged out-of-scope issues of any category remain visible as deferred work and must retain their IDs/categories; new issues block. Never relabel them resolved or claim the full audit passed. Do not infer values to clear an audit. This is the standing partial-publication policy, not a one-run exception.

For the 2026-11-10 Hana delta, see `doc/2026-11-10-verified-delta-policy-and-publication.md` and the exact manifest in `doc/verified-delta-manifests/2026-11-10-hana.json`. Its current KST subscription day membership must be exactly once in `active.json` and absent from `upcoming.json`, matching the canonical index row. Previous task141 validation alone did not publish anything; the present run must still complete this gate and public parity checks.

## Manual disposition — November4 21:31 KST

Latest user asks to reverify and either discard or publish held changes after the scheduled blocker disclosures. ALRobot seven demand/fundamental corrections were reverified against November4 DART final terms and selected for scoped manual publication; listingDate remains excluded/null. See `2026-11-04-manual-held-resolution.md` for exact scope, known audit errors and verification status. The 17h/18h/19h local logs are retained as historical records, superseded by this manual disposition, not instructions to restore the old candidate. Future scheduled publication still requires zero errors. No other held source corrections are certified by this operation.

## Latest handoff — 2026-11-04 16:51 KST

TNE recheck and public parity are complete: all 40 scoped stock details and six feeds matched Raw main and local/committed artifacts at `0ebaaa6f509bc2966d839a99c609f30e4ba82f5b`. Earlier TNE CDN-pending statements below are historical, not current blockers. No additional numeric correction was needed. See `2026-11-04-local-reconciliation.md`.

Remaining local drafts were reconciled with published HEAD after byte-for-byte backup: nine data/code/README files differed only in formatting, ordering or generated metadata; three historical tracked document revisions and 144 unpublished historical research files were archived under `build/local-reconciliation-20261104/original-files/` with original relative paths. `manifest.json` records hashes; `original.index` preserves original staging. Older references to an absent historical `doc/` file can be resolved there. This archive is local recovery material, not a public feed or new source of verified facts. Use the committed `monthly-ipo-audit-2026-11-04-repair.json` for current carried gaps. Do not restore old drafts wholesale or silently certify the outstanding finality/lockup/listing-conflict/high-price gaps.

## Historical execution contract (superseded only as stated above)

- Latest manual instruction2026-11-04 supersedes the earlier full-pass-only decision for this repair: **recheck unverified items and push all verified changes**. The scoped exception covers the October–December repair described in `doc/2026-11-04-three-month-repair.md`; remaining official-none lockup, ALRobot listing conflict and missing final-broker evidence are not certified or filled with guesses. It does not carry over to scheduled runs. Before declaring publication complete verify the repair commit and every scoped public file; candidate is `build/publication-repair-20261104`.

- Data repository: `E:\harness\projects\publicofferingshares\ipo-data`; verify Git root/origin on entry. App repository is its parent, not the publication target.
- Public data: `https://raw.githubusercontent.com/qnqnckck/publicofferingshares-ipo-data/main/ipo_competition_data`.
- User instruction on 2026-11-02: always finish data updates through scoped commit, main push and public-feed verification; do not repeatedly ask for routine push approval. This is NOT permission to fabricate values, bypass explicit audit gates, publish unrelated changes or force-push.
- If a current heartbeat explicitly prohibits publishing with audit errors, honor it. A manual exception must identify its precise blockers and scope. Resolve actionable review gaps instead of indefinitely repeating the same error count; never label missing research as confirmed unpublished.
- Scheduled runs: Asia/Seoul weekdays09:00–19:00; 13h full review + Dart analysis; 17h broker-only. Manual refresh requests are not restricted to these hours. The former zero-error publication condition is superseded for verified deltas by the current policy above; all other scope and provenance controls remain.

## Coverage: not subscription month alone

Default current and next month; use all explicitly requested months. Include subscription overlap, demand dates, refund dates AND listing dates in range. Always carry prior-run held IDs and user-reported IDs until verified published or explicitly excluded with evidence. Search official KRX/KIND and issuer/broker listing announcements independently of DART filing indexes. An unchanged filing index does not mean no new listing announcement.

For unknown listing dates, review closed subscriptions from the two preceding calendar months as a minimum safety net; retain older unresolved events explicitly in the ledger. The helper's date selection cannot discover an unknown event on the internet. Build a bidirectional ledger from official listing notices, three calendar views, canonical inputs and public feeds.

Regression example: `영광_2026-10-22` subscribed in October and listed November2; it belongs in November review. `영광와이케이엠씨_2026-11-04` is a different issuer. Never copy dates between them by name similarity.

## Completion and evidence

Latest repair publication2026-11-04: data commit `0835f9e0c254caa518b1de12ca9d3dc3ed84bf2f` pushed to origin/main. All40scoped stock files and6feeds are semantically identical between the local generated directory and committed artifact. At first remote check45/46Rawfiles matched; 티앤이코리아_2026-10-20 Raw remained cached, while GitHub Contents at the data commit confirms the new outcome. Retry Raw before declaring full public parity. Remaining research gaps: Dongwon/Baropharm/Brils final broker evidence, ALRobot listing-date conflict, iM1/Hanwha6official-none numeric lockup. Listing-day highs for12backfilled outcomes remain unverified/null. See the repair report and `build/publication-repair-20261104/remote-verification.json` for latest retry results. This supersedes historical publication states below.

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
