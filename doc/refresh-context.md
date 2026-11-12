# IPO refresh context — read before every refresh

## Current publication policy — verified deltas with deferred missing values (2026-11-10)

The user explicitly changed the operating policy: leave unsupported/missing values `null`, keep researching them, and continue publishing independently verified updates without waiting for every omission to be filled. This supersedes the full-zero-errors-only rule for **scoped verified deltas**. Every run still executes and records the full monthly phase and completeness audits. A scoped delta may proceed only through `scripts/check_verified_delta.py` with an exact manifest, same-candidate independent service and QA review, verified provenance, unchanged snapshots, null preservation, exact feed membership/parity, no changed paths outside the manifest, and no new audit errors. Any target-scope hard error, target source conflict, stale/wrong target membership, malformed value, or feed mismatch blocks publication. Unchanged out-of-scope issues of any category remain visible as deferred work and must retain their IDs/categories; new issues block. Never relabel them resolved or claim the full audit passed. Do not infer values to clear an audit. This is the standing partial-publication policy, not a one-run exception.

## Withdrawn IPO removal — 2026-11-12

For an official withdrawal, remove the offering from active canonical inputs, every public feed, and its public stock-detail file. Preserve the exact original canonical event and generated detail (including snapshots) in `data/withdrawn_ipo_events.json`, with official source URL, raw SHA-256, actual response times, Korean language and Asia/Seoul timezone. Validate only with `scripts/check_withdrawn_delta.py` and an exact multi-target manifest; do not weaken or repurpose the single-active-target gate. See `doc/2026-11-12-withdrawn-ipo-dispositions.md`. The C&M/JNTG withdrawal delta was published to main at `3186104874a2c62b6356bff1f400f3cfd3f40a4a`; same-hash operations/QA approvals and exact public feed/detail parity passed. Its original events and details remain in the withdrawal ledger.

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


## Verified MBD demand delta — 2026-11-12

MBD's corrected DART prospectus supports a KRW 7,000 final offer price, 925 institutional participants, reported institutional competition of 187.10:1 on the 1,500,000-share allocation pool, and no institutional requested-share lockups. Provenance and raw SHA-256 values are in `doc/2026-11-12-mbd-verified-delta.md` and the app run. Market cap remains null because it would be derived, listing date is unannounced, and no retail live snapshots were observed. The target is active for the official 2026-11-12–13 KST subscription dates; the live 10-minute collector remains separately blocked pending official stock-code mapping and roster.


## 2026-11-12 expired feed membership reconciliation candidate

The v2 candidate on published main (`edb23300b5fa08f5bf9bc1569b697f1e8c1fcbe0`) removes four completed subscriptions from active/upcoming: NexEye and Youngkwang YKMC from active; Optonics and AL.Robot from upcoming. The current issuer-search top filing is joined to its issuer-specific DART issuance-results schedule body; the body dates must match canonical schedule values. Hana Aerodynamic remains because the first candidate incorrectly linked MBD evidence. Raw hashes/timestamps and independent review results are retained in the app run. No canonical event/detail/history or other feed rows are changed; unobserved retail/final allocation fields remain null. The v1 candidate failed Ops and QA and was not published; v2 requires a new fail-closed gate and same-fingerprint Ops/QA passes before publication.

## Latest scoped reconciliation — 2026-11-12 Hana active expiry

Hana's official IBK/DART-confirmed window ended 2026-11-11. The 2026-11-12 current DART issuer query binds the newest filing `20261109000166` to its explicit general-investor window, with exact response timestamps and raw hashes in the app run. The generated active feed now excludes this expired row while retaining MBD for its active window. Full audit errors decrease 131→130; unchanged missing/source-review issues remain open. See `doc/2026-11-12-hana-active-expiry-reconciliation.md` and exact manifest `doc/verified-delta-manifests/2026-11-12-hana-active-expiry.json`. Same-candidate Ops #211 and QA #212 approved; scoped gate PASS. Published to main at `1cbcfd234a8cff4fcb5f9aaab9010a3272740892`; immutable raw active-feed SHA matched local. App screen/CDN was not checked.


## 2026-11-12 ALRobot verified delta — canonical-window correction pending review

DART securities issuance report `20261111000281` supports ALRobot's planned 2026-11-18 listing and final retail values. The first candidate omitted ALRobot from `recent.json` and failed QA. The second candidate added ALRobot but preserved all 60 old feed entries, creating 61 rows; independent Ops #218 and QA #219 rejected it and it was not published. The current candidate reuses the independently generated canonical 60-row recent membership/order, preserves unchanged historical rows and all detail/history files, adds the four newly eligible window rows (ALRobot, Hana, Optonics, YKMC), and rolls four oldest rows out of `recent.json` only. A fail-closed `canonical_window` manifest contract now checks the fixed 60-row cap, exact generator source hash, canonical order, exact membership additions/removals, added-row equality, and retained-row immutability. Fresh full audits and same-fingerprint Ops/QA approval are required; residual unrelated errors remain open. No push or public parity check has run for this candidate.


## ALRobot recent-window producer provenance follow-up — 2026-11-12

The ALRobot planned-listing correction remains unpublished until fresh same-fingerprint Ops and QA approve. Recent-window producer evidence is stored in the app operations run `generation-v5-provenance.json`; the exact record hash is pinned in the candidate manifest and the fail-closed delta validator verifies successful execution, timestamp bounds, generator hash and candidate/reference output hashes. Residual audit errors remain visible and unresolved.
