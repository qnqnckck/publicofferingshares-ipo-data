---
name: refresh-ipo-monthly-data
description: Reconcile, backfill, validate, and publish PublicOfferingShares Korean IPO data for the requested month range (default current and next month). Use for monthly IPO updates, missing institution/lockup/broker allocation metrics, schedule corrections, app feed regeneration, or verified data publication.
---

# Refresh Monthly IPO Data

Resolve the user-specified repository first; the current installation may be under `E:\harness\projects\publicofferingshares\ipo-data` rather than the older `D:` location. Verify its Git root and origin; never silently operate on a different copy. Use Asia/Seoul dates. The data repository is independent from the Flutter app.

## Required Workflow

1. Read the workspace, app, and repository `AGENTS.md` files that exist. Read the project context, `doc/refresh-context.md` when present, and current data README. Carry forward held corrections by ID, not just the latest run's changed rows.
2. Create or update the required project planning document before multi-step edits.
3. Check actual KST time and previous execution/publication records. Use all months explicitly requested, including past months and year crossings. Only when unspecified, default to current and next month. Record explicit inclusive start/exclusive end boundaries; pass the same boundaries to both audits. Never change the date merely to make an audit pass.
4. Build a candidate ledger from current web research before editing. Include subscription overlap, demand, refund and listing dates in range, not only subscription start. Review recent closed subscriptions with unknown listing dates (at least the preceding two calendar months), plus all older unresolved/held and user-reported IDs. Independently check KRX/KIND and issuer/broker listing announcements; unchanged DART indexes do not establish that listing dates remain unpublished. Follow [sources-and-identity.md](references/sources-and-identity.md). Save it as `doc/monthly-ipo-audit-YYYY-MM-DD.json`.
5. Compare every ledger row with source inputs, generated details and feeds in both directions. An unlisted local/feed event needs review too. Read [completeness-and-publication.md](references/completeness-and-publication.md) for the mandatory field checklist and publication states. Complete every issuer's gap review; do not stop after one reported omission or after finding non-null values.
6. Resolve company identity using exact company name plus subscription dates. Explicitly distinguish similarly named companies. Exclude rights offerings, SPAC mergers, already-listed securities, and non-public subscriptions unless the product intentionally supports them.
7. Populate source inputs, not generated JSON directly. Follow [data-contract.md](references/data-contract.md). Preserve null for facts not yet published; never substitute zero.
8. Regenerate through the repository batch. For reviewed manual corrections, use the local-only command in [commands.md](references/commands.md) so remote discovery cannot overwrite reviewed input.
9. Run `scripts/audit_monthly_ipo_data.ps1` and `scripts/check_refresh_completeness.py` using the exact requested range. Preserve both reports and report every residual error. A missing value with no authoritative evidence remains `null`; it is deferred, not fabricated or called resolved. A separate fail-closed verified-delta gate may publish an exact, independently reviewed scope when there are no new audit errors, target-scope integrity errors, provenance conflicts, or feed mismatches. Unchanged issues outside scope stay visible by ID/category and remain open; they do not block this scoped delta. Never use this rule to waive a target conflict, wrong value, stale target membership, malformed feed, or newly introduced error.
10. Run repository validation, Dart analysis for code changes, JSON parsing, the verified-delta gate when using partial publication, and `git diff --check`. Review the complete diff for unrelated score/date churn and restore only unintended generated noise.
11. Commit and push only when the user has authorized publication, including a recorded standing authorization; do not repeatedly request already-granted routine push permission. Authorization does not waive validation gates or allow unrelated changes. Push the nested data repository `main`, never only the app repository.
12. Verify the branch SHA and every changed public stock/feed JSON against the exact committed artifact, not the entire dirty worktree. Check each changed stock's applicable index/yearly/active/upcoming/recent membership. Distinguish CDN delay using GitHub Contents at the commit SHA; do not call a CDN-delayed file publicly refreshed yet. List reviewed-but-unpublished IDs explicitly, even after a successful partial push.

## Execution boundaries

- Respect the current task's hourly/17:00 limited-field scope. A full review never authorizes unrelated app changes. During a limited run, record other discovered gaps for the next full review instead of editing prohibited fields.
- Inspect latest related DART filings and both final-terms and prospectus sections; a prospectus may omit the results table found in final terms. Follow amendments and official broker/KRX notices rather than stale calendars.
- Preserve the original index/worktree and snapshot history. For dirty repositories, build a clean publication candidate from HEAD plus reviewed deltas; do not stage all pending work. Batch generation may rewrite discovered inputs: compare before/after and exclude out-of-scope normalization churn.
- Date migrations must update canonical input IDs, manual overrides and all feed references, remove the obsolete public stock file and preserve its history. Check case-sensitive filenames even on Windows.
- Re-run checks after generation and before committing. A local update, a commit, a push and verified public content are distinct states. Never say 'all updated' while any requested verified items remain unpublished.

## Phase Gates

- Before demand forecast closes: require identity, schedule, market, underwriters, price band when announced, and allocation baseline when announced.
- After demand forecast closes: require final offer price, institution competition, institution participant count, quantity-basis lockup rate, float rate, market cap, and public retail allocation. The audit treats the next KST weekday as a pending-publication warning; from the following day, missing values are errors. A documented publication delay may not be reported as complete.
- After retail subscription closes: require aggregate retail competition and broker snapshots. Require application count and equal-allocation estimate or confirmed value for each broker when publicly available. Label estimated splits as estimates.
- After listing: require listing outcome fields when the source pipeline supports them and ensure the security no longer appears as an active subscription.

## Non-Negotiable Checks

- Do not confuse `에이치엘지노믹스` with `알지노믹스` or any other normalized-name match.
- Store percentages as fractions (`1.91%` becomes `0.0191`), competition as ratios (`714.52:1` becomes `714.52`), and KRW/share counts as integers.
- Prefer quantity-basis lockup. If only application-basis lockup exists, do not store it as quantity-basis.
- Reject HTML entities such as `&amp;` in display names.
- Require one canonical ID and one stock file per company/subscription window.
- Require every feed `path` to resolve to an existing stock file and every target stock to occur once in its yearly feed.
- Never claim equal allocation, retail competition, or listing outcomes are final before the public result is final.
- Do not edit or revert unrelated user changes.

## Audit Command

```powershell
& "$env:USERPROFILE\.codex\skills\refresh-ipo-monthly-data\scripts\audit_monthly_ipo_data.ps1" `
  -RepoPath "D:\harness\projects\publicofferingshares\ipo-data" `
  -CandidateLedgerPath "doc\monthly-ipo-audit-YYYY-MM-DD.json"
```

Report the exact target months, candidate count, matched/excluded/unresolved count, errors, warnings, commands, commit SHA, push branch, and remote verification result.
