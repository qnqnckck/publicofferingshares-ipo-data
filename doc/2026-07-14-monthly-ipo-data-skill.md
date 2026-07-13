# Monthly IPO Data Skill Plan

## Idea Summary

Create a reusable Codex skill that updates and audits Korean IPO data for the current and next calendar month. The skill must distinguish similarly named companies, collect source-backed facts, regenerate every app-facing index, validate the raw GitHub delivery path, and push only after all gates pass.

## MVP Scope

### Core User Problem

Repeated ad hoc corrections leave IPOs missing, partially populated, mapped to the wrong company, or present in a stock file but absent from the feed the app actually reads.

### Must-Have Flows

1. Determine the current and next month in Asia/Seoul.
2. Build a complete candidate list from multiple current public sources.
3. reconcile candidates against source events, manual fundamentals, stock details, and feed indexes.
4. Populate required schedule, pricing, institutional, allocation, float, broker, and outcome fields only from attributable sources.
5. Regenerate app-facing data and run deterministic local audits.
6. Commit and push the nested data repository when the user has authorized publishing.
7. Verify the exact GitHub Raw stock and feed URLs consumed by the app.

### Out of Scope

- Inventing values that have not been publicly finalized.
- Changing Flutter UI behavior during a data-only run.
- Treating rights offerings, SPAC mergers, or already-listed securities as ordinary IPO subscriptions.
- Rewriting unrelated historical data or user changes.

### Success Criteria

- Every reconciled current/next-month IPO has one canonical event ID and one stock detail file.
- Required fields are present or explicitly classified as not-yet-published with evidence.
- Feed indexes reference existing stock files without duplicates or stale listed-company subscriptions.
- Local parser, analyzer, audit, diff, and remote delivery checks pass.

## Feature Specification

### Monthly Candidate Reconciliation

- Purpose: prevent missing and misidentified IPOs.
- Interaction: run the skill with no date arguments to select the current and next month.
- Data/state: compare external candidates with `data/discovered/ipo_events.json` and generated feeds.
- Error states: conflicting names/dates, secondary offering mistaken for IPO, insufficient source agreement.
- Acceptance: every candidate is matched, added, excluded with reason, or marked unresolved; unresolved candidates block publishing.

### Required Field Backfill

- Purpose: fill the report fields users expect.
- Interaction: verify facts using primary filings and current reputable sources before editing manual inputs.
- Data/state: update canonical events and `data/manual_fundamentals.json`, then regenerate stock details.
- Error states: conflicting percentages, application-basis versus quantity-basis lockup, preliminary retail competition.
- Acceptance: stored values include correct units and basis; unknown values remain null and are documented as pending.

### Deterministic Audit

- Purpose: catch omissions before publication.
- Interaction: execute a bundled PowerShell audit against the nested data repository.
- Data/state: read source events, manual fundamentals, generated stock files, and indexes without mutation.
- Error states: duplicates, missing files, broken paths, invalid ranges, absent required facts, inconsistent summaries.
- Acceptance: exit code zero and a month-by-month coverage report.

### Publication Verification

- Purpose: ensure the app can consume the correction.
- Interaction: commit, push `main`, then query the same Raw GitHub paths used by Flutter.
- Data/state: published repository only.
- Error states: wrong branch, CDN delay, local-only changes, stale null-cache behavior.
- Acceptance: remote branch SHA matches, target stock JSON contains expected values, and at least one correct feed resolves its path.

## Wireframe

```text
[User: update IPO data]
          |
          v
[Resolve KST current + next month]
          |
          v
[External candidate ledger]
  matched | excluded | unresolved
          |
          v
[Canonical source edits]
  event + manual fundamentals
          |
          v
[Batch regeneration]
  active/upcoming/recent/yearly/stocks
          |
          v
[Audit gates]
  identity -> fields -> paths -> ranges -> JSON -> analyze -> diff
          |
     fail | pass
          |   v
          | [Commit + push main]
          |          |
          +------> [Raw GitHub verification]
```
