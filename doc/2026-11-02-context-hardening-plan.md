# Refresh handoff hardening

## Idea summary
Prevent subscription-month-only review and local-only completion claims.

## MVP scope
Persist operational context and publication authorization; expand read-only completeness selection to lifecycle dates and recent closed subscriptions missing listing dates. Preserve source data and unrelated staged changes. No financial changes or automatic audit bypass in this task.

## Feature specification
Read context at entry. Select events by subscription overlap, refund, listing and demand dates, plus two-calendar-month lookback for unknown listing dates; older unresolved IDs remain explicit ledger carryovers. Compare public artifacts for selected stocks even when absent from the candidate ledger. Fail on unreviewed carryovers. Tests cover month/year crossings, null listing, omitted ledger, remote mismatch and unaffected old completed records.

## Wireframe / operational flow
Context + last run → lifecycle candidates + held IDs → sources → canonical inputs → isolated generation → validation → scoped commit/push → exact remote artifact verification → handoff.

## Verification result

- Updated data AGENTS entrypoint, persistent doc/refresh-context.md, parent app docs/project-context.md pointer, personal skill instructions and executable completeness selection.
- `py -X utf8 -m unittest discover -s <skill>/scripts -p test_*.py`:25tests passed (4new lifecycle regression cases).
- skill-creator quick_validate.py: valid.
- Scoped Git whitespace check: passed.
- Actual read-only completeness run now reports181errors rather than156, because previously omitted lifecycle records are surfaced. This is detection improvement, not a clean audit or permission to publish. Report build/completeness-context-hardening.json. Data/feed values were not changed by this hardening task.
