# Commands

Run from the resolved data Git root. Paths below are examples; use the actual repository and skill paths, and the user's requested month range.

## Reviewed Local Regeneration

```powershell
dart run tool/ipo_competition_batch.dart --backfill-years 3 --manual-fundamentals-path data/manual_fundamentals.json --no-discover --no-public-live-collect --no-identifier-discover --no-ipo-korea-supplement --no-article-lead-manager-discover
```

Use this after reviewed manual edits so remote collectors cannot change the reviewed candidate set during regeneration.

## Validation

```powershell
py -3 tool/validate_finuts_schedule_sync.py --warn-on-analysis-issues
dart analyze tool\ipo_competition_batch.dart
git diff --check
```

Parse every changed JSON file with `ConvertFrom-Json`. Run the bundled monthly audit with the candidate ledger. Review staged and unstaged changes separately.

```powershell
& <skill-path>/scripts/audit_monthly_ipo_data.ps1 -RepoPath <repo> -CandidateLedgerPath <ledger> -AsOfDate 2026-10-31 -RangeStart 2026-10-01 -RangeEndExclusive 2027-01-01
py -3 <skill-path>/scripts/check_refresh_completeness.py --repo <repo> --ledger <ledger> --start 2026-10-01 --end 2027-01-01 --as-of 2026-10-31 --output <run-report.json>
```

Use current actual as-of dates, not these examples. Explicit range arguments must match the ledger; omission retains the legacy current/next-month default. The completeness check is additional, not a replacement for the phase audit. It returns nonzero for errors. Add `--remote-base https://raw.githubusercontent.com/<owner>/<repo>/main` after verifying origin to check public contents. Supply an isolated exact commit candidate and `--publication-paths <json-list-of-changed-public-paths>` for partial-publication verification; the full gap audit still runs and its errors are not suppressed. Neither script commits or pushes.

Regression checks for skill changes:

```powershell
py -3 -m unittest discover -s <skill-path>/scripts -p "test_*.py"
```

## Publication

```powershell
git status --short
git diff --stat
git diff --check
git add <intentional-paths>
git commit -m "Refresh YYYY-MM and YYYY-MM IPO data"
git push origin main
git ls-remote origin refs/heads/main
```

Publishing requires explicit user authorization. If authorization exists in the current conversation, publish after all validation gates pass.

## Official withdrawal delta

For a multi-target withdrawal candidate, run both full audits on baseline and candidate, then use the withdrawal-specific fail-closed gate:

```powershell
py -3 <skill-path>/scripts/check_withdrawn_delta.py --repo <candidate-repo> --manifest <withdrawal-manifest.json> --source-record <official-observations.json> --source-root <raw-source-root> --baseline-phase <baseline-phase.log> --candidate-phase <candidate-phase.log> --baseline-completeness <baseline-completeness.json> --candidate-completeness <candidate-completeness.json> --report <withdrawal-gate.json>
py -3 -m unittest <skill-path>/scripts/test_check_withdrawn_delta.py
```

This gate only handles source-backed withdrawal removal; ordinary updates continue through `check_verified_delta.py`.
