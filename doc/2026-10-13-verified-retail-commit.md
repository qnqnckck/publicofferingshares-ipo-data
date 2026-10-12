# October 13 verified retail commit

## 1. Idea summary

At 2026-10-13 00:48 KST the user explicitly requested committing all already
verified latest metrics despite the unrelated monthly DTS gaps. This is a
scoped manual commit exception, not a change to the recurring zero-error gate.
Recheck and commit the held MS Bio observations, including the newest verified
October 12 16:01:25 observation. Do not manufacture October 13 live data.

## 2. MVP scope

Resolve the user's missing confirmed-data commit. Include MS Bio's source
observations and derived detail/feed metrics; preserve earlier history and all
unrelated user edits. Exclude DTS missing demand fields, new issuer research,
app releases, code changes and unrequested publication. The user subsequently
approved "commit then push" for this verified-only change set. Success requires scoped
diff review, JSON/feed validation and explicit reporting of the global gaps.

## 3. Feature specification

The [public calculator](https://m.stock.naver.com/front-api/ipo/calculator/operands?code=A0013Y0)
was re-fetched on October 13 and still reports October 12 16:01:25 KST.
KB applicationCount is 106172, total competition 14.43 and directly reported
proportional competition 27.85. Retail remains 1000000 shares, with 500000 in
each pool. Preserve the actual source timestamp and provisional day-one label.
Derived equal average is 4.7093; it is not a confirmed personal allotment.
Add the complete observation to canonical broker input, regenerate locally
without remote discovery, and merge only reviewed MS Bio changes and necessary
date-based current/upcoming membership. Keep other issuers' numerical fields,
all fundamentals, demand grades and historical detail files unchanged.

The October 12 candidate ledger remains evidence of that day's full review:
October-November range, 41 decisions, 27 included, 14 excluded, 0 unresolved.
This targeted commit does not pretend a new complete monthly research run.
Rerun the monthly audit at October 13 without hiding existing errors. Apply
the user's manual exception only to the verified MS Bio change set.

## 4. Wireframe

Verified observations -> canonical broker input -> isolated batch -> scoped
detail/feed merge -> JSON/schedule/static checks -> explicit-path local commit.
User-approved main push -> exact remote detail/feed verification.

## Validation and outcome

- Regenerated with the existing local-only Dart batch; only MS Bio detail and
  its corresponding summaries are replaced. The date has rolled to October 13,
  so MS Bio and DTS appear in active (2) instead of upcoming (22). The DTS row
  is byte-content-equivalent to its existing upcoming row; no DTS numeric field
  or detail changes. Index497, yearly2026 84 and dashboard503 are preserved.
- All three verified observations are present in chronological order. MS Bio
  demand grade A/86 and all company fundamentals remain unchanged. Indicative
  equal average is4.7093; derived proportional one-share deposit is108615 KRW.
- Python validation passed568 JSON documents and1594 feed references, all27
  monthly identities,496 unrelated details and the original identifier bytes.
- Flutter analysis of the generator and two regression tests passed. Both
  source-priority and equal-freshness regression suites passed.
- October13 monthly audit remains6 errors (DTS offerPrice,topBandConfirmation,
  institutionCompetitionRate,institutionParticipants,lockupCommitmentRate,
  marketCapKrw) and23 warnings (future listing dates). No error was suppressed;
  this is the expressly authorized verified-data-only exception, not a claim
  that the full monthly dataset passes.
- Live Finuts schedule validation returned one pre-existing name-matching
  failure: `에스케이증권제14호스팩` versus `SK증권제14호스팩`. The checker
  only strips whitespace/punctuation and lowercases; it does not equate these
  legal/display spellings. The existing canonical/detail/yearly/index record
  has the same October15-16 subscription window and SK Securities underwriter.
  The stock detail and its numerical feed row are unchanged from main. Do not
  duplicate the issuer or copy Finuts's October16 listing fallback into a null
  official listing date. This unrelated validator limitation is reported,
  not hidden or fixed during this scoped data commit.
- Read-only primary-source reconciliation confirmed the same issuer and
  October15-16 window in [DART prospectus20260917000001](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260917000001),
  section I, with SK Securities as underwriter. This resolves the apparent
  missing-record concern without changing the canonical issuer or validator.

Commands (from the E-drive nested data repository):

```powershell
& E:\harness\flutter\bin\cache\dart-sdk\bin\dart.exe run tool/ipo_competition_batch.dart --backfill-years 3 --manual-fundamentals-path data/manual_fundamentals.json --no-discover --no-public-live-collect --no-identifier-discover --no-ipo-korea-supplement --no-article-lead-manager-discover --discovered build/refresh-20261013-verified/reviewed_discovered.json --identifier-path build/refresh-20261013-verified/identifiers.json --out build/refresh-20261013-verified/output
py -X utf8 build/refresh_20261013_verified.py merge
py -X utf8 build/refresh_20261013_verified.py validate
& C:\Users\김용성\.codex\skills\refresh-ipo-monthly-data\scripts\audit_monthly_ipo_data.ps1 -RepoPath E:\harness\projects\publicofferingshares\ipo-data -CandidateLedgerPath doc/monthly-ipo-audit-2026-10-12.json -AsOfDate 2026-10-13
& E:\harness\flutter\bin\flutter.bat analyze --no-pub tool/ipo_competition_batch.dart tool/ipo_snapshot_priority_test.dart tool/ipo_equal_freshness_test.dart
& E:\harness\flutter\bin\cache\dart-sdk\bin\dart.exe run tool/ipo_snapshot_priority_test.dart
& E:\harness\flutter\bin\cache\dart-sdk\bin\dart.exe run tool/ipo_equal_freshness_test.dart
py -X utf8 tool/validate_finuts_schedule_sync.py --warn-on-analysis-issues
git -c safe.directory=E:/harness/projects/publicofferingshares/ipo-data diff --check
```

User-approved publication completed:

- Data commit `5b50c35ef7befaeb8e3492d1861f3b697f827e91`, 15 scoped files,
  pushed to `origin/main` with no force or history rewrite.
- `git ls-remote` confirmed the data commit on main. Exact GitHub Raw JSON
  comparison passed all8 documents: MS Bio and unchanged DTS details, index,
  active, upcoming, recent, yearly2026 and dashboard. App-facing data is now
  available on the same main URLs the app consumes.
- Both Python and PowerShell parsed568 JSON documents; staged whitespace
  validation passed. Unrelated identifier/audit changes and old untracked
  review documents were left outside the commit.
- The six DTS gaps and the separately reconciled SK alias-check limitation
  remain documented; the scoped exception does not claim a full audit pass.

Remote check: `py -X utf8 build/verify_remote_20261013.py`.
