# September 26, 2026 manual IPO refresh

## Idea summary

The user explicitly requested a current data refresh at 12:18 KST, independently of the weekday automation schedule. Reconcile September and October 2026 using current public evidence and the refresh-ipo-monthly-data contract.

## MVP scope

- Problem: verified corrections and recent listing results must not remain stale, while unpublished results must remain unknown.
- Required flow: current calendars -> exact identity/window reconciliation -> primary evidence -> canonical updates -> local-only regeneration -> validation -> publication gate.
- Out of scope: application UI, store releases, automation settings, audit-policy changes, historical unrelated corrections and the existing dirty identifier file.
- Success: verified changes prepared and validated, then published only if all mandatory audit checks pass. Otherwise report exact blockers and retain reviewed changes without claiming app publication.

## Feature specification

- Reuse prior candidate evidence only after fresh discovery confirms the current set; create today's ledger before source edits.
- Review Melcon's immediate float, Melcon/Jincostech putback descriptions and recently listed September IPOs; add only sourced figures.
- Preserve null for unannounced prices/demand results and broker counts. Ratios, fractional percentages and currency/share units must remain consistent.
- Regenerate deterministic app feeds from reviewed canonical inputs only. Check all JSON, feed identity/path consistency, schedule synchronization, Dart analysis and monthly audit.
- Publishing fails closed on any audit ERROR. Do not weaken validation or change real demand dates to bypass pending publications.

## Wireframe

Calendar discovery -> candidate ledger -> primary-source review -> canonical data changes
 -> generated stock detail + index/upcoming/recent feeds -> checks
 -> PASS: scoped main publication and remote verification
 -> ERROR: no commit/push; report corrected local fields and remaining blockers.

## Initial state

- Repository is now E:/harness/projects/publicofferingshares/ipo-data; former D: path is absent.
- Existing main HEAD: 470d596c6c633786cb38e238a9d00f9c04d7717a.
- Existing identifier modification and five untracked historical audit notes must remain untouched.
- Prior audit had fourteen errors affecting Melcon and Jincostech. Reassess using current evidence; do not assume those results were published.

## Discovery and identity review

- Target: September 1 through November 1, 2026, exclusive, Asia/Seoul. Started at 12:18 KST on September 26.
- Fresh calendar comparison: [38](https://www.38.co.kr/html/fund/index.htm?o=k), [Sonnimlab](https://www.sonnimlab.com/schedule/), [Mainpy September](https://ipo.mainpy.dev/2026-09), [Mainpy October](https://ipo.mainpy.dev/2026-10).
- Today's ledger contains 46 candidates: 27 matched (9 September, 18 October), 19 excluded, zero unresolved. No new eligible window or verified schedule replacement was found. There is no active retail subscription on September 26.
- Nine additional September discovery candidates were explicitly excluded: Fantagio, SG, Gyeyang Electric, Futurechem, Ngenbio, HLB Pharmaceutical, Edge Foundry and Toolgen are listed-issuer offerings; Stockkeeper is an investment-contract security. Mainpy dates are recorded as discovery observations, not adopted as corrected rights-offering dates.
- Classification corroboration: [38 rights/general calendar](https://www.38.co.kr/html/fund2/index.htm), [Stockkeeper issuer press release](https://www.newswire.co.kr/newsRead.php?no=1042955), [Korea Investment HLB notice](https://securities.koreainvestment.com/main/customer/notice/Notice.jsp?cmd=TF04ga000002&num=47626).
- Previously verified values and sources were retained where no fresh evidence superseded them; this is not a claim that all historic metrics were independently rediscovered today.

## Reviewed canonical changes (local only)

### Melcon and Jincostech

- Melcon immediate float corrected from 28.4% to 34.57%, stored as `0.3457`. Prospectus immediate float is 4,346,680 / 12,575,000 shares; the disclosed rounded 34.57% is distinct from later lockup-release percentages.
- Added the explicit `일반청약자 환매청구권 없음` summary for Melcon and Jincostech, consistent with the existing `hasPutbackRight: false` values.
- Melcon [current prospectus](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260914000034), [offer section](https://dart.fss.or.kr/report/viewer.do?rcpNo=20260914000034&dcmNo=11577565&eleId=6&offset=82782&length=193889&dtd=dart4.xsd), [risk/float section](https://dart.fss.or.kr/report/viewer.do?rcpNo=20260914000034&dcmNo=11577565&eleId=13&offset=289733&length=668596&dtd=dart4.xsd), [float corroboration](https://www.thebell.co.kr/front/newsview.asp?code=0405&key=202609221445304440105840).
- Jincostech [current corrected filing](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260910000579), [offer section](https://dart.fss.or.kr/report/viewer.do?rcpNo=20260910000579&dcmNo=11575443&eleId=9&offset=400897&length=218023&dtd=dart4.xsd).
- Fresh filing-chain checks found no later final demand-result filing. Announced final-price publication dates are September 29 (Melcon) and September 28 (Jincostech). Final offer prices, demand metrics and confirmed listing dates remain null. Media October 15 target listing dates were not promoted to confirmed dates.

### KB SPAC 34 final allotment

[Official KB allotment/refund notice](https://www.kbsec.com/go.able?idt=20260914&linkcd=s060901010000&seq=10010338), published September 14 at 14:26:52 KST:

- Offered shares 1,750,000; equal/proportional pools 875,000 each.
- Applications 102,589; subscribed shares 695,325,420.
- Published equal average 8.53 shares (actual allotment 8 or 9), not guaranteed fractional allotment.
- Published proportional competition 793.66:1. Aggregate 397.33:1 is rounded from the exact subscribed/offered ratio. Do not double aggregate to replace the published proportional value.
- Added a later official snapshot; kept the earlier secondary snapshot as dated history. The official source supersedes the old imputed 694,452,500 subscribed shares and missing application count.

### Regular-session listing outcomes

| Issuer | Listing date | Open | High | Close |
| --- | --- | ---: | ---: | ---: |
| KB SPAC 34 | 2026-09-22 | 4,000 | 4,580 | 1,914 |
| Korea SPAC 17 | 2026-09-22 | 3,415 | 5,220 | 2,155 |
| WisePlanet Company | 2026-09-23 | 42,750 | null | 46,300 |

- Cross-checks: [38 new listings](https://www.38.co.kr/html/fund/index.htm?o=nw), [Hankyung September 22 SPAC close/high report](https://www.hankyung.com/article/2026092217581), [WisePlanet open report](https://v.daum.net/v/20260923094528905), [Seoul Shinmun close report](https://www.seoul.co.kr/news/2026/09/23/20260923500253), [WisePlanet session summary](https://www.sonnimlab.com/results/wiseplanet/).
- WisePlanet high remains null: public feeds did not establish a consistent regular-session high. Do not infer 48,000 from the daily price limit. Conflicting after-hours/current prices were not used as regular-session closes.
- Naver daily-price endpoints returned HTTP 409; they were not treated as verified evidence. Korea SPAC 17 application count remains null because an official final count was not found.

## Generation and validation

- Canonical inputs changed: two manual fundamentals rows, one new official broker snapshot file, three appended listing outcomes. Regenerated to scratch with `tool/ipo_competition_batch.dart`, then applied only five reviewed stock details and their existing feed rows using `build/reconcile_refresh_20260926.py --apply`.
- Local-only generator flags: `--backfill-years 3 --manual-fundamentals-path data/manual_fundamentals.json --no-discover --no-public-live-collect --no-identifier-discover --no-ipo-korea-supplement --no-article-lead-manager-discover --out build/refresh-20260926/output`.
- Important batch behavior: even with discovery disabled, the batch rewrites default discovered/identifier source files. These incidental changes were detected and reversed after read-only proof: discovered event IDs/values were preserved; identifier initial bytes were exactly recovered (SHA256 `B389739C910688045D7E18E34ED1F9CD1C123CD8E85A7AD8E0A43B7766B1344A`). The new verified corrections were in separate files and preserved. Future scratch runs should redirect both mutable input paths to scratch copies.
- Scoped merge passed: all 488 main-index members retained, 483 unrelated main-index rows unchanged, no historical deletion. Upcoming remains 18, recent 60, yearly 2026 75, dashboard 494; active remains empty. Only reviewed rows carry updated analysis generated by the existing batch; no scoring logic changed.
- `py -X utf8 build/validate_refresh_20260926.py`: PASS after the scoped merge. Parsed 546 JSON files; verified 1,561 unique per-feed rows and every stock path, feed membership, all target windows/yearly identities, source-to-generated consistency and untouched unrelated stock details. Existing July source-outcome duplication is separately documented below rather than silently removed.
- `git diff --check`: PASS. Only line-ending conversion notices; no whitespace errors. Final branch/HEAD rechecked and unchanged.
- `flutter analyze --no-pub tool/ipo_competition_batch.dart`: PASS, no issues.
- `dart run tool/ipo_pending_counts_test.dart`: PASS (pending, mixed, published-rate, known-count, explicit-zero scenarios).
- `py -X utf8 tool/validate_finuts_schedule_sync.py --warn-on-analysis-issues`: PASS using local seed fallback. Live Finuts timed out with WinError 10060; this is not a successful live-source comparison.
- `audit_monthly_ipo_data.ps1 -RepoPath E:\harness\projects\publicofferingshares\ipo-data -CandidateLedgerPath doc\monthly-ipo-audit-2026-09-26.json -AsOfDate 2026-09-26`: FAIL, 46 candidates / 27 included / 19 excluded / zero unresolved; **12 errors and 19 warnings**.
- Errors: six unpublished demand-result fields for each of Melcon and Jincostech: `offerPrice`, `topBandConfirmation`, `institutionCompetitionRate`, `institutionParticipants`, `lockupCommitmentRate`, `marketCapKrw`. The prior two missing putback-summary errors are resolved locally.
- Warnings: 18 unannounced October listing dates; Korea SPAC 17 application count not verified. Accepted as pending; no zero/placeholder inserted.
- Additional read-only integrity review found a pre-existing duplicate July source outcome (`레메디_2026-07-01`, two rows in both HEAD and the working file). No duplicate was added by this refresh; it is outside the September/October scope and was preserved. Generated feeds/details still have unique identities. This is separate from the monthly audit's 19 warnings.
- The audit counts weekdays after demand close and does not accommodate the prospectuses' later publication dates. This explains the blockers but does not authorize changing the audit, moving actual dates or reporting the refresh complete.

## Initial publication hold (12:18 KST refresh)

**Not committed, not pushed, not published to the app.** The mandatory refresh skill blocks publication on any audit ERROR. Local verified changes are retained for completion once public demand results become available and the full audit passes.

- Branch: `main`.
- Existing HEAD: `470d596c6c633786cb38e238a9d00f9c04d7717a`; no new commit SHA.
- Remote SHA/Raw-feed confirmation of new changes: not applicable, no push attempted.
- Unrelated existing audit notes and identifier bytes were preserved. No app code, release workflow or automation settings changed.

## Explicit publication follow-up (16:18 KST)

The user responded to the documented publication hold with `푸시까지 해줘야지 반영되지` (push it so the changes take effect). This is an explicit instruction to publish the already-reviewed corrections despite the explained pending-publication audit findings. Apply this exception only to this manual publication, not to future scheduled refreshes.

### Publication scope and acceptance

- Commit only the reviewed canonical inputs, five regenerated stock files and corresponding feeds, today's evidence ledger, this report and the README note.
- Keep all twelve unpublished demand-result values null. Do not fabricate metrics, alter schedules, modify the audit script or describe the monthly audit as passing.
- Preserve the existing identifier state and unrelated historical notes; do not stage them.
- Re-run integrity checks and the monthly audit. Stop if there is any new discrepancy beyond the documented pending results, warnings and unchanged out-of-scope historical source duplication.
- Push `main` without force, then confirm the remote SHA and exact raw stock/feed responses match the committed JSON. A push alone is not the remote verification result.

Flow: explicit user publication request -> scoped revalidation -> unchanged pending results documented -> reviewed commit -> normal main push -> GitHub SHA and raw JSON verification.

Pre-push remote main was checked and still equals `470d596c6c633786cb38e238a9d00f9c04d7717a`; no concurrent remote changes were found. Publication result will be recorded below after verification.
