# October 4, 2026 - reviewed publication

## 1. Idea summary

The user corrected the relocated workspace to E:/harness and requested publication. The actual independent data repository is E:/harness/projects/publicofferingshares/ipo-data, main at 241228fd1736e75c5c36a74a7d4289c377dce718. KST clock checked October 4 at 05:49. Review October and November, including the two candidates held during the October 2 retail-only run.

## 2. MVP scope

- Problem: verified closing/day-one retail observations and two newly announced November IPOs are not yet public.
- Required flows: reconcile exact identities and primary filings, retain source timestamps, regenerate only reviewed data, validate and publish main if the publication gate allows it.
- Out of scope: store releases, app UI, fabricated final results, unrelated local changes and broad history/score rewrites.
- Success: source-backed data visible at exact GitHub Raw URLs, with pending results explicitly retained and all validation findings disclosed.

## 3. Feature specification

- New baselines: verify Dongwon Parts and KMF with DART and public calendars before adding canonical events/manual fundamentals. Do not confuse similarly named companies or insert forecast final prices.
- Retail: preserve earlier observations, add the newest publicly verifiable Melcon/Jincostech observations, distinguish calculated equal averages from confirmed allotments, and use retail rather than total offering pools.
- Month coverage: reconcile three independent current calendars with every candidate. Keep exclusions and unresolved candidates visible in the ledger.
- Gate: the preliminary October 4 audit has eight errors: two unresolved new candidates and six unpublished Elis demand-result fields. Do not suppress or backdate the audit. Resolve source-backed errors; any remaining exception requires explicit user direction.
- Error states: inaccessible sources and pending disclosures remain documented, not replaced with guesses or zeros. Preserve all unrelated user edits.

## 4. Wireframe / execution flow

E: repository + KST date -> primary/calendar reconciliation -> canonical inputs
-> local-only batch -> scoped feed reconciliation -> JSON / monthly / schedule / Dart checks
-> publication gate -> scoped main commit/push -> exact Raw readback.

## Evidence and validation

### Sources and reviewed changes

- Reopened [38](https://www.38.co.kr/html/fund/index.htm?o=k), [Mainpy October](https://ipo.mainpy.dev/2026-10), [Mainpy November](https://ipo.mainpy.dev/2026-11), and [Sonnimlab](https://www.sonnimlab.com/schedule/). The latest two additions are not yet covered by both secondary calendars; exact primary filings resolve them. Other covered October/November identities and subscription windows are unchanged. Listed issuers, investment-contract securities and the duplicate SPAC alias remain excluded.
- [Dongwon Parts initial filing, October 1](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20261001000579): KOSDAQ semiconductor-equipment parts company, code 0162D0, Samsung. Demand October 22-28, retail November 2-3, refund November 5; price band KRW 23000-26500. The pre-demand retail range is 507500-609000 shares; store the announced minimum baseline 507500, not a final allocation. Initial tradable shares 2734201 / 9276758, reported 29.47% (stored 0.2947). No general-subscriber putback. Final offer/demand metrics, market cap and listing date stay null.
- [KMF initial filing, October 2](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20261002000390): Daegu fermentation/food company headed by Jung Yongjin, code 0103V0, IBK; not a similarly named metalworking company. Demand November 16-20, retail November 24-25, refund November 27; band KRW 15000-17800. Pre-demand retail range 500000-600000; baseline 500000. Initial float 3250187 / 8483285, reported 38.31% (0.3831), not later lockup-release float. No putback; final values remain null.
- [Melcon calculator](https://m.stock.naver.com/front-api/ipo/calculator/operands?code=A179880): actual source time October 2 16:13:45 KST; 254009 applications, total competition 1741.48, proportional 3482.95, equal/proportional pools 312500 each. Total/applications are also corroborated by the [October 2 closing press release](https://newspim.com/news/view/20261002001171). Indicative equal average 1.2303; not an official final allotment. Preserve the quantity-basis institutional lockup 36.0714%; do not replace it with the news release's institution-count percentage.
- [Jincostech calculator](https://m.stock.naver.com/front-api/ipo/calculator/operands?code=A250030): source time October 2 16:13:45; 46394 applications, total 8.6, proportional 17.2, equal/proportional pools 106500 each. Indicative equal average 2.2956; day-one observation, subscription ends October 6. Unpublished subscribed-share counts remain null for both issuers.
- [Elis latest corrected filing, September 29](https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260929000656) explicitly schedules final-price publication October 6. The DART index still has no later final-price result. Its existing stock record is intentionally unchanged in this scoped publication, and its monthly findings remain visible. The missing putback summary is separate from the unpublished demand fields; this filing confirms no putback, but that additional correction is not included in the four-record change.

### Regeneration and validation

- Target interval: October 1 through December 1 exclusive (October/November). Ledger: 38 candidates, 24 included (18 October + 6 November), 14 excluded, zero unresolved identities.
- Local-only batch generated 480 scratch stock files. Scoped reconciliation retains all 14 legacy public identities missing from scratch and all unrelated source/score/history data. Published output prepared locally: index 494, dashboard 500, yearly/2026 81, active 1, upcoming 22, recent 60. Melcon is no longer active. Jincostech remains within the subscription date window (not a claim that weekend trading is open).
- Command: `dart run tool/ipo_competition_batch.dart --backfill-years 3 --manual-fundamentals-path data/manual_fundamentals.json --no-discover --no-public-live-collect --no-identifier-discover --no-ipo-korea-supplement --no-article-lead-manager-discover --identifier-path build/refresh-20261004/identifiers.json --out build/refresh-20261004/output`.
- Command: `py -X utf8 build/reconcile_refresh_20261004.py --apply`: passed. Only two new baselines and two new retail observations plus derived values are reconciled; existing overall grades/scores and all schedules remain unchanged. The batch's incidental cached-analysis rewrite was restored from the exact reviewed pre-batch source backup.
- Command: `flutter analyze --no-pub tool/ipo_competition_batch.dart tool/ipo_snapshot_priority_test.dart`: passed, no issues. `dart run tool/ipo_snapshot_priority_test.dart`: passed official priority, corrected filings, preserved history and provisional fallback.
- Command: `py -X utf8 tool/validate_finuts_schedule_sync.py --warn-on-analysis-issues`: exit 0 using local seed fallback. Live Finuts timed out with WinError 10060. This is NOT live Finuts confirmation; independent public calendars were checked separately.
- Command: `audit_monthly_ipo_data.ps1 -RepoPath E:/harness/projects/publicofferingshares/ipo-data -CandidateLedgerPath doc/monthly-ipo-audit-2026-10-04.json -AsOfDate 2026-10-04`: exit 1, six errors, 23 warnings. Errors are Elis offerPrice, topBandConfirmation, institutionCompetitionRate, institutionParticipants, lockupCommitmentRate and putbackSummary. Warnings are 22 unannounced listing dates and MS Bio pending demand publication through October 5. No error was hidden or the audit backdated.
- Identifier SHA256 is unchanged: B389739C910688045D7E18E34ED1F9CD1C123CD8E85A7AD8E0A43B7766B1344A. Earlier local notes and the October 2 working ledger remain untouched by this turn and must not be included in this publication.
- `py -X utf8 build/validate_refresh_20261004.py`: passed 557 JSON documents including the current ledger, 1584 unique feed references, all 24 monthly identities and semantic equality of 490 unrelated existing stock details. PowerShell `ConvertFrom-Json` independently parsed all 557 documents. `git diff --check` passed after formatting only the newly added README paragraph; no global newline normalization was performed.
- Saved automation 13 now targets E:/harness/projects/publicofferingshares/ipo-data. Its weekday 13:00/17:00 schedule, ACTIVE state, thread destination, scope, no-error publication gate and notification behavior are preserved. Readback confirmed the path update.

### Publication decision

The ordinary monthly gate remains blocked by the six documented Elis findings. After being told that Elis would remain unchanged and only the four verified records would be published, the user explicitly approved: "응 그렇게 해줘". Approval was processed October 4 at 06:20 KST. This is a one-off exception for Dongwon Parts, KMF, Melcon and Jincostech only, not a change to automated publication gates. No missing demand result is fabricated or declared complete.

Pre-publication remote main was 241228fd1736e75c5c36a74a7d4289c377dce718. Recheck the unchanged four-record scope, commit only its source inputs, generated feeds/details and current documentation, then push main. Verify the remote branch SHA and exact Raw URLs of all four stock details plus active, upcoming, recent, index, dashboard and yearly/2026 before reporting publication complete. Older local audit notes and the identifier working state are explicitly excluded.
