# Completeness and publication

## Review matrix

Lifecycle selection is a union, not a subscription-month filter. Include any target-period demand, subscription, refund or listing event, recent closed subscriptions without listing dates, and explicit held IDs from prior runs. The completeness checker uses a two-calendar-month unknown-listing lookback and includes prior-year yearly feeds; older unresolved IDs must be ledger carryovers. Unreviewed lifecycle records fail the check and are included in publication comparisons even before ledger reconciliation. Do not mistake the legacy subscription-based phase audit passing for a full lifecycle pass. Add discovered carryovers to the evidence ledger and verify their applicable phase fields before claiming completion.

For every included issuer, inspect identity, demand/subscription/refund/listing dates, underwriters, band, final price, institution ratio/count, quantity-basis lockup, float, market cap, retail allocation, putback and its terms. After subscription, inspect each lead manager independently: applications, offered/subscribed shares, total ratio, equal pool, equal-per-account expectation and proportional ratio. After listing, inspect the supported regular-session outcome fields. A score being present is not evidence that its inputs are correct.

Record primary evidence for new/changed values in ledger notes or a linked review. Inspect latest related filings; distinguish hoped-price floor from confirmed price, requested-share lockup from application-count lockup, retail pool from total IPO shares, pool size from shares per person, and expectation from actual personal allotment. Preserve precise nonzero ratios rather than rounding small percentages to zero.

Every missing matrix cell requires a `fieldReviews` entry on its ledger candidate. Example:

```json
"fieldReviews": {
  "listingDate": {
    "state": "unpublished",
    "reason": "Latest reviewed filing contains no listing date",
    "evidenceUrls": ["https://dart.fss.or.kr/dsaf001/main.do?rcpNo=..."],
    "checkedAt": "2026-10-31",
    "nextCheckOn": "2026-11-02"
  }
}
```

Use real receipt URLs, not the illustrative placeholder. States: `unpublished`, `official_none`, `not_applicable`, `conflict`; absent/invalid review means `unreviewed`. Check dates are actual KST review dates. `nextCheckOn` is a research reminder, never a forecast of when an issuer will publish. Refresh overdue reviews; do not merely move the date forward. Future unpublished results may remain null. Official-none must be supported by an explicit source statement; it is not interchangeable with 'not found'. Keep the user's no-zero rule. An annotation explains the gap but does not bypass phase gates.

## Final retail evidence

Do not use latest capture time or a source name containing 'final' as proof. Inspect the official result and bind the decision to exact snapshot content:

```json
"finalSnapshotReviews": [{
  "snapshotSha256": "<digest from check_refresh_completeness.digest(snapshot)>",
  "status": "final",
  "aggregateFinal": true,
  "sourceDate": "2026-10-26",
  "evidenceUrl": "<actual official result URL>"
}]
```

`partial_final` is allowed for a broker-specific final notice; set `aggregateFinal:false` unless it also confirms the aggregate. Review multiple notices for multi-broker issuers. Do not use final reviews to certify later provisional captures. The checker reports missing final broker coverage and unverified aggregate separately. It computes equal/proportional indicators only to check availability, never writes them into data or calls them confirmed allocations. Match broker aliases through verified existing normalization, not fuzzy guesses.

Old ledgers lack this evidence schema. Populate it from actual checked sources; do not auto-approve every existing snapshot just to migrate. The new checker will intentionally report unreviewed legacy records.

## Publication contract

Maintain per-run requested/reviewed/changed/committed/pushed/publicly-verified/held ID lists. Partial publication must state exactly what remains local. Approval for one exception is not standing permission to publish later errors.

Before publishing, compare semantic JSON for every selected issuer and applicable feed. Preserve unrelated staged/unstaged values. Verify all feed paths, no old migrated ID, one index/yearly entry per issuer, exact-case filename, no closed subscription in active. Compare source input deltas too; regeneration can rewrite more than its requested output directory.

The completeness script produces a field matrix, structural issues and local-vs-HEAD report. With `--remote-base`, it also compares publicly served JSON. `DIFF` means different, `UNVERIFIED` means absent/error, not success. HEAD match is not proof of remote publication. A partial publication must run remote checks against its isolated exact committed candidate, not conflate withheld local changes with a failed push. GitHub Contents at the pushed SHA may establish committed content while Raw is stale; record both states.

This checker cannot discover every issuer on the internet, verify that a citation supports a number, or guarantee publication finality. Three independent schedule views, primary-source reading, phase audit, Dart checks and remote comparisons remain required. Do not claim zero omissions solely because automated checks pass.
