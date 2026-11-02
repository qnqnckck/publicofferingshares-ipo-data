# Sources and Identity

## Candidate Ledger

Research every requested month (default current and next KST month) with at least three independent schedule views, then confirm each candidate with a primary filing or underwriter/company notice when available. Browse because schedules and results are time-sensitive. A provider with no next-month coverage is not proof of an empty month; record coverage and research limitations. Reconcile local-only and public-only rows too.

Use this ledger schema:

```json
{
  "asOfDate": "2026-07-14",
  "rangeStart": "2026-07-01",
  "rangeEndExclusive": "2026-09-01",
  "sources": [
    {"name": "source name", "url": "https://...", "checkedAt": "2026-07-14"}
  ],
  "candidates": [
    {
      "company": "canonical display name",
      "subscriptionStart": "2026-07-13",
      "subscriptionEnd": "2026-07-14",
      "classification": "ipo",
      "status": "matched",
      "eventId": "company_2026-07-13",
      "evidenceUrls": ["https://..."],
      "notes": ""
    }
  ]
}
```

Allowed status values are `matched`, `added`, `excluded`, and `unresolved`. An unresolved row blocks publishing. An excluded row requires a reason in `notes`.

## Identity Rules

Match exact legal/display company name, subscription start/end dates, security classification, market, and underwriter. Do not use fuzzy name similarity as sufficient evidence. Decode HTML entities before comparison and preserve a source URL for every added or corrected candidate.

## Source Priority

1. DART filing, KRX/KIND disclosure, underwriter notice, or issuer notice.
2. Official underwriter subscription/result page.
3. Multiple current IPO calendar providers.
4. Reputable financial news with explicit values and dates.
5. Community aggregators only as discovery or secondary corroboration.

For conflicting values, prefer the latest primary result and record the resolution in the ledger notes or planning document.
