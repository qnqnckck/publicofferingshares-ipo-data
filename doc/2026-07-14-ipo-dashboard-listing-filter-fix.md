# IPO Dashboard Existing-Listing Filter Fix

## Idea Summary

Prevent public offerings and rights issues from companies that are already listed from appearing as new IPO subscriptions. The immediate examples are Hyungji I&C and Hanwool Semiconductor, which were incorrectly published after `PUBLIC_OFFERING` rows were allowed to bypass the historical-listing guard.

## MVP Scope

### Core User Problem

The IPO dashboard presents existing listed-company financing events as IPOs. These rows also expose empty IPO-only metrics and, for Hyungji I&C, an HTML-encoded company name.

### Must-Have Flows

- Reject a discovered candidate when its normalized company matches a historical listing.
- Apply the rejection even when the candidate security type is `PUBLIC_OFFERING`.
- Regenerate dashboard feeds and remove orphaned stock files.
- Verify that Hyungji I&C and Hanwool Semiconductor are absent from active IPO feeds.

### Out of Scope

- Showing rights issues or follow-on offerings in a separate product surface.
- Inventing institution-demand, lockup, allocation, or expected-return values for non-IPO events.
- Changing Flutter UI presentation.

### Success Criteria

- The 23 previously added existing-listed-company rows are absent from generated IPO feeds.
- `PUBLIC_OFFERING` no longer bypasses the historical-listing guard.
- Generated JSON parses successfully and Dart diagnostics pass.
- The corrected commit is pushed to `origin/main`.

## Feature Specification

### Historical-Listing Guard

Purpose: keep the IPO feed limited to new listings.

User interaction flow: the user opens the IPO dashboard and sees only valid IPO subscriptions.

Data/state changes: discovered rows are matched by normalized company identity against historical listings and excluded before output generation.

Error states: missing or mismatched company identity can prevent a valid historical match; the guard report records rejected candidates for review.

Acceptance criteria: a `PUBLIC_OFFERING` row for an already listed company is rejected with `company_already_has_historical_listing`.

### Generated Feed Cleanup

Purpose: remove already-published invalid rows and their detail files.

User interaction flow: refreshed dashboard, recent, upcoming, yearly, and detail endpoints no longer expose invalid entries.

Data/state changes: regenerate index outputs and delete stock JSON files not present in the selected IPO set.

Error states: generation failure must leave the failure visible and block publication.

Acceptance criteria: Hyungji I&C and Hanwool Semiconductor cannot be found in current generated IPO outputs.

## Wireframe

```text
Discovered schedule
       |
       v
Normalize company identity
       |
       v
Historical listing match? -- yes --> Reject + guard report
       |
       no
       v
IPO eligibility filters
       |
       v
Dashboard / index / stock detail JSON
```
