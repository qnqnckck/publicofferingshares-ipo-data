# IPO Audit Pending-Result Validation

## Idea Summary

The monthly IPO audit must distinguish an unannounced schedule or a normal
post-demand-forecast publication delay from a genuinely missing required value.

## MVP Scope

- Core user problem: a valid upcoming IPO update is blocked by false-positive
  audit errors, while real omissions still need to block publication.
- Must-have flows: audit an event with no announced demand schedule, audit an
  event during the first business day after demand forecast, and audit an event
  after that grace period.
- Out of scope: automatically collecting demand-result disclosures.
- Success criteria: unknown demand dates do not fail baseline validation;
  demand-result fields become blocking only after one following business day;
  offline validation can read UTF-8 seed data.

## Feature Specification

### Demand-result gate

- Purpose: enforce results after a documented, realistic publication window.
- User interaction flow: run the monthly audit with its KST as-of date.
- Data/state changes: events with a known demand end date enter a warning state
  through the next weekday, then require complete demand-result fields.
- Error states: absent required results after the grace deadline remain errors.
- Acceptance criteria: a Friday close is a warning on the following Saturday,
  Sunday, and Monday, and becomes blocking on Tuesday.

### Offline schedule validation

- Purpose: retain deterministic validation when the remote Finuts endpoint is
  unavailable.
- User interaction flow: validator falls back to the checked-in seed feed.
- Data/state changes: none.
- Error states: malformed JSON remains an explicit validation failure.
- Acceptance criteria: UTF-8 Korean company names load without a codec error.

## Wireframe

```text
[Monthly audit]
       |
       +-- demand dates announced? -- no --> baseline passes without dates
       |                              yes
       +-- first following weekday passed? -- no --> pending warning
                                          yes --> missing result = error
```
