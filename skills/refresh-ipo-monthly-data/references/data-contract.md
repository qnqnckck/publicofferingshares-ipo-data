# Data Contract

## Canonical Inputs

- `data/discovered/ipo_events.json`: identity, schedule, market, underwriters, canonical event baseline.
- `data/manual_fundamentals.json`: reviewed factual overrides.
- `data/broker_snapshots/*.json`: confirmed or explicitly estimated retail broker metrics.
- `data/outcomes/*.json`: listing outcomes.
- `data/identifiers/ipo_identifiers.json`: durable external identifiers.

Never hand-edit `ipo_competition_data` as the source of truth. Regenerate it after canonical input changes.

## Baseline Fields

Require when announced: `company`, `market`, `industry`, demand forecast range, subscription range, refund/listing dates, underwriters, price band, and public retail allocation.

## Demand Result Fields

Require after the result: `offerPrice`, `topBandConfirmation`, `institutionCompetitionRate`, `institutionParticipants`, quantity-basis `lockupCommitmentRate`, `floatRate`, `marketCapKrw`, `hasPutbackRight`, and `putbackSummary`.

## Retail Result Fields

Require after retail results are final: aggregate competition, broker offered shares and competition, application count when published, equal allocation or estimate, proportional allocation or confirmed estimate inputs, capture timestamp, and source label.

Use null for pending facts. Never encode pending as `0`, an empty string, or a fabricated estimate. Mark generated estimates by their existing source/method fields.

## Unit Rules

- Percentages: decimal fractions from 0 through 1.
- Competition rates: positive ratio values, not percentages.
- Share counts, participants, applications, market cap, and KRW: non-negative integers.
- Dates: ISO `YYYY-MM-DD`; timestamps include an offset.
- IDs: canonical company plus subscription start date according to repository conventions.
