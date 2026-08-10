# Remove scheduled workflows

## Idea summary

Remove the three scheduled public IPO synchronization workflows so data refreshes cannot run automatically from GitHub Actions.

## MVP scope

- Core user problem: scheduled workflow definitions remain in the repository even though releases and updates should be operator initiated.
- Must-have flows: delete the baseline discovery, demand/fundamentals refresh, and live competition sync workflow files.
- Out of scope: changing data generators, generated feeds, or the two manual maintenance workflows.
- Success criteria: no workflow in the repository contains a `schedule` trigger and the manual targeted backfill and rebuild workflows remain available.

## Feature specification

- Purpose: ensure public IPO data operations only start through an explicit manual command.
- User interaction flow: an operator selects one of the retained manual workflows and runs it from GitHub Actions when needed.
- Data/state changes: remove three workflow definitions; public data files remain unchanged.
- Error states: deletion must not remove the manual recovery paths or alter published data.
- Acceptance criteria: GitHub lists only the two retained manual workflows and no scheduled run can be created from repository configuration.

## Wireframe

```text
Before: schedule -> baseline / fundamentals / live competition
After:  operator -> manual targeted backfill OR rebuild from repo data
```
