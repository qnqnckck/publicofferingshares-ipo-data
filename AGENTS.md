# Public IPO data repository rules

- Keep this repository limited to public IPO data generation and publication.
- Preserve deterministic rebuild and targeted manual backfill paths.
- Do not add secrets or private source data.
- Store project-specific plans under `doc/`.
- Before every data refresh, read `doc/refresh-context.md` and the latest execution record. This context includes lifecycle coverage, held corrections and the user's standing publication instruction; it does not waive validation gates.
- Validate generated JSON and workflow configuration before publishing.
