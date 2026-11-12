# Expired IPO feed membership reconciliation — 2026-11-12

As of 2026-11-12 (Asia/Seoul), four feed rows have official issuance-results reports confirming their exact subscription periods have ended: NexEye (Nov 3–4) and Youngkwang YKMC (Nov 4–5) from `active.json`; Optonics (Nov 5–6) and AL.Robot (Nov 6–9) from `upcoming.json`. Each removal is bound to a same-issuer DART search, its exact newest issuance-results filing, and that filing’s schedule body. Raw HTML hashes and actual request/response timestamps are retained in the app run.

Hana Aerodynamic is deliberately retained. The first candidate incorrectly attached an MBD prospectus to Hana; no Hana removal is included until issuer-specific current filing and schedule evidence is captured.

Only the four declared rows are removed. Canonical events, stock detail, confirmed values, histories, all other feed rows, and existing snapshots remain unchanged. No fundamentals, listing dates, retail counts, final broker allocation values, or institutional/retail ratios are inferred.

The fail-closed verifier binds each removal ID and issuer to its canonical event/detail, verifies the DART search query and newest filing row, confirms the schedule-report receipt matches that newest filing and issuer, parses start/end dates from the official issuance-results body, and compares those dates against canonical event, detail, and feed rows.

Validation and independent review are pending at candidate creation. Full audit residuals remain visible; no new audit errors, hard integrity errors, feed mismatch, or candidate changes may pass.
