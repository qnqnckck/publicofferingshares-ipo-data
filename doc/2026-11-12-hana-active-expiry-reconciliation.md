# Hana Aerodynamic expired active-feed row — 2026-11-12

At the actual KST as-of date 2026-11-12, the user's 2026-11-10 Hana Aerodynamic subscription (stock code 271160, IBK Securities) ended on 2026-11-11. The generated `active.json` still listed it. The canonical schedule was previously confirmed from official DART/IBK source material; this correction changes only current active membership and leaves the event, detail, history, and all other feed rows intact.

A fresh DART issuer search was captured on 2026-11-12 12:02 KST. It binds the exact issuer query to newest receipt 20261109000166, filed 2026-11-09 as `[기재정정] 투자설명서`. The captured report body was received at 12:03 KST and explicitly states general-investor subscription on 2026-11-10 through 2026-11-11. Its raw source, URL, response times and SHA-256 are preserved in the app run. This report is the newest issuer filing in the captured search; no later schedule correction appears in that query.

The fail-closed membership validator now accepts an exact newest issuer filing when its title is a prospectus/registration statement or issuance-results report, but requires the report title and receipt to match the newest DART search row and requires one unique explicit general-investor date pair in the source body. A later issuer filing without an unambiguous matching schedule still fails closed. This corrects the previous assumption that only an issuance-results report can confirm an expired subscription window.

The official local batch generator was run with public discovery and live collectors disabled, writing to an isolated output directory. Only its newly generated `active.json` was selected: the expired Hana row is absent, the still-open MBD row remains, and all other public files are unchanged. Baseline full-audit errors were 131; candidate errors are 130, with the expired-membership error removed. Remaining missing/null and source-review gaps are still open.

Publication remains pending same-candidate independent operations and QA approval, the fail-closed scoped delta gate, normal commit/push, and raw-main parity.
