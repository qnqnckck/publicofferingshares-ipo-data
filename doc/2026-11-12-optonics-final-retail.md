# Optonics final retail observation — 2026-11-12

## Source observations

- DART securities issuance results report `20261110000258`, Korean / Asia-Seoul. Request started `2026-11-12T14:13:14.650+09:00`, response completed `2026-11-12T14:13:15.032+09:00`; raw HTML SHA-256 `953af1138ad5e506a5195575f653b2ea7e76d14e14c08a5eed8909f05c01a84f`. Source: https://dart.fss.or.kr/report/viewer.do?rcpNo=20261110000258&dcmNo=11647869&eleId=1&offset=0&length=0&dtd=dart4.xsd
- Naver Npay IPO page for code `493260`, Korean / Asia-Seoul. Request started `2026-11-12T16:13:16.759+09:00`, response completed `2026-11-12T16:13:16.837+09:00`; raw HTML SHA-256 `234d71fa7e6766a1c74d0efc3114185333a26db2738b14c0253e8ef04562d74c`. The page labels its broker competition figures `26.11.12 16:13 기준` (KST, minute precision); embedded `fetchedAt` is `2026-11-12T16:13:16.820+09:00`. Source: https://ustock.naver.com/service/ipo/493260

## Values and treatment

DART reports 244,228 applications, 738,828,300 subscribed shares, 550,000 general-offering shares, and equal/proportional pools of 275,000 each. The overall retail rate is calculated from those published totals: `738,828,300 / 550,000 = 1,343.32418`, displayed as `1,343.32`.

Naver directly displays a proportional competition rate of `2,685.7:1` and indicative equal shares of `1.13` for Korea Investment & Securities. These are recorded in a separate Naver snapshot, not derived by doubling the aggregate rate. The DART proportional pool is preserved as a distinct source field; Naver does not expose application count, subscribed-share total, or proportional-pool size in this capture. The final actual allocation per applicant is not present in either captured source and remains `null`.

## Generator and audit corrections

The previous generator path substituted aggregate retail competition when proportional competition was missing, and calculated a proportional deposit/score from that fallback. It now keeps the proportional rate and dependent proportional score/deposit `null` unless a direct broker-level proportional value exists. Aggregate feed rate is now selected from snapshots that actually report an aggregate rate, so a later broker-only Naver observation cannot erase or masquerade as the DART aggregate. The completeness auditor no longer derives proportional rate from total subscribed shares divided by proportional pool, and combines reviewed fields from separate source snapshots by broker. The monthly phase audit and verified-delta gate now also select the latest snapshot that actually contains an aggregate; list-indexed verified-field paths are supported, and feed parity reads that same aggregate. On the unchanged monthly ledger, phase errors decreased 8→7 (Optonics retail-comp error removed; no new errors), completeness errors 144→142 (the two target retail errors removed; no new errors). The scoped gate passes while unrelated errors remain explicitly open.

This is a candidate report. The exact source-time conflict found by Ops #226 was corrected: the captured Naver HTML says `26.11.12 16:13 기준`, and its embedded fetchedAt is `2026-11-12T16:13:16.820+09:00`. The former candidate fingerprint is invalid for approval; a new gate and fresh same-candidate Ops/QA reviews are required before publication.
