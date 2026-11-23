"""Synthetic regression of the actual membership-evidence block, no IPO data edits."""
import ast
import copy
import json
from pathlib import Path
import tempfile
import unittest
from urllib.parse import quote

import check_verified_delta as validator


class SpacMembershipTests(unittest.TestCase):
    canonical = '신영해피투모로우제12호스팩'
    official = '신영해피투모로우제12호기업인수목적'

    def test_terminal_alias_only(self):
        key = validator.normalize_membership_issuer
        self.assertEqual(key(self.canonical), key(self.official))
        self.assertEqual(key(self.canonical), key('신영 해피투모로우 제12호 기업인수목적'))
        for wrong in ('신영해피투모로우제13호기업인수목적',
                      '신한해피투모로우제12호기업인수목적',
                      '신영해피투모로제12호기업인수목적',
                      '신영해피투모로우제012호기업인수목적',
                      self.official + '유사', '신영해피투모로우제12호',
                      '신영스팩서비스', None, ''):
            with self.subTest(wrong=wrong):
                self.assertNotEqual(key(self.canonical), key(wrong))
        self.assertNotEqual(key('일반스팩'), key('일반기업인수목적'))
        self.assertEqual(key('일반회사'), key('일반회사'))

    def run_evidence(self, mutate=lambda event, evidence: None):
        # Compile the production block unchanged to isolate its dependencies.
        # This executes all issuer, URL, raw-hash, newest-filing and date checks;
        # it is not a substitute for a full publication gate run.
        tree = ast.parse(Path(validator.__file__).read_text())
        function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'check')
        start = next(i for i, n in enumerate(function.body)
                     if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'declared_removals' for t in n.targets))
        end = next(i for i, n in enumerate(function.body)
                   if i > start and isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'event_path' for t in n.targets))
        code = compile(ast.Module(body=function.body[start:end], type_ignores=[]), '<membership-evidence>', 'exec')
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            body = root / 'body.html'
            body.write_text('일반투자자 청약일: 2026.11.24 ~ 2026.11.25')
            search = root / 'search.html'
            search.write_text('''<tr><td>1</td><td>회사</td><td><a onclick="openReportViewer('20261123000123','')">투자설명서</a></td><td>회사</td><td>2026.11.23</td></tr>''')
            source = 'https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20261123000123'
            event = dict(id='fixture', company=self.canonical, subscriptionStart='2026-11-24', subscriptionEnd='2026-11-25', sourceUrl=source)
            row = {k: event[k] for k in ('id', 'subscriptionStart', 'subscriptionEnd')}
            stamps = dict(requestStartedAtKst='2026-11-24T00:00:00+09:00', responseCompletedAtKst='2026-11-24T00:00:01+09:00', httpStatus=200)
            latest = dict(stamps, issuer=self.official, url='https://dart.fss.or.kr/dsab001/search.ax?textCrpNm='+quote(self.official), rawFile='search.html', rawSha256=validator.sha256(search), latestRcpNo='20261123000123', latestFilingDate='2026.11.23', latestFilingTitle='투자설명서')
            report = dict(stamps, issuer=self.official, url='https://dart.fss.or.kr/report/viewer.do?rcpNo=20261123000123', rawFile='body.html', rawSha256=validator.sha256(body), reportTitle='투자설명서', rcpNo='20261123000123', subscriptionStart=event['subscriptionStart'], subscriptionEnd=event['subscriptionEnd'])
            evidence = dict(stamps, path='ipo_competition_data/upcoming.json', id='fixture', issuer=self.official, subscriptionStart=event['subscriptionStart'], subscriptionEnd=event['subscriptionEnd'], sourceUrl=source, observedUrl=source, rawFile='body.html', rawSha256=validator.sha256(body), bytes=body.stat().st_size, asOfDate='2026-11-24', latestFilingSearch=latest, latestScheduleReport=report)
            stock = copy.deepcopy(event)
            mutate(event, evidence)
            manifest = dict(feedPaths=[evidence['path']], feedMembership={evidence['path']:'none'}, canonicalEventPath='events.json', membershipAsOfDate='2026-11-24', membershipRemovalEvidence=[evidence])
            baseline = {evidence['path']:[row], 'events.json':[event], 'ipo_competition_data/stocks/fixture.json':stock}
            scope = dict(vars(validator), manifest=manifest, target='fixture', repo=root, base='fixture-base', source_root=root, failures=[], base_json=lambda repo, base, rel: baseline.get(rel))
            exec(code, scope)
            return scope['failures']

    def test_official_alias_passes_all_membership_evidence_checks(self):
        self.assertEqual(self.run_evidence(), [])

    def test_exact_names_still_pass(self):
        def mutate(event, evidence):
            evidence['issuer'] = self.canonical
            evidence['latestScheduleReport']['issuer'] = self.canonical
            evidence['latestFilingSearch']['issuer'] = self.canonical
            evidence['latestFilingSearch']['url'] = 'https://dart.fss.or.kr/dsab001/search.ax?textCrpNm='+quote(self.canonical)
        self.assertEqual(self.run_evidence(mutate), [])

    def test_identity_mismatches_fail_at_each_binding(self):
        for wrong in ('신영해피투모로우제13호기업인수목적', '신한해피투모로우제12호기업인수목적', '신영해피투모로제12호기업인수목적'):
            for binding in ('issuer', 'search', 'query', 'report'):
                def mutate(event, evidence):
                    if binding == 'issuer': evidence['issuer'] = wrong
                    if binding == 'search': evidence['latestFilingSearch']['issuer'] = wrong
                    if binding == 'query': evidence['latestFilingSearch']['url'] = 'https://dart.fss.or.kr/dsab001/search.ax?textCrpNm='+quote(wrong)
                    if binding == 'report': evidence['latestScheduleReport']['issuer'] = wrong
                with self.subTest(wrong=wrong, binding=binding):
                    self.assertTrue(any('issuer' in x for x in self.run_evidence(mutate)))

    def test_existing_checks_remain_fail_closed(self):
        cases = [
            ('none_source', lambda e,v: (e.update(sourceUrl=None), v.update(sourceUrl=None)), 'source URL mismatch'),
            ('different_source', lambda e,v: v.update(sourceUrl='https://other.example'), 'source URL mismatch'),
            ('raw_hash', lambda e,v: v.update(rawSha256='bad'), 'raw hash mismatch'),
            ('search_hash', lambda e,v: v['latestFilingSearch'].update(rawSha256='bad'), 'search raw hash mismatch'),
            ('report_hash', lambda e,v: v['latestScheduleReport'].update(rawSha256='bad'), 'report raw hash mismatch'),
            ('latest_rcp', lambda e,v: v['latestScheduleReport'].update(rcpNo='20261122000123'), 'exact newest issuer filing'),
            ('future_filing', lambda e,v: v['latestFilingSearch'].update(latestFilingDate='2026.11.25'), 'after membership as-of'),
            ('schedule', lambda e,v: v['latestScheduleReport'].update(subscriptionEnd='2026-11-26'), 'schedule does not match'),
            ('not_yet_open', lambda e,v: v.update(asOfDate='2026-11-23'), 'as-of date mismatch'),
            ('unofficial_search', lambda e,v: v['latestFilingSearch'].update(url='https://example.com/dsab001/search.ax?textCrpNm='+quote(self.official)), 'not successful official HTTPS'),
        ]
        for name, mutate, error in cases:
            with self.subTest(name=name):
                self.assertTrue(any(error in x for x in self.run_evidence(mutate)))


if __name__ == '__main__':
    unittest.main()
