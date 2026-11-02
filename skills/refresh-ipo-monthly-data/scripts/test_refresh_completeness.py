import copy
import json
from pathlib import Path
import tempfile
import unittest
from check_refresh_completeness import audit, compare_publication, digest, present, in_review_window


class CompletenessTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.key = '테스트_2026-10-20'
        self.snapshot = dict(capturedAt='2026-10-23T17:00:00+09:00', sourceUrl='https://example.org/result',
            source='official', aggregate=dict(competitionRate=12), brokers=[dict(name='A증권',
            offeredShares=100, subscribedShares=1200, competitionRate=12, applicationCount=20,
            equalAllocationShares=50, proportionalAllocationShares=50)])
        self.stock = dict(id=self.key, company='테스트', market='KOSDAQ', industry='software',
            subscriptionStart='2026-10-20', subscriptionEnd='2026-10-21',
            demandForecastStart='2026-10-01', demandForecastEnd='2026-10-05',
            refundDate='2026-10-23', listingDate='2026-11-05', leadManagers=['A증권'],
            fundamentals=dict(priceBandMin=1000, priceBandMax=2000, offerPrice=2000,
            institutionCompetitionRate=100, institutionParticipants=200, lockupCommitmentRate=.1,
            floatRate=.3, marketCapKrw=10000000, publicAllocationShares=100,
            topBandConfirmation=True, hasPutbackRight=False, putbackSummary='없음'),
            snapshots=[self.snapshot], analysis=dict(brokerScores=[dict(broker='A증권',expectedEqualShares=2.5)]))
        self.candidate = dict(eventId=self.key, company='테스트', subscriptionStart='2026-10-20',
            subscriptionEnd='2026-10-21', status='matched', finalSnapshotReviews=[])
        self.bind()
        self.ledger = dict(rangeStart='2026-10-01', rangeEndExclusive='2027-01-01', candidates=[self.candidate])

    def bind(self):
        self.candidate['finalSnapshotReviews'] = [dict(snapshotSha256=digest(self.snapshot), status='final',
            aggregateFinal=True, sourceDate='2026-10-23', evidenceUrl='https://example.org/result')]

    def write(self, path, value):
        path = self.root/path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, ensure_ascii=False), encoding='utf-8')

    def fixtures(self):
        self.write('ipo_competition_data/stocks/'+self.key+'.json', self.stock)
        row = {k:self.stock[k] for k in ('id', 'company', 'subscriptionStart', 'subscriptionEnd', 'refundDate', 'listingDate')}
        row['path'] = 'stocks/'+self.key+'.json'
        for name in ('index.json','yearly/2026.json'):
            self.write('ipo_competition_data/'+name, dict(stocks=[row]))
        return row

    def run_audit(self):
        return audit(self.root,self.ledger,'2026-10-01','2027-01-01','2026-10-31')

    def codes(self): return {i['code'] for i in self.run_audit()['issues']}

    def test_valid_three_months_and_false(self):
        self.fixtures()
        self.assertEqual(self.codes(), set())
        self.assertTrue(present(False))
        self.assertTrue(present(0))  # present is not a permission to invent zero.

    def test_allocation_pool_alone_not_equal_per_person(self):
        self.snapshot['brokers'][0].pop('applicationCount'); self.bind(); self.fixtures()
        fields = {i['field'] for i in self.run_audit()['issues']}
        self.assertIn('brokers.A증권.expectedEqualShares', fields)
        self.assertIn('brokers.A증권.applicationCount', fields)

    def test_missing_manager(self):
        self.stock['leadManagers'].append('B증권'); self.fixtures()
        self.assertIn('missing_final_broker', self.codes())

    def test_equal_input_present_but_app_display_missing(self):
        self.stock['analysis']['brokerScores']=[];self.fixtures()
        self.assertIn('equal_display_missing',self.codes())

    def test_provisional_not_final(self):
        self.candidate['finalSnapshotReviews']=[]; self.fixtures()
        self.assertIn('aggregate_final_unverified',self.codes())

    def test_review_digest_does_not_survive_mutation(self):
        self.snapshot['aggregate']['competitionRate']=99; self.fixtures()
        self.assertIn('aggregate_final_unverified',self.codes())

    def test_partial_broker_notice_not_aggregate(self):
        self.candidate['finalSnapshotReviews'][0].update(status='partial_final',aggregateFinal=False)
        self.fixtures(); self.assertIn('aggregate_final_unverified',self.codes())

    def test_duplicate_snapshot(self):
        self.stock['snapshots'].append(copy.deepcopy(self.snapshot)); self.fixtures()
        self.assertIn('duplicate_snapshot',self.codes())

    def test_stale_feed_date(self):
        row=self.fixtures();row['subscriptionStart']='2026-11-20'
        self.write('ipo_competition_data/index.json',dict(stocks=[row]))
        self.assertIn('feed_detail_mismatch',self.codes())

    def test_duplicate_membership(self):
        row=self.fixtures();self.write('ipo_competition_data/index.json',dict(stocks=[row,row]))
        self.assertIn('duplicate_feed_row',self.codes())

    def test_wrong_case_feed_path(self):
        row=self.fixtures();row['path']='stocks/WRONG.json'
        self.write('ipo_competition_data/index.json',dict(stocks=[row]))
        self.assertIn('feed_path',self.codes())

    def test_invalid_date_and_range(self):
        self.stock['refundDate']='2026-02-30';self.ledger['rangeEndExclusive']='2026-12-01';self.fixtures()
        self.assertTrue({'invalid_date','range_mismatch'} <= self.codes())

    def test_omitted_local_issuer(self):
        self.fixtures();self.ledger['candidates']=[]
        self.assertIn('unreviewed_stock',self.codes())

    def test_pending_gap_requires_evidence_and_future_recheck(self):
        self.stock['listingDate']=None
        self.candidate['fieldReviews']={'listingDate':dict(state='unpublished',reason='not announced',
            evidenceUrls=['https://example.org/filing'],checkedAt='2026-10-31',nextCheckOn='2026-11-02')}
        self.fixtures()
        self.assertEqual(self.codes(),{'documented_gap'})
        self.assertFalse(any(i['severity']=='ERROR' for i in self.run_audit()['issues']))
        self.candidate['fieldReviews']['listingDate']['nextCheckOn']='2026-10-30'
        self.assertIn('missing_review',self.codes())

    def test_closed_active(self):
        row=self.fixtures();self.write('ipo_competition_data/active.json',dict(stocks=[row]))
        self.assertIn('closed_in_active',self.codes())

    def test_publication_different_and_unreachable(self):
        self.fixtures();path='ipo_competition_data/stocks/'+self.key+'.json'
        self.assertEqual(compare_publication(self.root,[path],lambda p:self.stock)[0]['status'],'MATCH')
        self.assertEqual(compare_publication(self.root,[path],lambda p:{})[0]['status'],'DIFF')
        def failed(p): raise OSError('offline')
        self.assertEqual(compare_publication(self.root,[path],failed)[0]['status'],'UNVERIFIED')

    def test_prior_subscription_current_listing_not_silently_omitted(self):
        self.fixtures()
        self.ledger.update(rangeStart='2026-11-01', candidates=[])
        report = audit(self.root,self.ledger,'2026-11-01','2027-01-01','2026-11-02')
        self.assertIn('unreviewed_stock', {i['code'] for i in report['issues']})
        self.assertIn('ipo_competition_data/stocks/'+self.key+'.json', report['publicationPaths'])
        remote = copy.deepcopy(self.stock); remote['listingDate'] = None
        path = 'ipo_competition_data/stocks/'+self.key+'.json'
        self.assertEqual(compare_publication(self.root,[path],lambda p:remote)[0]['status'],'DIFF')

    def test_recent_closed_unknown_listing_is_reviewed(self):
        stock = dict(subscriptionStart='2026-10-22',subscriptionEnd='2026-10-23',listingDate=None)
        self.assertTrue(in_review_window(stock,'2026-11-01','2027-01-01'))
        stock['listingDate']='2026-10-29'
        self.assertFalse(in_review_window(stock,'2026-11-01','2027-01-01'))

    def test_refund_and_subscription_overlap(self):
        self.assertTrue(in_review_window(dict(refundDate='2026-11-02'),'2026-11-01','2027-01-01'))
        self.assertTrue(in_review_window(dict(subscriptionStart='2026-10-30',subscriptionEnd='2027-01-02'),'2026-11-01','2027-01-01'))
        self.assertFalse(in_review_window(dict(listingDate='2027-01-01'),'2026-11-01','2027-01-01'))

    def test_year_crossing_includes_prior_year_feed(self):
        self.stock.update(subscriptionStart='2026-12-28',subscriptionEnd='2026-12-29',refundDate='2026-12-31',listingDate='2027-01-05')
        self.fixtures()
        self.ledger.update(rangeStart='2027-01-01',rangeEndExclusive='2027-03-01',candidates=[])
        report=audit(self.root,self.ledger,'2027-01-01','2027-03-01','2027-01-02')
        self.assertIn('ipo_competition_data/yearly/2026.json',report['publicationPaths'])
        self.assertTrue(in_review_window(dict(subscriptionEnd='2026-11-30'),'2027-01-01','2027-03-01'))

if __name__=='__main__':unittest.main()
