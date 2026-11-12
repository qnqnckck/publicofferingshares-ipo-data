"""Read-only gap, feed and publication checks. Evidence is reviewed by a human/agent.

Does not edit IPO inputs, infer finality, authorize publication or waive base audits.
"""
import argparse
from collections import Counter
from datetime import date, datetime
import json
from pathlib import Path
import subprocess
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

BASE = ('company', 'market', 'industry', 'subscriptionStart', 'subscriptionEnd',
        'demandForecastStart', 'demandForecastEnd', 'refundDate', 'listingDate')
FUND = ('priceBandMin', 'priceBandMax', 'offerPrice', 'institutionCompetitionRate',
        'institutionParticipants', 'lockupCommitmentRate', 'floatRate', 'marketCapKrw',
        'publicAllocationShares', 'topBandConfirmation', 'hasPutbackRight', 'putbackSummary')
STATES = {'unpublished', 'official_none', 'not_applicable', 'conflict'}

def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))

def present(value):
    return value is not None and value != '' and value != []

def iso(value):
    try:
        return date.fromisoformat(value) if isinstance(value, str) and len(value) == 10 else None
    except ValueError:
        return None

def digest(value):
    import hashlib
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode()).hexdigest()

def in_review_window(stock, start, end):
    """Lifecycle coverage, with a two-calendar-month unknown-listing safety net.

    Older held corrections must still be explicit ledger candidates.
    """
    for field in BASE:
        value = stock.get(field)
        if iso(value) and start <= value < end:
            return True
    sub_start, sub_end = stock.get('subscriptionStart'), stock.get('subscriptionEnd')
    if iso(sub_start) and iso(sub_end) and sub_start < end and sub_end >= start:
        return True
    first = iso(start)
    month_index = first.year * 12 + first.month - 1 - 2
    lookback = date(month_index // 12, month_index % 12 + 1, 1).isoformat()
    return not iso(stock.get('listingDate')) and bool(iso(sub_end)) and lookback <= sub_end < start

def audit(repo, ledger, start, end, as_of):
    repo = Path(repo)
    issues, matrix, paths = [], [], set()
    def add(code, event, field='', severity='ERROR', detail=''):
        issues.append(dict(code=code, eventId=event, field=field, severity=severity, detail=detail))
    if not iso(start) or not iso(end) or start >= end or start[-2:] != '01' or end[-2:] != '01':
        raise ValueError('Expected increasing ISO month boundaries')
    if not iso(as_of):
        raise ValueError('Invalid as-of date')
    if ledger.get('rangeStart') != start or ledger.get('rangeEndExclusive') != end:
        add('range_mismatch', '')
    candidates = [x for x in ledger['candidates'] if x['status'] in ('matched', 'added')]
    ids = {x['eventId'] for x in candidates}
    for key, count in Counter(x['eventId'] for x in candidates).items():
        if count != 1: add('duplicate_candidate', key)
    details = {}
    folder = repo/'ipo_competition_data'
    for path in (folder/'stocks').glob('*.json'):
        stock = read(path)
        if stock.get('id') in ids or in_review_window(stock, start, end):
            key = stock.get('id', '')
            if key in details: add('duplicate_stock_id', key)
            details[key] = stock
            paths.add('ipo_competition_data/stocks/'+path.name)
            if path.name != key+'.json': add('filename_case_or_id', key)
            if key not in ids: add('unreviewed_stock', key)
    windows = Counter((x.get('company'), x.get('subscriptionStart'), x.get('subscriptionEnd')) for x in details.values())
    for window, count in windows.items():
        if count > 1: add('duplicate_window', str(window))
    for candidate in candidates:
        key = candidate['eventId']
        stock = details.get(key)
        if not stock:
            add('missing_stock', key)
            continue
        paths.add('ipo_competition_data/stocks/'+key+'.json')
        for field in ('company', 'subscriptionStart', 'subscriptionEnd'):
            if stock.get(field) != candidate.get(field): add('candidate_identity', key, field)
        reviews = candidate.get('fieldReviews', {})
        def inspect(field, value, due=False):
            state = 'present' if present(value) else 'unreviewed'
            if not present(value):
                review = reviews.get(field, {})
                urls = review.get('evidenceUrls', [])
                valid = (review.get('state') in STATES and bool(review.get('reason'))
                         and isinstance(urls, list) and bool(urls)
                         and all(urlparse(u).scheme in ('https', 'http') and urlparse(u).netloc for u in urls)
                         and iso(review.get('checkedAt')) is not None
                         and review['checkedAt'] <= as_of)
                if valid and review['state'] == 'unpublished':
                    valid = iso(review.get('nextCheckOn')) is not None and review['nextCheckOn'] > as_of
                if valid: state = review['state']
                if state in ('unreviewed', 'conflict'): add('missing_review' if state == 'unreviewed' else 'source_conflict', key, field)
                else: add('documented_gap', key, field, 'ERROR' if due else 'WARNING', state)
            matrix.append(dict(eventId=key, field=field, value=value, status=state))
        for field in BASE:
            value = stock.get(field)
            inspect(field, value, field in ('company', 'market', 'subscriptionStart', 'subscriptionEnd'))
            if field.endswith(('Start', 'End', 'Date')) and present(value) and not iso(value):
                add('invalid_date', key, field)
        fund = stock.get('fundamentals') or {}
        for field in FUND: inspect('fundamentals.'+field, fund.get(field))
        managers = stock.get('leadManagers') or []
        if not managers: add('missing_lead_managers', key)
        closed = iso(stock.get('subscriptionEnd')) and stock['subscriptionEnd'] < as_of
        snapshots = stock.get('snapshots') or []
        seen = set()
        for snap in snapshots:
            identity = digest({k: snap.get(k) for k in ('source', 'sourceUrl', 'capturedAt', 'brokers', 'aggregate')})
            if identity in seen: add('duplicate_snapshot', key)
            seen.add(identity)
        if closed:
            finals = candidate.get('finalSnapshotReviews', [])
            covered = set()
            aggregate_final = False
            reviewed_broker_values = {}
            for snap in snapshots:
                # Explicit reviewed digest binds finality to exact content, not its source label.
                review = next((r for r in finals if r.get('snapshotSha256') == digest(snap)), None)
                if not review: continue
                evidence = urlparse(review.get('evidenceUrl', ''))
                if (review.get('status') not in ('final', 'partial_final') or evidence.scheme not in ('https', 'http') or not evidence.netloc
                        or not review.get('sourceDate') or not iso(review['sourceDate'])
                        or review['sourceDate'] > as_of):
                    add('invalid_final_review', key)
                    continue
                if not snap.get('sourceUrl'): add('snapshot_source_missing', key)
                try:
                    if datetime.fromisoformat(snap['capturedAt']).utcoffset() is None: raise ValueError()
                except (ValueError, KeyError): add('snapshot_timezone_missing', key)
                aggregate_final |= bool(review.get('aggregateFinal') is True and present((snap.get('aggregate') or {}).get('competitionRate')))
                for broker in snap.get('brokers') or []:
                    name = broker.get('name')
                    covered.add(name)
                    values = reviewed_broker_values.setdefault(name, {})
                    # Combine only values explicitly present in independently
                    # reviewed source snapshots. Per-broker sources often carry
                    # only allocation metrics, while the issuer filing carries
                    # application and aggregate totals.
                    for field in ('offeredShares', 'applicationCount', 'competitionRate',
                                  'expectedEqualShares', 'proportionalCompetitionRate',
                                  'equalAllocationShares', 'proportionalAllocationShares'):
                        if field not in values and present(broker.get(field)):
                            values[field] = broker.get(field)
            for name, values in reviewed_broker_values.items():
                for field in ('offeredShares', 'applicationCount', 'competitionRate'):
                    inspect('brokers.'+str(name)+'.'+field, values.get(field), True)
                # Pool size alone is not an equal-per-account estimate.
                equal = values.get('expectedEqualShares')
                if not present(equal) and (values.get('applicationCount') or 0) > 0 and present(values.get('equalAllocationShares')):
                    equal = values['equalAllocationShares']/values['applicationCount']
                inspect('brokers.'+str(name)+'.expectedEqualShares', equal, True)
                display = next((r for r in (stock.get('analysis') or {}).get('brokerScores', []) if r.get('broker') == name), {})
                if present(equal) and not present(display.get('expectedEqualShares')):
                    add('equal_display_missing', key, str(name))
                # Do not infer this ratio from total subscribed shares and the
                # proportional pool. Only a directly reported rate counts.
                inspect('brokers.'+str(name)+'.proportionalCompetitionRate', values.get('proportionalCompetitionRate'), True)
            for name in set(managers)-covered: add('missing_final_broker', key, name)
            if not aggregate_final: add('aggregate_final_unverified', key)
        if iso(stock.get('listingDate')) and stock['listingDate'] < as_of:
            inspect('outcome', stock.get('outcome'), True)
    feeds = ['index.json', 'active.json', 'upcoming.json', 'recent.json', 'dashboard.json']
    feeds += ['yearly/'+str(y)+'.json' for y in range(iso(start).year, iso(end).year+1)]
    feeds += ['yearly/'+s['subscriptionStart'][:4]+'.json' for s in details.values() if iso(s.get('subscriptionStart'))]
    feeds = list(dict.fromkeys(feeds))
    for name in feeds:
        path = folder/name
        if not path.exists():
            if name == 'index.json' or name.startswith('yearly/') and any(s.get('subscriptionStart', '')[:4] == Path(name).stem for s in details.values()):
                add('missing_feed', name)
            continue
        paths.add('ipo_competition_data/'+name)
        rows = read(path).get('stocks', [])
        counts = Counter(x.get('id') for x in rows)
        for key, count in counts.items():
            if key in ids and count != 1: add('duplicate_feed_row', key, name)
        for key in ids:
            if name == 'index.json' or name == 'yearly/'+details.get(key, {}).get('subscriptionStart', '')[:4]+'.json':
                if counts[key] != 1: add('missing_feed_membership', key, name)
        for row in rows:
            if row.get('id') not in ids:
                if row.get('id') in details or in_review_window(row, start, end): add('unreviewed_feed_row', row.get('id'), name)
                continue
            key = row['id']; stock = details.get(key, {})
            expected = 'stocks/'+key+'.json'
            if row.get('path') != expected or not (folder/expected).is_file(): add('feed_path', key, name)
            for field in ('company', 'subscriptionStart', 'subscriptionEnd', 'refundDate', 'listingDate'):
                if field in row and row[field] != stock.get(field): add('feed_detail_mismatch', key, name+':'+field)
            if name == 'active.json' and iso(stock.get('subscriptionEnd')) and stock['subscriptionEnd'] < as_of:
                add('closed_in_active', key)
    return dict(rangeStart=start, rangeEndExclusive=end, asOfDate=as_of,
                issues=issues, fields=matrix, publicationPaths=sorted(paths))

def compare_publication(repo, paths, loader):
    result = []
    for path in paths:
        try:
            actual = loader(path)
            result.append(dict(path=path, status='MATCH' if actual == read(Path(repo)/path) else 'DIFF'))
        except Exception as exc:
            result.append(dict(path=path, status='UNVERIFIED', detail=str(exc)[:250]))
    return result

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', required=True)
    parser.add_argument('--ledger', required=True)
    parser.add_argument('--start', required=True)
    parser.add_argument('--end', required=True, help='Exclusive month boundary')
    parser.add_argument('--as-of', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--remote-base', help='Verified public GitHub Raw base including branch; read-only')
    parser.add_argument('--publication-paths', help='JSON list of exact changed public paths for a scoped publication')
    args = parser.parse_args()
    report = audit(args.repo, read(args.ledger), args.start, args.end, args.as_of)
    if args.publication_paths:
        selected = read(args.publication_paths)
        if not isinstance(selected, list) or not selected or any(not isinstance(p, str) or not p.startswith('ipo_competition_data/') or not p.endswith('.json') or '..' in Path(p).parts or '\\' in p for p in selected):
            parser.error('publication-paths must contain safe relative public JSON paths')
        report['publicationPaths'] = sorted(set(selected))
    def head(path):
        return json.loads(subprocess.check_output(['git', '-c', 'safe.directory='+Path(args.repo).resolve().as_posix(),
                    '-C', args.repo, 'show', 'HEAD:'+path], stderr=subprocess.DEVNULL))
    report['localVersusHead'] = compare_publication(args.repo, report['publicationPaths'], head)
    if args.remote_base:
        parsed = urlparse(args.remote_base)
        if parsed.scheme != 'https' or parsed.hostname != 'raw.githubusercontent.com' or parsed.username or parsed.query:
            parser.error('remote-base must be a public HTTPS raw.githubusercontent.com repository/branch URL')
        def remote(path):
            req = Request(args.remote_base.rstrip('/')+'/'+quote(path), headers={'User-Agent':'IPO-completeness-check','Cache-Control':'no-cache'})
            with urlopen(req, timeout=30) as response: return json.load(response)
        report['remoteComparison'] = compare_publication(args.repo, report['publicationPaths'], remote)
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n', encoding='utf-8')
    errors = sum(i['severity'] == 'ERROR' for i in report['issues'])
    print(f"Fields: {len(report['fields'])}; errors: {errors}; report: {args.output}")
    print('Local versus HEAD:', dict(Counter(r['status'] for r in report['localVersusHead'])))
    remote_bad = any(r['status'] != 'MATCH' for r in report.get('remoteComparison', []))
    raise SystemExit(1 if errors or remote_bad else 0)

if __name__ == '__main__': main()
