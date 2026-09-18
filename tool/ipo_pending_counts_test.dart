import 'ipo_competition_batch.dart';

void check(bool condition, String message) {
  if (!condition) throw StateError(message);
}

IpoCompetitionSnapshot snapshot(
  List<IpoBrokerCompetition> brokers, {
  double? publishedRate,
}) {
  return IpoCompetitionSnapshot(
    capturedAt: '2026-09-18T11:29:15+09:00',
    source: 'test',
    sourceUrl: null,
    aggregateCompetitionRate: publishedRate,
    brokers: brokers,
  );
}

void main() {
  final pending = IpoBrokerCompetition.fromJson({
    'name': 'IBK투자증권',
    'offeredShares': 300000,
    'subscribedShares': null,
    'competitionRate': 649.63,
    'equalAllocationShares': 150000,
    'applicationCount': 63682,
  });
  check(
    pending.normalized().toJson()['subscribedShares'] == null,
    'A pending count must survive normalization and serialization as null.',
  );
  check(pending.competitionRate == 649.63, 'Keep published competition.');
  check(
    pending.equalExpectedSharesPerAccount == 150000 / 63682,
    'Equal expectation must still use the published pool and count.',
  );
  final published = snapshot([pending], publishedRate: 649.63);
  check(
    published.aggregate.subscribedShares == null,
    'Unknown broker count must not become a zero aggregate.',
  );
  check(
    published.aggregate.competitionRate == 649.63,
    'Keep published aggregate competition without a count.',
  );
  check(
    snapshot([pending]).aggregate.competitionRate == null,
    'Do not invent aggregate competition from an unknown count.',
  );

  final known = IpoBrokerCompetition.fromJson({
    'name': 'known',
    'offeredShares': 100,
    'subscribedShares': 1250,
  });
  check(known.competitionRate == 12.5, 'Known counts still derive a ratio.');
  check(
    snapshot([known]).aggregate.subscribedShares == 1250,
    'Known counts still aggregate.',
  );
  check(
    snapshot([known, pending]).aggregate.subscribedShares == null,
    'Partially known counts must not claim a complete total.',
  );
  final zero = IpoBrokerCompetition.fromJson({
    'name': 'zero',
    'offeredShares': 100,
    'subscribedShares': 0,
  });
  check(
    zero.subscribedShares == 0 && zero.competitionRate == 0,
    'An explicitly published zero is different from an unknown count.',
  );
  check(
    snapshot([]).aggregate.subscribedShares == null,
    'An empty broker list is not a known zero total.',
  );
  print(
    'PASS: pending, mixed, published-rate, known-count and explicit-zero cases.',
  );
}
