import 'ipo_competition_batch.dart';

void check(bool condition, String message) {
  if (!condition) throw StateError(message);
}

IpoCompetitionSnapshot observation(
  String source,
  String capturedAt,
  List<Map<String, Object?>> brokers,
) {
  return IpoCompetitionSnapshot(
    source: source,
    capturedAt: capturedAt,
    sourceUrl: 'https://example.com/public-observation',
    aggregateCompetitionRate: 400,
    brokers: brokers.map(IpoBrokerCompetition.fromJson).toList(),
  );
}

Map<String, Object?> broker(String name, int? accounts, [int pool = 100]) => {
  'name': name,
  'offeredShares': pool * 2,
  'equalAllocationShares': pool,
  'applicationCount': accounts,
  'competitionRate': 400,
};

IpoCompetitionStock subject(List<IpoCompetitionSnapshot> snapshots) =>
    IpoCompetitionStock.fromJson({
      'id': 'freshness_2026-10-07',
      'company': 'freshness',
      'snapshots': snapshots.map((s) => s.toJson()).toList(),
    });

void main() {
  final early = observation(
    'naver_calculator_live',
    '2026-10-07T09:00:00+09:00',
    [broker('A', 2), broker('B', 4)],
  );
  final latest = observation(
    'naver_calculator_live',
    '2026-10-08T16:00:00+09:00',
    [broker('A', 200), broker('B', 125)],
  );
  for (final rows in [
    [early, latest],
    [latest, early],
  ]) {
    final stock = subject(rows);
    check(
      bestEqualExpectedSharesPerAccount(stock) == 0.8,
      'Use the best current broker average, not historical50.',
    );
    final allocation = expectedAllocatedSharesFor(
      stock: stock,
      offerPrice: 10000,
      competitionRate: 400,
    );
    check(
      (allocation['minimumSubscription']! - 0.825).abs() < 1e-9,
      'Expected allocation must use current equal inputs.',
    );
    check(stock.snapshots.length == 2, 'Preserve history.');
  }
  final official = observation(
    'dart_final_allotment_report',
    '2026-10-08T16:10:00+09:00',
    [broker('A', 250), broker('B', 200)],
  );
  final laterPortal = observation(
    'naver_calculator_live',
    '2026-10-08T17:00:00+09:00',
    [broker('A', 10), broker('B', 10)],
  );
  check(
    bestEqualExpectedSharesPerAccount(
          subject([early, laterPortal, official]),
        ) ==
        0.5,
    'Official per-broker final data outranks a later portal fetch.',
  );
  final incomplete = observation(
    'naver_calculator_live',
    '2026-10-08T18:00:00+09:00',
    [broker('A', null), broker('B', null)],
  );
  check(
    bestEqualExpectedSharesPerAccount(subject([early, incomplete])) == null,
    'Do not resurrect stale counts when current rows lack counts.',
  );
  final aggregate = observation(
    'dart_subscription_close_report',
    '2026-10-08T19:00:00+09:00',
    [broker('통합', 1)],
  );
  check(
    bestEqualExpectedSharesPerAccount(subject([latest, aggregate])) == 0.8,
    'An aggregate row must not masquerade as an available broker.',
  );
  check(
    bestEqualExpectedSharesPerAccount(subject([])) == null,
    'Missing observations remain unknown.',
  );
  // Separate official broker notices can disclose valid account counts without
  // an aggregate or a numeric proportional ratio. Never resurrect portal values.
  final partialMirae = IpoCompetitionSnapshot.fromJson({
    'source': 'miraeasset_final_allotment_notice',
    'capturedAt': '2026-10-13T13:14:53+09:00',
    'brokers': [
      {
        'name': 'A',
        'offeredShares': 444400,
        'equalAllocationShares': 222200,
        'proportionalAllocationShares': 222200,
        'applicationCount': 229707,
      },
    ],
  });
  final partialSamsung = IpoCompetitionSnapshot.fromJson({
    'source': 'samsung_final_allotment_notice',
    'capturedAt': '2026-10-13T13:14:53+09:00',
    'brokers': [
      {
        'name': 'B',
        'offeredShares': 111100,
        'offerPrice': 90500,
        'depositRate': 0.5,
        'equalAllocationShares': 55550,
        'proportionalAllocationShares': 55550,
        'applicationCount': 59347,
        'proportionalCompetitionRate': 811.27,
      },
    ],
  });
  for (final rows in [
    [laterPortal, partialMirae, partialSamsung],
    [partialSamsung, partialMirae, laterPortal],
  ]) {
    final reviewed = subject(rows);
    check(
      reviewed.latestSnapshot?.aggregate.competitionRate == null,
      'A partial final notice must not claim an old provisional aggregate.',
    );
    final byBroker = {
      for (final score in brokerScoresFor(reviewed)) score.broker: score,
    };
    check(
      (byBroker['A']!.expectedEqualShares! - 222200 / 229707).abs() < 1e-9 &&
          (byBroker['B']!.expectedEqualShares! - 55550 / 59347).abs() < 1e-9,
      'Both independent official notices must replace provisional counts.',
    );
    check(
      byBroker['A']!.dataQuality == 'partial' &&
          byBroker['A']!.estimatedDepositForOneProportionalShare == null,
      'An undisclosed final proportional ratio remains unknown.',
    );
    check(
      byBroker['B']!.dataQuality == 'broker_verified' &&
          byBroker['B']!.estimatedDepositForOneProportionalShare != null,
      'The other broker can retain its directly disclosed final ratio.',
    );
    check(reviewed.snapshots.length == 3, 'Keep provisional history.');
  }
  print(
    'PASS: current equal estimates, order independence, source priority, nulls and aggregate exclusion.',
  );
}
