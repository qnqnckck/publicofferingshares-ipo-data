import 'ipo_competition_batch.dart';

void check(bool condition, String message) {
  if (!condition) throw StateError(message);
}

IpoCompetitionSnapshot snapshot(String source, String capturedAt, double rate) {
  return IpoCompetitionSnapshot(
    capturedAt: capturedAt,
    source: source,
    sourceUrl: 'https://example.com/public-result',
    aggregateCompetitionRate: rate,
    brokers: const [],
  );
}

IpoCompetitionStock stock(List<IpoCompetitionSnapshot> snapshots) {
  return IpoCompetitionStock.fromJson({
    'id': 'test_2026-09-15',
    'company': 'test',
    'snapshots': snapshots.map((value) => value.toJson()).toList(),
  });
}

void main() {
  final live = snapshot(
    'naver_calculator_post_close_observation',
    '2026-09-30T16:00:00+09:00',
    1375.34,
  );
  final finuts = snapshot('finuts', '2026-09-30T16:01:00+09:00', 1375.34);
  for (final source in [
    'dart_final_allotment_report',
    'dart_final_allotment_report_derived_ratios',
    'kbsec_final_allotment_notice',
  ]) {
    final official = snapshot(source, '2026-09-18T16:00:00+09:00', 1307.78);
    for (final observations in [
      [official, live, finuts],
      [finuts, live, official],
    ]) {
      final subject = stock(observations);
      check(subject.latestSnapshot?.source == source, '$source must win.');
      check(
        subject.latestSnapshot?.aggregate.competitionRate == 1307.78,
        'Use the official rate, not a later refetch of provisional data.',
      );
      check(subject.snapshots.length == 3, 'Keep all historical observations.');
    }
  }
  final original = snapshot(
    'dart_final_allotment_report',
    '2026-09-18T16:00:00+09:00',
    1307.78,
  );
  final correction = snapshot(
    'dart_final_allotment_report',
    '2026-09-30T14:17:31+09:00',
    1308.0,
  );
  check(
    stock([correction, original]).latestSnapshot?.aggregate.competitionRate ==
        1308.0,
    'Choose the latest reviewed correction among equal-priority filings.',
  );
  check(
    stock([live, finuts]).latestSnapshot?.source == 'finuts',
    'Preserve existing priority when no final disclosure is available.',
  );
  check(
    snapshotSourcePriority('article_final') <
        snapshotSourcePriority(live.source),
    'A secondary article containing final is not an official allotment.',
  );
  check(stock([]).latestSnapshot == null, 'Empty histories remain pending.');
  print(
    'PASS: official final priority, corrected filings, preserved history and provisional fallback.',
  );
}
